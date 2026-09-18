//! Source reads for the cross-partition routing cache. Every query is rooted in a permission
//! path whose recipient differs from the row's owning partition. No whole-table copy is used.

use crate::client::Client;
use crate::error::VStreamError;
use crate::execute::{QueryResult, query_with_client};
use crate::fill::sql_literal;
use crate::normalize::{TableProjection, query_fields};
use crate::subscriber::{SubscriberConfig, quote_ident};
use futures::{StreamExt, TryStreamExt, stream};
use orbit_protocol::cdc::RowChange;
use orbit_protocol::schema::{SyncSchema, TableSchema};
use orbit_protocol::value::{CellValue, Row};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;

fn fail(message: impl Into<String>) -> VStreamError {
    VStreamError::Malformed(message.into())
}
fn col(alias: &str, column: &str) -> String {
    format!("{}.{}", quote_ident(alias), quote_ident(column))
}
fn selected(table: &TableSchema, alias: &str) -> String {
    let mut columns = BTreeSet::new();
    for c in &table.columns {
        columns.insert(c.derived.as_ref().map_or(c.name.as_str(), |d| d.from.as_str()));
    }
    columns
        .into_iter()
        .map(|c| col(alias, c))
        .collect::<Vec<_>>()
        .join(", ")
}
fn literal(table: &TableSchema, column: &str, row: &Row) -> Result<String, VStreamError> {
    let c = table
        .columns
        .iter()
        .find(|c| c.name == column)
        .ok_or_else(|| fail("missing key column"))?;
    let value = match row.get(column) {
        Some(CellValue::String(v)) => v.clone(),
        Some(CellValue::Number(v)) => v.to_string(),
        _ => return Err(fail("unsupported shared projection key")),
    };
    sql_literal(c.kind, &value)
}

#[derive(Debug, Clone)]
pub struct SharedQuery {
    pub table: String,
    pub alias: String,
    pub sql: String,
}

/// With `changes`, select only sharing paths touched by a new/changed relationship. Deletes
/// need no hydration: their before-images are already retained until the revoke is routed.
pub fn queries(schema: &SyncSchema, changes: Option<&[RowChange]>) -> Result<Vec<SharedQuery>, VStreamError> {
    let mut result = BTreeMap::new();
    for root in schema.tables.iter().filter(|t| !t.partition_routes.is_empty()) {
        for path in &root.partition_routes {
            if path.is_empty() || path.len() > 8 {
                return Err(fail("invalid sharing path"));
            }
            let mut nodes = vec![(root, "s0".to_string())];
            let mut from = format!(" FROM {} AS {}", quote_ident(&root.name), quote_ident("s0"));
            for (i, step) in path.iter().enumerate() {
                let (current, alias) = nodes.last().expect("root");
                let relation = current
                    .relations
                    .iter()
                    .find(|r| &r.name == step)
                    .ok_or_else(|| fail("unknown sharing relation"))?;
                let target = schema
                    .table(&relation.target_table)
                    .ok_or_else(|| fail("unknown sharing table"))?;
                let next = format!("s{}", i + 1);
                let join = relation
                    .from_columns
                    .iter()
                    .zip(&relation.to_columns)
                    .map(|(a, b)| format!("{} = {}", col(alias, a), col(&next, b)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                if join.is_empty() {
                    return Err(fail("empty sharing join"));
                }
                from.push_str(&format!(
                    " JOIN {} AS {} ON {join}",
                    quote_ident(&target.name),
                    quote_ident(&next)
                ));
                nodes.push((target, next));
            }
            let (endpoint, endpoint_alias) = nodes.last().expect("endpoint");
            if endpoint.partition_parent.is_some() {
                return Err(fail("derived sharing endpoint"));
            }
            let mut owner_node = None;
            let owner = if let Some(parent) = &root.partition_parent {
                let parent = schema.table(parent).ok_or_else(|| fail("unknown owner parent"))?;
                if parent.primary_key.len() != 1 {
                    return Err(fail("invalid owner key"));
                }
                from.push_str(&format!(
                    " JOIN {} AS {} ON {} = {}",
                    quote_ident(&parent.name),
                    quote_ident("owner"),
                    col("s0", &root.partition_column),
                    col("owner", &parent.primary_key[0])
                ));
                owner_node = Some((parent, "owner".to_string()));
                col("owner", &parent.partition_column)
            } else {
                col("s0", &root.partition_column)
            };
            let recipient = col(endpoint_alias, &endpoint.partition_column);
            if let Some(node) = owner_node {
                nodes.push(node);
            }
            let mut predicate = format!("{recipient} IS NOT NULL AND NOT ({recipient} <=> {owner})");
            if let Some(changes) = changes {
                let mut affected = BTreeSet::new();
                for (table, alias) in &nodes {
                    let mut routing_columns: BTreeSet<&str> = table.primary_key.iter().map(String::as_str).collect();
                    routing_columns.insert(&table.partition_column);
                    for relation in &table.relations {
                        routing_columns.extend(relation.from_columns.iter().map(String::as_str));
                    }
                    for source in &schema.tables {
                        for relation in &source.relations {
                            if relation.target_table == table.name {
                                routing_columns.extend(relation.to_columns.iter().map(String::as_str));
                            }
                        }
                    }
                    for change in changes.iter().filter(|c| c.table == table.name) {
                        let Some(after) = &change.after else {
                            continue;
                        };
                        if change
                            .before
                            .as_ref()
                            .is_some_and(|before| routing_columns.iter().all(|c| before.get(*c) == after.get(*c)))
                        {
                            continue;
                        }
                        let key = table
                            .primary_key
                            .iter()
                            .map(|c| Ok(format!("{} = {}", col(alias, c), literal(table, c, after)?)))
                            .collect::<Result<Vec<_>, VStreamError>>()?
                            .join(" AND ");
                        affected.insert(format!("({key})"));
                    }
                }
                if affected.is_empty() {
                    continue;
                }
                predicate.push_str(&format!(
                    " AND ({})",
                    affected.into_iter().collect::<Vec<_>>().join(" OR ")
                ));
            }
            for (table, alias) in nodes {
                let sql = format!("SELECT {}{from} WHERE {predicate}", selected(table, &alias));
                result.insert(
                    sql.clone(),
                    SharedQuery {
                        table: table.name.clone(),
                        alias,
                        sql,
                    },
                );
            }
        }
    }
    Ok(result.into_values().collect())
}

pub async fn load(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    changes: Option<&[RowChange]>,
) -> Result<Vec<(String, Row)>, VStreamError> {
    let requests = queries(schema, changes)?;
    if requests.is_empty() {
        return Ok(vec![]);
    }
    let client = config.endpoint.connect().await?;
    load_requests(schema, requests, |sql| {
        let mut client = client.clone();
        async move { query_with_client(&mut client, &config.keyspace, &sql).await }
    })
    .await
}

/// Reuses the distributor's channel across transactions. A bounded set of independent reads
/// shares its HTTP/2 connection; transaction routing itself remains strictly ordered.
pub async fn load_with_client(
    client: &Client,
    keyspace: &str,
    schema: &SyncSchema,
    changes: Option<&[RowChange]>,
) -> Result<Vec<(String, Row)>, VStreamError> {
    load_requests(schema, queries(schema, changes)?, |sql| {
        let mut client = client.clone();
        async move { query_with_client(&mut client, keyspace, &sql).await }
    })
    .await
}

const READ_CONCURRENCY: usize = 4;

async fn load_requests<F, Fut>(
    schema: &SyncSchema,
    requests: Vec<SharedQuery>,
    execute: F,
) -> Result<Vec<(String, Row)>, VStreamError>
where
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<QueryResult, VStreamError>>,
{
    let result = stream::iter(requests)
        .map(|request| {
            let execute = &execute;
            async move {
                let mut result = BTreeMap::new();
                let table = schema.table(&request.table).expect("validated table");
                let keys = table
                    .primary_key
                    .iter()
                    .map(|c| col(&request.alias, c))
                    .collect::<Vec<_>>()
                    .join(", ");
                let mut last: Option<Row> = None;
                loop {
                    let after = if let Some(last) = &last {
                        let values = table
                            .primary_key
                            .iter()
                            .map(|c| literal(table, c, last))
                            .collect::<Result<Vec<_>, _>>()?
                            .join(", ");
                        format!(" AND ({keys}) > ({values})")
                    } else {
                        String::new()
                    };
                    let sql = format!("{}{after} ORDER BY {keys} LIMIT 1000", request.sql);
                    let page = execute(sql).await?;
                    let projection = TableProjection::build(table, &query_fields(table, &page.fields)?)?;
                    for raw in &page.rows {
                        let row = projection.project(raw)?;
                        let key = table
                            .primary_key
                            .iter()
                            .map(|c| row.get(c).expect("projected key"))
                            .collect::<Vec<_>>();
                        result.insert(
                            (table.name.clone(), serde_json::to_string(&key).expect("key serializes")),
                            row.clone(),
                        );
                        last = Some(row);
                    }
                    if page.rows.len() < 1000 {
                        break;
                    }
                }
                Ok::<_, VStreamError>(result)
            }
        })
        .buffered(READ_CONCURRENCY)
        // Drain in request order and deduplicate immediately. Retain at most four request
        // results, not a second copy of every bootstrap projection until all reads complete.
        .try_fold(BTreeMap::new(), |mut result, rows| async move {
            result.extend(rows);
            Ok(result)
        })
        .await?;
    Ok(result.into_iter().map(|((table, _), row)| (table, row)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::query::{Field, Type};
    use orbit_protocol::cdc::RowOp;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    fn fields() -> Vec<Field> {
        ["id", "org", "name"]
            .into_iter()
            .map(|name| Field {
                name: name.into(),
                r#type: Type::Varchar as i32,
                ..Default::default()
            })
            .collect()
    }

    #[tokio::test]
    async fn hydration_is_bounded_and_deduplicates_in_request_order() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let requests = (0..12)
            .map(|i| SharedQuery {
                table: "doc".into(),
                alias: "s0".into(),
                sql: format!("SELECT {i}"),
            })
            .collect();
        let result = load_requests(&schema(), requests, |sql| {
            let active = active.clone();
            let peak = peak.clone();
            async move {
                let n = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(n, Ordering::SeqCst);
                let i: usize = sql.split_whitespace().nth(1).unwrap().parse().unwrap();
                tokio::time::sleep(Duration::from_millis((4 - i % 4) as u64 * 3)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(QueryResult {
                    fields: fields(),
                    rows: vec![vec![
                        Some(bytes::Bytes::from_static(b"doc")),
                        Some(bytes::Bytes::from_static(b"owner")),
                        Some(i.to_string().into()),
                    ]],
                })
            }
        })
        .await
        .unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), READ_CONCURRENCY);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1.get("name"), Some(&CellValue::String("11".into())));
    }

    #[tokio::test]
    async fn hydration_keeps_keyset_pagination_and_propagates_query_errors() {
        let calls = AtomicUsize::new(0);
        let request = SharedQuery {
            table: "doc".into(),
            alias: "s0".into(),
            sql: "SELECT id, org, name FROM doc WHERE 1=1".into(),
        };
        let result = load_requests(&schema(), vec![request.clone()], |sql| {
            let call = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                let range = if call == 0 {
                    0..1000
                } else {
                    assert!(sql.contains("AND (`s0`.`id`) > ('0999')"));
                    1000..1001
                };
                Ok(QueryResult {
                    fields: fields(),
                    rows: range
                        .map(|i| {
                            vec![
                                Some(format!("{i:04}").into()),
                                Some(bytes::Bytes::from_static(b"owner")),
                                Some(bytes::Bytes::from_static(b"name")),
                            ]
                        })
                        .collect(),
                })
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(result.len(), 1001);
        let error = load_requests(&schema(), vec![request], |_| async { Err(fail("query failed")) })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("query failed"));
    }
    fn schema() -> SyncSchema {
        let c = |name: &str| json!({"name":name,"kind":"string","nullable":false,"source_type":"varchar(191)"});
        let mut schema:SyncSchema=serde_json::from_value(json!({"format_version":1,"schema_hash":"","app":"t","keyspace":"t",
            "partition":{"name":"org","key_kind":"string","placement":{"strategy":"one_per_partition","version":1}},
            "tables":[{"name":"doc","primary_key":["id"],"partition_column":"org","partition_routes":[["permissions"]],
                "columns":[c("id"),c("org"),c("name")],"relations":[{"name":"permissions","kind":"many","target_table":"permission","from_columns":["id"],"to_columns":["docId"]}]},
                {"name":"permission","primary_key":["id"],"partition_column":"org","columns":[c("id"),c("org"),c("docId"),c("role")],"relations":[]}]})).unwrap();
        schema.schema_hash = schema.compute_hash();
        schema.validate().unwrap();
        schema
    }
    #[test]
    fn bootstrap_queries_require_cross_org_permission_for_every_selected_table() {
        let result = queries(&schema(), None).unwrap();
        assert_eq!(result.len(), 2);
        for query in &result {
            assert!(query.sql.contains("JOIN `permission`"));
            assert!(
                query
                    .sql
                    .contains("WHERE `s1`.`org` IS NOT NULL AND NOT (`s1`.`org` <=> `s0`.`org`)")
            );
        }
        assert!(queries(&schema(), Some(&[])).unwrap().is_empty());
    }
    #[test]
    fn new_share_hydration_is_scoped_to_the_changed_permission_key() {
        let row: Row =
            serde_json::from_value(json!({"id":"grant'1","docId":"d1","org":"recipient","role":"VIEWER"})).unwrap();
        let change = RowChange {
            table: "permission".into(),
            op: RowOp::Insert,
            key: vec!["grant'1".into()],
            before: None,
            after: Some(row.clone()),
        };
        let result = queries(&schema(), Some(std::slice::from_ref(&change))).unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.iter().all(|q| q.sql.contains("`s1`.`id` = 'grant''1'")));
        let mut role_change = change.clone();
        role_change.op = RowOp::Update;
        role_change.before = Some(row.clone());
        role_change
            .after
            .as_mut()
            .unwrap()
            .insert("role".into(), "FULL_ACCESS".into());
        assert!(queries(&schema(), Some(&[role_change])).unwrap().is_empty());
        let revoke = RowChange {
            op: RowOp::Delete,
            before: Some(row),
            after: None,
            ..change
        };
        assert!(queries(&schema(), Some(&[revoke])).unwrap().is_empty());
    }
}
