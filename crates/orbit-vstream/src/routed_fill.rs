//! Demand fills for relation-routed partitions. All rows use the same source projection as
//! CDC. Each route is a separate indexed join (rather than an OR that scans the source table).
//! Duplicate permissions and overlapping routes are deduplicated by primary key.

use crate::error::VStreamError;
use crate::execute::query;
use crate::fill::{FillOutcome, sql_literal};
use crate::normalize::{TableProjection, query_fields};
use crate::subscriber::{SubscriberConfig, current_position, quote_ident};
use orbit_protocol::schema::{SyncSchema, TableSchema};
use orbit_protocol::value::Row;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

fn qualified(alias: &str, column: &str) -> String {
    format!("{}.{}", quote_ident(alias), quote_ident(column))
}
fn malformed(message: impl Into<String>) -> VStreamError {
    VStreamError::Malformed(message.into())
}

/// SQL for the owning partition and each additional relation path. Identifiers come only from
/// the validated schema; partition values always pass through the typed literal encoder.
pub fn queries(schema: &SyncSchema, table: &TableSchema, partition: &str) -> Result<Vec<String>, VStreamError> {
    let literal = sql_literal(schema.partition.key_kind, partition)?;
    let mut sources: Vec<&str> = Vec::new();
    for column in &table.columns {
        let name = column
            .derived
            .as_ref()
            .map_or(column.name.as_str(), |d| d.from.as_str());
        if !sources.contains(&name) {
            sources.push(name);
        }
    }
    let selected = sources
        .iter()
        .map(|c| qualified("r0", c))
        .collect::<Vec<_>>()
        .join(", ");
    let base = format!(
        "SELECT {selected} FROM {} AS {}",
        quote_ident(&table.name),
        quote_ident("r0")
    );
    let mut result = Vec::new();
    if let Some(parent) = &table.partition_parent {
        let parent = schema
            .table(parent)
            .ok_or_else(|| malformed("unknown partition parent"))?;
        if parent.primary_key.len() != 1 || parent.partition_parent.is_some() {
            return Err(malformed("invalid partition parent"));
        }
        result.push(format!(
            "{base} JOIN {} AS {} ON {} = {} WHERE {} = {literal}",
            quote_ident(&parent.name),
            quote_ident("owner"),
            qualified("r0", &table.partition_column),
            qualified("owner", &parent.primary_key[0]),
            qualified("owner", &parent.partition_column)
        ));
    } else {
        result.push(format!(
            "{base} WHERE {} = {literal}",
            qualified("r0", &table.partition_column)
        ));
    }
    for path in &table.partition_routes {
        if path.is_empty() || path.len() > 8 {
            return Err(malformed("partition route must have 1–8 relations"));
        }
        let mut sql = base.clone();
        let mut current = table;
        for (index, name) in path.iter().enumerate() {
            let relation = current
                .relations
                .iter()
                .find(|r| &r.name == name)
                .ok_or_else(|| malformed(format!("unknown relation {}.{name}", current.name)))?;
            if relation.from_columns.is_empty() || relation.from_columns.len() != relation.to_columns.len() {
                return Err(malformed("invalid relation columns"));
            }
            let from_alias = format!("r{index}");
            let to_alias = format!("r{}", index + 1);
            let predicate = relation
                .from_columns
                .iter()
                .zip(&relation.to_columns)
                .map(|(from, to)| format!("{} = {}", qualified(&from_alias, from), qualified(&to_alias, to)))
                .collect::<Vec<_>>()
                .join(" AND ");
            sql.push_str(&format!(
                " JOIN {} AS {} ON {predicate}",
                quote_ident(&relation.target_table),
                quote_ident(&to_alias)
            ));
            current = schema
                .table(&relation.target_table)
                .ok_or_else(|| malformed("unknown relation target"))?;
        }
        if current.partition_parent.is_some() {
            return Err(malformed("partition route ends at a derived table"));
        }
        sql.push_str(&format!(
            " WHERE {} = {literal}",
            qualified(&format!("r{}", path.len()), &current.partition_column)
        ));
        result.push(sql);
    }
    Ok(result)
}

pub async fn run_routed_fill(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    table: &str,
    partition: &str,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<FillOutcome, VStreamError> {
    let table = schema
        .table(table)
        .ok_or_else(|| malformed(format!("unknown table {table}")))?;
    let statements = queries(schema, table, partition)?;
    let started = Instant::now();
    let work = async {
        // Fence before SELECT, as with derived fills. Later full CDC images are replayed by
        // the Durable Object; changes at/before the fence are already reflected in the reads.
        let positions = current_position(config, schema).await?.positions;
        if positions.is_empty() {
            return Err(malformed("current position has no shard"));
        }
        let mut rows: BTreeMap<String, Row> = BTreeMap::new();
        for sql in statements {
            let result = query(&config.endpoint, &config.keyspace, &sql).await?;
            let projection = TableProjection::build(table, &query_fields(table, &result.fields)?)?;
            for raw in &result.rows {
                let row = projection.project(raw)?;
                let key = table
                    .primary_key
                    .iter()
                    .map(|c| row.get(c).ok_or_else(|| malformed(format!("missing primary key {c}"))))
                    .collect::<Result<Vec<_>, _>>()?;
                rows.insert(serde_json::to_string(&key).expect("key serializes"), row);
            }
        }
        Ok(FillOutcome {
            rows: rows.into_values().collect(),
            positions,
            duration: started.elapsed(),
        })
    };
    tokio::select! {
        _ = cancel.cancelled() => Err(VStreamError::Cancelled),
        _ = tokio::time::sleep(timeout) => Err(VStreamError::Timeout(timeout)),
        result = work => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn schema() -> SyncSchema {
        serde_json::from_value(json!({"format_version":1,"schema_hash":"","app":"t","keyspace":"t",
            "partition":{"name":"org","key_kind":"string","placement":{"strategy":"one_per_partition","version":1}},
            "tables":[
                {"name":"doc","primary_key":["id"],"partition_column":"org","partition_routes":[["permissions"]],
                    "columns":[{"name":"id","kind":"string","nullable":false,"source_type":"text"},{"name":"org","kind":"string","nullable":false,"source_type":"text"},
                        {"name":"uploaded","kind":"bool","nullable":false,"source_type":"derived","derived":{"from":"raw_url","rule":{"kind":"not_null"}}}],
                    "relations":[{"name":"permissions","kind":"many","target_table":"permission","from_columns":["id"],"to_columns":["docId"]}]},
                {"name":"permission","primary_key":["id"],"partition_column":"org","columns":[],"relations":[]}
            ]})).unwrap()
    }
    #[test]
    fn shared_fill_uses_recipient_permission_and_preserves_owner_column() {
        let schema = schema();
        let queries = queries(&schema, &schema.tables[0], "recipient'\\org").unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(
            queries[0],
            "SELECT `r0`.`id`, `r0`.`org`, `r0`.`raw_url` FROM `doc` AS `r0` WHERE `r0`.`org` = 'recipient''\\\\org'"
        );
        assert_eq!(
            queries[1],
            "SELECT `r0`.`id`, `r0`.`org`, `r0`.`raw_url` FROM `doc` AS `r0` JOIN `permission` AS `r1` ON `r0`.`id` = `r1`.`docId` WHERE `r1`.`org` = 'recipient''\\\\org'"
        );
        assert!(!queries[1].contains(" WHERE `r0`.`org`"));
    }
    #[test]
    fn malformed_routes_fail_instead_of_widening_the_fill() {
        let mut schema = schema();
        schema.tables[0].partition_routes = vec![vec!["missing".into()]];
        assert!(queries(&schema, &schema.tables[0], "recipient").is_err());
        schema.tables[0].partition_routes = vec![vec![]];
        assert!(queries(&schema, &schema.tables[0], "recipient").is_err());
    }
}
