//! Direct-partition fills without the VStream copy phase's table locks/binlog rotations.
//!
//! Read a start fence, keyset-page the partition, then read an end fence. Replay full CDC
//! images from the start through the end before publishing. The SELECTs need not share a
//! snapshot: every concurrent change is reconciled, including deletes and primary-key moves.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use orbit_protocol::cdc::{RowChange, RowOp};
use orbit_protocol::schema::{SyncSchema, TableSchema};
use orbit_protocol::value::Row;
use tokio_util::sync::CancellationToken;

use crate::client::Client;
use crate::error::VStreamError;
use crate::execute::{QueryResult, query_with_client_bindings};
use crate::fill::{FillOutcome, sql_literal};
use crate::normalize::{TableProjection, query_fields};
use crate::proto::binlogdata::{Filter, Rule};
use crate::proto::query::BindVariable;
use crate::proto::topodata::TabletType;
use crate::proto::vtgate::VStreamRequest;
use crate::stream::{Assembler, StepPolicy, StreamItem};
use crate::subscriber::{SubscriberConfig, current_position_with_client, flags_for, quote_ident, shards_or_all};

const PAGE_ROWS: usize = 1000;
type Bindings = HashMap<String, BindVariable>;

fn malformed(message: impl Into<String>) -> VStreamError {
    VStreamError::Malformed(message.into())
}

fn row_key(table: &TableSchema, row: &Row) -> Result<String, VStreamError> {
    let key = table
        .primary_key
        .iter()
        .map(|name| {
            row.get(name)
                .ok_or_else(|| malformed(format!("fill row lacks primary key {name}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string(&key).map_err(|e| malformed(e.to_string()))
}

fn page_sql(base: &str, table: &TableSchema, after: bool) -> String {
    let keys = table
        .primary_key
        .iter()
        .map(|k| format!("`r0`.{}", quote_ident(k)))
        .collect::<Vec<_>>();
    let predicate = if after {
        let parameters = (0..keys.len()).map(|i| format!(":after_{i}")).collect::<Vec<_>>();
        format!(" AND ({}) > ({})", keys.join(", "), parameters.join(", "))
    } else {
        String::new()
    };
    format!("{base}{predicate} ORDER BY {} LIMIT {PAGE_ROWS}", keys.join(", "))
}

fn page_cursor(table: &TableSchema, result: &QueryResult) -> Result<Bindings, VStreamError> {
    let row = result.rows.last().ok_or_else(|| malformed("empty cursor page"))?;
    table
        .primary_key
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let field_index = result
                .fields
                .iter()
                .position(|f| f.name == *key)
                .ok_or_else(|| malformed(format!("fill field list lacks primary key {key}")))?;
            let value = row
                .get(field_index)
                .and_then(Option::as_ref)
                .ok_or_else(|| malformed(format!("null fill primary key {key}")))?;
            Ok((
                format!("after_{i}"),
                BindVariable {
                    r#type: result.fields[field_index].r#type,
                    value: value.to_vec(),
                    values: vec![],
                },
            ))
        })
        .collect()
}

fn apply_change(
    table: &TableSchema,
    rows: &mut BTreeMap<String, Row>,
    change: RowChange,
    partition_value: &serde_json::Value,
) -> Result<(), VStreamError> {
    if change.table != table.name {
        return Ok(());
    }
    if let Some(before) = &change.before {
        rows.remove(&row_key(table, before)?);
    }
    match change.op {
        RowOp::Insert | RowOp::Update => {
            let after = change
                .after
                .ok_or_else(|| malformed("fill insert/update lacks after image"))?;
            if after.get(&table.partition_column) == Some(partition_value) {
                rows.insert(row_key(table, &after)?, after);
            }
        }
        RowOp::Delete => {
            // A delete's wire key is the old key, including when no before image was projected.
            let key = serde_json::to_string(&change.key).map_err(|e| malformed(e.to_string()))?;
            rows.remove(&key);
        }
    }
    Ok(())
}

pub async fn run_select_fill_with_client(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    table_name: &str,
    partition: &str,
    timeout: Duration,
    cancel: &CancellationToken,
    mut client: Client,
) -> Result<FillOutcome, VStreamError> {
    let table = schema
        .table(table_name)
        .ok_or_else(|| malformed(format!("unknown table {table_name}")))?;
    if table.partition_parent.is_some() || !table.partition_routes.is_empty() {
        return Err(VStreamError::Unsupported(
            "select fill requires a direct partition column".into(),
        ));
    }
    if config.tablet_type != TabletType::Primary {
        return Err(VStreamError::Unsupported(
            "select fill fences and reads require the primary".into(),
        ));
    }
    let base = crate::routed_fill::queries(schema, table, partition)?.remove(0);
    let partition_value = match schema.partition.key_kind {
        orbit_protocol::schema::ValueKind::String => serde_json::Value::String(partition.into()),
        orbit_protocol::schema::ValueKind::Int => serde_json::json!(
            partition
                .parse::<i64>()
                .map_err(|_| malformed("invalid integer partition"))?
        ),
        orbit_protocol::schema::ValueKind::BigInt => serde_json::Value::String(
            partition
                .parse::<i128>()
                .map_err(|_| malformed("invalid bigint partition"))?
                .to_string(),
        ),
        _ => return Err(malformed("unsupported partition kind")),
    };
    let started = Instant::now();
    let work = async {
        let start = current_position_with_client(config, schema, client.clone()).await?;
        if start.positions.is_empty() {
            return Err(malformed("fill start fence has no shard"));
        }
        let mut rows = BTreeMap::new();
        let mut cursor: Option<Bindings> = None;
        loop {
            let sql = page_sql(&base, table, cursor.is_some());
            let result =
                query_with_client_bindings(&mut client, &config.keyspace, &sql, cursor.clone().unwrap_or_default())
                    .await?;
            let projection = TableProjection::build(table, &query_fields(table, &result.fields)?)?;
            for raw in &result.rows {
                let row = projection.project(raw)?;
                rows.insert(row_key(table, &row)?, row);
            }
            if result.rows.len() < PAGE_ROWS {
                break;
            }
            let next = page_cursor(table, &result)?;
            if cursor.as_ref() == Some(&next) {
                return Err(malformed("fill keyset cursor did not advance"));
            }
            cursor = Some(next);
        }
        let end = current_position_with_client(config, schema, client.clone()).await?;
        if end.positions.keys().ne(start.positions.keys()) || !start.is_contained_in(&end)? {
            return Err(malformed("fill source topology or position changed across SELECTs"));
        }
        let mut applied = start.clone();
        if !end.is_contained_in(&applied)? {
            let filter = format!(
                "select * from {} where {} = {}",
                quote_ident(table_name),
                quote_ident(&table.partition_column),
                sql_literal(schema.partition.key_kind, partition)?
            );
            let request = VStreamRequest {
                tablet_type: config.tablet_type as i32,
                vgtid: Some(start.to_vgtid(&config.keyspace, &shards_or_all(config))),
                filter: Some(Filter {
                    rules: vec![Rule {
                        r#match: table_name.to_string(),
                        filter,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                // Never set tables_to_copy and never request an empty starting GTID.
                flags: Some(flags_for(config)),
                ..Default::default()
            };
            let mut stream = client
                .v_stream(request)
                .await
                .map_err(|s| VStreamError::from_status(s, "fill-reconcile"))?
                .into_inner();
            let mut assembler = Assembler::new(schema, start.parsed()?.into_iter().collect(), StepPolicy::Lenient);
            'catchup: loop {
                let response = stream
                    .message()
                    .await
                    .map_err(|s| VStreamError::from_status(s, "fill-reconcile"))?
                    .ok_or(VStreamError::EndedUnexpectedly)?;
                for event in &response.events {
                    for item in assembler.push(event, crate::subscriber::now_ms())? {
                        match item {
                            StreamItem::Transaction(tx) => {
                                for change in tx.changes {
                                    apply_change(table, &mut rows, change, &partition_value)?;
                                }
                                applied.set(
                                    crate::checkpoint::ShardId {
                                        keyspace: tx.keyspace,
                                        shard: tx.shard,
                                    },
                                    tx.position,
                                );
                            }
                            StreamItem::Position { shard, position } => applied.set(shard, position),
                            StreamItem::Ddl { .. } => {
                                return Err(malformed("DDL during partition fill; retry against the current schema"));
                            }
                            StreamItem::CopyCompleted { .. } => {
                                return Err(malformed("unexpected copy phase during SELECT reconciliation"));
                            }
                            StreamItem::Heartbeat => {}
                        }
                        if end.is_contained_in(&applied)? {
                            break 'catchup;
                        }
                    }
                }
            }
        }
        Ok::<_, VStreamError>((rows, applied))
    };
    let (rows, applied) = tokio::select! {
        _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
        _ = tokio::time::sleep(timeout) => return Err(VStreamError::Timeout(timeout)),
        result = work => result?,
    };
    let duration = started.elapsed();
    metrics::counter!("orbit_fill_completed_total").increment(1);
    metrics::histogram!("orbit_fill_duration_seconds").record(duration.as_secs_f64());
    tracing::info!(
        table = table_name,
        partition,
        rows = rows.len(),
        ms = duration.as_millis() as u64,
        "select fill reconciled"
    );
    Ok(FillOutcome {
        rows: rows.into_values().collect(),
        positions: applied.positions,
        duration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        VitessEndpoint,
        proto::query::{Field, Type},
    };
    use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig, ValueKind};

    fn schema() -> SyncSchema {
        let columns = [
            ("id", ValueKind::BigInt, "bigint unsigned"),
            ("bin", ValueKind::Bytes, "varbinary(8)"),
            ("org", ValueKind::String, "varchar(64)"),
            ("n", ValueKind::Int, "int"),
        ]
        .into_iter()
        .map(|(name, kind, source_type)| ColumnSchema {
            name: name.into(),
            kind,
            source_type: source_type.into(),
            nullable: false,
            enum_values: None,
            derived: None,
        })
        .collect();
        SyncSchema {
            format_version: 1,
            schema_hash: String::new(),
            app: "select-fill-test".into(),
            keyspace: "botscribe".into(),
            partition: PartitionConfig {
                name: "org".into(),
                key_kind: ValueKind::String,
                placement: PlacementConfig::OnePerPartition { version: 1 },
            },
            tables: vec![TableSchema {
                name: "_orbit_select_fill_it".into(),
                primary_key: vec!["id".into(), "bin".into()],
                partition_column: "org".into(),
                partition_parent: None,
                partition_routes: vec![],
                columns,
                relations: vec![],
            }],
        }
    }

    #[test]
    fn cursor_preserves_unsigned_integer_and_binary_key_bytes() {
        let s = schema();
        let t = &s.tables[0];
        let result = QueryResult {
            fields: vec![
                Field {
                    name: "id".into(),
                    r#type: Type::Uint64 as i32,
                    ..Default::default()
                },
                Field {
                    name: "bin".into(),
                    r#type: Type::Varbinary as i32,
                    ..Default::default()
                },
            ],
            rows: vec![vec![
                Some(bytes::Bytes::from_static(b"18446744073709551615")),
                Some(bytes::Bytes::from_static(b"\0\xff'\\")),
            ]],
        };
        let cursor = page_cursor(t, &result).unwrap();
        assert_eq!(cursor["after_0"].value, b"18446744073709551615");
        assert_eq!(cursor["after_0"].r#type, Type::Uint64 as i32);
        assert_eq!(cursor["after_1"].value, b"\0\xff'\\");
        assert_eq!(cursor["after_1"].r#type, Type::Varbinary as i32);
        assert!(
            page_sql("SELECT * FROM t AS r0 WHERE org = 'one'", t, true)
                .contains("AND (`r0`.`id`, `r0`.`bin`) > (:after_0, :after_1)")
        );
    }

    #[test]
    fn replay_removes_old_primary_keys_and_deleted_rows() {
        let s = schema();
        let t = &s.tables[0];
        let old: Row =
            serde_json::from_value(serde_json::json!({"id":"9007199254740993","bin":"AP8nXA==","org":"one","n":0}))
                .unwrap();
        let mut new = old.clone();
        new.insert("id".into(), "9007199254740994".into());
        let mut rows = BTreeMap::from([(row_key(t, &old).unwrap(), old.clone())]);
        let key = vec![new["id"].clone(), new["bin"].clone()];
        apply_change(
            t,
            &mut rows,
            RowChange {
                table: t.name.clone(),
                op: RowOp::Update,
                key: key.clone(),
                before: Some(old.clone()),
                after: Some(new.clone()),
            },
            &serde_json::json!("one"),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(!rows.contains_key(&row_key(t, &old).unwrap()));
        apply_change(
            t,
            &mut rows,
            RowChange {
                table: t.name.clone(),
                op: RowOp::Delete,
                key,
                before: None,
                after: None,
            },
            &serde_json::json!("one"),
        )
        .unwrap();
        assert!(rows.is_empty());
    }

    // Explicit opt-in and dedicated owned test source: never uses the developer's default DB.
    #[tokio::test]
    #[ignore = "requires isolated fleet Vitess on 127.0.0.1:43575, keyspace botscribe"]
    async fn live_select_fill_pages_and_reconciles_atomic_partition_moves() {
        let endpoint = VitessEndpoint::new("http://127.0.0.1:43575");
        let mut client = endpoint.connect().await.unwrap();
        async fn sql(client: &mut Client, statement: &str) {
            crate::execute::query_with_client(client, "botscribe", statement)
                .await
                .unwrap();
        }
        // The fixture name is exclusively owned by this test; all other source rows are untouched.
        sql(&mut client, "DROP TABLE IF EXISTS _orbit_select_fill_it").await;
        sql(&mut client, "CREATE TABLE _orbit_select_fill_it (id BIGINT UNSIGNED NOT NULL, bin VARBINARY(8) NOT NULL, org VARCHAR(64) NOT NULL, n INT NOT NULL, PRIMARY KEY(id,bin), KEY(org))").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        for chunk in 0..5 {
            let values = (0..500)
                .map(|i| format!("({},X'00FF275C','one',0)", 9007199254740993_u64 + chunk * 500 + i))
                .collect::<Vec<_>>()
                .join(",");
            sql(
                &mut client,
                &format!("INSERT INTO _orbit_select_fill_it VALUES {values}"),
            )
            .await;
        }
        let schema = schema();
        let mut config = SubscriberConfig::new(endpoint.clone(), "botscribe");
        config.heartbeat_interval = Duration::from_secs(1);
        let fill = run_select_fill_with_client(
            &config,
            &schema,
            "_orbit_select_fill_it",
            "one",
            Duration::from_secs(30),
            &CancellationToken::new(),
            client.clone(),
        )
        .await
        .unwrap();
        assert_eq!(fill.rows.len(), 2500);
        assert!(fill.rows.iter().all(|r| r["n"] == 0 && r["org"] == "one"));
        let stop = CancellationToken::new();
        let writer_stop = stop.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            // Each UPDATE is atomic. Partition membership and the marker change together.
            let mut c = endpoint.connect().await.unwrap();
            let mut generation = 0;
            let mut ready_tx = Some(ready_tx);
            while !writer_stop.is_cancelled() {
                generation += 1;
                sql(
                    &mut c,
                    &format!(
                        "UPDATE _orbit_select_fill_it SET n={generation}, org=IF(MOD(id,2)={},'one','two'), id=id{}10000",
                        generation % 2, if generation % 2 == 1 { "+" } else { "-" }
                    ),
                )
                .await;
                if let Some(tx) = ready_tx.take() {
                    let _ = tx.send(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            generation
        });
        tokio::time::timeout(Duration::from_secs(30), ready_rx)
            .await
            .unwrap()
            .unwrap();
        for _ in 0..12 {
            let fill = run_select_fill_with_client(
                &config,
                &schema,
                "_orbit_select_fill_it",
                "one",
                Duration::from_secs(30),
                &CancellationToken::new(),
                client.clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                fill.rows.len(),
                1250,
                "no missing/duplicate keys across page boundaries"
            );
            let generation = fill.rows[0]["n"].as_i64().unwrap();
            for row in fill.rows {
                assert_eq!(
                    row["n"], generation,
                    "the published image must represent one committed source fence"
                );
                assert_eq!(row["org"], "one", "moved-out rows must be removed");
                let id: u64 = row["id"].as_str().unwrap().parse().unwrap();
                assert_eq!(id % 2, (generation % 2) as u64);
            }
        }
        stop.cancel();
        assert!(writer.await.unwrap() > 1);
        sql(&mut client, "DROP TABLE _orbit_select_fill_it").await;
    }
}
