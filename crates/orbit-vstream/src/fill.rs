//! Demand fills via the VStream copy phase.
//!
//! A fill opens a VStream with an empty starting position (copy phase) and a filter
//! `select * from T where <partition_column> = <value>`, applies every row image it receives
//! (copy rows and interleaved catch-up transactions) to an in-memory map keyed by primary key,
//! and stops at the final `COPY_COMPLETED`. The result is the exact set of rows for the
//! partition at the last observed position of each shard. Vitess guarantees this: the copy phase
//! interleaves binlog catch-up so that copied rows are consistent up to each emitted VGTID.
//!
//! Vitess's copy phase does not include the keyspace prefix in `table_p_ks`, and the row events
//! use the normal `keyspace.table` form; both are handled by [`crate::decode::strip_keyspace`].
//!
//! A table with `partition_parent` cannot use the copy phase, because its filter would need a
//! subquery on the parent. [`run_derived_fill`] reads such a table through vtgate `Execute`
//! instead; see its documentation for why that is exact.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use orbit_gtid::GtidSet;
use orbit_protocol::cdc::RowOp;
use orbit_protocol::schema::{SyncSchema, ValueKind};
use orbit_protocol::value::{Row, RowKey};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::checkpoint::ShardId;
use crate::error::VStreamError;
use crate::execute::{execute_with_client, query_with_client};
use crate::normalize::{TableProjection, query_fields};
use crate::proto::binlogdata::{Filter, Rule, ShardGtid, VGtid};
use crate::proto::vtgate::VStreamRequest;
use crate::stream::{Assembler, StepPolicy, StreamItem};
use crate::subscriber::{SubscriberConfig, current_position_with_client, flags_for, quote_ident, shards_or_all};

/// Parent keys per `IN (...)` list in a derived fill.
pub const DERIVED_FILL_CHUNK: usize = 500;

#[derive(Debug, Clone)]
pub struct FillOutcome {
    pub rows: Vec<Row>,
    /// Position per shard at which `rows` is exact.
    pub positions: BTreeMap<ShardId, String>,
    pub duration: Duration,
}

/// Renders a partition key as a SQL literal for the copy filter.
pub fn sql_literal(kind: ValueKind, value: &str) -> Result<String, VStreamError> {
    match kind {
        ValueKind::Int | ValueKind::BigInt => {
            let s = value.strip_prefix('-').unwrap_or(value);
            if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
                return Err(VStreamError::Malformed(format!(
                    "partition key {value:?} is not an integer"
                )));
            }
            Ok(value.to_string())
        }
        ValueKind::String => {
            let mut out = String::with_capacity(value.len() + 2);
            out.push('\'');
            for c in value.chars() {
                match c {
                    '\'' => out.push_str("''"),
                    '\\' => out.push_str("\\\\"),
                    '\0' => out.push_str("\\0"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\u{1a}' => out.push_str("\\Z"),
                    other => out.push(other),
                }
            }
            out.push('\'');
            Ok(out)
        }
        other => Err(VStreamError::Unsupported(format!("partition key kind {other:?}"))),
    }
}

/// Runs one fill. `partition` is the rendered partition key.
pub async fn run_fill(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    table: &str,
    partition: &str,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<FillOutcome, VStreamError> {
    let client = config.endpoint.connect().await?;
    run_fill_with_client(config, schema, table, partition, timeout, cancel, client).await
}

pub async fn run_fill_with_client(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    table: &str,
    partition: &str,
    timeout: Duration,
    cancel: &CancellationToken,
    mut client: crate::client::Client,
) -> Result<FillOutcome, VStreamError> {
    let table_schema = schema
        .table(table)
        .ok_or_else(|| VStreamError::Unsupported(format!("unknown table {table}")))?;
    let literal = sql_literal(schema.partition.key_kind, partition)?;
    let query = format!(
        "select * from {} where {} = {}",
        quote_ident(table),
        quote_ident(&table_schema.partition_column),
        literal
    );
    let started = Instant::now();

    let shard_gtids = shards_or_all(config)
        .into_iter()
        .map(|shard| ShardGtid {
            keyspace: config.keyspace.clone(),
            shard,
            gtid: String::new(),
            table_p_ks: vec![],
        })
        .collect();
    let mut flags = flags_for(config);
    flags.tables_to_copy = vec![table.to_string()];
    let req = VStreamRequest {
        tablet_type: config.tablet_type as i32,
        vgtid: Some(VGtid { shard_gtids }),
        filter: Some(Filter {
            rules: vec![Rule {
                r#match: table.to_string(),
                filter: query.clone(),
                ..Default::default()
            }],
            ..Default::default()
        }),
        flags: Some(flags),
        ..Default::default()
    };
    debug!(%query, "opening copy-phase fill");
    let mut stream = client
        .v_stream(req)
        .await
        .map_err(|s| VStreamError::from_status(s, "copy"))?
        .into_inner();

    let mut assembler = Assembler::new(schema, HashMap::new(), StepPolicy::Lenient);
    let mut rows: BTreeMap<String, Row> = BTreeMap::new();
    let mut positions: BTreeMap<ShardId, GtidSet> = BTreeMap::new();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        let resp = tokio::select! {
            _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
            _ = &mut deadline => return Err(VStreamError::Timeout(timeout)),
            m = stream.message() => match m {
                Err(status) => return Err(VStreamError::from_status(status, "copy")),
                Ok(None) => return Err(VStreamError::EndedUnexpectedly),
                Ok(Some(r)) => r,
            },
        };
        let received_at_ms = crate::subscriber::now_ms();
        for ev in &resp.events {
            for item in assembler.push(ev, received_at_ms)? {
                match item {
                    StreamItem::Transaction(t) => {
                        positions.insert(
                            ShardId {
                                keyspace: t.keyspace,
                                shard: t.shard,
                            },
                            GtidSet::parse_position(&t.position)?,
                        );
                        for change in t.changes {
                            if change.table != table {
                                continue;
                            }
                            let k = key_string(&change.key);
                            match change.op {
                                RowOp::Insert | RowOp::Update => {
                                    rows.insert(k, change.after.expect("insert/update has after"));
                                }
                                RowOp::Delete => {
                                    rows.remove(&k);
                                }
                            }
                        }
                    }
                    StreamItem::Position { shard, position } | StreamItem::Ddl { shard, position, .. } => {
                        positions.insert(shard, GtidSet::parse_position(&position)?);
                    }
                    StreamItem::CopyCompleted { shard: None } => {
                        if positions.is_empty() {
                            return Err(VStreamError::Malformed("copy completed without any position".into()));
                        }
                        let duration = started.elapsed();
                        info!(
                            table,
                            partition,
                            rows = rows.len(),
                            ms = duration.as_millis() as u64,
                            "fill completed"
                        );
                        metrics::counter!("orbit_fill_completed_total").increment(1);
                        metrics::histogram!("orbit_fill_duration_seconds").record(duration.as_secs_f64());
                        return Ok(FillOutcome {
                            rows: rows.into_values().collect(),
                            positions: positions.into_iter().map(|(k, v)| (k, v.to_position())).collect(),
                            duration,
                        });
                    }
                    StreamItem::CopyCompleted { shard: Some(_) } | StreamItem::Heartbeat => {}
                }
            }
        }
    }
}

fn key_string(key: &RowKey) -> String {
    serde_json::to_string(key).expect("keys serialize")
}

/// Runs one fill of a table with `partition_parent`. `partition` is the rendered partition key.
///
/// Steps:
///
/// 1. Read the current stream position. This is the fill position.
/// 2. Read the keys of the parent rows in `partition` from the parent table.
/// 3. Read the child rows whose `partition_column` is in that key set, in chunks of
///    [`DERIVED_FILL_CHUNK`] keys, and project them with the same normalization the CDC path
///    uses, so the row images are identical to those of [`run_fill`].
///
/// Taking the position before the selects is correct. Every select runs after the position,
/// so the rows reflect every change up to the position, plus possibly some later ones. The
/// Durable Object applies every held change whose gtid is after the position, and those
/// changes are idempotent row images: a later change that the select already reflects is
/// applied again with the same result. A change at or before the position is skipped, and the
/// select reflects it.
///
/// The parent keys come from the source table, not from the distributor's parent index. The
/// persisted index only holds the parents of checkpointed transactions, so it can lag behind
/// the fill position; a parent inserted in that window would be missing, and the Durable
/// Object would skip its children's changes as "already in the fill". The source table is
/// always at or after the fill position.
pub async fn run_derived_fill(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    table: &str,
    partition: &str,
    timeout: Duration,
    cancel: &CancellationToken,
) -> Result<FillOutcome, VStreamError> {
    let client = config.endpoint.connect().await?;
    run_derived_fill_with_client(config, schema, table, partition, timeout, cancel, client).await
}

pub async fn run_derived_fill_with_client(
    config: &SubscriberConfig,
    schema: &SyncSchema,
    table: &str,
    partition: &str,
    timeout: Duration,
    cancel: &CancellationToken,
    mut client: crate::client::Client,
) -> Result<FillOutcome, VStreamError> {
    let table_schema = schema
        .table(table)
        .ok_or_else(|| VStreamError::Unsupported(format!("unknown table {table}")))?;
    let parent_name = table_schema
        .partition_parent
        .as_deref()
        .ok_or_else(|| VStreamError::Unsupported(format!("table {table} has no partition_parent")))?;
    let parent = schema
        .table(parent_name)
        .ok_or_else(|| VStreamError::Unsupported(format!("unknown parent table {parent_name}")))?;
    let parent_pk = parent
        .primary_key
        .first()
        .ok_or_else(|| VStreamError::Unsupported(format!("parent table {parent_name} has no primary key")))?;
    let key_kind = parent
        .columns
        .iter()
        .find(|c| &c.name == parent_pk)
        .map(|c| c.kind)
        .ok_or_else(|| VStreamError::Unsupported(format!("parent table {parent_name} lacks column {parent_pk}")))?;
    let started = Instant::now();

    let work = async {
        // 1. The fill position, before any select.
        let positions = current_position_with_client(config, schema, client.clone())
            .await?
            .positions;

        // 2. The parent keys of the partition.
        let keys_sql = format!(
            "SELECT {} FROM {} WHERE {} = {}",
            quote_ident(parent_pk),
            quote_ident(parent_name),
            quote_ident(&parent.partition_column),
            sql_literal(schema.partition.key_kind, partition)?
        );
        debug!(sql = %keys_sql, "reading parent keys for derived fill");
        let keys: Vec<String> = execute_with_client(&mut client, &config.keyspace, &keys_sql)
            .await?
            .into_iter()
            .filter_map(|row| row.into_iter().next().flatten())
            .collect();

        // 3. The child rows, in chunks, projected like CDC rows. Derived columns read their
        //    source column, selected once.
        let mut selected: Vec<&str> = Vec::with_capacity(table_schema.columns.len());
        for c in &table_schema.columns {
            let source = c.derived.as_ref().map_or(c.name.as_str(), |d| d.from.as_str());
            if !selected.contains(&source) {
                selected.push(source);
            }
        }
        let columns = selected.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let mut rows: Vec<Row> = Vec::new();
        for chunk in keys.chunks(DERIVED_FILL_CHUNK) {
            let list = chunk
                .iter()
                .map(|k| sql_literal(key_kind, k))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            let sql = format!(
                "SELECT {columns} FROM {} WHERE {} IN ({list})",
                quote_ident(table),
                quote_ident(&table_schema.partition_column),
            );
            let result = query_with_client(&mut client, &config.keyspace, &sql).await?;
            let fields = query_fields(table_schema, &result.fields)?;
            let projection = TableProjection::build(table_schema, &fields)?;
            for raw in &result.rows {
                rows.push(projection.project(raw)?);
            }
        }
        Ok::<_, VStreamError>((positions, keys.len(), rows))
    };
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    let (positions, parents, rows) = tokio::select! {
        _ = cancel.cancelled() => return Err(VStreamError::Cancelled),
        _ = &mut deadline => return Err(VStreamError::Timeout(timeout)),
        r = work => r?,
    };
    if positions.is_empty() {
        return Err(VStreamError::Malformed("current position has no shard".into()));
    }
    let duration = started.elapsed();
    info!(
        table,
        partition,
        parents,
        rows = rows.len(),
        ms = duration.as_millis() as u64,
        "fill completed"
    );
    metrics::counter!("orbit_fill_completed_total").increment(1);
    metrics::histogram!("orbit_fill_duration_seconds").record(duration.as_secs_f64());
    Ok(FillOutcome {
        rows,
        positions,
        duration,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literals_are_escaped() {
        assert_eq!(sql_literal(ValueKind::String, "org1").unwrap(), "'org1'");
        assert_eq!(sql_literal(ValueKind::String, "o'r\\g").unwrap(), "'o''r\\\\g'");
        assert_eq!(sql_literal(ValueKind::Int, "-42").unwrap(), "-42");
        assert!(sql_literal(ValueKind::Int, "4x").is_err());
        assert!(sql_literal(ValueKind::Json, "{}").is_err());
    }
}
