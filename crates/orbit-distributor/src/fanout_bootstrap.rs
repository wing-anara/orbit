//! Bootstrap the durable projected graph before enabling relation-based routing.
//! A new graph starts a new stream epoch, so no previously cached partition can mistake a
//! different routing history for a continuation of its old sequence assignments.

use crate::distributor::DistributorError;
use crate::state::StateStore;
use orbit_protocol::schema::SyncSchema;
use orbit_vstream::subscriber::{SubscriberConfig, current_position};

pub async fn bootstrap(
    schema: &SyncSchema,
    state: &StateStore,
    config: &SubscriberConfig,
) -> Result<(), DistributorError> {
    if !schema.tables.iter().any(|t| !t.partition_routes.is_empty()) || state.fanout_ready(&schema.schema_hash)? {
        return Ok(());
    }
    state.clear_fanout()?;
    let checkpoint = state.load_or_init(&schema.schema_hash)?.checkpoint;
    let position = if checkpoint.positions.is_empty() {
        current_position(config, schema).await?
    } else {
        // Preserve the parent-index fence; jumping ahead could skip ownership changes.
        checkpoint
    };
    let rows = orbit_vstream::shared_projection::load(config, schema, None).await?;
    let mut grouped = std::collections::BTreeMap::<String, Vec<_>>::new();
    for (table, row) in rows {
        grouped.entry(table).or_default().push(row);
    }
    for (table, rows) in grouped {
        for chunk in rows.chunks(1000) {
            state.seed_fanout(schema, &table, chunk)?;
        }
        tracing::info!(table=%table,rows=rows.len(),"shared-item cache bootstrapped");
    }
    // Reset before marking ready: a crash between these writes repeats bootstrap with a newer
    // epoch instead of exposing a completed graph with an incompatible old checkpoint.
    state.reset_checkpoint(&position)?;
    state.mark_fanout_ready(&schema.schema_hash)?;
    Ok(())
}
