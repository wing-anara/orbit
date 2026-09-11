//! Demand fill protocol.
//!
//! A Durable Object that lacks a scope (one table within its partition) registers a
//! [`FillRequest`]. The Rust server polls for requests, runs a Vitess VStream copy phase with a
//! `WHERE partition_column = ?` filter, and posts the result back as newline-delimited
//! [`FillChunk`]s followed by a final [`FillResult`].
//!
//! Correctness contract: the rows in a completed fill are exactly the table's rows for the
//! partition at position `FillResult::Completed::position`. The DO holds stream application for
//! the scope while the fill is in flight and, once the fill is applied, re-applies held
//! transactions while skipping every row change whose gtid is already contained in that
//! position. See `docs/bootstrap.md`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::value::Row;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FillRequest {
    /// Unique id chosen by the requesting Durable Object.
    pub fill_id: String,
    pub schema_hash: String,
    pub partition: String,
    pub table: String,
    /// Unix milliseconds when the DO registered the request.
    pub requested_at_ms: i64,
}

/// Long-poll response listing fills the Rust server should execute next.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FillPollResponse {
    pub requests: Vec<FillRequest>,
}

/// One NDJSON line of a fill upload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum FillChunk {
    Rows { fill_id: String, rows: Vec<Row> },
    Done { fill_id: String, result: FillResult },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum FillResult {
    Completed {
        /// Shard position (`MySQL56/...`) at which the delivered rows are exact.
        position: String,
        keyspace: String,
        shard: String,
        row_count: u64,
        duration_ms: u32,
    },
    Failed {
        error: crate::errors::EngineError,
    },
}
