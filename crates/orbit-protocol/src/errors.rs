//! Typed errors that cross a process or runtime boundary.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Error carried in fill results and internal HTTP error responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, thiserror::Error)]
#[serde(tag = "code", rename_all = "snake_case", deny_unknown_fields)]
pub enum EngineError {
    /// The authoritative database or Vitess refused or dropped the request.
    #[error("source unavailable: {message}")]
    SourceUnavailable { message: String, retryable: bool },
    /// The table or filter is rejected by Vitess (for example after a schema change).
    #[error("source rejected the request: {message}")]
    SourceRejected { message: String },
    /// A row could not be normalized to the sync schema.
    #[error("row normalization failed for {table}: {message}")]
    Normalization { table: String, message: String },
    /// The live schema no longer matches the sync schema.
    #[error("schema mismatch for {table}: {message}")]
    SchemaMismatch { table: String, message: String },
    #[error("unknown table {table}")]
    UnknownTable { table: String },
    #[error("unauthorized: {message}")]
    Unauthorized { message: String },
    #[error("timeout after {after_ms} ms")]
    Timeout { after_ms: u64 },
    #[error("internal error: {message}")]
    Internal { message: String },
}
