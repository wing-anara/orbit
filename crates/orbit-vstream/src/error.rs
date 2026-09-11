//! Failure model of the VStream subscriber.
//!
//! Every error is classified so the caller never has to guess:
//!
//! * [`VStreamError::is_retryable`] errors are transient transport or availability problems. The
//!   subscriber reconnects from its last checkpoint with backoff.
//! * Fatal errors stop the subscriber. They describe a state an operator must resolve:
//!   an invalid or purged checkpoint, a schema that no longer matches the sync schema, a stream
//!   shape the engine does not support (reshard journals, merged transactions), or bad credentials.
//!
//! No error is ever swallowed and no event is ever skipped to make progress.

use orbit_protocol::errors::EngineError;

#[derive(Debug, thiserror::Error)]
pub enum VStreamError {
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("stream failed: {0}")]
    Status(#[source] tonic::Status),
    #[error("stream ended without an error status")]
    EndedUnexpectedly,
    #[error("no heartbeat or event received for {0:?}")]
    Stalled(std::time::Duration),
    #[error(
        "no position progress from {position} for {elapsed:?} while the source advanced to {current}; \
         vtgate may be retrying a purged-binlog error internally (run `checkpoint reset` if it persists)"
    )]
    NoProgress {
        position: String,
        current: String,
        elapsed: std::time::Duration,
    },
    #[error("invalid credentials")]
    Unauthenticated,
    #[error("checkpoint {checkpoint} is not contained in the server's current position {current}")]
    InvalidCheckpoint { checkpoint: String, current: String },
    #[error("checkpoint {checkpoint} refers to purged binlogs: {message}")]
    PurgedBinlog { checkpoint: String, message: String },
    #[error("the server refused the stream at {position}: {message}")]
    PoisonPosition { position: String, message: String },
    #[error("schema mismatch for table {table}: {message}")]
    SchemaMismatch { table: String, message: String },
    #[error("malformed stream: {0}")]
    Malformed(String),
    #[error("row normalization failed for {table}: {message}")]
    Normalization { table: String, message: String },
    #[error("unsupported event: {0}")]
    Unsupported(String),
    #[error("gtid: {0}")]
    Gtid(#[from] orbit_gtid::GtidError),
    #[error("position step: {0}")]
    Step(#[from] orbit_gtid::StepError),
    #[error("cancelled")]
    Cancelled,
    #[error("timeout after {0:?}")]
    Timeout(std::time::Duration),
    #[error("consumer closed the channel")]
    ConsumerGone,
}

/// A status whose message describes the connection, not the request. The gRPC client reports a
/// server-side `GOAWAY` or a stream cut mid-message with a non-transient code (`InvalidArgument`
/// or `Internal`), so the code alone would stop the subscriber. PlanetScale's vtgate closes idle
/// HTTP/2 connections this way from time to time; reconnecting from the checkpoint is correct.
fn is_connection_failure(message: &str) -> bool {
    const MARKERS: [&str; 7] = [
        "GOAWAY",
        "incomplete envelope",
        "protocol error",
        "connection reset",
        "broken pipe",
        "connection closed",
        "transport error",
    ];
    MARKERS.iter().any(|m| message.contains(m))
}

impl VStreamError {
    /// Classifies a gRPC status from vtgate. Message substrings are the only signal Vitess gives
    /// for some conditions, so they are matched deliberately here and nowhere else.
    pub fn from_status(status: tonic::Status, requested_position: &str) -> Self {
        use tonic::Code;
        let msg = status.message();
        if status.code() == Code::Unauthenticated {
            return VStreamError::Unauthenticated;
        }
        if msg.contains("purged required binary logs") || msg.contains("purged required gtids") {
            return VStreamError::PurgedBinlog {
                checkpoint: requested_position.to_string(),
                message: msg.to_string(),
            };
        }
        if msg.contains("GTIDSet Mismatch") {
            return VStreamError::InvalidCheckpoint {
                checkpoint: requested_position.to_string(),
                current: msg.to_string(),
            };
        }
        if msg.contains("persistent error in vstream") || msg.contains("failed to build table replication plan") {
            return VStreamError::PoisonPosition {
                position: requested_position.to_string(),
                message: msg.to_string(),
            };
        }
        if status.code() == Code::InvalidArgument && msg.contains("could not decode position") {
            return VStreamError::InvalidCheckpoint {
                checkpoint: requested_position.to_string(),
                current: msg.to_string(),
            };
        }
        VStreamError::Status(status)
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            VStreamError::Transport(_)
            | VStreamError::EndedUnexpectedly
            | VStreamError::Stalled(_)
            | VStreamError::NoProgress { .. }
            | VStreamError::Timeout(_) => true,
            VStreamError::Status(s) => {
                matches!(
                    s.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Aborted
                        | tonic::Code::Cancelled
                        | tonic::Code::ResourceExhausted
                        | tonic::Code::Unknown
                        | tonic::Code::Internal
                ) || is_connection_failure(s.message())
            }
            _ => false,
        }
    }

    pub fn to_engine_error(&self) -> EngineError {
        match self {
            VStreamError::SchemaMismatch { table, message } => EngineError::SchemaMismatch {
                table: table.clone(),
                message: message.clone(),
            },
            VStreamError::Normalization { table, message } => EngineError::Normalization {
                table: table.clone(),
                message: message.clone(),
            },
            VStreamError::Unauthenticated => EngineError::Unauthorized {
                message: "invalid vitess credentials".into(),
            },
            VStreamError::Timeout(d) => EngineError::Timeout {
                after_ms: d.as_millis() as u64,
            },
            VStreamError::PoisonPosition { message, .. } | VStreamError::PurgedBinlog { message, .. } => {
                EngineError::SourceRejected {
                    message: message.clone(),
                }
            }
            other if other.is_retryable() => EngineError::SourceUnavailable {
                message: other.to_string(),
                retryable: true,
            },
            other => EngineError::Internal {
                message: other.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_failures_are_retryable_regardless_of_code() {
        let goaway = tonic::Status::invalid_argument(
            "protocol error: incomplete envelope: http2: server sent GOAWAY and closed the connection; LastStreamID=1, ErrCode=NO_ERROR",
        );
        assert!(VStreamError::from_status(goaway, "MySQL56/x:1-5").is_retryable());
        let reset = tonic::Status::internal("connection reset by peer");
        assert!(VStreamError::from_status(reset, "MySQL56/x:1-5").is_retryable());
    }

    #[test]
    fn request_errors_stay_fatal() {
        let bad = tonic::Status::invalid_argument("vstream: unknown table foo");
        assert!(!VStreamError::from_status(bad, "MySQL56/x:1-5").is_retryable());
        let denied = tonic::Status::permission_denied("no");
        assert!(!VStreamError::from_status(denied, "MySQL56/x:1-5").is_retryable());
    }
}
