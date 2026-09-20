//! Vitess VStream subscriber.
//!
//! * [`client`]     vtgate gRPC connection (TLS, basic auth, keepalive)
//! * [`decode`]     `FieldEvent` / `RowEvent` byte decoding
//! * [`normalize`]  projection of live rows onto the sync schema, with schema drift detection
//! * [`stream`]     transaction assembly state machine
//! * [`checkpoint`] resumable positions with epoch
//! * [`subscriber`] reconnecting live stream with backpressure
//! * [`fill`]       copy-phase demand fills
//! * [`error`]      the failure model

pub mod checkpoint;
pub mod client;
pub mod decode;
pub mod error;
pub mod execute;
pub mod fill;
pub mod normalize;
pub mod proto;
pub mod routed_fill;
pub mod select_fill;
pub mod shared_projection;
pub mod stream;
pub mod subscriber;

pub use checkpoint::{Checkpoint, ShardId};
pub use client::VitessEndpoint;
pub use error::VStreamError;
pub use stream::StreamItem;
pub use subscriber::{SubscriberConfig, run};
