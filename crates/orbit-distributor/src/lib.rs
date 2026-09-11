//! Event distributor: routes normalized transactions to logical partitions and delivers them to
//! the Durable Object that owns each partition.
//!
//! * [`router`]      partition routing derived purely from the sync schema
//! * [`state`]       durable checkpoint, sequence counters and quarantine (SQLite)
//! * [`delivery`]    the `Sink` trait and its HTTP implementation
//! * [`distributor`] the loop: batching, ordering, retries, checkpoint advancement

pub mod delivery;
pub mod distributor;
pub mod router;
pub mod state;

pub use delivery::{DeliveryError, HttpSink, Sink};
pub use distributor::{
    Distributor, DistributorConfig, DistributorError, DistributorStatus, bootstrap_parent_index, parent_tables,
};
pub use state::{ParentEntry, StateError, StateStore};
