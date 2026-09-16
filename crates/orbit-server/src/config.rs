use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "orbit-server",
    version,
    about = "Vitess VStream to Durable Object sync engine server"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the subscriber, distributor, fill worker and metrics endpoint.
    Run(RunArgs),
    /// Schema tooling.
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    /// Checkpoint inspection and operator resets.
    Checkpoint {
        #[command(subcommand)]
        command: CheckpointCommand,
    },
    /// Quarantined transactions.
    Quarantine {
        #[command(subcommand)]
        command: QuarantineCommand,
    },
}

#[derive(Args, Debug, Clone)]
pub struct VitessArgs {
    /// vtgate gRPC endpoint, for example https://aws.connect.psdb.cloud:443 or http://127.0.0.1:33575
    #[arg(long, env = "VITESS_GRPC_URI")]
    pub vitess_uri: String,
    #[arg(long, env = "VITESS_USERNAME")]
    pub vitess_username: Option<String>,
    #[arg(long, env = "VITESS_PASSWORD", hide_env_values = true)]
    pub vitess_password: Option<String>,
    /// Comma-separated cells for tablet selection (PlanetScale: planetscale_operator_default).
    #[arg(long, env = "VITESS_CELLS")]
    pub vitess_cells: Option<String>,
}

impl VitessArgs {
    pub fn endpoint(&self) -> orbit_vstream::VitessEndpoint {
        let mut ep = orbit_vstream::VitessEndpoint::new(self.vitess_uri.clone());
        if let (Some(u), Some(p)) = (&self.vitess_username, &self.vitess_password) {
            ep = ep.with_basic_auth(u.clone(), p.clone());
        }
        ep
    }
}

#[derive(Args, Debug, Clone)]
pub struct RunArgs {
    #[command(flatten)]
    pub vitess: VitessArgs,
    /// Path to the compiled sync schema artifact (orbit.schema.json). When omitted, the artifact
    /// is fetched from the Worker (`GET {worker_url}/internal/schema`), so the engine always runs
    /// the schema the Durable Objects were deployed with.
    #[arg(long, env = "SYNC_SCHEMA_PATH")]
    pub schema: Option<PathBuf>,
    /// SQLite file for checkpoints, counters and quarantine.
    #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
    pub state: PathBuf,
    /// Base URL of the Worker hosting the sync engine (internal endpoints under /internal).
    #[arg(long, env = "WORKER_URL")]
    pub worker_url: String,
    /// Shared secret for internal endpoints.
    #[arg(long, env = "WORKER_SECRET", hide_env_values = true)]
    pub worker_secret: String,
    /// Prometheus metrics listen address.
    #[arg(long, env = "METRICS_ADDR", default_value = "127.0.0.1:9464")]
    pub metrics_addr: String,
    /// Concurrent demand fills.
    #[arg(long, env = "FILL_CONCURRENCY", default_value_t = 4)]
    pub fill_concurrency: usize,
    /// Per-fill timeout in seconds.
    #[arg(long, env = "FILL_TIMEOUT_SECS", default_value_t = 120)]
    pub fill_timeout_secs: u64,
    /// Delivery request timeout in seconds.
    #[arg(long, env = "DELIVERY_TIMEOUT_SECS", default_value_t = 30)]
    pub delivery_timeout_secs: u64,
    /// Accept stream steps that merge several transactions (see docs/failure-model.md).
    #[arg(long, env = "ALLOW_MERGED_TRANSACTIONS", default_value_t = false)]
    pub allow_merged_transactions: bool,
    /// Disable the fill worker (for deployments where fills are served elsewhere).
    #[arg(long, env = "DISABLE_FILLS", default_value_t = false)]
    pub disable_fills: bool,
    /// Fail the stream when nothing (not even a heartbeat) arrives for this long.
    #[arg(long, env = "STALL_TIMEOUT_SECS", default_value_t = 30)]
    pub stall_timeout_secs: u64,
    /// Fail the stream when heartbeats arrive but the position does not move while the source
    /// advances (vtgate retries purged-binlog errors silently). See docs/failure-model.md.
    #[arg(long, env = "PROGRESS_TIMEOUT_SECS", default_value_t = 120)]
    pub progress_timeout_secs: u64,
    /// Give up after this many consecutive retryable failures without position progress.
    #[arg(long, env = "MAX_CONSECUTIVE_FAILURES", default_value_t = 20)]
    pub max_consecutive_failures: u32,
    #[arg(long, env = "MAX_BATCH_TRANSACTIONS", default_value_t = 200)]
    pub max_batch_transactions: usize,
    #[arg(long, env = "MAX_INFLIGHT_TRANSACTIONS", default_value_t = 2000)]
    pub max_inflight_transactions: usize,
}

impl RunArgs {
    pub fn fill_timeout(&self) -> Duration {
        Duration::from_secs(self.fill_timeout_secs)
    }
}

#[derive(Subcommand, Debug)]
pub enum SchemaCommand {
    /// Read table metadata from the keyspace and write it as JSON (or a TypeScript module).
    Introspect {
        #[command(flatten)]
        vitess: VitessArgs,
        #[arg(long, env = "VITESS_KEYSPACE")]
        keyspace: String,
        /// Tables to include (repeatable). All tables when omitted.
        #[arg(long = "table")]
        tables: Vec<String>,
        /// Output path. `.ts` writes a TypeScript module exporting `introspected` as const.
        #[arg(long)]
        out: PathBuf,
    },
    /// Accept the schema currently served by the authenticated Worker for an existing state file.
    AcceptCurrent {
        #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
        state: PathBuf,
        #[arg(long, env = "WORKER_URL")]
        worker_url: String,
        #[arg(long, env = "WORKER_SECRET", hide_env_values = true)]
        worker_secret: String,
    },
    /// Validate a compiled sync schema artifact.
    Validate {
        #[arg(long)]
        schema: PathBuf,
    },
}

#[derive(Subcommand, Debug)]
pub enum CheckpointCommand {
    Show {
        #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
        state: PathBuf,
        #[arg(long, env = "SYNC_SCHEMA_PATH")]
        schema: PathBuf,
    },
    /// Reset the checkpoint and bump the epoch. `--to current` resumes at the server's current
    /// position (skipping history); `--to <position>` resumes at an explicit position.
    Reset {
        #[command(flatten)]
        vitess: VitessArgs,
        #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
        state: PathBuf,
        #[arg(long, env = "SYNC_SCHEMA_PATH")]
        schema: PathBuf,
        #[arg(long, default_value = "current")]
        to: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum QuarantineCommand {
    List {
        #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
        state: PathBuf,
        #[arg(long, env = "SYNC_SCHEMA_PATH")]
        schema: PathBuf,
    },
    /// Re-deliver quarantined transactions for a partition, then clear them on success.
    Replay {
        #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
        state: PathBuf,
        #[arg(long, env = "SYNC_SCHEMA_PATH")]
        schema: PathBuf,
        #[arg(long, env = "WORKER_URL")]
        worker_url: String,
        #[arg(long, env = "WORKER_SECRET", hide_env_values = true)]
        worker_secret: String,
        #[arg(long)]
        partition: String,
    },
    /// Drop quarantined transactions for a partition (data for that partition stays stale until
    /// the Durable Object is reset and refilled).
    Drop {
        #[arg(long, env = "STATE_PATH", default_value = "data/orbit-state.sqlite")]
        state: PathBuf,
        #[arg(long, env = "SYNC_SCHEMA_PATH")]
        schema: PathBuf,
        #[arg(long)]
        partition: String,
    },
}
