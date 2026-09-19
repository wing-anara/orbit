mod config;
mod fill_worker;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::Parser;
use config::{CheckpointCommand, Cli, Command, QuarantineCommand, RunArgs, SchemaCommand};
use orbit_distributor::{Distributor, DistributorConfig, HttpSink, Sink, StateStore, bootstrap_parent_index};
use orbit_protocol::cdc::{CdcBatch, CdcBatchAck};
use orbit_protocol::schema::SyncSchema;
use orbit_vstream::subscriber::current_position;
use orbit_vstream::{Checkpoint, SubscriberConfig};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

fn load_schema(path: &std::path::Path) -> anyhow::Result<SyncSchema> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading sync schema {}", path.display()))?;
    parse_schema(&text, &path.display().to_string())
}

fn parse_schema(text: &str, source: &str) -> anyhow::Result<SyncSchema> {
    let schema: SyncSchema = serde_json::from_str(text).context("parsing sync schema")?;
    if let Err(errors) = schema.validate() {
        for e in &errors {
            error!(error = %e, "invalid sync schema");
        }
        bail!("sync schema {} is invalid ({} problems)", source, errors.len());
    }
    Ok(schema)
}

/// The artifact the Worker was deployed with, from `GET {worker_url}/internal/schema`.
async fn fetch_schema(worker_url: &str, secret: &str) -> anyhow::Result<SyncSchema> {
    let url = format!("{}/internal/schema", worker_url.trim_end_matches('/'));
    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(secret)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .with_context(|| format!("fetching sync schema from {url}"))?;
    let status = response.status();
    let text = response.text().await.context("reading sync schema response")?;
    if !status.is_success() {
        bail!("fetching sync schema from {url}: http {status}");
    }
    parse_schema(&text, &url)
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,h2=warn,hyper=warn"));
    let json = std::env::var("LOG_FORMAT").map(|v| v == "json").unwrap_or(false);
    if json {
        tracing_subscriber::fmt().json().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => run(args).await,
        Command::Schema { command } => schema_command(command).await,
        Command::Checkpoint { command } => checkpoint_command(command).await,
        Command::Quarantine { command } => quarantine_command(command).await,
    }
}

fn subscriber_config(vitess: &config::VitessArgs, keyspace: &str, allow_merged: bool) -> SubscriberConfig {
    let mut cfg = SubscriberConfig::new(vitess.endpoint(), keyspace);
    cfg.cells = vitess.vitess_cells.clone();
    cfg.allow_merged_transactions = allow_merged;
    cfg
}

async fn run(args: RunArgs) -> anyhow::Result<()> {
    let schema = Arc::new(match &args.schema {
        Some(path) => load_schema(path)?,
        None => fetch_schema(&args.worker_url, &args.worker_secret).await?,
    });
    info!(app = %schema.app, keyspace = %schema.keyspace, schema_hash = %schema.schema_hash, tables = schema.tables.len(), "sync schema loaded");

    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(
            args.metrics_addr
                .parse::<std::net::SocketAddr>()
                .context("METRICS_ADDR")?,
        )
        .install()
        .context("installing prometheus exporter")?;
    info!(addr = %args.metrics_addr, "metrics endpoint listening");

    if let Some(parent) = args.state.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let state = StateStore::open(&args.state).context("opening state store")?;
    let sink: Arc<dyn Sink> = Arc::new(HttpSink::new(
        &args.worker_url,
        &args.worker_secret,
        Duration::from_secs(args.delivery_timeout_secs),
    )?);
    let dist_cfg = DistributorConfig {
        max_batch_transactions: args.max_batch_transactions,
        max_concurrent_deliveries: args.max_concurrent_deliveries.max(1),
        max_inflight_transactions: args.max_inflight_transactions,
        ..Default::default()
    };
    let mut sub_cfg = subscriber_config(&args.vitess, &schema.keyspace, args.allow_merged_transactions);
    sub_cfg.stall_timeout = Duration::from_secs(args.stall_timeout_secs.max(1));
    sub_cfg.progress_timeout = Duration::from_secs(args.progress_timeout_secs.max(1));
    sub_cfg.max_consecutive_failures = args.max_consecutive_failures;

    // Derived partitioning needs the parent index before the first child change is routed.
    state.load_or_init(&schema.schema_hash).context("loading state store")?;
    bootstrap_parent_index(&schema, &state, &sub_cfg)
        .await
        .context("bootstrapping parent index")?;

    orbit_distributor::fanout_bootstrap::bootstrap(&schema, &state, &sub_cfg)
        .await
        .context("bootstrapping relation routing")?;

    let distributor = Arc::new(
        Distributor::new((*schema).clone(), dist_cfg, state, sink)
            .context("initializing distributor")?
            .with_fanout_source(sub_cfg.clone()),
    );
    let checkpoint = distributor.checkpoint();
    info!(checkpoint = %checkpoint.render(), epoch = checkpoint.epoch, "resuming");

    let cancel = CancellationToken::new();
    let (tx, rx) = mpsc::channel(sub_cfg.channel_capacity);

    let subscriber = tokio::spawn(orbit_vstream::run(
        sub_cfg.clone(),
        (*schema).clone(),
        checkpoint,
        tx,
        cancel.clone(),
    ));
    let dist = {
        let d = distributor.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    let fills = if args.disable_fills {
        None
    } else {
        let worker = Arc::new(fill_worker::FillWorker {
            client: reqwest::Client::builder().build()?,
            worker_url: args.worker_url.clone(),
            secret: args.worker_secret.clone(),
            subscriber: sub_cfg,
            source_client: tokio::sync::OnceCell::new(),
            schema: schema.clone(),
            concurrency: args.fill_concurrency,
            timeout: args.fill_timeout(),
        });
        Some(tokio::spawn(worker.run(cancel.clone())))
    };

    let status_cancel = cancel.clone();
    let status_dist = distributor.clone();
    tokio::spawn(async move {
        while !status_cancel.is_cancelled() {
            tokio::time::sleep(Duration::from_secs(10)).await;
            let s = status_dist.status();
            metrics::gauge!("orbit_distributor_quarantined_partitions").set(s.quarantined_partitions.len() as f64);
            info!(checkpoint = %s.checkpoint.render(), inflight = s.inflight_transactions, queued = s.queued_partition_transactions, quarantined = s.quarantined_partitions.len(), "status");
        }
    });

    let result = tokio::select! {
        _ = tokio::signal::ctrl_c() => { info!("shutdown requested"); Ok(()) }
        r = subscriber => match r { Ok(Ok(())) => Ok(()), Ok(Err(e)) => Err(anyhow::anyhow!("subscriber: {e}")), Err(e) => Err(anyhow::anyhow!("subscriber task: {e}")) },
    };
    cancel.cancel();
    let _ = dist.await;
    if let Some(f) = fills {
        let _ = f.await;
    }
    result
}

async fn schema_command(command: SchemaCommand) -> anyhow::Result<()> {
    match command {
        SchemaCommand::Introspect {
            vitess,
            keyspace,
            tables,
            out,
        } => {
            let schema = orbit_vstream::execute::introspect(&vitess.endpoint(), &keyspace, &tables).await?;
            if let Some(parent) = out.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            if out.extension().map(|e| e == "ts").unwrap_or(false) {
                let json = serde_json::to_string_pretty(&schema)?;
                let ts = format!(
                    "// Generated by `orbit-server schema introspect`. Do not edit.\n// Source: keyspace {} ({})\n\nexport const introspected = {} as const\n",
                    schema.keyspace, schema.server_version, json
                );
                std::fs::write(&out, ts)?;
            } else {
                std::fs::write(&out, serde_json::to_string_pretty(&schema)? + "\n")?;
            }
            info!(path = %out.display(), tables = schema.tables.len(), "introspected schema written");
            Ok(())
        }
        SchemaCommand::AcceptCurrent {
            state,
            worker_url,
            worker_secret,
        } => {
            let schema = fetch_schema(&worker_url, &worker_secret).await?;
            let state = StateStore::open(&state).context("opening state store")?;
            state.migrate_schema(&schema.schema_hash).context("accepting schema")?;
            println!("accepted schema {}", schema.schema_hash);
            Ok(())
        }
        SchemaCommand::Validate { schema } => {
            let s = load_schema(&schema)?;
            println!("ok: {} tables, hash {}", s.tables.len(), s.schema_hash);
            Ok(())
        }
    }
}

async fn checkpoint_command(command: CheckpointCommand) -> anyhow::Result<()> {
    match command {
        CheckpointCommand::Show { state, schema } => {
            let schema = load_schema(&schema)?;
            let store = StateStore::open(&state)?;
            let st = store.load_or_init(&schema.schema_hash)?;
            println!("epoch: {}", st.checkpoint.epoch);
            println!("positions: {}", st.checkpoint.render());
            println!("partitions with counters: {}", st.counters.len());
            println!("quarantined partitions: {:?}", store.quarantined_partitions()?);
            Ok(())
        }
        CheckpointCommand::Reset {
            vitess,
            state,
            schema,
            to,
        } => {
            let schema = load_schema(&schema)?;
            let store = StateStore::open(&state)?;
            let _ = store.load_or_init(&schema.schema_hash)?;
            let target = if to == "current" {
                let cfg = subscriber_config(&vitess, &schema.keyspace, false);
                current_position(&cfg, &schema).await?
            } else {
                let mut cp = Checkpoint::empty(0);
                for part in to.split('|') {
                    let (shard, pos) = part.split_once('@').context("expected keyspace/shard@position")?;
                    let (keyspace, shard) = shard.split_once('/').context("expected keyspace/shard")?;
                    cp.set(
                        orbit_vstream::ShardId {
                            keyspace: keyspace.into(),
                            shard: shard.into(),
                        },
                        pos.into(),
                    );
                }
                cp
            };
            let epoch = store.reset_checkpoint(&target)?;
            println!("checkpoint reset to {} (epoch {epoch})", target.render());
            Ok(())
        }
    }
}

async fn quarantine_command(command: QuarantineCommand) -> anyhow::Result<()> {
    match command {
        QuarantineCommand::List { state, schema } => {
            let schema = load_schema(&schema)?;
            let store = StateStore::open(&state)?;
            let _ = store.load_or_init(&schema.schema_hash)?;
            for (partition, tx, reason) in store.quarantined()? {
                println!(
                    "{partition}\tseq={}\tgtid={}\treason={reason}\tchanges={}",
                    tx.seq,
                    tx.gtid,
                    tx.changes.len()
                );
            }
            Ok(())
        }
        QuarantineCommand::Replay {
            state,
            schema,
            worker_url,
            worker_secret,
            partition,
        } => {
            let schema = load_schema(&schema)?;
            let store = StateStore::open(&state)?;
            let st = store.load_or_init(&schema.schema_hash)?;
            let sink = HttpSink::new(&worker_url, &worker_secret, Duration::from_secs(60))?;
            let txns: Vec<_> = store
                .quarantined()?
                .into_iter()
                .filter(|(p, _, _)| p == &partition)
                .map(|(_, t, _)| t)
                .collect();
            if txns.is_empty() {
                bail!("no quarantined transactions for partition {partition}");
            }
            let batch = CdcBatch {
                protocol_version: orbit_protocol::INTERNAL_PROTOCOL_VERSION,
                schema_hash: schema.schema_hash.clone(),
                stream_epoch: st.checkpoint.epoch,
                partition: partition.clone(),
                transactions: txns,
                delivery_id: format!("replay-{}", uuid_like()),
            };
            match sink.deliver(&batch).await? {
                CdcBatchAck::Applied {
                    applied_seq,
                    duplicates,
                    ..
                } => {
                    let n = store.clear_quarantine(&partition)?;
                    println!(
                        "replayed {n} transactions (applied_seq={applied_seq}, duplicates={duplicates}); partition {partition} is no longer quarantined after restart"
                    );
                    Ok(())
                }
                CdcBatchAck::Rejected { reason } => bail!("replay rejected: {reason:?}"),
            }
        }
        QuarantineCommand::Drop {
            state,
            schema,
            partition,
        } => {
            let schema = load_schema(&schema)?;
            let store = StateStore::open(&state)?;
            let _ = store.load_or_init(&schema.schema_hash)?;
            let n = store.clear_quarantine(&partition)?;
            println!("dropped {n} quarantined transactions for {partition}");
            Ok(())
        }
    }
}

fn uuid_like() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}
