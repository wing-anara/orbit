//! Distributor behaviour under failures, using a fake Durable Object that implements the same
//! sequence rules as the real one (dedupe on `seq <= applied`, reject gaps).

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indexmap::IndexMap;
use orbit_distributor::delivery::{DeliveryError, Sink};
use orbit_distributor::{Distributor, DistributorConfig, StateStore};
use orbit_protocol::cdc::{CdcBatch, CdcBatchAck, RejectReason, RowChange, RowOp, SourceTransaction, TraceContext};
use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig, SyncSchema, TableSchema, ValueKind};
use orbit_protocol::value::CellValue;
use orbit_vstream::StreamItem;
use orbit_vstream::checkpoint::ShardId;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const U1: &str = "a2523813-adbe-11f1-b19c-0a2250a7ed6c";

fn schema() -> SyncSchema {
    let mut s = SyncSchema {
        format_version: 1,
        schema_hash: String::new(),
        app: "t".into(),
        keyspace: "ks".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![TableSchema {
            name: "t".into(),
            primary_key: vec!["id".into()],
            partition_column: "org".into(),
            partition_parent: None,
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(1)".into(),
                    enum_values: None,
                },
                ColumnSchema {
                    name: "org".into(),
                    kind: ValueKind::String,
                    nullable: true,
                    source_type: "varchar(1)".into(),
                    enum_values: None,
                },
                ColumnSchema {
                    name: "n".into(),
                    kind: ValueKind::Int,
                    nullable: true,
                    source_type: "int".into(),
                    enum_values: None,
                },
            ],
            relations: vec![],
        }],
    };
    s.schema_hash = s.compute_hash();
    s
}

fn row(id: &str, org: Option<&str>, n: i64) -> orbit_protocol::value::Row {
    let mut r = IndexMap::new();
    r.insert("id".into(), CellValue::from(id));
    r.insert("org".into(), org.map(CellValue::from).unwrap_or(CellValue::Null));
    r.insert("n".into(), CellValue::from(n));
    r
}

fn insert(id: &str, org: &str, n: i64) -> RowChange {
    RowChange {
        table: "t".into(),
        op: RowOp::Insert,
        key: vec![id.into()],
        before: None,
        after: Some(row(id, Some(org), n)),
    }
}

fn txn(gno: u64, changes: Vec<RowChange>) -> StreamItem {
    StreamItem::Transaction(SourceTransaction {
        keyspace: "ks".into(),
        shard: "0".into(),
        gtid: format!("{U1}:{gno}"),
        position: format!("MySQL56/{U1}:1-{gno}"),
        commit_timestamp: 1_700_000_000 + gno as i64,
        changes,
        trace: TraceContext::default(),
    })
}

#[derive(Default)]
struct DoState {
    applied_seq: u64,
    gtids: BTreeMap<u64, String>,
    rows: Vec<(u64, String)>, // (seq, row id)
    duplicates: u64,
}

#[derive(Default)]
struct FailurePlan {
    /// Number of upcoming deliveries that fail with a transport error before reaching the DO.
    fail_before: u32,
    /// Number of upcoming deliveries that are applied but whose ack is lost.
    lose_ack_after: u32,
    /// Partitions whose batches are rejected with the given reason.
    reject: HashMap<String, RejectReason>,
    /// Artificial latency per delivery.
    latency: Duration,
}

#[derive(Default)]
struct FakeDo {
    partitions: Mutex<HashMap<String, DoState>>,
    plan: Mutex<FailurePlan>,
    deliveries: Mutex<u64>,
}

impl FakeDo {
    fn apply(&self, batch: &CdcBatch) -> CdcBatchAck {
        let mut parts = self.partitions.lock().unwrap();
        let st = parts.entry(batch.partition.clone()).or_default();
        let mut duplicates = 0;
        for t in &batch.transactions {
            if st.applied_seq != 0 && t.seq <= st.applied_seq {
                if let Some(g) = st.gtids.get(&t.seq)
                    && g != &t.gtid
                {
                    return CdcBatchAck::Rejected {
                        reason: RejectReason::SequenceConflict {
                            seq: t.seq,
                            applied_gtid: g.clone(),
                            got_gtid: t.gtid.clone(),
                        },
                    };
                }
                duplicates += 1;
                st.duplicates += 1;
                continue;
            }
            if st.applied_seq != 0 && t.seq != st.applied_seq + 1 {
                return CdcBatchAck::Rejected {
                    reason: RejectReason::SequenceGap {
                        applied_seq: st.applied_seq,
                        first_seq: t.seq,
                    },
                };
            }
            for c in &t.changes {
                st.rows.push((t.seq, c.key[0].as_str().unwrap().to_string()));
            }
            st.applied_seq = t.seq;
            st.gtids.insert(t.seq, t.gtid.clone());
        }
        CdcBatchAck::Applied {
            applied_seq: st.applied_seq,
            duplicates,
            apply_ms: 1,
        }
    }

    fn rows(&self, partition: &str) -> Vec<String> {
        self.partitions
            .lock()
            .unwrap()
            .get(partition)
            .map(|s| s.rows.iter().map(|(_, id)| id.clone()).collect())
            .unwrap_or_default()
    }

    fn seqs(&self, partition: &str) -> Vec<u64> {
        self.partitions
            .lock()
            .unwrap()
            .get(partition)
            .map(|s| s.rows.iter().map(|(seq, _)| *seq).collect())
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl Sink for FakeDo {
    async fn deliver(&self, batch: &CdcBatch) -> Result<CdcBatchAck, DeliveryError> {
        *self.deliveries.lock().unwrap() += 1;
        let (latency, action) = {
            let mut plan = self.plan.lock().unwrap();
            let action = if plan.fail_before > 0 {
                plan.fail_before -= 1;
                "fail_before"
            } else if let Some(r) = plan.reject.get(&batch.partition) {
                return Ok(CdcBatchAck::Rejected { reason: r.clone() });
            } else if plan.lose_ack_after > 0 {
                plan.lose_ack_after -= 1;
                "lose_ack"
            } else {
                "ok"
            };
            (plan.latency, action)
        };
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        match action {
            "fail_before" => Err(DeliveryError::Transport("injected".into())),
            "lose_ack" => {
                let _ = self.apply(batch);
                Err(DeliveryError::Transport("ack lost".into()))
            }
            _ => Ok(self.apply(batch)),
        }
    }
}

fn config() -> DistributorConfig {
    DistributorConfig {
        retry_backoff_min: Duration::from_millis(5),
        retry_backoff_max: Duration::from_millis(20),
        checkpoint_interval: Duration::from_millis(20),
        max_reject_attempts: 3,
        ..Default::default()
    }
}

async fn wait_until(timeout: Duration, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn shard0() -> ShardId {
    ShardId {
        keyspace: "ks".into(),
        shard: "0".into(),
    }
}

#[tokio::test]
async fn routes_orders_and_checkpoints() {
    let fake = Arc::new(FakeDo::default());
    let d = Distributor::new(schema(), config(), StateStore::open_in_memory().unwrap(), fake.clone()).unwrap();
    let (tx, rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let run = {
        let cancel = cancel.clone();
        let d = Arc::new(d);
        let d2 = d.clone();
        (d, tokio::spawn(async move { d2.run(rx, cancel).await }))
    };
    tx.send(StreamItem::Position {
        shard: shard0(),
        position: format!("MySQL56/{U1}:1-9"),
    })
    .await
    .unwrap();
    for g in 10..=14u64 {
        let changes = vec![
            insert(&format!("a{g}"), "A", 1),
            insert(&format!("b{g}"), "B", 1),
            insert(&format!("x{g}"), "A", 2),
        ];
        tx.send(txn(g, changes)).await.unwrap();
    }
    // A transaction with only unpartitioned rows still advances the checkpoint.
    tx.send(txn(
        15,
        vec![RowChange {
            table: "t".into(),
            op: RowOp::Insert,
            key: vec!["n".into()],
            before: None,
            after: Some(row("n", None, 0)),
        }],
    ))
    .await
    .unwrap();
    tx.send(StreamItem::Heartbeat).await.unwrap();
    wait_until(Duration::from_secs(5), || {
        run.0.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-15").as_str())
    })
    .await;
    assert_eq!(
        fake.rows("A"),
        (10..=14)
            .flat_map(|g| [format!("a{g}"), format!("x{g}")])
            .collect::<Vec<_>>()
    );
    assert_eq!(fake.rows("B"), (10..=14).map(|g| format!("b{g}")).collect::<Vec<_>>());
    assert_eq!(fake.seqs("A"), vec![1, 1, 2, 2, 3, 3, 4, 4, 5, 5]);
    assert_eq!(run.0.status().inflight_transactions, 0);
    cancel.cancel();
    let _ = run.1.await;
}

#[tokio::test]
async fn transport_failures_and_lost_acks_do_not_duplicate_or_reorder() {
    let fake = Arc::new(FakeDo::default());
    {
        let mut plan = fake.plan.lock().unwrap();
        plan.fail_before = 4;
        plan.lose_ack_after = 3;
    }
    let d =
        Arc::new(Distributor::new(schema(), config(), StateStore::open_in_memory().unwrap(), fake.clone()).unwrap());
    let (tx, rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    tx.send(StreamItem::Position {
        shard: shard0(),
        position: format!("MySQL56/{U1}:1-1"),
    })
    .await
    .unwrap();
    for g in 2..=30u64 {
        tx.send(txn(
            g,
            vec![insert(&format!("r{g}"), if g % 2 == 0 { "A" } else { "B" }, 0)],
        ))
        .await
        .unwrap();
    }
    wait_until(Duration::from_secs(10), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-30").as_str())
    })
    .await;
    assert_eq!(
        fake.rows("A"),
        (2..=30)
            .filter(|g| g % 2 == 0)
            .map(|g| format!("r{g}"))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        fake.rows("B"),
        (2..=30)
            .filter(|g| g % 2 == 1)
            .map(|g| format!("r{g}"))
            .collect::<Vec<_>>()
    );
    let dup: u64 = fake.partitions.lock().unwrap().values().map(|s| s.duplicates).sum();
    assert!(
        dup > 0,
        "lost acks must have produced duplicate deliveries that the DO skipped"
    );
    assert!(*fake.deliveries.lock().unwrap() > 7);
    cancel.cancel();
    let _ = h.await;
}

#[tokio::test]
async fn restart_replays_from_checkpoint_with_identical_sequences() {
    let dir = std::env::temp_dir().join(format!("orbit-dist-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.sqlite");
    let _ = std::fs::remove_file(&path);
    let fake = Arc::new(FakeDo::default());
    fake.plan.lock().unwrap().latency = Duration::from_millis(15);
    let mut cfg = config();
    cfg.max_batch_transactions = 1;

    // First run: feed 1..=20 but cancel while deliveries are in flight.
    let d = Arc::new(Distributor::new(schema(), cfg.clone(), StateStore::open(&path).unwrap(), fake.clone()).unwrap());
    let (tx, rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    tx.send(StreamItem::Position {
        shard: shard0(),
        position: format!("MySQL56/{U1}:1-0"),
    })
    .await
    .unwrap();
    for g in 1..=20u64 {
        tx.send(txn(
            g,
            vec![insert(&format!("r{g}"), "A", 0), insert(&format!("s{g}"), "B", 0)],
        ))
        .await
        .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    cancel.cancel();
    let _ = h.await;
    let cp = d.checkpoint();
    let persisted = StateStore::open(&path)
        .unwrap()
        .load_or_init(&schema().schema_hash)
        .unwrap();
    assert_eq!(
        persisted.checkpoint, cp,
        "clean shutdown persists the in-memory checkpoint"
    );
    let resumed_gno: u64 = cp
        .position(&shard0())
        .map(|p| p.rsplit('-').next().unwrap().parse().unwrap())
        .unwrap_or(0);
    assert!(
        resumed_gno < 20,
        "the test should stop before everything was acked (got {resumed_gno})"
    );
    drop(d);

    // Second run from the persisted state, replaying the stream from the checkpoint. The fake DO
    // keeps its state, like a real Durable Object would.
    let d = Arc::new(Distributor::new(schema(), cfg, StateStore::open(&path).unwrap(), fake.clone()).unwrap());
    assert_eq!(d.checkpoint(), cp);
    let (tx, rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    for g in (resumed_gno + 1)..=20u64 {
        tx.send(txn(
            g,
            vec![insert(&format!("r{g}"), "A", 0), insert(&format!("s{g}"), "B", 0)],
        ))
        .await
        .unwrap();
    }
    wait_until(Duration::from_secs(10), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-20").as_str())
    })
    .await;
    assert_eq!(
        fake.rows("A"),
        (1..=20).map(|g| format!("r{g}")).collect::<Vec<_>>(),
        "no gaps, no double application"
    );
    assert_eq!(
        fake.seqs("A"),
        (1..=20).collect::<Vec<_>>(),
        "sequence numbers are re-derived identically"
    );
    cancel.cancel();
    let _ = h.await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn poison_partition_is_quarantined_and_others_continue() {
    let fake = Arc::new(FakeDo::default());
    fake.plan.lock().unwrap().reject.insert(
        "BAD".into(),
        RejectReason::InvalidRow {
            table: "t".into(),
            seq: 1,
            message: "boom".into(),
        },
    );
    let store = StateStore::open_in_memory().unwrap();
    let d = Arc::new(Distributor::new(schema(), config(), store, fake.clone()).unwrap());
    let (tx, rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    tx.send(StreamItem::Position {
        shard: shard0(),
        position: format!("MySQL56/{U1}:1-0"),
    })
    .await
    .unwrap();
    for g in 1..=6u64 {
        tx.send(txn(
            g,
            vec![insert(&format!("g{g}"), "GOOD", 0), insert(&format!("b{g}"), "BAD", 0)],
        ))
        .await
        .unwrap();
    }
    wait_until(Duration::from_secs(10), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-6").as_str())
    })
    .await;
    let status = d.status();
    assert_eq!(status.quarantined_partitions, vec!["BAD".to_string()]);
    assert_eq!(fake.rows("GOOD").len(), 6);
    assert!(fake.rows("BAD").is_empty());
    // Later transactions for the quarantined partition go straight to quarantine, and the
    // checkpoint still advances.
    tx.send(txn(7, vec![insert("b7", "BAD", 0)])).await.unwrap();
    wait_until(Duration::from_secs(5), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-7").as_str())
    })
    .await;
    cancel.cancel();
    let _ = h.await;
}

#[tokio::test]
async fn schema_mismatch_rejections_are_retried_not_quarantined() {
    let fake = Arc::new(FakeDo::default());
    fake.plan.lock().unwrap().reject.insert(
        "A".into(),
        RejectReason::SchemaMismatch {
            do_schema_hash: "old".into(),
            got: "new".into(),
        },
    );
    let mut cfg = config();
    cfg.max_reject_attempts = 1000;
    let d = Arc::new(Distributor::new(schema(), cfg, StateStore::open_in_memory().unwrap(), fake.clone()).unwrap());
    let (tx, rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    tx.send(StreamItem::Position {
        shard: shard0(),
        position: format!("MySQL56/{U1}:1-0"),
    })
    .await
    .unwrap();
    tx.send(txn(1, vec![insert("a1", "A", 0)])).await.unwrap();
    wait_until(Duration::from_secs(5), || *fake.deliveries.lock().unwrap() >= 3).await;
    assert_eq!(
        d.status().checkpoint.position(&shard0()),
        Some(format!("MySQL56/{U1}:1-0").as_str()),
        "checkpoint must not pass an unacked transaction"
    );
    // The DO gets redeployed with the right schema: delivery succeeds.
    fake.plan.lock().unwrap().reject.clear();
    wait_until(Duration::from_secs(5), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-1").as_str())
    })
    .await;
    assert_eq!(fake.rows("A"), vec!["a1"]);
    cancel.cancel();
    let _ = h.await;
}

#[tokio::test]
async fn backpressure_pauses_consumption() {
    let fake = Arc::new(FakeDo::default());
    fake.plan.lock().unwrap().latency = Duration::from_millis(200);
    let mut cfg = config();
    cfg.max_inflight_transactions = 3;
    let d = Arc::new(Distributor::new(schema(), cfg, StateStore::open_in_memory().unwrap(), fake.clone()).unwrap());
    let (tx, rx) = mpsc::channel(2);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    for g in 1..=12u64 {
        tx.send(txn(g, vec![insert(&format!("r{g}"), "A", 0)])).await.unwrap();
    }
    // Producer would have blocked: 3 in flight + 2 in the channel; sends beyond that wait.
    let inflight = d.status().inflight_transactions;
    assert!(inflight <= 3, "in-flight bounded, got {inflight}");
    wait_until(Duration::from_secs(20), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-12").as_str())
    })
    .await;
    assert_eq!(fake.rows("A").len(), 12);
    cancel.cancel();
    let _ = h.await;
}

#[tokio::test]
async fn randomized_failures_preserve_exactly_once_order() {
    use rand::{RngExt, SeedableRng};
    for seed in 0..6u64 {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let fake = Arc::new(FakeDo::default());
        {
            let mut plan = fake.plan.lock().unwrap();
            plan.fail_before = rng.random_range(0..5);
            plan.lose_ack_after = rng.random_range(0..5);
        }
        let mut cfg = config();
        cfg.max_batch_transactions = rng.random_range(1..8);
        cfg.max_inflight_transactions = rng.random_range(2..50);
        let d = Arc::new(Distributor::new(schema(), cfg, StateStore::open_in_memory().unwrap(), fake.clone()).unwrap());
        let (tx, rx) = mpsc::channel(8);
        let cancel = CancellationToken::new();
        let h = {
            let d = d.clone();
            let c = cancel.clone();
            tokio::spawn(async move { d.run(rx, c).await })
        };
        tx.send(StreamItem::Position {
            shard: shard0(),
            position: format!("MySQL56/{U1}:1-0"),
        })
        .await
        .unwrap();
        let n = 60u64;
        let mut expected: HashMap<String, Vec<String>> = HashMap::new();
        for g in 1..=n {
            let mut changes = vec![];
            for j in 0..rng.random_range(0..4) {
                let p = ["A", "B", "C"][rng.random_range(0..3)];
                let id = format!("r{g}_{j}");
                expected.entry(p.into()).or_default().push(id.clone());
                changes.push(insert(&id, p, 0));
            }
            tx.send(txn(g, changes)).await.unwrap();
            if rng.random_bool(0.1) {
                fake.plan.lock().unwrap().fail_before += 1;
            }
            if rng.random_bool(0.1) {
                fake.plan.lock().unwrap().lose_ack_after += 1;
            }
        }
        wait_until(Duration::from_secs(20), || {
            d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-{n}").as_str())
        })
        .await;
        for (p, rows) in expected {
            assert_eq!(fake.rows(&p), rows, "seed {seed} partition {p}");
            let seqs = fake.seqs(&p);
            let mut dedup = seqs.clone();
            dedup.dedup();
            assert!(
                dedup.windows(2).all(|w| w[1] == w[0] + 1),
                "seed {seed}: seqs dense and increasing: {seqs:?}"
            );
        }
        cancel.cancel();
        let _ = h.await;
    }
}

// ---------------------------------------------------------------------------------------------
// Derived partitioning: a child table `c(id, pid)` partitioned through its parent `p(id, org)`.
// ---------------------------------------------------------------------------------------------

fn derived_schema() -> SyncSchema {
    let col = |name: &str, kind: ValueKind, nullable: bool| ColumnSchema {
        name: name.into(),
        kind,
        nullable,
        source_type: "varchar(8)".into(),
        enum_values: None,
    };
    let mut s = SyncSchema {
        format_version: 1,
        schema_hash: String::new(),
        app: "t".into(),
        keyspace: "ks".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![
            TableSchema {
                name: "p".into(),
                primary_key: vec!["id".into()],
                partition_column: "org".into(),
                partition_parent: None,
                columns: vec![col("id", ValueKind::String, false), col("org", ValueKind::String, true)],
                relations: vec![],
            },
            TableSchema {
                name: "c".into(),
                primary_key: vec!["id".into()],
                partition_column: "pid".into(),
                partition_parent: Some("p".into()),
                columns: vec![col("id", ValueKind::String, false), col("pid", ValueKind::String, true)],
                relations: vec![],
            },
        ],
    };
    s.schema_hash = s.compute_hash();
    s.validate().unwrap();
    s
}

fn parent_row(id: &str, org: Option<&str>) -> orbit_protocol::value::Row {
    let mut r = IndexMap::new();
    r.insert("id".into(), CellValue::from(id));
    r.insert("org".into(), org.map(CellValue::from).unwrap_or(CellValue::Null));
    r
}

fn child_row(id: &str, pid: &str) -> orbit_protocol::value::Row {
    let mut r = IndexMap::new();
    r.insert("id".into(), CellValue::from(id));
    r.insert("pid".into(), CellValue::from(pid));
    r
}

fn parent_insert(id: &str, org: &str) -> RowChange {
    RowChange {
        table: "p".into(),
        op: RowOp::Insert,
        key: vec![id.into()],
        before: None,
        after: Some(parent_row(id, Some(org))),
    }
}

fn parent_move(id: &str, from: &str, to: &str) -> RowChange {
    RowChange {
        table: "p".into(),
        op: RowOp::Update,
        key: vec![id.into()],
        before: Some(parent_row(id, Some(from))),
        after: Some(parent_row(id, Some(to))),
    }
}

fn child_insert(id: &str, pid: &str) -> RowChange {
    RowChange {
        table: "c".into(),
        op: RowOp::Insert,
        key: vec![id.into()],
        before: None,
        after: Some(child_row(id, pid)),
    }
}

fn child_move(id: &str, from: &str, to: &str) -> RowChange {
    RowChange {
        table: "c".into(),
        op: RowOp::Update,
        key: vec![id.into()],
        before: Some(child_row(id, from)),
        after: Some(child_row(id, to)),
    }
}

#[tokio::test]
async fn derived_tables_route_through_the_parent_index() {
    let dir = std::env::temp_dir().join(format!("orbit-dist-derived-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.sqlite");
    let _ = std::fs::remove_file(&path);
    let fake = Arc::new(FakeDo::default());

    // Bootstrap index: a parent that existed before the stream started.
    {
        let store = StateStore::open(&path).unwrap();
        store.load_or_init(&derived_schema().schema_hash).unwrap();
        store.bulk_upsert_parents("p", [("p0", "A")]).unwrap();
        store
            .mark_parent_index_ready("p", &format!("MySQL56/{U1}:1-0"))
            .unwrap();
    }
    let d = Arc::new(
        Distributor::new(
            derived_schema(),
            config(),
            StateStore::open(&path).unwrap(),
            fake.clone(),
        )
        .unwrap(),
    );
    let (tx, rx) = mpsc::channel(16);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    tx.send(StreamItem::Position {
        shard: shard0(),
        position: format!("MySQL56/{U1}:1-0"),
    })
    .await
    .unwrap();
    // 1: parent and child in one transaction; the child resolves through the parent.
    tx.send(txn(1, vec![parent_insert("p1", "A"), child_insert("c1", "p1")]))
        .await
        .unwrap();
    // 2: a child of the bootstrapped parent.
    tx.send(txn(2, vec![child_insert("c0", "p0")])).await.unwrap();
    // 3: a parent in another partition.
    tx.send(txn(3, vec![parent_insert("p2", "B")])).await.unwrap();
    // 4: the child moves to p2: delete in A, insert in B.
    tx.send(txn(4, vec![child_move("c1", "p1", "p2")])).await.unwrap();
    // 5: unknown parent: skipped, the checkpoint still advances.
    tx.send(txn(5, vec![child_insert("c9", "nope")])).await.unwrap();
    // 6: the parent p1 moves to B; 7: a later child of p1 follows it.
    tx.send(txn(6, vec![parent_move("p1", "A", "B")])).await.unwrap();
    tx.send(txn(7, vec![child_insert("c7", "p1")])).await.unwrap();
    wait_until(Duration::from_secs(5), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-7").as_str())
    })
    .await;
    assert_eq!(fake.rows("A"), vec!["p1", "c1", "c0", "c1", "p1"]);
    assert_eq!(fake.seqs("A"), vec![1, 1, 2, 3, 4]);
    assert_eq!(fake.rows("B"), vec!["p2", "c1", "p1", "c7"]);
    assert_eq!(fake.seqs("B"), vec![1, 2, 3, 4]);
    assert!(fake.partitions.lock().unwrap().get("nope").is_none());
    assert_eq!(d.status().quarantined_partitions, Vec::<String>::new());
    cancel.cancel();
    let _ = h.await;
    let store = StateStore::open(&path).unwrap();
    assert_eq!(store.parent_partition("p", "p0").unwrap().as_deref(), Some("A"));
    assert_eq!(store.parent_partition("p", "p1").unwrap().as_deref(), Some("B"));
    assert_eq!(store.parent_partition("p", "p2").unwrap().as_deref(), Some("B"));
    assert_eq!(
        store.parent_index_ready("p").unwrap().as_deref(),
        Some(format!("MySQL56/{U1}:1-0").as_str())
    );
    drop(store);
    drop(d);

    // Restart replay: feed 8..=20 slowly, stop while deliveries are in flight, then resume from
    // the persisted checkpoint. The persisted index must be exactly the index as of the
    // checkpoint, and the replay must re-derive identical (partition, seq) assignments.
    fake.plan.lock().unwrap().latency = Duration::from_millis(15);
    let mut cfg = config();
    cfg.max_batch_transactions = 1;
    let changes_for = |g: u64| {
        let org = if g.is_multiple_of(2) { "A" } else { "B" };
        vec![
            parent_insert(&format!("p{g}"), org),
            child_insert(&format!("c{g}"), &format!("p{g}")),
            // References the previous parent, which lives in the other partition (p7 is unknown).
            child_insert(&format!("d{g}"), &format!("p{}", g - 1)),
        ]
    };
    let d = Arc::new(
        Distributor::new(
            derived_schema(),
            cfg.clone(),
            StateStore::open(&path).unwrap(),
            fake.clone(),
        )
        .unwrap(),
    );
    let (tx, rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    for g in 8..=20u64 {
        tx.send(txn(g, changes_for(g))).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(60)).await;
    cancel.cancel();
    let _ = h.await;
    let cp = d.checkpoint();
    let resumed_gno: u64 = cp
        .position(&shard0())
        .map(|p| p.rsplit('-').next().unwrap().parse().unwrap())
        .unwrap();
    assert!(
        (7..20).contains(&resumed_gno),
        "the test should stop before everything was acked (got {resumed_gno})"
    );
    drop(d);
    {
        let store = StateStore::open(&path).unwrap();
        for g in 8..=20u64 {
            let persisted = store.parent_partition("p", &format!("p{g}")).unwrap();
            assert_eq!(
                persisted.is_some(),
                g <= resumed_gno,
                "index entry for p{g} persisted iff covered by the checkpoint {resumed_gno}"
            );
        }
    }
    let d = Arc::new(Distributor::new(derived_schema(), cfg, StateStore::open(&path).unwrap(), fake.clone()).unwrap());
    assert_eq!(d.checkpoint(), cp);
    let (tx, rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let h = {
        let d = d.clone();
        let c = cancel.clone();
        tokio::spawn(async move { d.run(rx, c).await })
    };
    for g in (resumed_gno + 1)..=20u64 {
        tx.send(txn(g, changes_for(g))).await.unwrap();
    }
    wait_until(Duration::from_secs(10), || {
        d.status().checkpoint.position(&shard0()) == Some(format!("MySQL56/{U1}:1-20").as_str())
    })
    .await;
    assert_eq!(
        d.status().quarantined_partitions,
        Vec::<String>::new(),
        "no sequence conflicts on replay"
    );
    let mut expected_a = vec!["p1", "c1", "c0", "c1", "p1"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    let mut expected_b = vec!["p2", "c1", "p1", "c7"]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();
    for g in 8..=20u64 {
        let (own, other) = if g.is_multiple_of(2) {
            (&mut expected_a, &mut expected_b)
        } else {
            (&mut expected_b, &mut expected_a)
        };
        own.push(format!("p{g}"));
        own.push(format!("c{g}"));
        if g > 8 {
            other.push(format!("d{g}"));
        }
    }
    assert_eq!(fake.rows("A"), expected_a, "no gaps, no double application");
    assert_eq!(fake.rows("B"), expected_b, "no gaps, no double application");
    for p in ["A", "B"] {
        let seqs = fake.seqs(p);
        let mut dedup = seqs.clone();
        dedup.dedup();
        assert!(
            dedup.windows(2).all(|w| w[1] == w[0] + 1),
            "partition {p}: seqs dense and increasing: {seqs:?}"
        );
    }
    cancel.cancel();
    let _ = h.await;
    let store = StateStore::open(&path).unwrap();
    for g in 8..=20u64 {
        let org = if g.is_multiple_of(2) { "A" } else { "B" };
        assert_eq!(
            store.parent_partition("p", &format!("p{g}")).unwrap().as_deref(),
            Some(org)
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
