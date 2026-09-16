//! Integration tests against a real Vitess (the local `vttestserver` from `infra/vitess`).
//!
//! Run with `ORBIT_TEST_VITESS=1`. Skipped otherwise so unit test runs stay hermetic.

use std::collections::HashMap;
use std::time::Duration;

use orbit_protocol::cdc::RowOp;
use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig, SyncSchema, TableSchema, ValueKind};
use orbit_vstream::fill::run_fill;
use orbit_vstream::subscriber::{SubscriberConfig, current_position};
use orbit_vstream::{Checkpoint, StreamItem, VitessEndpoint};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod support;
use support::{TestDb, mysql};

fn enabled() -> bool {
    support::init_tracing();
    std::env::var("ORBIT_TEST_VITESS").is_ok()
}

fn schema() -> SyncSchema {
    let mut s = SyncSchema {
        format_version: 1,
        schema_hash: String::new(),
        app: "vstream-test".into(),
        keyspace: "orbit".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![TableSchema {
            name: "_orbit_it".into(),
            primary_key: vec!["id".into()],
            partition_column: "org".into(),
            partition_parent: None,
            partition_routes: vec![],
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(64)".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "org".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(64)".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "n".into(),
                    kind: ValueKind::Int,
                    nullable: true,
                    source_type: "int".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "flag".into(),
                    kind: ValueKind::Bool,
                    nullable: false,
                    source_type: "tinyint(1)".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "doc".into(),
                    kind: ValueKind::Json,
                    nullable: true,
                    source_type: "json".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "created".into(),
                    kind: ValueKind::DateTime,
                    nullable: true,
                    source_type: "datetime(3)".into(),
                    enum_values: None,
                    derived: None,
                },
            ],
            relations: vec![],
        }],
    };
    s.schema_hash = s.compute_hash();
    s
}

fn config() -> SubscriberConfig {
    let mut c = SubscriberConfig::new(VitessEndpoint::new(support::grpc_uri()), "orbit");
    c.heartbeat_interval = Duration::from_secs(1);
    c.stall_timeout = Duration::from_secs(10);
    c
}

#[tokio::test]
async fn live_stream_delivers_insert_update_delete_with_transaction_boundaries() {
    if !enabled() {
        return;
    }
    let db = TestDb::new("_orbit_it").await;
    let schema = schema();
    let start = current_position(&config(), &schema).await.unwrap();
    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(orbit_vstream::run(
        config(),
        schema.clone(),
        start.clone(),
        tx,
        cancel.clone(),
    ));

    // Wait for the first heartbeat so we know the stream is live before writing.
    support::wait_for(&mut rx, Duration::from_secs(20), |i| matches!(i, StreamItem::Heartbeat)).await;

    mysql(&[
        "START TRANSACTION",
        "INSERT INTO _orbit_it (id, org, n, flag, doc, created) VALUES ('a','org1',1,1,'{\"k\":[1,2]}','2026-01-02 03:04:05.678'),('b','org2',NULL,0,NULL,NULL)",
        "UPDATE _orbit_it SET n = 2 WHERE id = 'a'",
        "DELETE FROM _orbit_it WHERE id = 'b'",
        "COMMIT",
    ])
    .await;

    let tx = support::wait_for(
        &mut rx,
        Duration::from_secs(20),
        |i| matches!(i, StreamItem::Transaction(t) if !t.changes.is_empty()),
    )
    .await;
    let StreamItem::Transaction(t) = tx else { unreachable!() };
    assert_eq!(t.changes.len(), 4, "all four row changes arrive in one transaction");
    assert_eq!(t.changes[0].op, RowOp::Insert);
    assert_eq!(
        t.changes[0].after.as_ref().unwrap()["doc"],
        serde_json::json!({"k": [1, 2]})
    );
    assert_eq!(t.changes[0].after.as_ref().unwrap()["flag"], true);
    assert_eq!(
        t.changes[0].after.as_ref().unwrap()["created"],
        "2026-01-02 03:04:05.678"
    );
    assert_eq!(t.changes[1].op, RowOp::Insert);
    assert_eq!(t.changes[1].after.as_ref().unwrap()["n"], serde_json::Value::Null);
    assert_eq!(t.changes[2].op, RowOp::Update);
    assert_eq!(t.changes[2].before.as_ref().unwrap()["n"], 1);
    assert_eq!(t.changes[2].after.as_ref().unwrap()["n"], 2);
    assert_eq!(t.changes[3].op, RowOp::Delete);
    assert_eq!(t.changes[3].key, vec![serde_json::json!("b")]);
    assert!(t.gtid.contains(':'), "gtid is a single uuid:gno, got {}", t.gtid);
    assert!(t.position.starts_with("MySQL56/"));

    // Resume from the position before the transaction and check the same transaction is
    // delivered again with the same identity (at-least-once, deterministic).
    cancel.cancel();
    let _ = handle.await;
    let (tx2, mut rx2) = mpsc::channel(64);
    let cancel2 = CancellationToken::new();
    let handle2 = tokio::spawn(orbit_vstream::run(
        config(),
        schema.clone(),
        start,
        tx2,
        cancel2.clone(),
    ));
    let again = support::wait_for(
        &mut rx2,
        Duration::from_secs(20),
        |i| matches!(i, StreamItem::Transaction(t2) if !t2.changes.is_empty()),
    )
    .await;
    let StreamItem::Transaction(t2) = again else {
        unreachable!()
    };
    assert_eq!(t2.gtid, t.gtid);
    assert_eq!(t2.position, t.position);
    assert_eq!(t2.changes, t.changes);
    cancel2.cancel();
    let _ = handle2.await;
    db.drop().await;
}

#[tokio::test]
async fn large_transaction_arrives_intact() {
    if !enabled() {
        return;
    }
    let db = TestDb::new("_orbit_it").await;
    let schema = schema();
    let start = current_position(&config(), &schema).await.unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(orbit_vstream::run(config(), schema.clone(), start, tx, cancel.clone()));
    support::wait_for(&mut rx, Duration::from_secs(20), |i| matches!(i, StreamItem::Heartbeat)).await;

    let n = 5000;
    let values: Vec<String> = (0..n)
        .map(|i| format!("('k{i}','org{}',{i},0,NULL,NULL)", i % 3))
        .collect();
    mysql(&[&format!(
        "INSERT INTO _orbit_it (id, org, n, flag, doc, created) VALUES {}",
        values.join(",")
    )])
    .await;
    let StreamItem::Transaction(t) = support::wait_for(
        &mut rx,
        Duration::from_secs(60),
        |i| matches!(i, StreamItem::Transaction(t) if !t.changes.is_empty()),
    )
    .await
    else {
        unreachable!()
    };
    assert_eq!(t.changes.len(), n);
    cancel.cancel();
    let _ = handle.await;
    db.drop().await;
}

#[tokio::test]
async fn invalid_checkpoint_is_rejected_before_streaming() {
    if !enabled() {
        return;
    }
    let schema = schema();
    let current = current_position(&config(), &schema).await.unwrap();
    let mut bad = Checkpoint::empty(0);
    for (shard, pos) in &current.positions {
        // Bump the last interval far into the future.
        let (head, _) = pos.rsplit_once('-').unwrap();
        bad.set(shard.clone(), format!("{head}-999999999"));
    }
    let (tx, _rx) = mpsc::channel(8);
    let err = orbit_vstream::run(config(), schema, bad, tx, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err, orbit_vstream::VStreamError::InvalidCheckpoint { .. }),
        "got {err:?}"
    );
}

#[tokio::test]
async fn copy_phase_fill_returns_exact_partition_rows_and_position() {
    if !enabled() {
        return;
    }
    let db = TestDb::new("_orbit_it").await;
    let schema = schema();
    mysql(&[
        "INSERT INTO _orbit_it (id, org, n, flag, doc, created) VALUES ('f1','fill-org',1,1,NULL,NULL),('f2','fill-org',2,0,NULL,NULL),('o1','other',3,0,NULL,NULL),('q1','it''s \\\\ weird',4,0,NULL,NULL)",
    ])
    .await;
    let out = run_fill(
        &config(),
        &schema,
        "_orbit_it",
        "fill-org",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let ids: Vec<&str> = out.rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["f1", "f2"]);
    assert_eq!(out.positions.len(), 1);
    let (_, pos) = out.positions.iter().next().unwrap();
    assert!(pos.starts_with("MySQL56/"));

    // Odd characters in the partition key are escaped correctly.
    let out = run_fill(
        &config(),
        &schema,
        "_orbit_it",
        "it's \\ weird",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0]["id"], "q1");

    // Empty partition still completes with a position.
    let out = run_fill(
        &config(),
        &schema,
        "_orbit_it",
        "nobody",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.positions.is_empty());

    // The fill position is at or after the live stream's current position.
    let current = current_position(&config(), &schema).await.unwrap();
    let fill_cp = Checkpoint {
        epoch: 0,
        positions: out.positions.clone(),
    };
    let _ = HashMap::<String, String>::new();
    // current was read after the fill, so the fill position must be contained in current.
    assert!(fill_cp.is_contained_in(&current).unwrap());
    db.drop().await;
}

#[tokio::test]
async fn schema_drift_is_a_fatal_error() {
    if !enabled() {
        return;
    }
    let db = TestDb::new("_orbit_it").await;
    let mut schema = schema();
    // Pretend the sync schema expects an extra column the live table lacks.
    schema.tables[0].columns.push(ColumnSchema {
        name: "missing_col".into(),
        kind: ValueKind::String,
        nullable: true,
        source_type: "text".into(),
        enum_values: None,
        derived: None,
    });
    schema.schema_hash = schema.compute_hash();
    let start = current_position(&config(), &schema).await.unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(orbit_vstream::run(config(), schema, start, tx, cancel.clone()));
    support::wait_for(&mut rx, Duration::from_secs(20), |i| matches!(i, StreamItem::Heartbeat)).await;
    mysql(&["INSERT INTO _orbit_it (id, org, n, flag) VALUES ('d1','org',1,0)"]).await;
    let res = tokio::time::timeout(Duration::from_secs(20), handle)
        .await
        .expect("subscriber stops")
        .unwrap();
    assert!(
        matches!(res, Err(orbit_vstream::VStreamError::SchemaMismatch { .. })),
        "got {res:?}"
    );
    db.drop().await;
}

/// `_orbit_parent(id, org)` partitioned by `org`, and `_orbit_child(id, parent_id, ...)`
/// partitioned through `_orbit_parent`.
fn derived_schema() -> SyncSchema {
    let col = |name: &str, kind: ValueKind, nullable: bool, source_type: &str| ColumnSchema {
        name: name.into(),
        kind,
        nullable,
        source_type: source_type.into(),
        enum_values: None,
        derived: None,
    };
    let mut s = SyncSchema {
        format_version: 1,
        schema_hash: String::new(),
        app: "vstream-test".into(),
        keyspace: "orbit".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![
            TableSchema {
                name: "_orbit_parent".into(),
                primary_key: vec!["id".into()],
                partition_column: "org".into(),
                partition_parent: None,
                partition_routes: vec![],
                columns: vec![
                    col("id", ValueKind::String, false, "varchar(64)"),
                    col("org", ValueKind::String, false, "varchar(64)"),
                ],
                relations: vec![],
            },
            TableSchema {
                name: "_orbit_child".into(),
                primary_key: vec!["id".into()],
                partition_column: "parent_id".into(),
                partition_parent: Some("_orbit_parent".into()),
                partition_routes: vec![],
                columns: vec![
                    col("id", ValueKind::String, false, "varchar(64)"),
                    col("parent_id", ValueKind::String, false, "varchar(64)"),
                    col("n", ValueKind::Int, true, "int"),
                    col("flag", ValueKind::Bool, false, "tinyint(1)"),
                    col("doc", ValueKind::Json, true, "json"),
                    col("created", ValueKind::DateTime, true, "datetime(3)"),
                    col("kind", ValueKind::String, false, "enum('A','B')"),
                    col("big", ValueKind::BigInt, true, "bigint unsigned"),
                    col("price", ValueKind::Decimal, true, "decimal(10,2)"),
                    col("blob", ValueKind::Bytes, true, "varbinary(16)"),
                ],
                relations: vec![],
            },
        ],
    };
    s.tables[1].columns[6].enum_values = Some(vec!["A".into(), "B".into()]);
    s.schema_hash = s.compute_hash();
    s.validate().unwrap();
    s
}

#[tokio::test]
async fn derived_fill_returns_exact_child_rows_with_cdc_images_and_position() {
    if !enabled() {
        return;
    }
    let parent = TestDb::create(
        "_orbit_parent",
        "id varchar(64) NOT NULL, org varchar(64) NOT NULL, PRIMARY KEY (id)",
    )
    .await;
    let child = TestDb::create(
        "_orbit_child",
        "id varchar(64) NOT NULL, parent_id varchar(64) NOT NULL, n int NULL, flag tinyint(1) NOT NULL DEFAULT 0, doc json NULL, created datetime(3) NULL, kind enum('A','B') NOT NULL DEFAULT 'A', big bigint unsigned NULL, price decimal(10,2) NULL, blob varbinary(16) NULL, PRIMARY KEY (id)",
    )
    .await;
    let schema = derived_schema();
    mysql(&[
        "INSERT INTO _orbit_parent (id, org) VALUES ('p1','fill-org'),('p2','fill-org'),('p3','other'),('q1','it''s \\\\ weird')",
        "INSERT INTO _orbit_child (id, parent_id, n, flag, doc, created, kind, big, price, blob) VALUES ('c1','p1',1,1,'{\"k\":[1,2]}','2026-01-02 03:04:05.678','B',18446744073709551615,'1234.50',X'00FF10'),('c2','p2',NULL,0,NULL,NULL,'A',NULL,NULL,NULL),('c3','p3',3,0,NULL,NULL,'A',NULL,NULL,NULL),('c4','p1',4,0,NULL,NULL,'A',NULL,NULL,NULL),('c5','q1',5,0,NULL,NULL,'A',NULL,NULL,NULL)",
    ])
    .await;

    // Capture the CDC image of c1 from the live stream to compare against the fill image.
    let start = current_position(&config(), &schema).await.unwrap();
    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(orbit_vstream::run(config(), schema.clone(), start, tx, cancel.clone()));
    support::wait_for(&mut rx, Duration::from_secs(20), |i| matches!(i, StreamItem::Heartbeat)).await;
    mysql(&["UPDATE _orbit_child SET n = 11 WHERE id = 'c1'"]).await;
    let StreamItem::Transaction(t) = support::wait_for(
        &mut rx,
        Duration::from_secs(20),
        |i| matches!(i, StreamItem::Transaction(t) if !t.changes.is_empty()),
    )
    .await
    else {
        unreachable!()
    };
    let cdc_c1 = t.changes[0].after.clone().unwrap();
    cancel.cancel();
    let _ = handle.await;

    let out = orbit_vstream::fill::run_derived_fill(
        &config(),
        &schema,
        "_orbit_child",
        "fill-org",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut ids: Vec<&str> = out.rows.iter().map(|r| r["id"].as_str().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["c1", "c2", "c4"]);
    let fill_c1 = out.rows.iter().find(|r| r["id"] == "c1").unwrap();
    assert_eq!(*fill_c1, cdc_c1, "fill image equals the CDC image");
    assert_eq!(fill_c1["flag"], true);
    assert_eq!(fill_c1["doc"], serde_json::json!({"k": [1, 2]}));
    assert_eq!(fill_c1["created"], "2026-01-02 03:04:05.678");
    assert_eq!(fill_c1["kind"], "B");
    assert_eq!(fill_c1["big"], "18446744073709551615");
    assert_eq!(fill_c1["price"], "1234.50");
    assert_eq!(fill_c1["blob"], "AP8Q");
    let fill_c2 = out.rows.iter().find(|r| r["id"] == "c2").unwrap();
    assert_eq!(fill_c2["n"], serde_json::Value::Null);
    assert_eq!(fill_c2["flag"], false);
    assert_eq!(
        fill_c2.keys().cloned().collect::<Vec<_>>(),
        schema.tables[1]
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>(),
        "columns in sync schema order"
    );
    assert_eq!(out.positions.len(), 1);
    let (_, pos) = out.positions.iter().next().unwrap();
    assert!(pos.starts_with("MySQL56/"), "got {pos}");
    let current = current_position(&config(), &schema).await.unwrap();
    let fill_cp = Checkpoint {
        epoch: 0,
        positions: out.positions.clone(),
    };
    assert!(fill_cp.is_contained_in(&current).unwrap());

    // Odd characters in the partition key are escaped correctly.
    let out = orbit_vstream::fill::run_derived_fill(
        &config(),
        &schema,
        "_orbit_child",
        "it's \\ weird",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(out.rows.len(), 1);
    assert_eq!(out.rows[0]["id"], "c5");

    // A partition without parents: no rows, but a position.
    let out = orbit_vstream::fill::run_derived_fill(
        &config(),
        &schema,
        "_orbit_child",
        "nobody",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(out.rows.is_empty());
    assert!(!out.positions.is_empty());

    // More parents than one IN list: every child is found exactly once.
    let parents: Vec<String> = (0..1200).map(|i| format!("('m{i}','many')")).collect();
    let children: Vec<String> = (0..1200)
        .map(|i| format!("('mc{i}','m{i}',{i},0,NULL,NULL,'A',NULL,NULL,NULL)"))
        .collect();
    mysql(&[
        &format!("INSERT INTO _orbit_parent (id, org) VALUES {}", parents.join(",")),
        &format!(
            "INSERT INTO _orbit_child (id, parent_id, n, flag, doc, created, kind, big, price, blob) VALUES {}",
            children.join(",")
        ),
    ])
    .await;
    let out = orbit_vstream::fill::run_derived_fill(
        &config(),
        &schema,
        "_orbit_child",
        "many",
        Duration::from_secs(30),
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    let mut ns: Vec<i64> = out.rows.iter().map(|r| r["n"].as_i64().unwrap()).collect();
    ns.sort_unstable();
    assert_eq!(ns, (0..1200).collect::<Vec<_>>());
    child.drop().await;
    parent.drop().await;
}
