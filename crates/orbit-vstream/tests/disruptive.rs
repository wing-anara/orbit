//! Disruptive failure tests against the local docker Vitess: the container is frozen while a
//! subscriber is running, and binlogs are purged under a stale checkpoint.
//!
//! Run with `ORBIT_TEST_VITESS=1 ORBIT_TEST_VITESS_DISRUPTIVE=1`. Requires `sudo -n docker`.
//!
//! Note: `docker restart` is not used. `vttestserver` re-initialises its data directory and its
//! MySQL server UUID on every start, so a restart is a brand-new source, not an outage. A source
//! with a new GTID history needs `orbit-server checkpoint reset` (see docs/failure-model.md).

use std::time::Duration;

use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig, SyncSchema, TableSchema, ValueKind};
use orbit_vstream::subscriber::{SubscriberConfig, current_position};
use orbit_vstream::{StreamItem, VStreamError, VitessEndpoint};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

mod support;
use support::{TestDb, mysql};

fn enabled() -> bool {
    support::init_tracing();
    std::env::var("ORBIT_TEST_VITESS").is_ok() && std::env::var("ORBIT_TEST_VITESS_DISRUPTIVE").is_ok()
}

fn schema() -> SyncSchema {
    let mut s = SyncSchema {
        format_version: 1,
        schema_hash: String::new(),
        app: "disruptive".into(),
        keyspace: "orbit".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![TableSchema {
            name: "_orbit_dis".into(),
            primary_key: vec!["id".into()],
            partition_column: "org".into(),
            partition_parent: None,
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(64)".into(),
                    enum_values: None,
                },
                ColumnSchema {
                    name: "org".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(64)".into(),
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

fn config() -> SubscriberConfig {
    let mut c = SubscriberConfig::new(VitessEndpoint::new(support::grpc_uri()), "orbit");
    c.heartbeat_interval = Duration::from_secs(1);
    c.stall_timeout = Duration::from_secs(8);
    c.progress_timeout = Duration::from_secs(5);
    c.reconnect_backoff_min = Duration::from_millis(200);
    c.reconnect_backoff_max = Duration::from_secs(2);
    c
}

async fn docker(args: &[&str]) -> std::process::Output {
    let mut cmd = tokio::process::Command::new("sudo");
    cmd.arg("-n").arg("docker").args(args);
    let out = cmd.output().await.expect("docker");
    assert!(
        out.status.success(),
        "docker {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Runs SQL on the MySQL behind vttablet (not through vtgate) over its unix socket.
async fn mysql_on_tablet(sql: &str) -> std::process::Output {
    let script = format!(
        "for s in $(find /vt/vtdataroot -name mysql.sock); do \
           if mysql -uroot -S \"$s\" -e 'select 1' >/dev/null 2>&1; then exec mysql -uroot -S \"$s\" -e {}; fi; \
         done; echo 'no live mysqld socket' >&2; exit 1",
        shell_quote(sql)
    );
    let mut cmd = tokio::process::Command::new("sudo");
    cmd.args(["-n", "docker", "exec", "orbit-vitess", "sh", "-c", &script]);
    cmd.output().await.expect("docker exec")
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

async fn wait_for_mysql() {
    for _ in 0..90 {
        let ok = tokio::process::Command::new("mysqladmin")
            .args(["ping", "-h", "127.0.0.1", "-P", "33577", "-u", "root", "--silent"])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if ok {
            tokio::time::sleep(Duration::from_secs(3)).await;
            return;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    panic!("vitess did not come back");
}

/// The subscriber survives a source outage: the container is frozen for longer than the stall
/// timeout, the subscriber drops the dead stream, reconnects from the last emitted position, and
/// delivers transactions committed after the outage. Transactions committed before the outage are
/// never lost and may be delivered twice (at-least-once).
#[tokio::test]
async fn subscriber_reconnects_after_source_outage() {
    if !enabled() {
        return;
    }
    let db = TestDb::new("_orbit_dis").await;
    let schema = schema();
    let start = current_position(&config(), &schema).await.unwrap();
    let (tx, mut rx) = mpsc::channel(64);
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(orbit_vstream::run(config(), schema.clone(), start, tx, cancel.clone()));
    support::wait_for(&mut rx, Duration::from_secs(20), |i| matches!(i, StreamItem::Heartbeat)).await;
    mysql(&["INSERT INTO _orbit_dis (id, org, n) VALUES ('before', 'o', 1)"]).await;
    let StreamItem::Transaction(t1) = support::wait_for(
        &mut rx,
        Duration::from_secs(20),
        |i| matches!(i, StreamItem::Transaction(t) if !t.changes.is_empty()),
    )
    .await
    else {
        unreachable!()
    };
    assert_eq!(t1.changes[0].key, vec![serde_json::json!("before")]);

    docker(&["pause", "orbit-vitess"]).await;
    tokio::time::sleep(Duration::from_secs(20)).await;
    docker(&["unpause", "orbit-vitess"]).await;
    wait_for_mysql().await;
    mysql(&["INSERT INTO _orbit_dis (id, org, n) VALUES ('after', 'o', 2)"]).await;
    let StreamItem::Transaction(t2) = support::wait_for(&mut rx, Duration::from_secs(120), |i| matches!(i, StreamItem::Transaction(t) if t.changes.iter().any(|c| c.key == vec![serde_json::json!("after")]))).await else { unreachable!() };
    assert!(t2.changes.iter().any(|c| c.key == vec![serde_json::json!("after")]));
    cancel.cancel();
    let res = handle.await.unwrap();
    assert!(matches!(res, Err(VStreamError::Cancelled)), "{res:?}");
    db.drop().await;
}

/// A checkpoint whose binlogs were purged is reported explicitly, never silently skipped.
///
/// vtgate does not surface the tablet's "purged required binary logs" error: it retries the
/// tablet stream internally and keeps sending heartbeats. The subscriber detects the missing
/// position progress against the source's current position and fails with `NoProgress`.
#[tokio::test]
async fn purged_binlogs_are_reported_explicitly() {
    if !enabled() {
        return;
    }
    let db = TestDb::new("_orbit_dis").await;
    let schema = schema();
    let stale = current_position(&config(), &schema).await.unwrap();
    // Commit past the stale position, then rotate and purge the binlogs on the MySQL behind the
    // tablet so the stale position is no longer replayable.
    mysql(&["INSERT INTO _orbit_dis (id, org, n) VALUES ('x', 'o', 1)"]).await;
    let out = mysql_on_tablet("FLUSH BINARY LOGS; PURGE BINARY LOGS BEFORE NOW() + INTERVAL 1 DAY;").await;
    assert!(
        out.status.success(),
        "purge failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let (tx, mut rx) = mpsc::channel(8);
    // Drain heartbeats so channel backpressure cannot mask the failure.
    tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let mut cfg = config();
    cfg.max_consecutive_failures = 2;
    let res = orbit_vstream::run(cfg, schema, stale, tx, CancellationToken::new()).await;
    match res {
        Err(VStreamError::PurgedBinlog { .. })
        | Err(VStreamError::InvalidCheckpoint { .. })
        | Err(VStreamError::PoisonPosition { .. })
        | Err(VStreamError::NoProgress { .. }) => {}
        other => panic!("expected an explicit purged/invalid checkpoint error, got {other:?}"),
    }
    db.drop().await;
}
