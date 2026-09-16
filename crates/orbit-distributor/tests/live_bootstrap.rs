//! Parent index bootstrap against a real Vitess (the local `vttestserver` from `infra/vitess`).
//!
//! Run with `ORBIT_TEST_VITESS=1`. Skipped otherwise so unit test runs stay hermetic.

use std::time::Duration;

use orbit_distributor::{StateStore, bootstrap_parent_index};
use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig, SyncSchema, TableSchema, ValueKind};
use orbit_vstream::{SubscriberConfig, VitessEndpoint};

fn grpc_uri() -> String {
    std::env::var("ORBIT_TEST_VITESS_GRPC").unwrap_or_else(|_| "http://127.0.0.1:33575".into())
}

async fn mysql(statements: &[&str]) {
    use tokio::io::AsyncWriteExt;
    let host = std::env::var("ORBIT_TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("ORBIT_TEST_MYSQL_PORT").unwrap_or_else(|_| "33577".into());
    let sql = statements.join(";\n") + ";";
    let mut child = tokio::process::Command::new("mysql")
        .args(["-h", &host, "-P", &port, "-u", "root", "orbit"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("mysql cli");
    let mut stdin = child.stdin.take().expect("stdin");
    stdin.write_all(sql.as_bytes()).await.expect("write sql");
    drop(stdin);
    let out = child.wait_with_output().await.expect("mysql cli");
    assert!(
        out.status.success(),
        "mysql failed: {}\nsql: {}",
        String::from_utf8_lossy(&out.stderr),
        &sql[..sql.len().min(500)]
    );
}

fn schema() -> SyncSchema {
    let col = |name: &str, kind: ValueKind, nullable: bool| ColumnSchema {
        name: name.into(),
        kind,
        nullable,
        source_type: if kind == ValueKind::Int {
            "int".into()
        } else {
            "varchar(64)".into()
        },
        enum_values: None,
        derived: None,
    };
    let mut s = SyncSchema {
        format_version: 1,
        schema_hash: String::new(),
        app: "dist-test".into(),
        keyspace: "orbit".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![
            TableSchema {
                name: "_orbit_bs_parent".into(),
                primary_key: vec!["id".into()],
                partition_column: "org".into(),
                partition_parent: None,
                partition_routes: vec![],
                columns: vec![col("id", ValueKind::Int, false), col("org", ValueKind::String, true)],
                relations: vec![],
            },
            TableSchema {
                name: "_orbit_bs_child".into(),
                primary_key: vec!["id".into()],
                partition_column: "parent_id".into(),
                partition_parent: Some("_orbit_bs_parent".into()),
                partition_routes: vec![],
                columns: vec![
                    col("id", ValueKind::String, false),
                    col("parent_id", ValueKind::Int, false),
                ],
                relations: vec![],
            },
        ],
    };
    s.schema_hash = s.compute_hash();
    s.validate().unwrap();
    s
}

#[tokio::test]
async fn bootstrap_copies_every_parent_row_in_pages_and_runs_once() {
    if std::env::var("ORBIT_TEST_VITESS").is_err() {
        return;
    }
    mysql(&[
        "DROP TABLE IF EXISTS `_orbit_bs_parent`",
        "CREATE TABLE `_orbit_bs_parent` (id int NOT NULL, org varchar(64) NULL, PRIMARY KEY (id))",
        "DROP TABLE IF EXISTS `_orbit_bs_child`",
        "CREATE TABLE `_orbit_bs_child` (id varchar(64) NOT NULL, parent_id int NOT NULL, PRIMARY KEY (id))",
    ])
    .await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    // More than two pages of 5000, plus a NULL partition that must be skipped.
    let n = 12_345;
    let values: Vec<String> = (1..=n).map(|i| format!("({i},'org{}')", i % 7)).collect();
    mysql(&[
        &format!("INSERT INTO _orbit_bs_parent (id, org) VALUES {}", values.join(",")),
        "INSERT INTO _orbit_bs_parent (id, org) VALUES (99999, NULL)",
    ])
    .await;
    let schema = schema();
    let store = StateStore::open_in_memory().unwrap();
    store.load_or_init(&schema.schema_hash).unwrap();
    let config = SubscriberConfig::new(VitessEndpoint::new(grpc_uri()), "orbit");
    bootstrap_parent_index(&schema, &store, &config).await.unwrap();
    for i in [1, 4999, 5000, 5001, 10_000, 10_001, n] {
        assert_eq!(
            store
                .parent_partition("_orbit_bs_parent", &i.to_string())
                .unwrap()
                .as_deref(),
            Some(format!("org{}", i % 7).as_str()),
            "row {i}"
        );
    }
    assert_eq!(store.parent_partition("_orbit_bs_parent", "99999").unwrap(), None);
    let total: usize = (0..7)
        .map(|o| {
            store
                .parent_keys_in_partition("_orbit_bs_parent", &format!("org{o}"))
                .unwrap()
                .len()
        })
        .sum();
    assert_eq!(total, n as usize);
    let position = store.parent_index_ready("_orbit_bs_parent").unwrap().unwrap();
    assert!(
        position.starts_with("orbit/") && position.contains("@MySQL56/"),
        "got {position}"
    );
    assert_eq!(
        store.parent_index_ready("_orbit_bs_child").unwrap(),
        None,
        "children are not parents"
    );

    // A second run is a no-op: the table is gone but the marker keeps the bootstrap from running.
    mysql(&["DROP TABLE IF EXISTS `_orbit_bs_parent`"]).await;
    bootstrap_parent_index(&schema, &store, &config).await.unwrap();
    assert_eq!(store.parent_index_ready("_orbit_bs_parent").unwrap().unwrap(), position);
    mysql(&["DROP TABLE IF EXISTS `_orbit_bs_child`"]).await;
}
