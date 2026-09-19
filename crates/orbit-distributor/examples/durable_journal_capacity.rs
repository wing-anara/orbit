//! Read-only-source capacity diagnostic: local synthetic routing cache, no network or source DB.
use orbit_distributor::StateStore;
use orbit_protocol::{
    cdc::{RowChange, RowOp, SourceTransaction},
    schema::SyncSchema,
    value::Row,
};
use serde_json::json;
use std::time::Instant;
fn schema() -> SyncSchema {
    let column = |name: &str| json!({"name":name,"kind":"string","nullable":false,"source_type":"varchar(191)"});
    let mut s: SyncSchema = serde_json::from_value(json!({
            "format_version":1,"schema_hash":"","app":"sharing","keyspace":"test",
            "partition":{"name":"org","key_kind":"string","placement":{"strategy":"one_per_partition","version":1}},
            "tables":[
                {"name":"doc","primary_key":["id"],"partition_column":"org",
                 "partition_routes":[["permissions"]],"columns":[column("id"),column("org"),column("name")],
                 "relations":[{"name":"permissions","kind":"many","target_table":"permission","from_columns":["id"],"to_columns":["docId"]}]},
                {"name":"permission","primary_key":["id"],"partition_column":"org",
                 "columns":[column("id"),column("org"),column("docId"),column("user")],"relations":[]},
                {"name":"metadata","primary_key":["id"],"partition_column":"docId","partition_parent":"doc",
                 "partition_routes":[["document","permissions"]],"columns":[column("id"),column("docId"),column("summary")],
                 "relations":[{"name":"document","kind":"one","target_table":"doc","from_columns":["docId"],"to_columns":["id"]}]}
            ]
        })).unwrap();
    s.schema_hash = s.compute_hash();
    s.validate().unwrap();
    s
}

fn row(v: serde_json::Value) -> Row {
    serde_json::from_value(v).unwrap()
}
fn tx(gtid: &str, before: Option<Row>, after: Row) -> SourceTransaction {
    SourceTransaction {
        keyspace: "test".into(),
        shard: "0".into(),
        gtid: gtid.into(),
        position: format!("MySQL56/{gtid}"),
        commit_timestamp: 0,
        trace: Default::default(),
        changes: vec![RowChange {
            table: "doc".into(),
            key: vec![after["id"].clone()],
            op: if before.is_some() { RowOp::Update } else { RowOp::Insert },
            before,
            after: Some(after),
        }],
    }
}
// Pass a new directory on the disk being evaluated. Never opens an existing state file.
fn main() {
    let root = std::path::PathBuf::from(std::env::args().nth(1).expect("new benchmark directory"));
    std::fs::create_dir(&root).expect("benchmark directory must not exist");
    let schema = schema();
    let sources: Vec<_> = (0..4096)
        .map(|i| {
            let before = row(json!({"id":format!("private-{i}"),"org":format!("org-{i}"),"name":"Before"}));
            let mut after = before.clone();
            after.insert("name".into(), "After".into());
            tx(&format!("source:{}", i + 1), Some(before), after)
        })
        .collect();
    for size in [1usize, 4, 16, 64] {
        let store = StateStore::open(&root.join(format!("group-{size}.sqlite"))).unwrap();
        let started = Instant::now();
        for group in sources.chunks(size) {
            let input: Vec<_> = group.iter().map(|source| (source, &[][..])).collect();
            let routed = store.route_fanout_batch(&schema, &input).unwrap();
            assert!(routed.iter().all(|r| r.is_empty()));
        }
        let elapsed = started.elapsed().as_secs_f64();
        println!(
            "{}",
            json!({"groupSize":size,"transactions":sources.len(),"seconds":elapsed,"transactionsPerSecond":sources.len() as f64/elapsed,"journal":"WAL","synchronous":"FULL","scope":"disk-backed component benchmark, not fleet acceptance"})
        );
    }
}
