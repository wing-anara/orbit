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
fn main() {
    for n in [100usize, 1_000, 10_000, 50_000] {
        let schema = schema();
        let store = StateStore::open_in_memory().unwrap();
        store.load_or_init(&schema.schema_hash).unwrap();
        let started = Instant::now();
        let docs: Vec<_> = (0..n)
            .map(|i| row(json!({"id":format!("doc-{i}"),"org":format!("org-{i}"),"name":"Shared"})))
            .collect();
        let perms: Vec<_> = (0..n).map(|i| row(json!({"id":format!("perm-{i}"),"org":format!("org-{}",(i+1)%n),"docId":format!("doc-{i}"),"user":"reader"}))).collect();
        let metadata: Vec<_> = (0..n)
            .map(|i| row(json!({"id":format!("meta-{i}"),"docId":format!("doc-{i}"),"summary":"Summary"})))
            .collect();
        store.seed_fanout(&schema, "doc", &docs).unwrap();
        store.seed_fanout(&schema, "permission", &perms).unwrap();
        store.seed_fanout(&schema, "metadata", &metadata).unwrap();
        store.mark_fanout_ready(&schema.schema_hash).unwrap();
        let seed_ms = started.elapsed().as_secs_f64() * 1000.0;
        let mut changed = docs[0].clone();
        changed.insert("name".into(), "Renamed".into());
        let start = Instant::now();
        let routed = store
            .route_fanout(&schema, &tx("source:1", Some(docs[0].clone()), changed), &[])
            .unwrap();
        let rename_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(routed.len(), 1);
        assert!(routed.contains_key("org-1"));
        let start = Instant::now();
        let routed = store
            .route_fanout(
                &schema,
                &tx(
                    "source:2",
                    None,
                    row(json!({"id":"private-new","org":"org-0","name":"Private"})),
                ),
                &[],
            )
            .unwrap();
        let private_insert_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert!(routed.is_empty());
        println!(
            "{}",
            json!({"organizations":n,"sharedDocuments":n,"rows":n*3,"seedMs":seed_ms,"sharedRenameMs":rename_ms,"unrelatedPrivateInsertMs":private_insert_ms,"privateInsertRecipients":routed.len(),"storage":"in-memory SQLite","source":"synthetic; not live fleet"})
        );
    }
}
