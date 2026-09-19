//! Durable source projection and routing journal for relation-based fanout.
//!
//! The graph advances when a source transaction is routed, before network delivery. Its exact
//! routing decision is committed in the same SQLite transaction. Replayed source transactions
//! read that decision instead of evaluating the newer graph. The delivery checkpoint may lag
//! this journal safely; journal entries can only be collected after that checkpoint commits.

use std::collections::BTreeMap;

use indexmap::IndexMap;
use orbit_protocol::cdc::{RowChange, SourceTransaction};
use orbit_protocol::schema::SyncSchema;
use orbit_protocol::value::{CellValue, Row, RowKey};
use rusqlite::{Connection, OptionalExtension, params};

use crate::fanout::{FanoutError, RoutingSnapshot, route_fanout};

type Routed = IndexMap<String, Vec<RowChange>>;
type OverlayRows = BTreeMap<(String, String), Option<Row>>;

fn error(e: impl std::fmt::Display) -> FanoutError {
    FanoutError::Snapshot(e.to_string())
}
fn json(value: &impl serde::Serialize) -> Result<String, FanoutError> {
    serde_json::to_string(value).map_err(error)
}

pub fn initialize(db: &Connection) -> Result<(), FanoutError> {
    db.execute_batch("
        CREATE TABLE IF NOT EXISTS fanout_rows (tbl TEXT NOT NULL, key TEXT NOT NULL, image TEXT NOT NULL, PRIMARY KEY(tbl,key));
        CREATE TABLE IF NOT EXISTS fanout_cells (tbl TEXT NOT NULL, col TEXT NOT NULL, value TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(tbl,col,value,key));
        CREATE INDEX IF NOT EXISTS fanout_cells_row ON fanout_cells(tbl,key);
        CREATE TABLE IF NOT EXISTS fanout_journal (keyspace TEXT NOT NULL, shard TEXT NOT NULL, gtid TEXT NOT NULL, routed TEXT NOT NULL, PRIMARY KEY(keyspace,shard,gtid));
    ").map_err(error)
}

struct Snapshot<'a>(&'a Connection);
impl RoutingSnapshot for Snapshot<'_> {
    fn get(&self, table: &str, key: &RowKey) -> Result<Option<Row>, FanoutError> {
        let row: Option<String> = self
            .0
            .query_row(
                "SELECT image FROM fanout_rows WHERE tbl=?1 AND key=?2",
                params![table, json(key)?],
                |r| r.get(0),
            )
            .optional()
            .map_err(error)?;
        row.map(|r| serde_json::from_str(&r).map_err(error)).transpose()
    }
    fn matching(&self, table: &str, columns: &[String], values: &[CellValue]) -> Result<Vec<Row>, FanoutError> {
        if columns.is_empty() || columns.len() != values.len() {
            return Err(error("invalid relation lookup"));
        }
        if values.iter().any(|v| matches!(v, CellValue::Null)) {
            return Ok(vec![]);
        }
        // The first join column narrows the candidates through a durable index. Composite
        // joins check the remaining cells in the projected image, preserving exact equality.
        let mut statement = self.0.prepare_cached("SELECT r.image FROM fanout_cells c JOIN fanout_rows r ON r.tbl=c.tbl AND r.key=c.key WHERE c.tbl=?1 AND c.col=?2 AND c.value=?3 ORDER BY c.key").map_err(error)?;
        let images = statement
            .query_map(params![table, columns[0], json(&values[0])?], |r| r.get::<_, String>(0))
            .map_err(error)?;
        let mut result = Vec::new();
        for image in images {
            let row: Row = serde_json::from_str(&image.map_err(error)?).map_err(error)?;
            if columns.iter().zip(values).all(|(c, v)| row.get(c) == Some(v)) {
                result.push(row);
            }
        }
        Ok(result)
    }
}

struct Before<'a> {
    current: Snapshot<'a>,
    rows: &'a OverlayRows,
    schema: &'a SyncSchema,
}
impl RoutingSnapshot for Before<'_> {
    fn get(&self, table: &str, key: &RowKey) -> Result<Option<Row>, FanoutError> {
        match self.rows.get(&(table.into(), json(key)?)) {
            Some(row) => Ok(row.clone()),
            None => self.current.get(table, key),
        }
    }
    fn matching(&self, table: &str, columns: &[String], values: &[CellValue]) -> Result<Vec<Row>, FanoutError> {
        let mut result = Vec::new();
        for row in self.current.matching(table, columns, values)? {
            let key = row_key(self.schema, table, &row)?;
            if !self.rows.contains_key(&(table.into(), json(&key)?)) {
                result.push(row);
            }
        }
        for ((name, _), image) in self.rows {
            if name != table {
                continue;
            }
            if let Some(row) = image
                && !values.iter().any(|v| matches!(v, CellValue::Null))
                && columns.iter().zip(values).all(|(c, v)| row.get(c) == Some(v))
            {
                result.push(row.clone());
            }
        }
        Ok(result)
    }
}
fn row_key(schema: &SyncSchema, table: &str, row: &Row) -> Result<RowKey, FanoutError> {
    schema
        .table(table)
        .ok_or_else(|| error(format!("unknown table {table}")))?
        .primary_key
        .iter()
        .map(|c| {
            row.get(c)
                .cloned()
                .ok_or_else(|| error(format!("missing key {table}.{c}")))
        })
        .collect()
}

/// Seed a projected row during bootstrap. Call inside a transaction for batches. Only declared
/// relation join columns are indexed; large summary/metadata cells never enter the cell index.
pub fn put(db: &Connection, schema: &SyncSchema, table: &str, row: &Row) -> Result<(), FanoutError> {
    let key = json(&row_key(schema, table, row)?)?;
    db.execute("DELETE FROM fanout_cells WHERE tbl=?1 AND key=?2", params![table, key])
        .map_err(error)?;
    db.execute("INSERT INTO fanout_rows(tbl,key,image) VALUES (?1,?2,?3) ON CONFLICT(tbl,key) DO UPDATE SET image=excluded.image",params![table,key,json(row)?]).map_err(error)?;
    let mut columns = std::collections::BTreeSet::new();
    for source in &schema.tables {
        for relation in &source.relations {
            if source.name == table {
                columns.extend(relation.from_columns.iter());
            }
            if relation.target_table == table {
                columns.extend(relation.to_columns.iter());
            }
        }
    }
    for column in columns {
        let value = row
            .get(column)
            .ok_or_else(|| error(format!("missing join column {table}.{column}")))?;
        if matches!(value, CellValue::Null) {
            continue;
        }
        db.execute(
            "INSERT INTO fanout_cells(tbl,col,value,key) VALUES (?1,?2,?3,?4)",
            params![table, column, json(value)?, key],
        )
        .map_err(error)?;
    }
    Ok(())
}
fn remove(db: &Connection, table: &str, key: &RowKey) -> Result<(), FanoutError> {
    let key = json(key)?;
    db.execute("DELETE FROM fanout_cells WHERE tbl=?1 AND key=?2", params![table, key])
        .map_err(error)?;
    db.execute("DELETE FROM fanout_rows WHERE tbl=?1 AND key=?2", params![table, key])
        .map_err(error)?;
    Ok(())
}

/// Atomically advance the source projection and record its fanout decision. Safe to call again
/// with a replayed transaction even when many later transactions have already been projected.
pub fn route(db: &Connection, schema: &SyncSchema, source: &SourceTransaction) -> Result<Routed, FanoutError> {
    route_with_hydration(db, schema, source, &[])
}

pub fn route_with_hydration(
    db: &Connection,
    schema: &SyncSchema,
    source: &SourceTransaction,
    hydrated: &[(String, Row)],
) -> Result<Routed, FanoutError> {
    let tx = db.unchecked_transaction().map_err(error)?;
    let routed = route_with_hydration_uncommitted(&tx, schema, source, hydrated)?;
    tx.commit().map_err(error)?;
    Ok(routed)
}

/// Called only inside the caller's SQLite transaction. Every decision in a group is persisted
/// before any delivery can observe it; a failure rolls back the entire group.
pub(crate) fn route_with_hydration_uncommitted(
    db: &Connection,
    schema: &SyncSchema,
    source: &SourceTransaction,
    hydrated: &[(String, Row)],
) -> Result<Routed, FanoutError> {
    let saved: Option<String> = db
        .query_row(
            "SELECT routed FROM fanout_journal WHERE keyspace=?1 AND shard=?2 AND gtid=?3",
            params![source.keyspace, source.shard, source.gtid],
            |r| r.get(0),
        )
        .optional()
        .map_err(error)?;
    if let Some(saved) = saved {
        return serde_json::from_str(&saved).map_err(error);
    }
    let mut old: OverlayRows = BTreeMap::new();
    let tracked = tracked_tables(schema)?;
    // Content-only updates cannot change reachability. Avoid walking the entire shared graph
    // (or temporarily inserting private rows into it) for status polling, names and metadata.
    let routing_changed = !hydrated.is_empty()
        || source
            .changes
            .iter()
            .any(|change| tracked.contains(&change.table) && changes_routing(schema, change));
    for (table, row) in hydrated {
        let key = row_key(schema, table, row)?;
        let identity = (table.clone(), json(&key)?);
        if let std::collections::btree_map::Entry::Vacant(entry) = old.entry(identity) {
            entry.insert(Snapshot(db).get(table, &key)?);
        }
        put(db, schema, table, row)?;
    }

    for change in &source.changes {
        if !tracked.contains(&change.table) {
            continue;
        }
        if !routing_changed && Snapshot(db).get(&change.table, &change.key)?.is_none() {
            continue;
        }
        let before_key = change
            .before
            .as_ref()
            .map(|r| row_key(schema, &change.table, r))
            .transpose()?;
        for key in before_key.iter().chain(std::iter::once(&change.key)) {
            let identity = (change.table.clone(), json(key)?);
            if let std::collections::btree_map::Entry::Vacant(entry) = old.entry(identity) {
                entry.insert(Snapshot(db).get(&change.table, key)?);
            }
        }
        if let Some(key) = before_key {
            remove(db, &change.table, &key)?;
        }
        if let Some(row) = &change.after {
            put(db, schema, &change.table, row)?;
        } else {
            remove(db, &change.table, &change.key)?;
        }
    }
    let before = Before {
        current: Snapshot(db),
        rows: &old,
        schema,
    };
    let routed = route_fanout(schema, &source.changes, &before, &Snapshot(db))?;
    if routing_changed {
        collect_affected_unshared(db, schema, &before, &old)?;
    }
    db.execute(
        "INSERT INTO fanout_journal(keyspace,shard,gtid,routed) VALUES (?1,?2,?3,?4)",
        params![source.keyspace, source.shard, source.gtid, json(&routed)?],
    )
    .map_err(error)?;
    Ok(routed)
}

fn changes_routing(schema: &SyncSchema, change: &RowChange) -> bool {
    let (Some(before), Some(after), Some(table)) = (&change.before, &change.after, schema.table(&change.table)) else {
        return true;
    };
    let mut columns = std::collections::BTreeSet::new();
    columns.extend(table.primary_key.iter());
    columns.insert(&table.partition_column);
    for source in &schema.tables {
        for relation in &source.relations {
            if source.name == table.name {
                columns.extend(relation.from_columns.iter());
            }
            if relation.target_table == table.name {
                columns.extend(relation.to_columns.iter());
            }
        }
    }
    columns
        .into_iter()
        .any(|column| before.get(column) != after.get(column))
}

/// Collect only the relationship components touched by this transaction. Walk both images:
/// a removed relationship must still lead us to dependencies that became unreachable.
fn collect_affected_unshared(
    db: &Connection,
    schema: &SyncSchema,
    before: &dyn RoutingSnapshot,
    changed: &OverlayRows,
) -> Result<(), FanoutError> {
    // Older/custom schemas may express ownership without a traversable relation. Preserve
    // their existing collector until that implicit edge has an indexed representation.
    if schema.tables.iter().any(|table| {
        table.partition_parent.as_ref().is_some_and(|parent| {
            !table.relations.iter().any(|relation| {
                &relation.target_table == parent
                    && relation.from_columns == [table.partition_column.clone()]
                    && schema
                        .table(parent)
                        .is_some_and(|p| relation.to_columns == p.primary_key)
            })
        })
    }) {
        return collect_unshared(db, schema);
    }
    let current = Snapshot(db);
    let mut candidates = std::collections::BTreeSet::new();
    let mut pending: Vec<_> = changed.keys().cloned().collect();
    while let Some((table_name, encoded_key)) = pending.pop() {
        if !candidates.insert((table_name.clone(), encoded_key.clone())) {
            continue;
        }
        let key: RowKey = serde_json::from_str(&encoded_key).map_err(error)?;
        let table = schema
            .table(&table_name)
            .ok_or_else(|| error("unknown affected table"))?;
        for snapshot in [before, &current as &dyn RoutingSnapshot] {
            let Some(row) = snapshot.get(&table_name, &key)? else {
                continue;
            };
            for source in &schema.tables {
                for relation in &source.relations {
                    let edge = if source.name == table.name {
                        Some((&relation.target_table, &relation.from_columns, &relation.to_columns))
                    } else {
                        None
                    };
                    let reverse = if relation.target_table == table.name {
                        Some((&source.name, &relation.to_columns, &relation.from_columns))
                    } else {
                        None
                    };
                    for (target, from, to) in edge.into_iter().chain(reverse) {
                        let values: Vec<_> = from
                            .iter()
                            .map(|c| row.get(c).cloned().ok_or_else(|| error("missing relation column")))
                            .collect::<Result<_, _>>()?;
                        for neighbor in snapshot.matching(target, to, &values)? {
                            pending.push((target.clone(), json(&row_key(schema, target, &neighbor)?)?));
                        }
                    }
                }
            }
        }
    }
    let mut keep = std::collections::BTreeSet::new();
    for (table_name, encoded_key) in &candidates {
        let table = schema
            .table(table_name)
            .ok_or_else(|| error("unknown affected table"))?;
        if table.partition_routes.is_empty() {
            continue;
        }
        let key: RowKey = serde_json::from_str(encoded_key).map_err(error)?;
        if let Some(row) = current.get(table_name, &key)?
            && !crate::fanout::partitions(schema, table, &row, &current)?.is_empty()
        {
            crate::fanout::retain_dependencies(schema, table, &row, &current, &mut keep)?;
        }
    }
    for (table, encoded_key) in candidates {
        if !keep.contains(&(table.clone(), encoded_key.clone())) {
            let key: RowKey = serde_json::from_str(&encoded_key).map_err(error)?;
            remove(db, &table, &key)?;
        }
    }
    Ok(())
}

fn collect_unshared(db: &Connection, schema: &SyncSchema) -> Result<(), FanoutError> {
    let mut keep = std::collections::BTreeSet::new();
    for table in schema.tables.iter().filter(|t| !t.partition_routes.is_empty()) {
        let mut stmt = db
            .prepare("SELECT image FROM fanout_rows WHERE tbl=?1")
            .map_err(error)?;
        let images = stmt
            .query_map(params![table.name], |r| r.get::<_, String>(0))
            .map_err(error)?;
        for image in images {
            let row: Row = serde_json::from_str(&image.map_err(error)?).map_err(error)?;
            if !crate::fanout::partitions(schema, table, &row, &Snapshot(db))?.is_empty() {
                crate::fanout::retain_dependencies(schema, table, &row, &Snapshot(db), &mut keep)?;
            }
        }
    }
    let mut stmt = db.prepare("SELECT tbl,key FROM fanout_rows").map_err(error)?;
    let identities = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .map_err(error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(error)?;
    for (table, key) in identities {
        if !keep.contains(&(table.clone(), key.clone())) {
            db.execute("DELETE FROM fanout_cells WHERE tbl=?1 AND key=?2", params![table, key])
                .map_err(error)?;
            db.execute("DELETE FROM fanout_rows WHERE tbl=?1 AND key=?2", params![table, key])
                .map_err(error)?;
        }
    }
    Ok(())
}

/// Tables whose projected rows are needed to traverse any routing path.
pub fn tracked_tables(schema: &SyncSchema) -> Result<std::collections::BTreeSet<String>, FanoutError> {
    let mut result = std::collections::BTreeSet::new();
    for root in schema.tables.iter().filter(|t| !t.partition_routes.is_empty()) {
        result.insert(root.name.clone());
        if let Some(parent) = &root.partition_parent {
            result.insert(parent.clone());
        }
        for path in &root.partition_routes {
            let mut current = root;
            for step in path {
                let relation = current
                    .relations
                    .iter()
                    .find(|r| &r.name == step)
                    .ok_or_else(|| error("unknown routing relation"))?;
                current = schema
                    .table(&relation.target_table)
                    .ok_or_else(|| error("unknown routing table"))?;
                result.insert(current.name.clone());
            }
        }
    }
    Ok(result)
}

/// Collect only decisions covered by the durable delivery checkpoint. Runs in the same SQLite
/// transaction as that checkpoint, so a crash cannot remove a decision that may be replayed.
pub fn prune(db: &Connection, checkpoint: &orbit_vstream::checkpoint::Checkpoint) -> Result<(), FanoutError> {
    use orbit_gtid::{Gtid, GtidSet};
    for (shard, position) in &checkpoint.positions {
        let Ok(position) = GtidSet::parse_position(position) else {
            continue;
        };
        let mut stmt = db
            .prepare("SELECT gtid FROM fanout_journal WHERE keyspace=?1 AND shard=?2")
            .map_err(error)?;
        let gtids = stmt
            .query_map(params![shard.keyspace, shard.shard], |r| r.get::<_, String>(0))
            .map_err(error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        for gtid in gtids {
            if Gtid::parse(&gtid).is_ok_and(|g| position.contains(&g)) {
                db.execute(
                    "DELETE FROM fanout_journal WHERE keyspace=?1 AND shard=?2 AND gtid=?3",
                    params![shard.keyspace, shard.shard, gtid],
                )
                .map_err(error)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::tests::schema;
    use orbit_protocol::cdc::RowOp;
    use serde_json::json;
    fn transaction(gtid: &str, row: serde_json::Value, op: RowOp) -> SourceTransaction {
        let row: Row = serde_json::from_value(row).unwrap();
        SourceTransaction {
            keyspace: "test".into(),
            shard: "0".into(),
            gtid: gtid.into(),
            position: format!("MySQL56/{gtid}"),
            commit_timestamp: 0,
            trace: Default::default(),
            changes: vec![RowChange {
                table: "permission".into(),
                key: vec![row["id"].clone()],
                op,
                before: if op == RowOp::Delete { Some(row.clone()) } else { None },
                after: if op == RowOp::Delete { None } else { Some(row) },
            }],
        }
    }
    #[test]
    fn grouped_decisions_match_sequential_and_survive_reopen_before_checkpoint() {
        let path = std::env::temp_dir().join(format!("orbit-fanout-group-{}.sqlite", uuid::Uuid::new_v4()));
        let schema = schema();
        let permission = json!({"id":"p1","docId":"d1","org":"recipient","user":"alice"});
        let grant = transaction("source:1", permission.clone(), RowOp::Insert);
        let revoke = transaction("source:2", permission, RowOp::Delete);
        let hydrated = vec![(
            "doc".into(),
            serde_json::from_value(json!({"id":"d1","org":"owner","name":"Original"})).unwrap(),
        )];
        let oracle = crate::state::StateStore::open_in_memory().unwrap();
        let expected = vec![
            oracle.route_fanout(&schema, &grant, &hydrated).unwrap(),
            oracle.route_fanout(&schema, &revoke, &[]).unwrap(),
        ];
        assert_eq!(expected[0]["recipient"][0].op, RowOp::Insert);
        assert_eq!(expected[1]["recipient"][0].op, RowOp::Delete);
        {
            let store = crate::state::StateStore::open(&path).unwrap();
            assert_eq!(
                store
                    .route_fanout_batch(&schema, &[(&grant, &hydrated), (&revoke, &[])])
                    .unwrap(),
                expected
            );
        }
        {
            let store = crate::state::StateStore::open(&path).unwrap();
            // No checkpoint has covered these decisions. Replaying a grant must return its
            // original insert even though the durable graph already contains the revocation.
            assert_eq!(
                store
                    .route_fanout_batch(&schema, &[(&grant, &[]), (&revoke, &[])])
                    .unwrap(),
                expected
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn late_group_failure_rolls_back_earlier_graph_and_journal_writes() {
        let path = std::env::temp_dir().join(format!("orbit-fanout-group-failure-{}.sqlite", uuid::Uuid::new_v4()));
        let schema = schema();
        let grant = transaction(
            "source:1",
            json!({"id":"p1","docId":"d1","org":"recipient","user":"alice"}),
            RowOp::Insert,
        );
        let hydrated = vec![(
            "doc".into(),
            serde_json::from_value(json!({"id":"d1","org":"owner"})).unwrap(),
        )];
        let mut invalid = grant.clone();
        invalid.gtid = "source:2".into();
        invalid.changes.push(RowChange {
            table: "doc".into(),
            op: RowOp::Insert,
            key: vec!["bad".into()],
            before: None,
            after: Some(serde_json::from_value(json!({"name":"missing key"})).unwrap()),
        });
        {
            let store = crate::state::StateStore::open(&path).unwrap();
            assert!(
                store
                    .route_fanout_batch(&schema, &[(&grant, &hydrated), (&invalid, &[])])
                    .is_err()
            );
            assert!(!store.fanout_has_journal(&grant).unwrap());
            assert!(!store.fanout_has_journal(&invalid).unwrap());
        }
        {
            let db = Connection::open(&path).unwrap();
            assert_eq!(
                db.query_row("SELECT count(*) FROM fanout_rows", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                db.query_row("SELECT count(*) FROM fanout_cells", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn content_updates_preserve_shared_fanout_without_caching_private_rows() {
        let db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        let schema = schema();
        let permission = json!({"id":"p1","docId":"d1","org":"recipient","user":"alice"});
        let original: Row = serde_json::from_value(json!({"id":"d1","org":"owner","name":"First"})).unwrap();
        route_with_hydration(
            &db,
            &schema,
            &transaction("source:1", permission.clone(), RowOp::Insert),
            &[("doc".into(), original.clone())],
        )
        .unwrap();
        let mut rename = transaction("source:2", permission.clone(), RowOp::Insert);
        let mut renamed = original.clone();
        renamed.insert("name".into(), "Renamed".into());
        rename.changes = vec![RowChange {
            table: "doc".into(),
            op: RowOp::Update,
            key: vec!["d1".into()],
            before: Some(original),
            after: Some(renamed.clone()),
        }];
        assert!(!changes_routing(&schema, &rename.changes[0]));
        let result = route(&db, &schema, &rename).unwrap();
        assert_eq!(result["recipient"][0].after.as_ref().unwrap()["name"], "Renamed");
        let mut private = rename.clone();
        private.gtid = "source:3".into();
        private.changes[0].key = vec!["private".into()];
        let change = &mut private.changes[0];
        for row in change.before.iter_mut().chain(change.after.iter_mut()) {
            row.insert("id".into(), "private".into());
        }
        assert!(route(&db, &schema, &private).unwrap().is_empty());
        assert!(Snapshot(&db).get("doc", &vec!["private".into()]).unwrap().is_none());
        let mut moved = rename.changes[0].clone();
        moved.after.as_mut().unwrap().insert("org".into(), "recipient".into());
        assert!(changes_routing(&schema, &moved));
        let revoke = transaction("source:4", permission, RowOp::Delete);
        assert_eq!(route(&db, &schema, &revoke).unwrap()["recipient"][0].op, RowOp::Delete);
        assert_eq!(
            db.query_row("SELECT count(*) FROM fanout_rows", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(route(&db, &schema, &rename).unwrap(), result);
    }

    #[test]
    fn affected_collection_matches_full_collection_across_relationship_changes() {
        use rand::{RngExt, SeedableRng};
        let schema = schema();
        for seed in 0..8 {
            let local = Connection::open_in_memory().unwrap();
            let oracle = Connection::open_in_memory().unwrap();
            initialize(&local).unwrap();
            initialize(&oracle).unwrap();
            for i in 0..20 {
                for (table, value) in [
                    (
                        "doc",
                        json!({"id":format!("d{i}"),"org":format!("o{}",i%3),"name":"Original"}),
                    ),
                    (
                        "permission",
                        json!({"id":format!("p{i}"),"docId":format!("d{i}"),"org":format!("o{}",(i+1)%3),"user":"reader"}),
                    ),
                    (
                        "metadata",
                        json!({"id":format!("m{i}"),"docId":format!("d{i}"),"summary":"Original"}),
                    ),
                ] {
                    let row: Row = serde_json::from_value(value).unwrap();
                    for db in [&local, &oracle] {
                        put(db, &schema, table, &row).unwrap();
                    }
                }
            }
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
            for step in 0..150 {
                let i = rng.random_range(0..20);
                let target = rng.random_range(0..20);
                let (table, value) = match rng.random_range(0..4) {
                    0 => (
                        "doc",
                        json!({"id":format!("d{i}"),"org":format!("o{}",i%3),"name":format!("name{step}")}),
                    ),
                    1 => (
                        "metadata",
                        json!({"id":format!("m{i}"),"docId":format!("d{target}"),"summary":"Summary"}),
                    ),
                    _ => (
                        "permission",
                        json!({"id":format!("p{i}"),"docId":format!("d{target}"),"org":format!("o{}",i%3),"user":"reader"}),
                    ),
                };
                let row: Row = serde_json::from_value(value).unwrap();
                let key = row_key(&schema, table, &row).unwrap();
                let old = OverlayRows::from([(
                    (table.to_string(), json(&key).unwrap()),
                    Snapshot(&local).get(table, &key).unwrap(),
                )]);
                let delete = rng.random_range(0..5) == 0;
                for db in [&local, &oracle] {
                    if delete {
                        remove(db, table, &key).unwrap();
                    } else {
                        put(db, &schema, table, &row).unwrap();
                    }
                }
                let before = Before {
                    current: Snapshot(&local),
                    rows: &old,
                    schema: &schema,
                };
                collect_affected_unshared(&local, &schema, &before, &old).unwrap();
                collect_unshared(&oracle, &schema).unwrap();
                let rows = |db: &Connection| -> Vec<(String, String, String)> {
                    db.prepare("SELECT tbl,key,image FROM fanout_rows ORDER BY tbl,key")
                        .unwrap()
                        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap()
                };
                assert_eq!(rows(&local), rows(&oracle), "seed={seed} step={step}");
            }
        }
    }

    #[test]
    fn replay_after_later_revocation_uses_durable_original_decision() {
        let path = std::env::temp_dir().join(format!("orbit-fanout-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let schema = schema();
        let permission = json!({"id":"p1","docId":"d1","org":"recipient","user":"alice"});
        let grant = transaction("source:1", permission.clone(), RowOp::Insert);
        let revoke = transaction("source:2", permission, RowOp::Delete);
        let original;
        {
            let db = Connection::open(&path).unwrap();
            initialize(&db).unwrap();
            put(
                &db,
                &schema,
                "doc",
                &serde_json::from_value(json!({"id":"d1","org":"owner","name":"Original"})).unwrap(),
            )
            .unwrap();
            original = route(&db, &schema, &grant).unwrap();
            assert_eq!(original["recipient"][0].op, RowOp::Insert);
            assert_eq!(route(&db, &schema, &revoke).unwrap()["recipient"][0].op, RowOp::Delete);
        }
        {
            let db = Connection::open(&path).unwrap();
            initialize(&db).unwrap();
            assert_eq!(route(&db, &schema, &grant).unwrap(), original);
            assert!(
                Snapshot(&db).get("permission", &vec!["p1".into()]).unwrap().is_none(),
                "replay must not roll the graph backwards"
            );
        }
        std::fs::remove_file(path).unwrap();
    }
    #[test]
    fn failed_projection_rolls_back_all_rows_and_does_not_journal() {
        let db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        let mut source = transaction(
            "source:1",
            json!({"id":"p1","docId":"d1","org":"recipient","user":"alice"}),
            RowOp::Insert,
        );
        source.changes.push(RowChange {
            table: "doc".into(),
            op: RowOp::Insert,
            key: vec!["d1".into()],
            before: None,
            after: Some(serde_json::from_value(json!({"name":"missing key"})).unwrap()),
        });
        assert!(route(&db, &schema(), &source).is_err());
        assert!(Snapshot(&db).get("permission", &vec!["p1".into()]).unwrap().is_none());
        assert_eq!(
            db.query_row("SELECT count(*) FROM fanout_journal", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn sparse_cache_hydrates_on_grant_and_drops_every_row_after_last_revoke() {
        let db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        let schema = schema();
        let permission = json!({"id":"p1","docId":"d1","org":"recipient","user":"alice"});
        let grant = transaction("source:1", permission.clone(), RowOp::Insert);
        let hydrated = vec![
            (
                "doc".into(),
                serde_json::from_value(json!({"id":"d1","org":"owner","name":"First"})).unwrap(),
            ),
            (
                "metadata".into(),
                serde_json::from_value(json!({"id":"m1","docId":"d1","summary":"Shared metadata"})).unwrap(),
            ),
        ];
        let result = route_with_hydration(&db, &schema, &grant, &hydrated).unwrap();
        assert_eq!(result["recipient"].len(), 2);
        assert_eq!(
            db.query_row("SELECT count(*) FROM fanout_rows", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
        let revoke = transaction("source:2", permission.clone(), RowOp::Delete);
        assert_eq!(route(&db, &schema, &revoke).unwrap()["recipient"].len(), 2);
        assert_eq!(
            db.query_row("SELECT count(*) FROM fanout_rows", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM fanout_cells", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        let hydrated = vec![(
            "doc".into(),
            serde_json::from_value(json!({"id":"d1","org":"owner","name":"Fresh on regrant"})).unwrap(),
        )];
        let regrant = transaction("source:3", permission, RowOp::Insert);
        let result = route_with_hydration(&db, &schema, &regrant, &hydrated).unwrap();
        assert_eq!(
            result["recipient"][0].after.as_ref().unwrap()["name"],
            "Fresh on regrant"
        );
    }

    #[test]
    fn unrelated_private_activity_does_not_accumulate_in_the_cross_org_cache() {
        let db = Connection::open_in_memory().unwrap();
        initialize(&db).unwrap();
        for i in 0..100 {
            let id = format!("private-{i}");
            let mut source = transaction(
                &format!("source:{}", i + 1),
                json!({"id":"unused","docId":id,"org":"owner","user":"alice"}),
                RowOp::Insert,
            );
            source.changes = vec![RowChange {
                table: "doc".into(),
                op: RowOp::Insert,
                key: vec![id.clone().into()],
                before: None,
                after: Some(serde_json::from_value(json!({"id":id,"org":"owner","name":"Private"})).unwrap()),
            }];
            assert!(route(&db, &schema(), &source).unwrap().is_empty());
            assert_eq!(
                db.query_row("SELECT count(*) FROM fanout_rows", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
    }
}
