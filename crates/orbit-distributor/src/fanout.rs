//! Permission-driven replication through declared relation paths.
//!
//! Routing compares two immutable source snapshots surrounding a transaction. Looking up
//! permissions in the live database here would make retry/replay non-deterministic. A caller
//! must persist the snapshots with its checkpoint, or journal this result before delivery.
//! A grant emits the existing row as an insert; the last revoke emits a delete. Multiple
//! permission rows for the same recipient count as one membership. Ownership is never rewritten.

use std::collections::{BTreeMap, BTreeSet};

use indexmap::IndexMap;
use orbit_protocol::cdc::{RowChange, RowOp};
use orbit_protocol::schema::{RelationSchema, SyncSchema, TableSchema};
use orbit_protocol::value::{CellValue, Row, RowKey};

use crate::router::render_partition_key;

#[derive(Debug, thiserror::Error)]
pub enum FanoutError {
    #[error("invalid partition route: {0}")]
    InvalidRoute(String),
    #[error("routing snapshot: {0}")]
    Snapshot(String),
}

/// An immutable, indexed view of projected source rows at a transaction boundary. Matching
/// uses SQL equality: NULL must not join to NULL. Implementations must return full row images.
pub trait RoutingSnapshot {
    fn get(&self, table: &str, key: &RowKey) -> Result<Option<Row>, FanoutError>;
    fn matching(&self, table: &str, columns: &[String], values: &[CellValue]) -> Result<Vec<Row>, FanoutError>;
}

type Path<'a> = Vec<(&'a TableSchema, &'a RelationSchema)>;

fn resolve<'a>(schema: &'a SyncSchema, root: &'a TableSchema, names: &[String]) -> Result<Path<'a>, FanoutError> {
    if names.is_empty() || names.len() > 8 {
        return Err(FanoutError::InvalidRoute(format!(
            "{}: expected 1–8 relations",
            root.name
        )));
    }
    let mut table = root;
    let mut path = Vec::new();
    for name in names {
        let relation = table
            .relations
            .iter()
            .find(|r| &r.name == name)
            .ok_or_else(|| FanoutError::InvalidRoute(format!("{}.{}", table.name, name)))?;
        path.push((table, relation));
        table = schema
            .table(&relation.target_table)
            .ok_or_else(|| FanoutError::InvalidRoute(relation.target_table.clone()))?;
    }
    if table.partition_parent.is_some() {
        return Err(FanoutError::InvalidRoute(format!("{} is derived", table.name)));
    }
    Ok(path)
}

fn cells(row: &Row, columns: &[String]) -> Result<Vec<CellValue>, FanoutError> {
    columns
        .iter()
        .map(|c| {
            row.get(c)
                .cloned()
                .ok_or_else(|| FanoutError::Snapshot(format!("missing column {c}")))
        })
        .collect()
}

fn key(table: &TableSchema, row: &Row) -> Result<RowKey, FanoutError> {
    cells(row, &table.primary_key)
}

fn matches(
    snapshot: &dyn RoutingSnapshot,
    table: &str,
    columns: &[String],
    values: Vec<CellValue>,
) -> Result<Vec<Row>, FanoutError> {
    if values.iter().any(|v| matches!(v, CellValue::Null)) {
        return Ok(vec![]);
    }
    snapshot.matching(table, columns, &values)
}

fn owning_partition(
    schema: &SyncSchema,
    table: &TableSchema,
    row: &Row,
    snapshot: &dyn RoutingSnapshot,
) -> Result<Option<String>, FanoutError> {
    let own = cells(row, std::slice::from_ref(&table.partition_column))?.remove(0);
    let owner = if let Some(parent) = &table.partition_parent {
        let parent_schema = schema
            .table(parent)
            .ok_or_else(|| FanoutError::InvalidRoute(parent.clone()))?;
        snapshot
            .get(parent, &vec![own])?
            .and_then(|r| r.get(&parent_schema.partition_column).cloned())
            .unwrap_or(CellValue::Null)
    } else {
        own
    };
    render_partition_key(schema.partition.key_kind, &owner)
        .map_err(|v| FanoutError::Snapshot(format!("invalid owner partition {v}")))
}

pub(crate) fn partitions(
    schema: &SyncSchema,
    table: &TableSchema,
    row: &Row,
    snapshot: &dyn RoutingSnapshot,
) -> Result<BTreeSet<String>, FanoutError> {
    let owner = owning_partition(schema, table, row, snapshot)?;
    let mut partitions = BTreeSet::new();
    let mut add = |value: &CellValue| -> Result<(), FanoutError> {
        if let Some(p) = render_partition_key(schema.partition.key_kind, value)
            .map_err(|v| FanoutError::Snapshot(format!("invalid partition {v}")))?
            && Some(&p) != owner.as_ref()
        {
            partitions.insert(p);
        }
        Ok(())
    };
    for names in &table.partition_routes {
        let path = resolve(schema, table, names)?;
        let mut rows = vec![row.clone()];
        for (_, relation) in &path {
            let mut next = Vec::new();
            for row in rows {
                next.extend(matches(
                    snapshot,
                    &relation.target_table,
                    &relation.to_columns,
                    cells(&row, &relation.from_columns)?,
                )?);
            }
            rows = next;
        }
        let target = schema
            .table(&path.last().expect("nonempty").1.target_table)
            .expect("resolved");
        for row in rows {
            add(&cells(&row, std::slice::from_ref(&target.partition_column))?.remove(0))?;
        }
    }
    Ok(partitions)
}

/// Route affected roots, including unchanged roots whose permission path changed. The caller
/// appends this to ordinary routing. Owning partitions remain on the existing direct path;
/// this result contains only additional recipients.
pub fn route_fanout(
    schema: &SyncSchema,
    changes: &[RowChange],
    before: &dyn RoutingSnapshot,
    after: &dyn RoutingSnapshot,
) -> Result<IndexMap<String, Vec<RowChange>>, FanoutError> {
    let mut out: IndexMap<String, Vec<RowChange>> = IndexMap::new();
    for root in schema.tables.iter().filter(|t| !t.partition_routes.is_empty()) {
        // JSON keys sort deterministically, independent of source row lookup order.
        let mut candidates: BTreeMap<String, RowKey> = BTreeMap::new();
        let mut collect = |row: &Row| -> Result<(), FanoutError> {
            let key = key(root, row)?;
            candidates.insert(serde_json::to_string(&key).expect("key serializes"), key);
            Ok(())
        };
        for change in changes {
            if change.table == root.name {
                for row in change.before.iter().chain(change.after.iter()) {
                    collect(row)?;
                }
            }
            for names in &root.partition_routes {
                let path = resolve(schema, root, names)?;
                for (offset, (_, relation)) in path.iter().enumerate() {
                    if relation.target_table != change.table {
                        continue;
                    }
                    for (snapshot, image) in [(before, &change.before), (after, &change.after)] {
                        let Some(image) = image else {
                            continue;
                        };
                        let mut rows = vec![image.clone()];
                        for (source, relation) in path[..=offset].iter().rev() {
                            let mut previous = Vec::new();
                            for row in rows {
                                previous.extend(matches(
                                    snapshot,
                                    &source.name,
                                    &relation.from_columns,
                                    cells(&row, &relation.to_columns)?,
                                )?);
                            }
                            rows = previous;
                        }
                        for row in rows {
                            collect(&row)?;
                        }
                    }
                }
            }
        }
        for key in candidates.into_values() {
            let old = before.get(&root.name, &key)?;
            let new = after.get(&root.name, &key)?;
            let old_partitions = old
                .as_ref()
                .map(|r| partitions(schema, root, r, before))
                .transpose()?
                .unwrap_or_default();
            let new_partitions = new
                .as_ref()
                .map(|r| partitions(schema, root, r, after))
                .transpose()?
                .unwrap_or_default();
            let new_owner = new
                .as_ref()
                .map(|r| owning_partition(schema, root, r, after))
                .transpose()?
                .flatten();
            for partition in old_partitions.union(&new_partitions) {
                // Direct routing owns this row now; a foreign-membership delete must not
                // erase the owner's insert when ownership moves into a recipient org.
                if new_owner.as_ref() == Some(partition) {
                    continue;
                }
                let had = old_partitions.contains(partition);
                let has = new_partitions.contains(partition);
                let op = match (had, has) {
                    (true, true) if old == new => continue,
                    (true, true) => RowOp::Update,
                    (true, false) => RowOp::Delete,
                    (false, true) => RowOp::Insert,
                    (false, false) => unreachable!(),
                };
                out.entry(partition.clone()).or_default().push(RowChange {
                    table: root.name.clone(),
                    op,
                    key: key.clone(),
                    before: if had { old.clone() } else { None },
                    after: if has { new.clone() } else { None },
                });
            }
        }
    }
    Ok(out)
}

pub(crate) fn retain_dependencies(
    schema: &SyncSchema,
    table: &TableSchema,
    row: &Row,
    snapshot: &dyn RoutingSnapshot,
    keep: &mut BTreeSet<(String, String)>,
) -> Result<(), FanoutError> {
    let mut retain = |table: &TableSchema, row: &Row| -> Result<(), FanoutError> {
        keep.insert((
            table.name.clone(),
            serde_json::to_string(&key(table, row)?).expect("key serializes"),
        ));
        Ok(())
    };
    retain(table, row)?;
    if let Some(parent) = &table.partition_parent {
        let parent = schema
            .table(parent)
            .ok_or_else(|| FanoutError::InvalidRoute(parent.clone()))?;
        if let Some(parent_row) = snapshot.get(&parent.name, &vec![row[&table.partition_column].clone()])? {
            retain(parent, &parent_row)?;
        }
    }
    for path in &table.partition_routes {
        let mut rows = vec![row.clone()];
        for (_, relation) in resolve(schema, table, path)? {
            let target = schema.table(&relation.target_table).expect("resolved");
            let mut next = Vec::new();
            for row in rows {
                next.extend(matches(
                    snapshot,
                    &target.name,
                    &relation.to_columns,
                    cells(&row, &relation.from_columns)?,
                )?);
            }
            for row in &next {
                retain(target, row)?;
            }
            rows = next;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Clone, Default)]
    struct Snapshot(BTreeMap<(String, String), Row>);
    impl Snapshot {
        fn put(&mut self, table: &str, value: serde_json::Value) {
            let row: Row = serde_json::from_value(value).unwrap();
            let key = serde_json::to_string(&vec![row["id"].clone()]).unwrap();
            self.0.insert((table.into(), key), row);
        }
        fn remove(&mut self, table: &str, id: &str) {
            self.0
                .remove(&(table.into(), serde_json::to_string(&vec![id]).unwrap()));
        }
    }
    impl RoutingSnapshot for Snapshot {
        fn get(&self, table: &str, key: &RowKey) -> Result<Option<Row>, FanoutError> {
            Ok(self
                .0
                .get(&(table.into(), serde_json::to_string(key).unwrap()))
                .cloned())
        }
        fn matching(&self, table: &str, columns: &[String], values: &[CellValue]) -> Result<Vec<Row>, FanoutError> {
            Ok(self
                .0
                .iter()
                .filter(|((t, _), r)| t == table && columns.iter().zip(values).all(|(c, v)| r.get(c) == Some(v)))
                .map(|(_, r)| r.clone())
                .collect())
        }
    }
    pub(crate) fn schema() -> SyncSchema {
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
    fn base() -> Snapshot {
        let mut s = Snapshot::default();
        s.put("doc", json!({"id":"shared","org":"owner","name":"Shared"}));
        s.put("doc", json!({"id":"private","org":"owner","name":"Secret"}));
        s.put("metadata", json!({"id":"meta","docId":"shared","summary":"Summary"}));
        s
    }
    fn grant(s: &mut Snapshot, id: &str, user: &str, org: &str) {
        s.put("permission", json!({"id":id,"docId":"shared","org":org,"user":user}));
    }
    fn change(before: &Snapshot, after: &Snapshot, table: &str, id: &str) -> RowChange {
        let key = vec![CellValue::from(id)];
        let before = before.get(table, &key).unwrap();
        let after = after.get(table, &key).unwrap();
        let op = if before.is_none() {
            RowOp::Insert
        } else if after.is_none() {
            RowOp::Delete
        } else {
            RowOp::Update
        };
        RowChange {
            table: table.into(),
            key,
            before,
            after,
            op,
        }
    }
    #[test]
    fn grant_hydrates_existing_document_and_metadata_without_private_neighbors() {
        let before = base();
        let mut after = before.clone();
        grant(&mut after, "p1", "alice", "recipient");
        let routed = route_fanout(
            &schema(),
            &[change(&before, &after, "permission", "p1")],
            &before,
            &after,
        )
        .unwrap();
        assert_eq!(routed.keys().collect::<Vec<_>>(), vec!["recipient"]);
        assert_eq!(routed["recipient"].len(), 2);
        assert!(routed["recipient"].iter().all(|c| c.op == RowOp::Insert));
        assert_eq!(routed["recipient"][0].after.as_ref().unwrap()["org"], "owner");
        assert!(
            !routed["recipient"]
                .iter()
                .any(|c| c.key == vec![CellValue::from("private")])
        );
    }
    #[test]
    fn revoking_one_user_preserves_other_users_share_and_last_revoke_removes_rows() {
        let mut before = base();
        grant(&mut before, "p1", "alice", "recipient");
        grant(&mut before, "p2", "bob", "recipient");
        let mut after = before.clone();
        after.remove("permission", "p1");
        assert!(
            route_fanout(
                &schema(),
                &[change(&before, &after, "permission", "p1")],
                &before,
                &after
            )
            .unwrap()
            .is_empty()
        );
        let before = after.clone();
        after.remove("permission", "p2");
        let result = route_fanout(
            &schema(),
            &[change(&before, &after, "permission", "p2")],
            &before,
            &after,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result["recipient"].len(), 2);
        assert!(
            result["recipient"]
                .iter()
                .all(|c| c.op == RowOp::Delete && c.after.is_none())
        );
    }
    #[test]
    fn live_edit_fans_out_to_each_foreign_recipient_once() {
        let mut before = base();
        grant(&mut before, "p1", "alice", "recipient");
        grant(&mut before, "p2", "bob", "recipient");
        grant(&mut before, "p3", "eve", "another");
        grant(&mut before, "p4", "author", "owner");
        let mut after = before.clone();
        after.put("doc", json!({"id":"shared","org":"owner","name":"Renamed"}));
        let result = route_fanout(&schema(), &[change(&before, &after, "doc", "shared")], &before, &after).unwrap();
        assert_eq!(result.len(), 2);
        for changes in result.values() {
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].op, RowOp::Update);
            assert_eq!(changes[0].after.as_ref().unwrap()["name"], "Renamed");
        }
    }
    #[test]
    fn ownership_transfer_to_recipient_does_not_delete_its_existing_rows() {
        let mut before = base();
        grant(&mut before, "p1", "alice", "recipient");
        let mut after = before.clone();
        after.put("doc", json!({"id":"shared","org":"recipient","name":"Shared"}));
        let result = route_fanout(&schema(), &[change(&before, &after, "doc", "shared")], &before, &after).unwrap();
        assert!(!result.contains_key("recipient"));
    }
    #[test]
    fn moving_permission_between_orgs_sends_delete_and_insert() {
        let mut before = base();
        grant(&mut before, "p1", "alice", "old");
        let mut after = before.clone();
        grant(&mut after, "p1", "alice", "new");
        let changes = vec![change(&before, &after, "permission", "p1")];
        let result = route_fanout(&schema(), &changes, &before, &after).unwrap();
        assert!(result["old"].iter().all(|c| c.op == RowOp::Delete));
        assert!(result["new"].iter().all(|c| c.op == RowOp::Insert));
        // Identical immutable inputs always produce byte-identical routing decisions.
        assert_eq!(
            serde_json::to_string(&result).unwrap(),
            serde_json::to_string(&route_fanout(&schema(), &changes, &before, &after).unwrap()).unwrap()
        );
    }
    #[test]
    fn simultaneous_grant_and_insert_does_not_require_an_earlier_parent_snapshot() {
        let before = Snapshot::default();
        let mut after = base();
        grant(&mut after, "p1", "alice", "recipient");
        let changes = vec![
            change(&before, &after, "permission", "p1"),
            change(&before, &after, "metadata", "meta"),
            change(&before, &after, "doc", "shared"),
        ];
        let result = route_fanout(&schema(), &changes, &before, &after).unwrap();
        assert_eq!(result["recipient"].len(), 2);
        assert!(!result.contains_key("owner"));
    }
    #[test]
    fn reverse_tag_routes_hydrate_and_remove_only_the_shared_tag() {
        let mut schema = schema();
        let column = |name: &str| json!({"name":name,"kind":"string","nullable":false,"source_type":"varchar(191)"});
        schema.tables.push(serde_json::from_value(json!({"name":"tag","primary_key":["id"],"partition_column":"org",
            "partition_routes":[["links","document","permissions"]],"columns":[column("id"),column("org")],
            "relations":[{"name":"links","kind":"many","target_table":"link","from_columns":["id"],"to_columns":["tagId"]}]})).unwrap());
        schema.tables.push(serde_json::from_value(json!({"name":"link","primary_key":["id"],"partition_column":"docId","partition_parent":"doc",
            "partition_routes":[["document","permissions"]],"columns":[column("id"),column("docId"),column("tagId")],
            "relations":[{"name":"document","kind":"one","target_table":"doc","from_columns":["docId"],"to_columns":["id"]}]})).unwrap());
        schema.schema_hash = schema.compute_hash();
        schema.validate().unwrap();
        let mut before = base();
        before.put("tag", json!({"id":"tag1","org":"owner"}));
        before.put("tag", json!({"id":"secret-tag","org":"owner"}));
        before.put("link", json!({"id":"link1","docId":"shared","tagId":"tag1"}));
        before.put("link", json!({"id":"link2","docId":"private","tagId":"secret-tag"}));
        let mut after = before.clone();
        grant(&mut after, "p1", "alice", "recipient");
        let result = route_fanout(&schema, &[change(&before, &after, "permission", "p1")], &before, &after).unwrap();
        assert_eq!(result["recipient"].len(), 4);
        assert!(
            result["recipient"]
                .iter()
                .any(|r| r.table == "tag" && r.key == vec![CellValue::from("tag1")])
        );
        assert!(
            !result["recipient"]
                .iter()
                .any(|r| r.key == vec![CellValue::from("secret-tag")])
        );
        let before = after.clone();
        after.remove("link", "link1");
        let result = route_fanout(&schema, &[change(&before, &after, "link", "link1")], &before, &after).unwrap();
        assert_eq!(result["recipient"].len(), 2);
        assert!(result["recipient"].iter().all(|r| r.op == RowOp::Delete));
    }
}
