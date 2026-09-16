//! Routing: which logical partition does each row change belong to?
//!
//! Routing is a pure function of the sync schema and the row images. There is no
//! application-specific code path: the partition of a row is the value of the table's
//! `partition_column` in the relevant image.
//!
//! * insert: partition of `after`
//! * delete: partition of `before`
//! * update with the same partition before and after: that partition
//! * update that changes the partition ("move"): a delete to the old partition and an insert to
//!   the new one, so each partition sees a self-contained change
//! * NULL partition: the change is not routed anywhere and is counted as unpartitioned
//!
//! A table with `partition_parent` is partitioned through its parent: `partition_column` holds
//! the parent's primary key, and the partition is looked up in the parent index
//! ([`ParentLookup`]). A NULL or unknown parent yields no partition; the change is counted as
//! `unresolved_parent` and skipped. A change of the parent column is a move, like a change of
//! the partition column of a direct table.

use indexmap::IndexMap;
use orbit_protocol::cdc::{RowChange, RowOp};
use orbit_protocol::schema::{SyncSchema, TableSchema, ValueKind};
use orbit_protocol::value::CellValue;
use tracing::warn;

/// Rendered partition key. Strings are used as-is; integers as decimal digits.
pub type PartitionKey = String;

/// Resolves the partition of a parent row: `(parent table, rendered parent key)` to the rendered
/// partition, or `None` when the parent is unknown.
pub type ParentLookup<'a> = &'a dyn Fn(&str, &str) -> Option<PartitionKey>;

/// A lookup that knows no parent. For schemas without derived tables.
pub fn no_parents(_table: &str, _key: &str) -> Option<PartitionKey> {
    None
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RoutingError {
    #[error("table {table} is not in the sync schema")]
    UnknownTable { table: String },
    #[error("table {table}: partition column {column} is missing from the row image")]
    MissingPartitionColumn { table: String, column: String },
    #[error("table {table}: partition value {value} does not have kind {kind:?}")]
    InvalidPartitionValue {
        table: String,
        value: String,
        kind: ValueKind,
    },
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RoutingStats {
    pub routed: u64,
    pub unpartitioned: u64,
    pub moves: u64,
    /// Changes of derived tables whose parent is NULL or not in the parent index.
    pub unresolved_parent: u64,
}

/// Routes the changes of one transaction. Order within each partition follows the transaction's
/// row order, so intra-transaction dependencies (parent before child) are preserved.
///
/// `lookup` resolves the partition of parent rows for tables with `partition_parent`. The caller
/// must apply the parent changes of this transaction to the index before calling `route`, so a
/// child inserted together with its parent resolves.
pub fn route(
    schema: &SyncSchema,
    changes: &[RowChange],
    lookup: ParentLookup<'_>,
) -> Result<(IndexMap<PartitionKey, Vec<RowChange>>, RoutingStats), RoutingError> {
    let mut out: IndexMap<PartitionKey, Vec<RowChange>> = IndexMap::new();
    let mut stats = RoutingStats::default();
    for change in changes {
        let table = schema.table(&change.table).ok_or_else(|| RoutingError::UnknownTable {
            table: change.table.clone(),
        })?;
        let before_key = change
            .before
            .as_ref()
            .map(|r| partition_of(schema, table, r, lookup))
            .transpose()?;
        let after_key = change
            .after
            .as_ref()
            .map(|r| partition_of(schema, table, r, lookup))
            .transpose()?;
        match change.op {
            RowOp::Insert => push(&mut out, &mut stats, table, after_key.flatten(), change.clone()),
            RowOp::Delete => push(&mut out, &mut stats, table, before_key.flatten(), change.clone()),
            RowOp::Update => {
                let b = before_key.flatten();
                let a = after_key.flatten();
                if a == b {
                    push(&mut out, &mut stats, table, a, change.clone());
                } else {
                    stats.moves += 1;
                    let delete = RowChange {
                        table: change.table.clone(),
                        op: RowOp::Delete,
                        key: table_key(table, change.before.as_ref().expect("update has before")),
                        before: change.before.clone(),
                        after: None,
                    };
                    let insert = RowChange {
                        table: change.table.clone(),
                        op: RowOp::Insert,
                        key: change.key.clone(),
                        before: None,
                        after: change.after.clone(),
                    };
                    push(&mut out, &mut stats, table, b, delete);
                    push(&mut out, &mut stats, table, a, insert);
                }
            }
        }
    }
    Ok((out, stats))
}

fn push(
    out: &mut IndexMap<PartitionKey, Vec<RowChange>>,
    stats: &mut RoutingStats,
    table: &TableSchema,
    key: Option<PartitionKey>,
    change: RowChange,
) {
    match key {
        Some(k) => {
            stats.routed += 1;
            out.entry(k).or_default().push(change);
        }
        None if table.partition_parent.is_some() => {
            stats.unresolved_parent += 1;
            let image = change.after.as_ref().or(change.before.as_ref());
            let parent_key = image
                .and_then(|r| r.get(&table.partition_column))
                .map(|v| v.to_string())
                .unwrap_or_default();
            warn!(
                table = %table.name,
                key = %serde_json::to_string(&change.key).unwrap_or_default(),
                parent = %table.partition_parent.as_deref().unwrap_or_default(),
                parent_key = %parent_key,
                "parent partition unresolved; change skipped"
            );
        }
        None => stats.unpartitioned += 1,
    }
}

fn table_key(table: &TableSchema, row: &orbit_protocol::value::Row) -> Vec<CellValue> {
    table
        .primary_key
        .iter()
        .map(|c| row.get(c).cloned().unwrap_or(CellValue::Null))
        .collect()
}

/// Reads and renders the partition key of a row image. `None` for SQL NULL, and for a derived
/// table also when the parent row is unknown to `lookup`.
pub fn partition_of(
    schema: &SyncSchema,
    table: &TableSchema,
    row: &orbit_protocol::value::Row,
    lookup: ParentLookup<'_>,
) -> Result<Option<PartitionKey>, RoutingError> {
    let value = row
        .get(&table.partition_column)
        .ok_or_else(|| RoutingError::MissingPartitionColumn {
            table: table.name.clone(),
            column: table.partition_column.clone(),
        })?;
    let Some(parent_name) = &table.partition_parent else {
        return render_partition_key(schema.partition.key_kind, value).map_err(|value| {
            RoutingError::InvalidPartitionValue {
                table: table.name.clone(),
                value,
                kind: schema.partition.key_kind,
            }
        });
    };
    let parent = schema.table(parent_name).ok_or_else(|| RoutingError::UnknownTable {
        table: parent_name.clone(),
    })?;
    let kind = parent_key_kind(parent);
    let key = render_partition_key(kind, value).map_err(|value| RoutingError::InvalidPartitionValue {
        table: table.name.clone(),
        value,
        kind,
    })?;
    Ok(key.and_then(|k| lookup(parent_name, &k)))
}

/// The kind of the single primary key column of a parent table (validated by the schema).
pub fn parent_key_kind(parent: &TableSchema) -> ValueKind {
    parent
        .columns
        .iter()
        .find(|c| Some(&c.name) == parent.primary_key.first())
        .map(|c| c.kind)
        .unwrap_or(ValueKind::String)
}

/// Renders the primary key of a parent row image, as stored in the parent index. `None` for a
/// NULL key (which a validated schema does not allow).
pub fn parent_key_of(parent: &TableSchema, row: &orbit_protocol::value::Row) -> Result<Option<String>, RoutingError> {
    let column = parent.primary_key.first().cloned().unwrap_or_default();
    let value = row.get(&column).ok_or_else(|| RoutingError::MissingPartitionColumn {
        table: parent.name.clone(),
        column: column.clone(),
    })?;
    let kind = parent_key_kind(parent);
    render_partition_key(kind, value).map_err(|value| RoutingError::InvalidPartitionValue {
        table: parent.name.clone(),
        value,
        kind,
    })
}

/// Renders a partition value according to the configured key kind. Err carries the offending
/// value rendered for diagnostics.
pub fn render_partition_key(kind: ValueKind, value: &CellValue) -> Result<Option<PartitionKey>, String> {
    match (kind, value) {
        (_, CellValue::Null) => Ok(None),
        (ValueKind::String, CellValue::String(s)) => Ok(Some(s.clone())),
        (ValueKind::Int, CellValue::Number(n)) if n.is_i64() || n.is_u64() => Ok(Some(n.to_string())),
        (ValueKind::BigInt, CellValue::String(s)) if is_integer_text(s) => Ok(Some(s.clone())),
        (ValueKind::BigInt, CellValue::Number(n)) if n.is_i64() || n.is_u64() => Ok(Some(n.to_string())),
        (_, other) => Err(other.to_string()),
    }
}

fn is_integer_text(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig};

    fn schema(kind: ValueKind) -> SyncSchema {
        SyncSchema {
            format_version: 1,
            schema_hash: String::new(),
            app: "t".into(),
            keyspace: "k".into(),
            partition: PartitionConfig {
                name: "org".into(),
                key_kind: kind,
                placement: PlacementConfig::OnePerPartition { version: 1 },
            },
            tables: vec![TableSchema {
                name: "t".into(),
                primary_key: vec!["id".into()],
                partition_column: "org".into(),
                partition_parent: None,
                partition_routes: vec![],
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        kind: ValueKind::String,
                        nullable: false,
                        source_type: "varchar(1)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "org".into(),
                        kind,
                        nullable: true,
                        source_type: "varchar(1)".into(),
                        enum_values: None,
                        derived: None,
                    },
                ],
                relations: vec![],
            }],
        }
    }

    fn row(id: &str, org: CellValue) -> orbit_protocol::value::Row {
        let mut r = IndexMap::new();
        r.insert("id".into(), CellValue::from(id));
        r.insert("org".into(), org);
        r
    }

    #[test]
    fn routes_by_partition_column_and_splits_moves() {
        let s = schema(ValueKind::String);
        let changes = vec![
            RowChange {
                table: "t".into(),
                op: RowOp::Insert,
                key: vec!["a".into()],
                before: None,
                after: Some(row("a", "o1".into())),
            },
            RowChange {
                table: "t".into(),
                op: RowOp::Update,
                key: vec!["a".into()],
                before: Some(row("a", "o1".into())),
                after: Some(row("a", "o2".into())),
            },
            RowChange {
                table: "t".into(),
                op: RowOp::Delete,
                key: vec!["b".into()],
                before: Some(row("b", "o2".into())),
                after: None,
            },
            RowChange {
                table: "t".into(),
                op: RowOp::Insert,
                key: vec!["c".into()],
                before: None,
                after: Some(row("c", CellValue::Null)),
            },
        ];
        let (routed, stats) = route(&s, &changes, &no_parents).unwrap();
        assert_eq!(
            stats,
            RoutingStats {
                routed: 4,
                unpartitioned: 1,
                moves: 1,
                unresolved_parent: 0,
            }
        );
        assert_eq!(routed["o1"].len(), 2);
        assert_eq!(routed["o1"][1].op, RowOp::Delete);
        assert_eq!(routed["o1"][1].key, vec![CellValue::from("a")]);
        assert_eq!(routed["o2"].len(), 2);
        assert_eq!(routed["o2"][0].op, RowOp::Insert);
        assert_eq!(routed["o2"][0].after.as_ref().unwrap()["org"], "o2");
        assert_eq!(routed["o2"][1].op, RowOp::Delete);
        assert_eq!(
            routed.keys().collect::<Vec<_>>(),
            vec!["o1", "o2"],
            "partition order follows first appearance"
        );
    }

    #[test]
    fn integer_keys_render_as_digits_and_wrong_kinds_fail() {
        let s = schema(ValueKind::Int);
        let changes = vec![RowChange {
            table: "t".into(),
            op: RowOp::Insert,
            key: vec!["a".into()],
            before: None,
            after: Some(row("a", CellValue::from(42))),
        }];
        let (routed, _) = route(&s, &changes, &no_parents).unwrap();
        assert!(routed.contains_key("42"));
        let bad = vec![RowChange {
            table: "t".into(),
            op: RowOp::Insert,
            key: vec!["a".into()],
            before: None,
            after: Some(row("a", "x".into())),
        }];
        assert!(matches!(
            route(&s, &bad, &no_parents),
            Err(RoutingError::InvalidPartitionValue { .. })
        ));
        let unknown = vec![RowChange {
            table: "nope".into(),
            op: RowOp::Insert,
            key: vec![],
            before: None,
            after: Some(row("a", CellValue::from(1))),
        }];
        assert!(matches!(
            route(&s, &unknown, &no_parents),
            Err(RoutingError::UnknownTable { .. })
        ));
    }

    /// A parent table `p(id, org)` and a derived child `c(id, pid)` partitioned through `p`.
    fn derived_schema(kind: ValueKind) -> SyncSchema {
        let mut s = schema(ValueKind::String);
        s.tables[0].name = "p".into();
        s.tables[0].columns[0].kind = kind;
        s.tables.push(TableSchema {
            name: "c".into(),
            primary_key: vec!["id".into()],
            partition_column: "pid".into(),
            partition_parent: Some("p".into()),
            partition_routes: vec![],
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(1)".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "pid".into(),
                    kind,
                    nullable: true,
                    source_type: "varchar(1)".into(),
                    enum_values: None,
                    derived: None,
                },
            ],
            relations: vec![],
        });
        s.schema_hash = s.compute_hash();
        s.validate().unwrap();
        s
    }

    fn child(id: &str, pid: CellValue) -> orbit_protocol::value::Row {
        let mut r = IndexMap::new();
        r.insert("id".into(), CellValue::from(id));
        r.insert("pid".into(), pid);
        r
    }

    fn index(entries: &[(&str, &str)]) -> impl Fn(&str, &str) -> Option<PartitionKey> {
        let entries: Vec<(String, String)> = entries.iter().map(|(k, p)| (k.to_string(), p.to_string())).collect();
        move |table: &str, key: &str| {
            assert_eq!(table, "p");
            entries.iter().find(|(k, _)| k == key).map(|(_, p)| p.clone())
        }
    }

    #[test]
    fn derived_tables_route_through_the_parent_index() {
        let s = derived_schema(ValueKind::String);
        let lookup = index(&[("p1", "o1"), ("p2", "o2")]);
        let changes = vec![
            RowChange {
                table: "c".into(),
                op: RowOp::Insert,
                key: vec!["a".into()],
                before: None,
                after: Some(child("a", "p1".into())),
            },
            RowChange {
                table: "c".into(),
                op: RowOp::Update,
                key: vec!["a".into()],
                before: Some(child("a", "p1".into())),
                after: Some(child("a", "p1".into())),
            },
            RowChange {
                table: "c".into(),
                op: RowOp::Delete,
                key: vec!["b".into()],
                before: Some(child("b", "p2".into())),
                after: None,
            },
            // The parent's own row routes directly, as before.
            RowChange {
                table: "p".into(),
                op: RowOp::Insert,
                key: vec!["p2".into()],
                before: None,
                after: Some(row("p2", "o2".into())),
            },
        ];
        let (routed, stats) = route(&s, &changes, &lookup).unwrap();
        assert_eq!(
            stats,
            RoutingStats {
                routed: 4,
                unpartitioned: 0,
                moves: 0,
                unresolved_parent: 0,
            }
        );
        assert_eq!(routed["o1"].len(), 2);
        assert_eq!(routed["o1"][0].op, RowOp::Insert);
        assert_eq!(routed["o1"][1].op, RowOp::Update);
        assert_eq!(routed["o2"].len(), 2);
        assert_eq!(routed["o2"][0].op, RowOp::Delete);
        assert_eq!(routed["o2"][0].table, "c");
        assert_eq!(routed["o2"][1].table, "p");
    }

    #[test]
    fn derived_move_through_the_parent_splits_into_delete_and_insert() {
        let s = derived_schema(ValueKind::Int);
        let lookup = index(&[("1", "o1"), ("2", "o2")]);
        let changes = vec![RowChange {
            table: "c".into(),
            op: RowOp::Update,
            key: vec!["a".into()],
            before: Some(child("a", CellValue::from(1))),
            after: Some(child("a", CellValue::from(2))),
        }];
        let (routed, stats) = route(&s, &changes, &lookup).unwrap();
        assert_eq!(stats.moves, 1);
        assert_eq!(stats.routed, 2);
        assert_eq!(routed["o1"].len(), 1);
        assert_eq!(routed["o1"][0].op, RowOp::Delete);
        assert_eq!(routed["o1"][0].key, vec![CellValue::from("a")]);
        assert_eq!(routed["o2"].len(), 1);
        assert_eq!(routed["o2"][0].op, RowOp::Insert);
        assert_eq!(routed["o2"][0].after.as_ref().unwrap()["pid"], 2);
    }

    #[test]
    fn unknown_or_null_parent_is_counted_as_unresolved() {
        let s = derived_schema(ValueKind::String);
        let lookup = index(&[("p1", "o1")]);
        let changes = vec![
            RowChange {
                table: "c".into(),
                op: RowOp::Insert,
                key: vec!["a".into()],
                before: None,
                after: Some(child("a", "nope".into())),
            },
            RowChange {
                table: "c".into(),
                op: RowOp::Insert,
                key: vec!["b".into()],
                before: None,
                after: Some(child("b", CellValue::Null)),
            },
            // A move from an unknown parent to a known one: only the insert side is routed.
            RowChange {
                table: "c".into(),
                op: RowOp::Update,
                key: vec!["c".into()],
                before: Some(child("c", "nope".into())),
                after: Some(child("c", "p1".into())),
            },
            // A direct-table NULL is still counted as unpartitioned, not as unresolved.
            RowChange {
                table: "p".into(),
                op: RowOp::Insert,
                key: vec!["p9".into()],
                before: None,
                after: Some(row("p9", CellValue::Null)),
            },
        ];
        let (routed, stats) = route(&s, &changes, &lookup).unwrap();
        assert_eq!(
            stats,
            RoutingStats {
                routed: 1,
                unpartitioned: 1,
                moves: 1,
                unresolved_parent: 3,
            }
        );
        assert_eq!(routed.len(), 1);
        assert_eq!(routed["o1"][0].key, vec![CellValue::from("c")]);
        assert_eq!(routed["o1"][0].op, RowOp::Insert);

        // A parent key of the wrong kind is an error, like a bad partition value.
        let bad = vec![RowChange {
            table: "c".into(),
            op: RowOp::Insert,
            key: vec!["a".into()],
            before: None,
            after: Some(child("a", CellValue::from(7))),
        }];
        assert!(matches!(
            route(&s, &bad, &lookup),
            Err(RoutingError::InvalidPartitionValue {
                kind: ValueKind::String,
                ..
            })
        ));
    }

    #[test]
    fn parent_keys_render_like_partition_keys() {
        let s = derived_schema(ValueKind::Int);
        let p = s.table("p").unwrap();
        assert_eq!(parent_key_kind(p), ValueKind::Int);
        assert_eq!(
            parent_key_of(p, &row("x", "o".into())).unwrap_err(),
            RoutingError::InvalidPartitionValue {
                table: "p".into(),
                value: "\"x\"".into(),
                kind: ValueKind::Int,
            }
        );
        let mut r = IndexMap::new();
        r.insert("id".to_string(), CellValue::from(42));
        r.insert("org".to_string(), CellValue::from("o"));
        assert_eq!(parent_key_of(p, &r).unwrap().as_deref(), Some("42"));
        let s = derived_schema(ValueKind::String);
        let p = s.table("p").unwrap();
        assert_eq!(parent_key_of(p, &row("k", "o".into())).unwrap().as_deref(), Some("k"));
    }
}
