//! The sync schema artifact and the raw introspected database schema.
//!
//! The application compiles its sync configuration into a [`SyncSchema`] (see
//! `packages/schema`). The Rust distributor, the Durable Object and the browser client all load
//! the same artifact. Its `schema_hash` is the identity that every runtime compares.

use std::collections::{BTreeMap, BTreeSet};

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How a column's values are encoded on the wire and typed in consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ValueKind {
    Bool,
    Int,
    BigInt,
    Float,
    Decimal,
    String,
    Bytes,
    Json,
    DateTime,
    Date,
    Time,
}

impl ValueKind {
    /// True when SQLite `ORDER BY` and range comparisons on the stored representation agree with
    /// MySQL's semantics closely enough to be allowed in queries (binary string collation).
    pub fn is_orderable(self) -> bool {
        !matches!(self, ValueKind::Decimal | ValueKind::Json | ValueKind::Bytes)
    }
}

/// The compiled sync schema. `schema_hash` is computed by [`SyncSchema::compute_hash`] over the
/// canonical JSON of every other field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SyncSchema {
    /// Artifact format version. See `SYNC_SCHEMA_FORMAT_VERSION`.
    pub format_version: u32,
    /// SHA-256 hex of the canonical artifact without this field.
    pub schema_hash: String,
    /// Application-chosen name, used in metrics and Durable Object namespaces.
    pub app: String,
    /// Vitess keyspace the tables live in.
    pub keyspace: String,
    pub partition: PartitionConfig,
    /// Tables in dependency-friendly order (parents before children where relations exist).
    pub tables: Vec<TableSchema>,
}

/// Logical partitioning and physical placement configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PartitionConfig {
    /// Human name of the logical partition key, for example `organization`.
    pub name: String,
    /// Value kind of the partition key. Only `string`, `int` and `bigint` are allowed.
    pub key_kind: ValueKind,
    /// Physical placement strategy. Changing it invalidates every Durable Object's state.
    pub placement: PlacementConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
pub enum PlacementConfig {
    /// One Durable Object per logical partition. `version` namespaces the DO names so a future
    /// strategy can migrate without colliding with old objects.
    OnePerPartition { version: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableSchema {
    /// Source table name in the keyspace.
    pub name: String,
    /// Primary key column names in key order. Must be non-empty and be a subset of `columns`.
    pub primary_key: Vec<String>,
    /// Column that holds the logical partition key. Must be in `columns`. Rows with a NULL
    /// partition value are not routed anywhere and are counted as `unpartitioned`.
    ///
    /// When `partition_parent` is set, this column holds the primary key of a row of the parent
    /// table instead, and the row's partition is derived from that parent row.
    pub partition_column: String,
    /// Derived partitioning. When set, `partition_column` holds the primary key value of a row of
    /// this table (a synced table that is partitioned directly, with no `partition_parent` of its
    /// own), and the row's partition is the partition of that parent row. The parent's primary
    /// key must be a single `string`, `int` or `bigint` column, and `partition_column` must have
    /// the same kind. Only one level is allowed. A row whose parent is NULL or unknown is not
    /// routed and is counted as `unresolved_parent`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_parent: Option<String>,
    /// Additional partitions reached through declared relation paths. The last table must be
    /// directly partitioned. These routes replicate rows; caller authorization still belongs
    /// in the subscription query. Empty routes preserve existing schema hashes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_routes: Vec<Vec<String>>,
    /// Synced columns, in order. Source columns not listed are ignored.
    pub columns: Vec<ColumnSchema>,
    /// Declared relations to other synced tables, usable in query includes.
    pub relations: Vec<RelationSchema>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnSchema {
    pub name: String,
    pub kind: ValueKind,
    pub nullable: bool,
    /// Full MySQL column type as introspected, for example `varchar(191)` or `enum('A','B')`.
    pub source_type: String,
    /// Allowed values for enum columns, in definition order (index 1 is the first value).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
    /// Set when the engine computes this column from a source column instead of reading it.
    /// A derived column is a non-nullable `bool`; the source column itself need not be synced,
    /// so its value never leaves the engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived: Option<DerivedColumn>,
}

/// How the engine computes a derived column. Both the live stream and every fill apply the rule
/// to the raw source cell, so the value is the same on every path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DerivedColumn {
    /// Source column in the live table.
    pub from: String,
    pub rule: DerivedRule,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DerivedRule {
    /// `true` when the source cell is not NULL.
    NotNull,
    /// `true` when the source cell is not NULL and its text starts with `prefix`.
    StartsWith { prefix: String },
}

impl DerivedRule {
    /// Applies the rule to a raw source cell (`None` is SQL NULL).
    pub fn apply(&self, raw: Option<&[u8]>) -> bool {
        match (self, raw) {
            (_, None) => false,
            (DerivedRule::NotNull, Some(_)) => true,
            (DerivedRule::StartsWith { prefix }, Some(bytes)) => bytes.starts_with(prefix.as_bytes()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    /// `from_columns` on this table reference `to_columns` (the primary key) on the target.
    One,
    /// `to_columns` on the target reference `from_columns` (the primary key) on this table.
    Many,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RelationSchema {
    pub name: String,
    pub kind: RelationKind,
    pub target_table: String,
    pub from_columns: Vec<String>,
    pub to_columns: Vec<String>,
}

impl SyncSchema {
    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.iter().find(|t| t.name == name)
    }

    /// Canonical hash over every field except `schema_hash`.
    pub fn compute_hash(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut clone = self.clone();
        clone.schema_hash = String::new();
        let canonical = canonical_json(&serde_json::to_value(&clone).expect("schema serializes"));
        let digest = Sha256::digest(canonical.as_bytes());
        base16(&digest)
    }

    /// Structural validation. Returns every problem found, so a misconfiguration is reported once.
    pub fn validate(&self) -> Result<(), Vec<SchemaValidationError>> {
        let mut errors = Vec::new();
        if self.format_version != crate::SYNC_SCHEMA_FORMAT_VERSION {
            errors.push(SchemaValidationError::FormatVersion {
                expected: crate::SYNC_SCHEMA_FORMAT_VERSION,
                got: self.format_version,
            });
        }
        if self.schema_hash != self.compute_hash() {
            errors.push(SchemaValidationError::HashMismatch {
                expected: self.compute_hash(),
                got: self.schema_hash.clone(),
            });
        }
        if !matches!(
            self.partition.key_kind,
            ValueKind::String | ValueKind::Int | ValueKind::BigInt
        ) {
            errors.push(SchemaValidationError::PartitionKeyKind(self.partition.key_kind));
        }
        if self.tables.is_empty() {
            errors.push(SchemaValidationError::NoTables);
        }
        let mut seen = BTreeSet::new();
        let names: BTreeMap<&str, &TableSchema> = self.tables.iter().map(|t| (t.name.as_str(), t)).collect();
        for t in &self.tables {
            if !seen.insert(t.name.as_str()) {
                errors.push(SchemaValidationError::DuplicateTable(t.name.clone()));
            }
            let cols: IndexMap<&str, &ColumnSchema> = t.columns.iter().map(|c| (c.name.as_str(), c)).collect();
            if cols.len() != t.columns.len() {
                errors.push(SchemaValidationError::DuplicateColumn { table: t.name.clone() });
            }
            for c in &t.columns {
                let Some(derived) = &c.derived else { continue };
                if c.kind != ValueKind::Bool || c.nullable {
                    errors.push(SchemaValidationError::DerivedColumnShape {
                        table: t.name.clone(),
                        column: c.name.clone(),
                    });
                }
                let source_is_derived = cols.get(derived.from.as_str()).is_some_and(|s| s.derived.is_some());
                if derived.from == c.name || source_is_derived {
                    errors.push(SchemaValidationError::DerivedFromDerived {
                        table: t.name.clone(),
                        column: c.name.clone(),
                    });
                }
                if t.primary_key.contains(&c.name) || t.partition_column == c.name {
                    errors.push(SchemaValidationError::DerivedKeyColumn {
                        table: t.name.clone(),
                        column: c.name.clone(),
                    });
                }
            }
            if t.primary_key.is_empty() {
                errors.push(SchemaValidationError::EmptyPrimaryKey { table: t.name.clone() });
            }
            for pk in &t.primary_key {
                match cols.get(pk.as_str()) {
                    None => errors.push(SchemaValidationError::UnknownColumn {
                        table: t.name.clone(),
                        column: pk.clone(),
                        role: "primary_key",
                    }),
                    Some(c) if c.nullable => errors.push(SchemaValidationError::NullablePrimaryKey {
                        table: t.name.clone(),
                        column: pk.clone(),
                    }),
                    Some(c) if matches!(c.kind, ValueKind::Json | ValueKind::Float) => {
                        errors.push(SchemaValidationError::UnsupportedKeyKind {
                            table: t.name.clone(),
                            column: pk.clone(),
                            kind: c.kind,
                        })
                    }
                    Some(_) => {}
                }
            }
            // The kind the partition column must have: the partition key kind for a directly
            // partitioned table, the parent's primary key kind for a derived one.
            let mut expected_kind = Some(self.partition.key_kind);
            if let Some(parent_name) = &t.partition_parent {
                match names.get(parent_name.as_str()) {
                    None => {
                        errors.push(SchemaValidationError::UnknownPartitionParent {
                            table: t.name.clone(),
                            parent: parent_name.clone(),
                        });
                        expected_kind = None;
                    }
                    Some(parent) => {
                        if parent.partition_parent.is_some() {
                            errors.push(SchemaValidationError::NestedPartitionParent {
                                table: t.name.clone(),
                                parent: parent_name.clone(),
                            });
                        }
                        let parent_key = if parent.primary_key.len() == 1 {
                            parent.columns.iter().find(|c| c.name == parent.primary_key[0])
                        } else {
                            None
                        };
                        match parent_key {
                            Some(k) if matches!(k.kind, ValueKind::String | ValueKind::Int | ValueKind::BigInt) => {
                                expected_kind = Some(k.kind);
                            }
                            _ => {
                                errors.push(SchemaValidationError::PartitionParentKey {
                                    table: t.name.clone(),
                                    parent: parent_name.clone(),
                                });
                                expected_kind = None;
                            }
                        }
                    }
                }
            }
            match cols.get(t.partition_column.as_str()) {
                None => errors.push(SchemaValidationError::UnknownColumn {
                    table: t.name.clone(),
                    column: t.partition_column.clone(),
                    role: "partition_column",
                }),
                Some(c) => {
                    if let Some(expected) = expected_kind
                        && c.kind != expected
                    {
                        errors.push(SchemaValidationError::PartitionColumnKind {
                            table: t.name.clone(),
                            column: t.partition_column.clone(),
                            expected,
                            got: c.kind,
                        })
                    }
                }
            }
            for path in &t.partition_routes {
                let mut current = t;
                let mut valid = !path.is_empty() && path.len() <= 8;
                for step in path {
                    let next = current
                        .relations
                        .iter()
                        .find(|r| &r.name == step)
                        .and_then(|r| names.get(r.target_table.as_str()).copied());
                    match next {
                        Some(next) => current = next,
                        None => {
                            valid = false;
                            break;
                        }
                    }
                }
                if !valid || current.partition_parent.is_some() {
                    errors.push(SchemaValidationError::InvalidPartitionRoute {
                        table: t.name.clone(),
                        path: path.clone(),
                    });
                }
            }
            let mut rel_names = BTreeSet::new();
            for r in &t.relations {
                if !rel_names.insert(r.name.as_str()) {
                    errors.push(SchemaValidationError::DuplicateRelation {
                        table: t.name.clone(),
                        relation: r.name.clone(),
                    });
                }
                let Some(target) = names.get(r.target_table.as_str()) else {
                    errors.push(SchemaValidationError::UnknownRelationTarget {
                        table: t.name.clone(),
                        relation: r.name.clone(),
                        target: r.target_table.clone(),
                    });
                    continue;
                };
                if r.from_columns.is_empty() || r.from_columns.len() != r.to_columns.len() {
                    errors.push(SchemaValidationError::RelationArity {
                        table: t.name.clone(),
                        relation: r.name.clone(),
                    });
                }
                for c in &r.from_columns {
                    if !cols.contains_key(c.as_str()) {
                        errors.push(SchemaValidationError::UnknownColumn {
                            table: t.name.clone(),
                            column: c.clone(),
                            role: "relation.from_columns",
                        });
                    }
                }
                for c in &r.to_columns {
                    if !target.columns.iter().any(|tc| &tc.name == c) {
                        errors.push(SchemaValidationError::UnknownColumn {
                            table: r.target_table.clone(),
                            column: c.clone(),
                            role: "relation.to_columns",
                        });
                    }
                }
                let (pk_side, pk_cols) = match r.kind {
                    RelationKind::One => (r.target_table.as_str(), (&r.to_columns, &target.primary_key)),
                    RelationKind::Many => (t.name.as_str(), (&r.from_columns, &t.primary_key)),
                };
                if pk_cols.0 != pk_cols.1 {
                    errors.push(SchemaValidationError::RelationNotOnPrimaryKey {
                        table: t.name.clone(),
                        relation: r.name.clone(),
                        pk_table: pk_side.to_string(),
                    });
                }
            }
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaValidationError {
    #[error("unsupported sync schema format version: expected {expected}, got {got}")]
    FormatVersion { expected: u32, got: u32 },
    #[error("schema_hash mismatch: expected {expected}, got {got}")]
    HashMismatch { expected: String, got: String },
    #[error("partition key kind {0:?} is not supported (use string, int or bigint)")]
    PartitionKeyKind(ValueKind),
    #[error("sync schema has no tables")]
    NoTables,
    #[error("duplicate table {0}")]
    DuplicateTable(String),
    #[error("table {table} has duplicate columns")]
    DuplicateColumn { table: String },
    #[error("table {table} has an empty primary key")]
    EmptyPrimaryKey { table: String },
    #[error("table {table}: {role} references unknown column {column}")]
    UnknownColumn {
        table: String,
        column: String,
        role: &'static str,
    },
    #[error("table {table}: primary key column {column} is nullable")]
    NullablePrimaryKey { table: String, column: String },
    #[error("table {table}: primary key column {column} has unsupported kind {kind:?}")]
    UnsupportedKeyKind {
        table: String,
        column: String,
        kind: ValueKind,
    },
    #[error("table {table}: derived column {column} must be a non-nullable bool")]
    DerivedColumnShape { table: String, column: String },
    #[error("table {table}: derived column {column} must derive from a plain source column")]
    DerivedFromDerived { table: String, column: String },
    #[error("table {table}: derived column {column} cannot be a primary key or partition column")]
    DerivedKeyColumn { table: String, column: String },
    #[error("table {table}: partition column {column} has kind {got:?}, expected {expected:?}")]
    PartitionColumnKind {
        table: String,
        column: String,
        expected: ValueKind,
        got: ValueKind,
    },
    #[error("table {table}: partition_parent {parent} is not a synced table")]
    UnknownPartitionParent { table: String, parent: String },
    #[error("table {table}: partition_parent {parent} has a partition_parent of its own (one level only)")]
    NestedPartitionParent { table: String, parent: String },
    #[error("table {table}: partition_parent {parent} must have a single string, int or bigint primary key column")]
    PartitionParentKey { table: String, parent: String },
    #[error(
        "table {table}: partition route {path:?} must contain 1–8 declared relations and end at a directly partitioned table"
    )]
    InvalidPartitionRoute { table: String, path: Vec<String> },
    #[error("table {table}: duplicate relation {relation}")]
    DuplicateRelation { table: String, relation: String },
    #[error("table {table}: relation {relation} targets unknown table {target}")]
    UnknownRelationTarget {
        table: String,
        relation: String,
        target: String,
    },
    #[error("table {table}: relation {relation} has mismatched column lists")]
    RelationArity { table: String, relation: String },
    #[error("table {table}: relation {relation} must reference the full primary key of {pk_table}")]
    RelationNotOnPrimaryKey {
        table: String,
        relation: String,
        pk_table: String,
    },
}

/// Raw metadata read from `information_schema`. Produced by `orbit-server schema introspect`
/// and consumed by the TypeScript sync config DSL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntrospectedSchema {
    pub keyspace: String,
    /// Server version string, for example `8.0.43-Vitess`.
    pub server_version: String,
    pub tables: Vec<IntrospectedTable>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntrospectedTable {
    pub name: String,
    pub primary_key: Vec<String>,
    pub columns: Vec<IntrospectedColumn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IntrospectedColumn {
    pub name: String,
    /// `information_schema.COLUMNS.COLUMN_TYPE`, for example `varchar(191)`.
    pub column_type: String,
    /// `information_schema.COLUMNS.DATA_TYPE`, for example `varchar`.
    pub data_type: String,
    pub nullable: bool,
    /// The value kind the engine infers for this column. `tinyint(1)` maps to `bool`.
    pub kind: ValueKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<String>>,
}

/// Infers the wire value kind from MySQL column metadata. `tinyint(1)` is treated as bool, as
/// MySQL itself does for the `BOOLEAN` alias.
pub fn infer_kind(data_type: &str, column_type: &str) -> ValueKind {
    let ct = column_type.to_ascii_lowercase();
    match data_type.to_ascii_lowercase().as_str() {
        "tinyint" if ct.starts_with("tinyint(1)") => ValueKind::Bool,
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "year" => ValueKind::Int,
        "bigint" | "bit" => ValueKind::BigInt,
        "float" | "double" | "real" => ValueKind::Float,
        "decimal" | "numeric" => ValueKind::Decimal,
        "json" => ValueKind::Json,
        "datetime" | "timestamp" => ValueKind::DateTime,
        "date" => ValueKind::Date,
        "time" => ValueKind::Time,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" | "geometry" | "vector" => {
            ValueKind::Bytes
        }
        _ => ValueKind::String,
    }
}

/// Parses `enum('A','B')` or `set('x','y')` into its values. Handles doubled quotes.
pub fn parse_enum_values(column_type: &str) -> Option<Vec<String>> {
    let lower = column_type.to_ascii_lowercase();
    let body = if let Some(rest) = lower.strip_prefix("enum(") {
        &column_type[5..5 + rest.len()]
    } else {
        let rest = lower.strip_prefix("set(")?;
        &column_type[4..4 + rest.len()]
    };
    let body = body.strip_suffix(')')?;
    let mut values = Vec::new();
    let mut chars = body.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut cur = String::new();
        loop {
            match chars.next() {
                Some('\'') => {
                    if chars.peek() == Some(&'\'') {
                        chars.next();
                        cur.push('\'');
                    } else {
                        break;
                    }
                }
                Some('\\') => {
                    if let Some(n) = chars.next() {
                        cur.push(n);
                    }
                }
                Some(ch) => cur.push(ch),
                None => return None,
            }
        }
        values.push(cur);
    }
    Some(values)
}

/// Canonical JSON: object keys sorted, no whitespace. Used for hashing only.
pub fn canonical_json(v: &serde_json::Value) -> String {
    fn write(v: &serde_json::Value, out: &mut String) {
        match v {
            serde_json::Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                out.push('{');
                for (i, k) in keys.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    out.push_str(&serde_json::to_string(k).expect("string"));
                    out.push(':');
                    write(&map[*k], out);
                }
                out.push('}');
            }
            serde_json::Value::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&serde_json::to_string(other).expect("scalar")),
        }
    }
    let mut out = String::new();
    write(v, &mut out);
    out
}

fn base16(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn sample() -> SyncSchema {
        let mut s = SyncSchema {
            format_version: crate::SYNC_SCHEMA_FORMAT_VERSION,
            schema_hash: String::new(),
            app: "demo".into(),
            keyspace: "orbit".into(),
            partition: PartitionConfig {
                name: "organization".into(),
                key_kind: ValueKind::String,
                placement: PlacementConfig::OnePerPartition { version: 1 },
            },
            tables: vec![
                TableSchema {
                    name: "organization".into(),
                    primary_key: vec!["id".into()],
                    partition_column: "id".into(),
                    partition_parent: None,
                    partition_routes: vec![],
                    columns: vec![
                        ColumnSchema {
                            name: "id".into(),
                            kind: ValueKind::String,
                            nullable: false,
                            source_type: "varchar(191)".into(),
                            enum_values: None,
                            derived: None,
                        },
                        ColumnSchema {
                            name: "name".into(),
                            kind: ValueKind::String,
                            nullable: false,
                            source_type: "text".into(),
                            enum_values: None,
                            derived: None,
                        },
                    ],
                    relations: vec![],
                },
                TableSchema {
                    name: "Chatbot".into(),
                    primary_key: vec!["id".into()],
                    partition_column: "organizationId".into(),
                    partition_parent: None,
                    partition_routes: vec![],
                    columns: vec![
                        ColumnSchema {
                            name: "id".into(),
                            kind: ValueKind::String,
                            nullable: false,
                            source_type: "varchar(191)".into(),
                            enum_values: None,
                            derived: None,
                        },
                        ColumnSchema {
                            name: "organizationId".into(),
                            kind: ValueKind::String,
                            nullable: true,
                            source_type: "varchar(191)".into(),
                            enum_values: None,
                            derived: None,
                        },
                        ColumnSchema {
                            name: "groupId".into(),
                            kind: ValueKind::String,
                            nullable: true,
                            source_type: "varchar(191)".into(),
                            enum_values: None,
                            derived: None,
                        },
                        ColumnSchema {
                            name: "type".into(),
                            kind: ValueKind::String,
                            nullable: false,
                            source_type: "enum('DOCUMENT','GROUP')".into(),
                            enum_values: Some(vec!["DOCUMENT".into(), "GROUP".into()]),
                            derived: None,
                        },
                        ColumnSchema {
                            name: "deleted".into(),
                            kind: ValueKind::Bool,
                            nullable: false,
                            source_type: "tinyint(1)".into(),
                            enum_values: None,
                            derived: None,
                        },
                    ],
                    relations: vec![
                        RelationSchema {
                            name: "folder".into(),
                            kind: RelationKind::One,
                            target_table: "Chatbot".into(),
                            from_columns: vec!["groupId".into()],
                            to_columns: vec!["id".into()],
                        },
                        RelationSchema {
                            name: "organization".into(),
                            kind: RelationKind::One,
                            target_table: "organization".into(),
                            from_columns: vec!["organizationId".into()],
                            to_columns: vec!["id".into()],
                        },
                    ],
                },
            ],
        };
        s.schema_hash = s.compute_hash();
        s
    }

    #[test]
    fn sample_validates_and_hash_is_stable() {
        let s = sample();
        s.validate().unwrap();
        let again = sample();
        assert_eq!(s.schema_hash, again.schema_hash);
        let mut changed = sample();
        changed.tables[1].columns.pop();
        assert_ne!(changed.compute_hash(), s.schema_hash);
    }

    #[test]
    fn derived_columns_must_be_plain_bools_off_the_key() {
        let derived = |name: &str, from: &str, kind: ValueKind, nullable: bool| ColumnSchema {
            name: name.into(),
            kind,
            nullable,
            source_type: "derived".into(),
            enum_values: None,
            derived: Some(DerivedColumn {
                from: from.into(),
                rule: DerivedRule::NotNull,
            }),
        };
        let mut ok = sample();
        ok.tables[0]
            .columns
            .push(derived("named", "name", ValueKind::Bool, false));
        ok.schema_hash = ok.compute_hash();
        ok.validate().unwrap();

        let mut bad = sample();
        bad.tables[0]
            .columns
            .push(derived("wrong", "name", ValueKind::String, true));
        bad.tables[0]
            .columns
            .push(derived("self_ref", "self_ref", ValueKind::Bool, false));
        bad.tables[0]
            .columns
            .push(derived("chained", "wrong", ValueKind::Bool, false));
        bad.tables[0]
            .columns
            .push(derived("keyed", "name", ValueKind::Bool, false));
        bad.tables[0].primary_key.push("keyed".into());
        bad.schema_hash = bad.compute_hash();
        let errs = bad.validate().unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::DerivedColumnShape { column, .. } if column == "wrong"))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::DerivedFromDerived { column, .. } if column == "self_ref"))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::DerivedFromDerived { column, .. } if column == "chained"))
        );
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::DerivedKeyColumn { column, .. } if column == "keyed"))
        );
        assert_eq!(
            DerivedRule::StartsWith { prefix: "gs://".into() }.apply(Some(b"gs://x")),
            true
        );
        assert_eq!(
            DerivedRule::StartsWith { prefix: "gs://".into() }.apply(Some(b"s3://x")),
            false
        );
        assert_eq!(DerivedRule::NotNull.apply(None), false);
    }

    #[test]
    fn validation_reports_all_problems() {
        let mut s = sample();
        s.tables[1].partition_column = "nope".into();
        s.tables[1].relations[0].to_columns = vec!["groupId".into()];
        s.schema_hash = "bad".into();
        let errs = s.validate().unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::HashMismatch { .. }))
        );
        assert!(errs.iter().any(|e| matches!(
            e,
            SchemaValidationError::UnknownColumn {
                role: "partition_column",
                ..
            }
        )));
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::RelationNotOnPrimaryKey { .. }))
        );
    }

    /// A child table that carries the parent's primary key instead of the partition key.
    fn derived_table(parent: &str, key_kind: ValueKind) -> TableSchema {
        TableSchema {
            name: "DocumentEntityLink".into(),
            primary_key: vec!["chatbotId".into(), "entityId".into()],
            partition_column: "chatbotId".into(),
            partition_parent: Some(parent.into()),
            partition_routes: vec![],
            columns: vec![
                ColumnSchema {
                    name: "chatbotId".into(),
                    kind: key_kind,
                    nullable: false,
                    source_type: "varchar(191)".into(),
                    enum_values: None,
                    derived: None,
                },
                ColumnSchema {
                    name: "entityId".into(),
                    kind: ValueKind::String,
                    nullable: false,
                    source_type: "varchar(191)".into(),
                    enum_values: None,
                    derived: None,
                },
            ],
            relations: vec![],
        }
    }

    #[test]
    fn partition_parent_is_omitted_from_json_and_hash_when_absent() {
        let s = sample();
        let json = serde_json::to_value(&s).unwrap();
        for t in json["tables"].as_array().unwrap() {
            assert!(t.get("partition_parent").is_none());
        }
        let parsed: SyncSchema = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, s);
        assert_eq!(parsed.compute_hash(), s.schema_hash);
    }

    #[test]
    fn partition_parent_validates_and_changes_the_hash() {
        let mut s = sample();
        s.tables.push(derived_table("Chatbot", ValueKind::String));
        s.schema_hash = s.compute_hash();
        s.validate().unwrap();
        assert_ne!(s.schema_hash, sample().schema_hash);
        let json = serde_json::to_value(&s).unwrap();
        assert_eq!(json["tables"][2]["partition_parent"], "Chatbot");
        let parsed: SyncSchema = serde_json::from_value(json).unwrap();
        assert_eq!(parsed, s);

        // Unknown parent.
        let mut s = sample();
        s.tables.push(derived_table("Nope", ValueKind::String));
        s.schema_hash = s.compute_hash();
        let errs = s.validate().unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::UnknownPartitionParent { .. }))
        );

        // A parent that is itself derived: one level only.
        let mut s = sample();
        s.tables.push(derived_table("Chatbot", ValueKind::String));
        let mut second = derived_table("DocumentEntityLink", ValueKind::String);
        second.name = "Grandchild".into();
        second.partition_column = "entityId".into();
        s.tables.push(second);
        s.schema_hash = s.compute_hash();
        let errs = s.validate().unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            SchemaValidationError::NestedPartitionParent { parent, .. } if parent == "DocumentEntityLink"
        )));

        // The parent's primary key must be a single column.
        let mut s = sample();
        s.tables.push(derived_table("Chatbot", ValueKind::String));
        s.tables[1].primary_key = vec!["id".into(), "type".into()];
        s.schema_hash = s.compute_hash();
        let errs = s.validate().unwrap_err();
        assert!(
            errs.iter()
                .any(|e| matches!(e, SchemaValidationError::PartitionParentKey { .. }))
        );

        // The child's partition column must have the parent's key kind, not the partition kind.
        let mut s = sample();
        s.tables.push(derived_table("Chatbot", ValueKind::Int));
        s.schema_hash = s.compute_hash();
        let errs = s.validate().unwrap_err();
        assert!(errs.iter().any(|e| matches!(
            e,
            SchemaValidationError::PartitionColumnKind {
                expected: ValueKind::String,
                got: ValueKind::Int,
                ..
            }
        )));
    }

    #[test]
    fn kind_inference() {
        assert_eq!(infer_kind("tinyint", "tinyint(1)"), ValueKind::Bool);
        assert_eq!(infer_kind("tinyint", "tinyint"), ValueKind::Int);
        assert_eq!(infer_kind("int", "int unsigned"), ValueKind::Int);
        assert_eq!(infer_kind("bigint", "bigint unsigned"), ValueKind::BigInt);
        assert_eq!(infer_kind("datetime", "datetime(3)"), ValueKind::DateTime);
        assert_eq!(infer_kind("enum", "enum('A','B')"), ValueKind::String);
        assert_eq!(infer_kind("json", "json"), ValueKind::Json);
        assert_eq!(infer_kind("blob", "blob"), ValueKind::Bytes);
    }

    #[test]
    fn enum_parsing() {
        assert_eq!(
            parse_enum_values("enum('A','B','C')"),
            Some(vec!["A".into(), "B".into(), "C".into()])
        );
        assert_eq!(parse_enum_values("set('x','y')"), Some(vec!["x".into(), "y".into()]));
        assert_eq!(
            parse_enum_values("enum('it''s','a\\'b')"),
            Some(vec!["it's".into(), "a'b".into()])
        );
        assert_eq!(parse_enum_values("varchar(10)"), None);
    }

    #[test]
    fn canonical_json_sorts_keys() {
        let v = serde_json::json!({"b": [1, {"z": 1, "a": 2}], "a": "x"});
        assert_eq!(canonical_json(&v), r#"{"a":"x","b":[1,{"a":2,"z":1}]}"#);
    }
    #[test]
    fn partition_routes_validate_and_preserve_old_hash_when_empty() {
        let mut schema = sample();
        let original = schema.compute_hash();
        assert!(
            serde_json::to_value(&schema).unwrap()["tables"][1]
                .get("partition_routes")
                .is_none()
        );
        let relation = schema.tables[1].relations[0].name.clone();
        schema.tables[1].partition_routes = vec![vec![relation.clone()]];
        schema.schema_hash = schema.compute_hash();
        schema.validate().unwrap();
        assert_ne!(schema.schema_hash, original);
        for path in [vec![], vec!["missing".into()], vec![relation; 9]] {
            schema.tables[1].partition_routes = vec![path];
            schema.schema_hash = schema.compute_hash();
            assert!(
                schema
                    .validate()
                    .unwrap_err()
                    .iter()
                    .any(|e| matches!(e, SchemaValidationError::InvalidPartitionRoute { .. }))
            );
        }
    }
}
