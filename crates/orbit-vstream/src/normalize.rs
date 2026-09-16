//! Projection of raw Vitess rows onto the sync schema.
//!
//! A [`TableProjection`] is built once per FIELD event. It validates that every synced column
//! exists in the live table with a compatible type, and records where each synced column sits in
//! the field list. Any mismatch is a [`VStreamError::SchemaMismatch`], which is fatal: the engine
//! never guesses what a renamed or retyped column means.

use base64::Engine as _;
use indexmap::IndexMap;
use orbit_protocol::schema::{ColumnSchema, TableSchema, ValueKind, infer_kind, parse_enum_values};
use orbit_protocol::value::{CellValue, Row, RowKey};

use crate::decode::{RawRow, TableFields};
use crate::error::VStreamError;
use crate::proto::query::{Field, Type as PbType};

#[derive(Debug, Clone)]
pub struct TableProjection {
    pub table: String,
    /// For each synced column: its index in the live field list.
    columns: Vec<ProjectedColumn>,
    /// Indexes (into `columns`) of the primary key columns, in key order.
    key_indexes: Vec<usize>,
    /// Index (into `columns`) of the partition column.
    partition_index: usize,
    enum_set_string_values: bool,
}

#[derive(Debug, Clone)]
struct ProjectedColumn {
    schema: ColumnSchema,
    field_index: usize,
    /// Enum / set values from the live column definition, for ordinal decoding.
    live_enum_values: Option<Vec<String>>,
    is_set: bool,
}

impl TableProjection {
    pub fn build(schema: &TableSchema, fields: &TableFields) -> Result<Self, VStreamError> {
        let mut columns = Vec::with_capacity(schema.columns.len());
        for col in &schema.columns {
            // A derived column reads its source cell; the source need not be synced.
            let source = col.derived.as_ref().map_or(col.name.as_str(), |d| d.from.as_str());
            let (idx, field) = fields
                .fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.name == source)
                .ok_or_else(|| VStreamError::SchemaMismatch {
                    table: schema.name.clone(),
                    message: format!("synced column {source} is missing from the live table"),
                })?;
            if col.derived.is_some() {
                columns.push(ProjectedColumn {
                    schema: col.clone(),
                    field_index: idx,
                    live_enum_values: None,
                    is_set: false,
                });
                continue;
            }
            let live_kind = live_kind(field);
            if live_kind != col.kind {
                return Err(VStreamError::SchemaMismatch {
                    table: schema.name.clone(),
                    message: format!(
                        "column {} has live type {} (kind {:?}) but the sync schema expects kind {:?}",
                        col.name, field.column_type, live_kind, col.kind
                    ),
                });
            }
            let lower = field.column_type.to_ascii_lowercase();
            let is_set = lower.starts_with("set(");
            let live_enum_values = parse_enum_values(&field.column_type);
            if let (Some(expected), Some(live)) = (&col.enum_values, &live_enum_values)
                && expected != live
            {
                return Err(VStreamError::SchemaMismatch {
                    table: schema.name.clone(),
                    message: format!(
                        "column {} enum values changed: expected {expected:?}, live {live:?}",
                        col.name
                    ),
                });
            }
            columns.push(ProjectedColumn {
                schema: col.clone(),
                field_index: idx,
                live_enum_values,
                is_set,
            });
        }
        let key_indexes = schema
            .primary_key
            .iter()
            .map(|pk| {
                columns
                    .iter()
                    .position(|c| &c.schema.name == pk)
                    .expect("validated schema")
            })
            .collect();
        let partition_index = columns
            .iter()
            .position(|c| c.schema.name == schema.partition_column)
            .expect("validated schema");
        Ok(Self {
            table: schema.name.clone(),
            columns,
            key_indexes,
            partition_index,
            enum_set_string_values: fields.enum_set_string_values,
        })
    }

    /// Projects a raw row to a sync schema row.
    pub fn project(&self, raw: &RawRow) -> Result<Row, VStreamError> {
        let mut row: Row = IndexMap::with_capacity(self.columns.len());
        for col in &self.columns {
            let cell = raw.get(col.field_index).ok_or_else(|| {
                VStreamError::Malformed(format!("table {}: missing cell {}", self.table, col.schema.name))
            })?;
            let value = self.cell(col, cell.as_deref())?;
            row.insert(col.schema.name.clone(), value);
        }
        Ok(row)
    }

    pub fn key_of(&self, row: &Row) -> RowKey {
        self.key_indexes
            .iter()
            .map(|i| row[&self.columns[*i].schema.name].clone())
            .collect()
    }

    pub fn partition_of<'a>(&self, row: &'a Row) -> &'a CellValue {
        &row[&self.columns[self.partition_index].schema.name]
    }

    fn cell(&self, col: &ProjectedColumn, raw: Option<&[u8]>) -> Result<CellValue, VStreamError> {
        if let Some(derived) = &col.schema.derived {
            return Ok(CellValue::Bool(derived.rule.apply(raw)));
        }
        let Some(bytes) = raw else {
            if !col.schema.nullable {
                return Err(self.norm_err(&col.schema.name, "NULL in a non-nullable column"));
            }
            return Ok(CellValue::Null);
        };
        let text = || std::str::from_utf8(bytes).map_err(|_| self.norm_err(&col.schema.name, "invalid UTF-8"));
        Ok(match col.schema.kind {
            ValueKind::Bool => {
                let t = text()?;
                let n: i64 = t
                    .parse()
                    .map_err(|_| self.norm_err(&col.schema.name, &format!("not a boolean integer: {t:?}")))?;
                CellValue::Bool(n != 0)
            }
            ValueKind::Int => {
                let t = text()?;
                let n: i64 = t
                    .parse()
                    .map_err(|_| self.norm_err(&col.schema.name, &format!("not an integer: {t:?}")))?;
                if n.unsigned_abs() > (1u64 << 53) {
                    return Err(self.norm_err(&col.schema.name, "integer exceeds 2^53; declare the column as bigint"));
                }
                CellValue::from(n)
            }
            ValueKind::BigInt => {
                let t = text()?;
                let valid = {
                    let s = t.strip_prefix('-').unwrap_or(t);
                    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
                };
                if !valid {
                    return Err(self.norm_err(&col.schema.name, &format!("not an integer: {t:?}")));
                }
                CellValue::String(t.to_string())
            }
            ValueKind::Float => {
                let t = text()?;
                let f: f64 = t
                    .parse()
                    .map_err(|_| self.norm_err(&col.schema.name, &format!("not a float: {t:?}")))?;
                serde_json::Number::from_f64(f)
                    .map(CellValue::Number)
                    .ok_or_else(|| self.norm_err(&col.schema.name, "non-finite float"))?
            }
            ValueKind::Decimal | ValueKind::DateTime | ValueKind::Date | ValueKind::Time => {
                CellValue::String(text()?.to_string())
            }
            ValueKind::String => {
                let t = text()?;
                if col.live_enum_values.is_some() && !self.enum_set_string_values {
                    self.decode_enum_ordinal(col, t)?
                } else {
                    CellValue::String(t.to_string())
                }
            }
            ValueKind::Bytes => CellValue::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
            ValueKind::Json => {
                let t = text()?;
                serde_json::from_str::<CellValue>(t)
                    .map_err(|e| self.norm_err(&col.schema.name, &format!("invalid JSON: {e}")))?
            }
        })
    }

    fn decode_enum_ordinal(&self, col: &ProjectedColumn, text: &str) -> Result<CellValue, VStreamError> {
        let values = col.live_enum_values.as_ref().expect("checked");
        let n: u64 = text
            .parse()
            .map_err(|_| self.norm_err(&col.schema.name, &format!("enum/set ordinal is not numeric: {text:?}")))?;
        if col.is_set {
            let mut parts = Vec::new();
            for (i, v) in values.iter().enumerate() {
                if n & (1u64 << i) != 0 {
                    parts.push(v.as_str());
                }
            }
            if n >> values.len() != 0 {
                return Err(self.norm_err(
                    &col.schema.name,
                    &format!("set bitmask {n} has bits outside the definition"),
                ));
            }
            Ok(CellValue::String(parts.join(",")))
        } else {
            if n == 0 {
                // MySQL stores an invalid enum value as index 0 (empty string).
                return Ok(CellValue::String(String::new()));
            }
            values
                .get((n - 1) as usize)
                .map(|v| CellValue::String(v.clone()))
                .ok_or_else(|| self.norm_err(&col.schema.name, &format!("enum ordinal {n} is out of range")))
        }
    }

    fn norm_err(&self, column: &str, message: &str) -> VStreamError {
        VStreamError::Normalization {
            table: self.table.clone(),
            message: format!("column {column}: {message}"),
        }
    }
}

/// Infers the value kind of a live field from its MySQL column type and Vitess type.
pub fn live_kind(field: &Field) -> ValueKind {
    let ct = field.column_type.as_str();
    let data_type: String = ct.split(['(', ' ']).next().unwrap_or_default().to_ascii_lowercase();
    if data_type.is_empty() {
        // Fall back to the Vitess type when column_type is absent.
        return kind_from_vitess_type(field.r#type);
    }
    infer_kind(&data_type, ct)
}

/// The value kind implied by a Vitess type code alone. `tinyint(1)` is not distinguishable from
/// other `tinyint` columns here, so it maps to `Int`.
pub fn kind_from_vitess_type(ty: i32) -> ValueKind {
    match PbType::try_from(ty).unwrap_or(PbType::NullType) {
        PbType::Int8
        | PbType::Uint8
        | PbType::Int16
        | PbType::Uint16
        | PbType::Int24
        | PbType::Uint24
        | PbType::Int32
        | PbType::Uint32
        | PbType::Year => ValueKind::Int,
        PbType::Int64 | PbType::Uint64 | PbType::Bit => ValueKind::BigInt,
        PbType::Float32 | PbType::Float64 => ValueKind::Float,
        PbType::Decimal => ValueKind::Decimal,
        PbType::Json => ValueKind::Json,
        PbType::Datetime | PbType::Timestamp => ValueKind::DateTime,
        PbType::Date => ValueKind::Date,
        PbType::Time => ValueKind::Time,
        PbType::Blob | PbType::Varbinary | PbType::Binary | PbType::Geometry | PbType::Vector => ValueKind::Bytes,
        _ => ValueKind::String,
    }
}

/// Builds the field list for a `TableProjection` from the fields of a vtgate `Execute` result.
///
/// `Execute` results carry the Vitess type but no `column_type`, and enum and set cells are
/// strings. This function checks every synced column against the Vitess type (a `bool` column
/// arrives as `INT8`, the type of `tinyint(1)`) and fills `column_type` from the sync schema's
/// `source_type`, so the projection decodes the cells exactly like a VStream row of the same
/// table. A synced column that is missing or has another Vitess type is a `SchemaMismatch`.
pub fn query_fields(schema: &TableSchema, fields: &[Field]) -> Result<TableFields, VStreamError> {
    let mut out = Vec::with_capacity(schema.columns.len());
    for col in &schema.columns {
        let source = col.derived.as_ref().map_or(col.name.as_str(), |d| d.from.as_str());
        let field = fields
            .iter()
            .find(|f| f.name == source)
            .ok_or_else(|| VStreamError::SchemaMismatch {
                table: schema.name.clone(),
                message: format!("synced column {source} is missing from the query result"),
            })?;
        if col.derived.is_some() {
            // The rule reads the raw cell; its type does not matter.
            out.push(field.clone());
            continue;
        }
        let live = if field.column_type.is_empty() {
            let vitess_kind = kind_from_vitess_type(field.r#type);
            let compatible = vitess_kind == col.kind || (col.kind == ValueKind::Bool && vitess_kind == ValueKind::Int);
            if !compatible {
                return Err(VStreamError::SchemaMismatch {
                    table: schema.name.clone(),
                    message: format!(
                        "column {} has live Vitess type {} (kind {:?}) but the sync schema expects kind {:?}",
                        col.name, field.r#type, vitess_kind, col.kind
                    ),
                });
            }
            Field {
                column_type: col.source_type.clone(),
                ..field.clone()
            }
        } else {
            field.clone()
        };
        out.push(live);
    }
    Ok(TableFields {
        table: schema.name.clone(),
        fields: out,
        enum_set_string_values: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_protocol::schema::{PartitionConfig, PlacementConfig, SyncSchema};

    fn field(name: &str, column_type: &str, ty: PbType) -> Field {
        Field {
            name: name.into(),
            r#type: ty as i32,
            column_type: column_type.into(),
            ..Default::default()
        }
    }

    fn table_schema() -> TableSchema {
        let s = SyncSchema {
            format_version: 1,
            schema_hash: String::new(),
            app: "t".into(),
            keyspace: "k".into(),
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
                partition_routes: vec![],
                columns: vec![
                    ColumnSchema {
                        name: "id".into(),
                        kind: ValueKind::String,
                        nullable: false,
                        source_type: "varchar(32)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "org".into(),
                        kind: ValueKind::String,
                        nullable: false,
                        source_type: "varchar(32)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "e".into(),
                        kind: ValueKind::String,
                        nullable: false,
                        source_type: "enum('A','B','C')".into(),
                        enum_values: Some(vec!["A".into(), "B".into(), "C".into()]),
                        derived: None,
                    },
                    ColumnSchema {
                        name: "s".into(),
                        kind: ValueKind::String,
                        nullable: true,
                        source_type: "set('x','y','z')".into(),
                        enum_values: Some(vec!["x".into(), "y".into(), "z".into()]),
                        derived: None,
                    },
                    ColumnSchema {
                        name: "b".into(),
                        kind: ValueKind::Bool,
                        nullable: false,
                        source_type: "tinyint(1)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "j".into(),
                        kind: ValueKind::Json,
                        nullable: true,
                        source_type: "json".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "d".into(),
                        kind: ValueKind::Decimal,
                        nullable: true,
                        source_type: "decimal(12,4)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "f".into(),
                        kind: ValueKind::Float,
                        nullable: true,
                        source_type: "float".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "bl".into(),
                        kind: ValueKind::Bytes,
                        nullable: true,
                        source_type: "blob".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "bu".into(),
                        kind: ValueKind::BigInt,
                        nullable: true,
                        source_type: "bigint unsigned".into(),
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
                ],
                relations: vec![],
            }],
        };
        s.tables.into_iter().next().unwrap()
    }

    fn live_fields(enum_strings: bool) -> TableFields {
        TableFields {
            table: "t".into(),
            enum_set_string_values: enum_strings,
            fields: vec![
                field("id", "varchar(32)", PbType::Varchar),
                field("extra", "text", PbType::Text),
                field("org", "varchar(32)", PbType::Varchar),
                field("e", "enum('A','B','C')", PbType::Enum),
                field("s", "set('x','y','z')", PbType::Set),
                field("b", "tinyint(1)", PbType::Int8),
                field("j", "json", PbType::Json),
                field("d", "decimal(12,4)", PbType::Decimal),
                field("f", "float", PbType::Float32),
                field("bl", "blob", PbType::Blob),
                field("bu", "bigint unsigned", PbType::Uint64),
                field("n", "int", PbType::Int32),
            ],
        }
    }

    fn raw(cells: &[Option<&str>]) -> RawRow {
        cells
            .iter()
            .map(|c| c.map(|s| bytes::Bytes::copy_from_slice(s.as_bytes())))
            .collect()
    }

    #[test]
    fn projects_all_kinds_with_string_enums() {
        let p = TableProjection::build(&table_schema(), &live_fields(true)).unwrap();
        let row = p
            .project(&raw(&[
                Some("r1"),
                Some("ignored"),
                Some("org1"),
                Some("B"),
                Some("x,z"),
                Some("1"),
                Some(r#"{"a": [1, 2]}"#),
                Some("1234.5678"),
                Some("1.5E+00"),
                Some("\u{0}"),
                Some("18446744073709551615"),
                Some("-5"),
            ]))
            .unwrap();
        assert_eq!(row["id"], "r1");
        assert!(!row.contains_key("extra"));
        assert_eq!(row["e"], "B");
        assert_eq!(row["s"], "x,z");
        assert_eq!(row["b"], true);
        assert_eq!(row["j"], serde_json::json!({"a": [1, 2]}));
        assert_eq!(row["d"], "1234.5678");
        assert_eq!(row["f"], 1.5);
        assert_eq!(row["bl"], "AA==");
        assert_eq!(row["bu"], "18446744073709551615");
        assert_eq!(row["n"], -5);
        assert_eq!(p.key_of(&row), vec![CellValue::from("r1")]);
        assert_eq!(p.partition_of(&row), "org1");
    }

    #[test]
    fn derives_bool_columns_from_an_unsynced_source() {
        use orbit_protocol::schema::{DerivedColumn, DerivedRule};
        let mut schema = table_schema();
        schema.columns.push(ColumnSchema {
            name: "has_doc".into(),
            kind: ValueKind::Bool,
            nullable: false,
            source_type: "derived".into(),
            enum_values: None,
            derived: Some(DerivedColumn {
                from: "doc".into(),
                rule: DerivedRule::NotNull,
            }),
        });
        schema.columns.push(ColumnSchema {
            name: "legacy_doc".into(),
            kind: ValueKind::Bool,
            nullable: false,
            source_type: "derived".into(),
            enum_values: None,
            derived: Some(DerivedColumn {
                from: "doc".into(),
                rule: DerivedRule::StartsWith { prefix: "gs://".into() },
            }),
        });
        let mut fields = live_fields(true);
        fields.fields.push(field("doc", "text", PbType::Text));
        let p = TableProjection::build(&schema, &fields).unwrap();
        let mut cells: Vec<Option<&str>> = vec![
            Some("r1"),
            None,
            Some("org1"),
            Some("A"),
            None,
            Some("0"),
            None,
            None,
            None,
            None,
            None,
            None,
        ];
        cells.push(Some("gs://bucket/file.pdf"));
        let row = p.project(&raw(&cells)).unwrap();
        assert!(!row.contains_key("doc"));
        assert_eq!(row["has_doc"], true);
        assert_eq!(row["legacy_doc"], true);
        cells[12] = Some("s3://bucket/file.pdf");
        let row = p.project(&raw(&cells)).unwrap();
        assert_eq!(row["has_doc"], true);
        assert_eq!(row["legacy_doc"], false);
        cells[12] = None;
        let row = p.project(&raw(&cells)).unwrap();
        assert_eq!(row["has_doc"], false);
        assert_eq!(row["legacy_doc"], false);

        // A missing source field is a schema mismatch, like any other synced column.
        let err = TableProjection::build(&schema, &live_fields(true)).unwrap_err();
        assert!(matches!(err, VStreamError::SchemaMismatch { .. }));

        // Query results (derived fills) need the source field too, and skip the kind check.
        let mut query = Vec::new();
        for f in &live_fields(true).fields {
            query.push(Field {
                column_type: String::new(),
                ..f.clone()
            });
        }
        query.push(field("doc", "", PbType::Text));
        let checked = query_fields(&schema, &query).unwrap();
        assert!(checked.fields.iter().filter(|f| f.name == "doc").count() >= 1);
    }

    #[test]
    fn decodes_enum_and_set_ordinals() {
        let p = TableProjection::build(&table_schema(), &live_fields(false)).unwrap();
        let row = p
            .project(&raw(&[
                Some("r1"),
                None,
                Some("o"),
                Some("3"),
                Some("5"),
                Some("0"),
                None,
                None,
                None,
                None,
                None,
                None,
            ]))
            .unwrap();
        assert_eq!(row["e"], "C");
        assert_eq!(row["s"], "x,z");
        assert_eq!(row["b"], false);
        let err = p
            .project(&raw(&[
                Some("r1"),
                None,
                Some("o"),
                Some("9"),
                None,
                Some("0"),
                None,
                None,
                None,
                None,
                None,
                None,
            ]))
            .unwrap_err();
        assert!(matches!(err, VStreamError::Normalization { .. }));
    }

    #[test]
    fn rejects_schema_drift() {
        let mut f = live_fields(true);
        f.fields.retain(|x| x.name != "org");
        assert!(matches!(
            TableProjection::build(&table_schema(), &f),
            Err(VStreamError::SchemaMismatch { .. })
        ));
        let mut f = live_fields(true);
        f.fields[5] = field("b", "int", PbType::Int32);
        assert!(matches!(
            TableProjection::build(&table_schema(), &f),
            Err(VStreamError::SchemaMismatch { .. })
        ));
        let mut f = live_fields(true);
        f.fields[3] = field("e", "enum('A','B')", PbType::Enum);
        assert!(matches!(
            TableProjection::build(&table_schema(), &f),
            Err(VStreamError::SchemaMismatch { .. })
        ));
    }

    #[test]
    fn rejects_bad_values_explicitly() {
        let p = TableProjection::build(&table_schema(), &live_fields(true)).unwrap();
        let base = |j: Option<&str>, n: Option<&str>| {
            raw(&[
                Some("r1"),
                None,
                Some("o"),
                Some("A"),
                None,
                Some("1"),
                j,
                None,
                None,
                None,
                None,
                n,
            ])
        };
        assert!(matches!(
            p.project(&base(Some("{not json"), None)),
            Err(VStreamError::Normalization { .. })
        ));
        assert!(matches!(
            p.project(&base(None, Some("abc"))),
            Err(VStreamError::Normalization { .. })
        ));
        // NULL in non-nullable
        let r = raw(&[
            None,
            None,
            Some("o"),
            Some("A"),
            None,
            Some("1"),
            None,
            None,
            None,
            None,
            None,
            None,
        ]);
        assert!(matches!(p.project(&r), Err(VStreamError::Normalization { .. })));
    }

    #[test]
    fn query_fields_fill_column_type_and_check_vitess_types() {
        // Execute results carry only the Vitess type; bool columns arrive as INT8.
        let live: Vec<Field> = [
            ("id", PbType::Varchar),
            ("org", PbType::Varchar),
            ("e", PbType::Enum),
            ("s", PbType::Set),
            ("b", PbType::Int8),
            ("j", PbType::Json),
            ("d", PbType::Decimal),
            ("f", PbType::Float32),
            ("bl", PbType::Blob),
            ("bu", PbType::Uint64),
            ("n", PbType::Int32),
        ]
        .into_iter()
        .map(|(n, t)| field(n, "", t))
        .collect();
        let tf = query_fields(&table_schema(), &live).unwrap();
        assert!(tf.enum_set_string_values);
        assert_eq!(tf.fields[4].column_type, "tinyint(1)");
        assert_eq!(tf.fields[2].column_type, "enum('A','B','C')");
        let p = TableProjection::build(&table_schema(), &tf).unwrap();
        let row = p
            .project(&raw(&[
                Some("r1"),
                Some("org1"),
                Some("B"),
                Some("x,z"),
                Some("1"),
                Some(r#"{"a": [1, 2]}"#),
                Some("1234.5678"),
                Some("1.5"),
                Some("\u{0}"),
                Some("18446744073709551615"),
                Some("-5"),
            ]))
            .unwrap();
        assert_eq!(row["b"], true);
        assert_eq!(row["e"], "B");
        assert_eq!(row["j"], serde_json::json!({"a": [1, 2]}));
        assert_eq!(row["bl"], "AA==");
        assert_eq!(row["bu"], "18446744073709551615");
        assert_eq!(row["n"], -5);
        assert_eq!(
            row.keys().cloned().collect::<Vec<_>>(),
            table_schema()
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>()
        );

        // A missing column or another Vitess type is schema drift.
        let mut missing = live.clone();
        missing.retain(|f| f.name != "n");
        assert!(matches!(
            query_fields(&table_schema(), &missing),
            Err(VStreamError::SchemaMismatch { .. })
        ));
        let mut retyped = live.clone();
        retyped[10] = field("n", "", PbType::Varchar);
        assert!(matches!(
            query_fields(&table_schema(), &retyped),
            Err(VStreamError::SchemaMismatch { .. })
        ));
        // A populated column_type is used as is.
        let mut typed = live.clone();
        typed[4] = field("b", "tinyint(1)", PbType::Int8);
        assert_eq!(
            query_fields(&table_schema(), &typed).unwrap().fields[4].column_type,
            "tinyint(1)"
        );
    }
}
