//! Decoding of Vitess `FieldEvent` and `RowEvent` payloads into raw byte cells.
//!
//! Vitess encodes a row as `lengths: [i64]` and `values: bytes`. A length of `-1` is SQL NULL;
//! otherwise the next `length` bytes of `values` are the textual MySQL representation of the cell.

use crate::error::VStreamError;
use crate::proto::binlogdata::{FieldEvent, RowChange as PbRowChange};
use crate::proto::query::{Field, Row as PbRow};

/// Column metadata for one table as last announced by a FIELD event.
#[derive(Debug, Clone)]
pub struct TableFields {
    pub table: String,
    pub fields: Vec<Field>,
    /// True when ENUM/SET cells are strings; false when they are ordinal indexes / bitmasks.
    pub enum_set_string_values: bool,
}

impl TableFields {
    pub fn from_event(ev: &FieldEvent) -> Self {
        Self {
            table: strip_keyspace(&ev.table_name).to_string(),
            fields: ev.fields.clone(),
            enum_set_string_values: ev.enum_set_string_values,
        }
    }
}

/// Strips a leading `keyspace.` from a table name. Vitess prefixes table names with the keyspace
/// unless `exclude_keyspace_from_table_name` is set; the engine accepts both.
pub fn strip_keyspace(table_name: &str) -> &str {
    match table_name.rsplit_once('.') {
        Some((_, t)) => t,
        None => table_name,
    }
}

/// A decoded row: one optional byte slice per field, in field order.
pub type RawRow = Vec<Option<bytes::Bytes>>;

pub fn decode_row(row: &PbRow, field_count: usize, table: &str) -> Result<RawRow, VStreamError> {
    if row.lengths.len() != field_count {
        return Err(VStreamError::Malformed(format!(
            "table {table}: row has {} lengths but {field_count} fields",
            row.lengths.len()
        )));
    }
    let mut out = Vec::with_capacity(field_count);
    let mut offset: usize = 0;
    for len in &row.lengths {
        if *len < 0 {
            out.push(None);
            continue;
        }
        let len = *len as usize;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| VStreamError::Malformed(format!("table {table}: length overflow")))?;
        if end > row.values.len() {
            return Err(VStreamError::Malformed(format!(
                "table {table}: row values truncated (need {end} bytes, have {})",
                row.values.len()
            )));
        }
        out.push(Some(bytes::Bytes::copy_from_slice(&row.values[offset..end])));
        offset = end;
    }
    if offset != row.values.len() {
        return Err(VStreamError::Malformed(format!(
            "table {table}: {} trailing bytes after decoding row",
            row.values.len() - offset
        )));
    }
    Ok(out)
}

/// A decoded row change with raw cells.
#[derive(Debug, Clone)]
pub struct RawRowChange {
    pub before: Option<RawRow>,
    pub after: Option<RawRow>,
}

pub fn decode_row_change(change: &PbRowChange, fields: &TableFields) -> Result<RawRowChange, VStreamError> {
    let n = fields.fields.len();
    let before = change
        .before
        .as_ref()
        .map(|r| decode_row(r, n, &fields.table))
        .transpose()?;
    let after = change
        .after
        .as_ref()
        .map(|r| decode_row(r, n, &fields.table))
        .transpose()?;
    if before.is_none() && after.is_none() {
        return Err(VStreamError::Malformed(format!(
            "table {}: row change with neither before nor after image",
            fields.table
        )));
    }
    if change.data_columns.is_some() {
        // Partial row images only occur when binlog_row_image != FULL. The engine's row
        // representation is a full image, so refuse rather than fabricate missing cells.
        return Err(VStreamError::Unsupported(format!(
            "table {}: partial row image (binlog_row_image must be FULL)",
            fields.table
        )));
    }
    Ok(RawRowChange { before, after })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_lengths_and_nulls() {
        let row = PbRow {
            lengths: vec![1, 4, -1, 7],
            values: b"aorg1changed".to_vec(),
        };
        let cells = decode_row(&row, 4, "t").unwrap();
        assert_eq!(cells[0].as_deref(), Some(&b"a"[..]));
        assert_eq!(cells[1].as_deref(), Some(&b"org1"[..]));
        assert_eq!(cells[2], None);
        assert_eq!(cells[3].as_deref(), Some(&b"changed"[..]));
    }

    #[test]
    fn rejects_truncated_and_trailing() {
        let row = PbRow {
            lengths: vec![5],
            values: b"abc".to_vec(),
        };
        assert!(matches!(decode_row(&row, 1, "t"), Err(VStreamError::Malformed(_))));
        let row = PbRow {
            lengths: vec![1],
            values: b"abc".to_vec(),
        };
        assert!(matches!(decode_row(&row, 1, "t"), Err(VStreamError::Malformed(_))));
        let row = PbRow {
            lengths: vec![1, 1],
            values: b"ab".to_vec(),
        };
        assert!(matches!(decode_row(&row, 3, "t"), Err(VStreamError::Malformed(_))));
    }

    #[test]
    fn strips_keyspace_prefix() {
        assert_eq!(strip_keyspace("orbit.Chatbot"), "Chatbot");
        assert_eq!(strip_keyspace("Chatbot"), "Chatbot");
    }
}
