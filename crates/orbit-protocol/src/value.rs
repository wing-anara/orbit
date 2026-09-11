//! Column value encoding.
//!
//! A cell is a plain JSON value. Its interpretation is fixed by the column's [`ValueKind`]
//! (see [`crate::schema::ValueKind`]), and every consumer validates cells against the sync
//! schema before use. The encoding per kind is:
//!
//! | kind      | JSON                                   | notes                                   |
//! |-----------|----------------------------------------|-----------------------------------------|
//! | bool      | `true` / `false`                       | MySQL `tinyint(1)`                      |
//! | int       | number                                 | up to 32-bit signed/unsigned            |
//! | bigint    | string of decimal digits               | 64-bit; strings avoid float precision   |
//! | float     | number                                 | MySQL float/double                      |
//! | decimal   | string                                 | exact decimal text                      |
//! | string    | string                                 | char/varchar/text/enum/set              |
//! | bytes     | string, standard base64                | binary/varbinary/blob                   |
//! | json      | the parsed JSON value                  |                                         |
//! | datetime  | string `YYYY-MM-DD hh:mm:ss[.ffffff]`  | as delivered by Vitess, no time zone    |
//! | date      | string `YYYY-MM-DD`                    |                                         |
//! | time      | string `[-]hh:mm:ss[.ffffff]`          |                                         |
//! | any kind  | `null`                                 | SQL NULL                                |

use indexmap::IndexMap;

/// A single column value. See the module docs for the encoding rules.
pub type CellValue = serde_json::Value;

/// A full row image: column name to cell, in sync schema column order.
pub type Row = IndexMap<String, CellValue>;

/// Primary key values of a row, in primary key column order.
pub type RowKey = Vec<CellValue>;
