//! Minimal query execution over vtgate gRPC, used for schema introspection, the parent index
//! bootstrap and derived fills.

use orbit_protocol::schema::{
    IntrospectedColumn, IntrospectedSchema, IntrospectedTable, infer_kind, parse_enum_values,
};

use crate::client::{Client, VitessEndpoint};
use crate::decode::{RawRow, decode_row};
use crate::error::VStreamError;
use crate::proto::query::{BoundQuery, Field};
use crate::proto::vtgate::{ExecuteRequest, Session};

/// A decoded query result: the field list and one raw byte row per result row.
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub fields: Vec<Field>,
    pub rows: Vec<RawRow>,
}

/// Executes a read-only query and returns the fields and the raw cells.
pub async fn query(endpoint: &VitessEndpoint, keyspace: &str, sql: &str) -> Result<QueryResult, VStreamError> {
    let mut client = endpoint.connect().await?;
    query_with_client(&mut client, keyspace, sql).await
}

/// Executes over an existing multiplexed gRPC channel. Callers can cheaply clone the client.
pub async fn query_with_client(client: &mut Client, keyspace: &str, sql: &str) -> Result<QueryResult, VStreamError> {
    let req = ExecuteRequest {
        session: Some(Session {
            target_string: keyspace.to_string(),
            autocommit: true,
            ..Default::default()
        }),
        query: Some(BoundQuery {
            sql: sql.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let resp = client
        .execute(req)
        .await
        .map_err(|s| VStreamError::from_status(s, "execute"))?
        .into_inner();
    if let Some(err) = resp.error {
        return Err(VStreamError::Malformed(format!(
            "execute failed: {} ({})",
            err.message, err.code
        )));
    }
    let result = resp
        .result
        .ok_or_else(|| VStreamError::Malformed("execute returned no result".into()))?;
    let n = result.fields.len();
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        rows.push(decode_row(row, n, "execute")?);
    }
    Ok(QueryResult {
        fields: result.fields,
        rows,
    })
}

/// Executes a read-only query and returns rows as text cells.
pub async fn execute(
    endpoint: &VitessEndpoint,
    keyspace: &str,
    sql: &str,
) -> Result<Vec<Vec<Option<String>>>, VStreamError> {
    let result = query(endpoint, keyspace, sql).await?;
    Ok(result
        .rows
        .into_iter()
        .map(|cells| {
            cells
                .into_iter()
                .map(|c| c.map(|b| String::from_utf8_lossy(&b).into_owned()))
                .collect()
        })
        .collect())
}

/// Reads column and primary key metadata for `tables` (all tables when empty).
pub async fn introspect(
    endpoint: &VitessEndpoint,
    keyspace: &str,
    tables: &[String],
) -> Result<IntrospectedSchema, VStreamError> {
    let version = execute(endpoint, keyspace, "select @@version").await?;
    let server_version = version
        .first()
        .and_then(|r| r.first().cloned().flatten())
        .unwrap_or_default();
    let columns = execute(
        endpoint,
        keyspace,
        "select table_name, column_name, column_type, data_type, is_nullable from information_schema.columns where table_schema = database() order by table_name, ordinal_position",
    )
    .await?;
    let pks = execute(
        endpoint,
        keyspace,
        "select table_name, column_name from information_schema.statistics where table_schema = database() and index_name = 'PRIMARY' order by table_name, seq_in_index",
    )
    .await?;
    let mut out: Vec<IntrospectedTable> = Vec::new();
    for row in columns {
        let get = |i: usize| row.get(i).cloned().flatten().unwrap_or_default();
        let table = get(0);
        if !tables.is_empty() && !tables.iter().any(|t| t == &table) {
            continue;
        }
        let column_type = get(2);
        let data_type = get(3);
        let kind = infer_kind(&data_type, &column_type);
        let col = IntrospectedColumn {
            name: get(1),
            column_type: column_type.clone(),
            data_type,
            nullable: get(4).eq_ignore_ascii_case("YES"),
            kind,
            enum_values: parse_enum_values(&column_type),
        };
        match out.last_mut() {
            Some(t) if t.name == table => t.columns.push(col),
            _ => out.push(IntrospectedTable {
                name: table,
                primary_key: vec![],
                columns: vec![col],
            }),
        }
    }
    for row in pks {
        let table = row.first().cloned().flatten().unwrap_or_default();
        let column = row.get(1).cloned().flatten().unwrap_or_default();
        if let Some(t) = out.iter_mut().find(|t| t.name == table) {
            t.primary_key.push(column);
        }
    }
    for t in tables {
        if !out.iter().any(|x| &x.name == t) {
            return Err(VStreamError::Unsupported(format!(
                "table {t} does not exist in keyspace {keyspace}"
            )));
        }
    }
    Ok(IntrospectedSchema {
        keyspace: keyspace.to_string(),
        server_version,
        tables: out,
    })
}
