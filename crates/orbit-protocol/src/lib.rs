//! Cross-language protocol and schema definitions for the sync engine.
//!
//! Every type in this crate derives `JsonSchema`. The `orbit-protocol-schema` binary exports one
//! JSON Schema document; `packages/codegen` turns it into Effect `Schema` definitions for
//! TypeScript. Changing a type here therefore changes the generated TypeScript, and CI fails when
//! the generated file is stale. That is the mechanism that keeps Rust and TypeScript from drifting.
//!
//! Module map:
//! * [`schema`]    the compiled sync schema artifact and the raw introspected database schema
//! * [`value`]     the JSON encoding of column values per value kind
//! * [`cdc`]       normalized change events and the distributor to Durable Object batch protocol
//! * [`fill`]      demand fill requests and results
//! * [`errors`]    typed errors that cross a process boundary

pub mod cdc;
pub mod errors;
pub mod fill;
pub mod schema;
pub mod value;

/// Version of the distributor to Durable Object protocol (`CdcBatch`, `FillChunk`, ...).
/// Bump on any incompatible change. Both ends reject mismatches explicitly.
pub const INTERNAL_PROTOCOL_VERSION: u32 = 1;

/// Format version of the compiled sync schema artifact.
pub const SYNC_SCHEMA_FORMAT_VERSION: u32 = 1;

/// All root types exported to the JSON Schema document, in a stable order.
pub fn export_document() -> serde_json::Value {
    use schemars::generate::SchemaSettings;

    let mut generator = SchemaSettings::draft2020_12().into_generator();
    macro_rules! roots {
        ($($t:ty),* $(,)?) => {{
            let mut names: Vec<String> = Vec::new();
            $(
                let s = generator.subschema_for::<$t>();
                // subschema_for returns a $ref for named types; record the root name.
                let name = s
                    .get("$ref")
                    .and_then(|r| r.as_str())
                    .and_then(|r| r.rsplit('/').next())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| stringify!($t).to_string());
                names.push(name);
            )*
            names
        }};
    }
    let roots = roots!(
        schema::SyncSchema,
        schema::IntrospectedSchema,
        cdc::CdcBatch,
        cdc::CdcBatchAck,
        cdc::SourceTransaction,
        fill::FillRequest,
        fill::FillPollResponse,
        fill::FillChunk,
        fill::FillResult,
        errors::EngineError,
    );
    let defs = generator.take_definitions(true);
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://orbit.dev/schemas/protocol.json",
        "title": "orbit protocol",
        "x-internal-protocol-version": INTERNAL_PROTOCOL_VERSION,
        "x-sync-schema-format-version": SYNC_SCHEMA_FORMAT_VERSION,
        "x-roots": roots,
        "$defs": defs,
    })
}

#[cfg(test)]
mod tests {
    /// The checked-in JSON Schema must match the code. Regenerate with
    /// `cargo run -p orbit-protocol --bin orbit-protocol-schema -- schema/protocol.schema.json`.
    #[test]
    fn checked_in_schema_is_current() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../schema/protocol.schema.json");
        let on_disk: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).expect("schema/protocol.schema.json exists"))
                .expect("valid json");
        let generated = super::export_document();
        assert_eq!(
            on_disk, generated,
            "schema/protocol.schema.json is stale; regenerate it"
        );
    }
}
