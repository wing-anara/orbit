//! Writes one JSON fixture per protocol root type with every variant populated, and asserts the
//! checked-in fixtures are current. `packages/protocol` decodes the same files with the
//! generated Effect Schemas and re-encodes them byte-for-byte, which proves both languages agree
//! on the wire format.
//!
//! Regenerate with `UPDATE_FIXTURES=1 cargo test -p orbit-protocol --test fixtures`.

use indexmap::IndexMap;
use orbit_protocol::cdc::*;
use orbit_protocol::errors::EngineError;
use orbit_protocol::fill::*;
use orbit_protocol::schema::*;
use orbit_protocol::value::{CellValue, Row};
use serde::Serialize;

fn row(pairs: &[(&str, CellValue)]) -> Row {
    let mut r: Row = IndexMap::new();
    for (k, v) in pairs {
        r.insert((*k).to_string(), v.clone());
    }
    r
}

fn sample_schema() -> SyncSchema {
    let mut s = SyncSchema {
        format_version: orbit_protocol::SYNC_SCHEMA_FORMAT_VERSION,
        schema_hash: String::new(),
        app: "fixture".into(),
        keyspace: "ks".into(),
        partition: PartitionConfig {
            name: "org".into(),
            key_kind: ValueKind::String,
            placement: PlacementConfig::OnePerPartition { version: 1 },
        },
        tables: vec![
            TableSchema {
                name: "organization".into(),
                primary_key: vec!["id".into()],
                partition_column: "id".into(),
                partition_parent: None,
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
                    ColumnSchema {
                        name: "created_at".into(),
                        kind: ValueKind::DateTime,
                        nullable: false,
                        source_type: "timestamp".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "hipaa_enabled".into(),
                        kind: ValueKind::Bool,
                        nullable: false,
                        source_type: "tinyint(1)".into(),
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
                        name: "displayOrder".into(),
                        kind: ValueKind::Int,
                        nullable: true,
                        source_type: "int".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "contents".into(),
                        kind: ValueKind::Json,
                        nullable: true,
                        source_type: "json".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "createdAt".into(),
                        kind: ValueKind::DateTime,
                        nullable: false,
                        source_type: "datetime(3)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "score".into(),
                        kind: ValueKind::Float,
                        nullable: true,
                        source_type: "double".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "big".into(),
                        kind: ValueKind::BigInt,
                        nullable: true,
                        source_type: "bigint unsigned".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "price".into(),
                        kind: ValueKind::Decimal,
                        nullable: true,
                        source_type: "decimal(10,2)".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "blob".into(),
                        kind: ValueKind::Bytes,
                        nullable: true,
                        source_type: "blob".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "day".into(),
                        kind: ValueKind::Date,
                        nullable: true,
                        source_type: "date".into(),
                        enum_values: None,
                        derived: None,
                    },
                    ColumnSchema {
                        name: "at".into(),
                        kind: ValueKind::Time,
                        nullable: true,
                        source_type: "time(3)".into(),
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
                    RelationSchema {
                        name: "documents".into(),
                        kind: RelationKind::Many,
                        target_table: "Chatbot".into(),
                        from_columns: vec!["id".into()],
                        to_columns: vec!["groupId".into()],
                    },
                ],
            },
        ],
    };
    s.schema_hash = s.compute_hash();
    s.validate().expect("fixture schema valid");
    s
}

fn chatbot_row(id: &str) -> Row {
    row(&[
        ("id", CellValue::from(id)),
        ("organizationId", CellValue::from("org_1")),
        ("groupId", CellValue::Null),
        ("type", CellValue::from("DOCUMENT")),
        ("displayOrder", CellValue::from(3)),
        (
            "contents",
            serde_json::json!({"blocks": [1, 2, {"t": "x"}], "s": "he\"llo \u{1F600}"}),
        ),
        ("createdAt", CellValue::from("2026-01-02 03:04:05.678")),
        ("score", CellValue::from(1.5)),
        ("big", CellValue::from("18446744073709551615")),
        ("price", CellValue::from("1234.50")),
        ("blob", CellValue::from("AP8Q")),
        ("day", CellValue::from("2026-01-02")),
        ("at", CellValue::from("13:14:15.123")),
    ])
}

fn fixtures() -> Vec<(&'static str, serde_json::Value)> {
    let schema = sample_schema();
    let changes = vec![
        RowChange {
            table: "Chatbot".into(),
            op: RowOp::Insert,
            key: vec![CellValue::from("c1")],
            before: None,
            after: Some(chatbot_row("c1")),
        },
        RowChange {
            table: "Chatbot".into(),
            op: RowOp::Update,
            key: vec![CellValue::from("c1")],
            before: Some(chatbot_row("c1")),
            after: Some(chatbot_row("c1")),
        },
        RowChange {
            table: "Chatbot".into(),
            op: RowOp::Delete,
            key: vec![CellValue::from("c1")],
            before: Some(chatbot_row("c1")),
            after: None,
        },
    ];
    let trace = TraceContext {
        subscriber_received_at_ms: Some(1_700_000_000_123),
        distributor_dispatched_at_ms: Some(1_700_000_000_456),
    };
    let source = SourceTransaction {
        keyspace: "ks".into(),
        shard: "-".into(),
        gtid: "a2523813-adbe-11f1-b19c-0a2250a7ed6c:275".into(),
        position: "MySQL56/a2523813-adbe-11f1-b19c-0a2250a7ed6c:1-275".into(),
        commit_timestamp: 1_789_119_616,
        changes: changes.clone(),
        trace: trace.clone(),
    };
    let ptx = PartitionTransaction {
        seq: 42,
        keyspace: "ks".into(),
        shard: "-".into(),
        gtid: source.gtid.clone(),
        position: source.position.clone(),
        commit_timestamp: source.commit_timestamp,
        changes,
        trace,
    };
    let batch = CdcBatch {
        protocol_version: orbit_protocol::INTERNAL_PROTOCOL_VERSION,
        schema_hash: schema.schema_hash.clone(),
        stream_epoch: 3,
        partition: "org_1".into(),
        transactions: vec![
            ptx.clone(),
            PartitionTransaction {
                seq: 43,
                changes: vec![],
                ..ptx
            },
        ],
        delivery_id: "d-1".into(),
    };
    let introspected = IntrospectedSchema {
        keyspace: "ks".into(),
        server_version: "8.0.43-Vitess".into(),
        tables: vec![IntrospectedTable {
            name: "Chatbot".into(),
            primary_key: vec!["id".into()],
            columns: vec![
                IntrospectedColumn {
                    name: "id".into(),
                    column_type: "varchar(191)".into(),
                    data_type: "varchar".into(),
                    nullable: false,
                    kind: ValueKind::String,
                    enum_values: None,
                },
                IntrospectedColumn {
                    name: "type".into(),
                    column_type: "enum('DOCUMENT','GROUP')".into(),
                    data_type: "enum".into(),
                    nullable: false,
                    kind: ValueKind::String,
                    enum_values: Some(vec!["DOCUMENT".into(), "GROUP".into()]),
                },
                IntrospectedColumn {
                    name: "deleted".into(),
                    column_type: "tinyint(1)".into(),
                    data_type: "tinyint".into(),
                    nullable: true,
                    kind: ValueKind::Bool,
                    enum_values: None,
                },
            ],
        }],
    };
    let rejects: Vec<RejectReason> = vec![
        RejectReason::ProtocolVersionMismatch { expected: 1, got: 2 },
        RejectReason::SchemaMismatch {
            do_schema_hash: "a".into(),
            got: "b".into(),
        },
        RejectReason::SequenceGap {
            applied_seq: 10,
            first_seq: 12,
        },
        RejectReason::SequenceConflict {
            seq: 5,
            applied_gtid: "u:5".into(),
            got_gtid: "u:6".into(),
        },
        RejectReason::StaleEpoch { do_epoch: 4, got: 3 },
        RejectReason::InvalidRow {
            table: "Chatbot".into(),
            seq: 7,
            message: "bad".into(),
        },
        RejectReason::WrongPartition {
            do_partition: "a".into(),
            got: "b".into(),
        },
        RejectReason::Internal {
            code: "x".into(),
            message: "y".into(),
        },
    ];
    let errors: Vec<EngineError> = vec![
        EngineError::SourceUnavailable {
            message: "down".into(),
            retryable: true,
        },
        EngineError::SourceRejected { message: "no".into() },
        EngineError::Normalization {
            table: "t".into(),
            message: "m".into(),
        },
        EngineError::SchemaMismatch {
            table: "t".into(),
            message: "m".into(),
        },
        EngineError::UnknownTable { table: "t".into() },
        EngineError::Unauthorized { message: "m".into() },
        EngineError::Timeout { after_ms: 1000 },
        EngineError::Internal { message: "m".into() },
    ];
    fn v<T: Serialize>(t: &T) -> serde_json::Value {
        serde_json::to_value(t).expect("serializes")
    }
    vec![
        ("SyncSchema", v(&schema)),
        ("IntrospectedSchema", v(&introspected)),
        ("SourceTransaction", v(&source)),
        ("CdcBatch", v(&batch)),
        (
            "CdcBatchAck",
            serde_json::Value::Array(
                std::iter::once(v(&CdcBatchAck::Applied {
                    applied_seq: 43,
                    duplicates: 1,
                    apply_ms: 12,
                }))
                .chain(rejects.iter().map(|r| v(&CdcBatchAck::Rejected { reason: r.clone() })))
                .collect(),
            ),
        ),
        (
            "FillRequest",
            v(&FillRequest {
                fill_id: "f-1".into(),
                schema_hash: schema.schema_hash.clone(),
                partition: "org_1".into(),
                table: "Chatbot".into(),
                requested_at_ms: 1_700_000_000_000,
            }),
        ),
        (
            "FillPollResponse",
            v(&FillPollResponse {
                requests: vec![FillRequest {
                    fill_id: "f-1".into(),
                    schema_hash: schema.schema_hash.clone(),
                    partition: "org_1".into(),
                    table: "Chatbot".into(),
                    requested_at_ms: 1_700_000_000_000,
                }],
            }),
        ),
        (
            "FillChunk",
            serde_json::Value::Array(vec![
                v(&FillChunk::Rows {
                    fill_id: "f-1".into(),
                    rows: vec![chatbot_row("c1"), chatbot_row("c2")],
                }),
                v(&FillChunk::Done {
                    fill_id: "f-1".into(),
                    result: FillResult::Completed {
                        position: "MySQL56/u:1-9".into(),
                        keyspace: "ks".into(),
                        shard: "-".into(),
                        row_count: 2,
                        duration_ms: 35,
                    },
                }),
                v(&FillChunk::Done {
                    fill_id: "f-1".into(),
                    result: FillResult::Failed {
                        error: EngineError::Timeout { after_ms: 5 },
                    },
                }),
            ]),
        ),
        (
            "FillResult",
            serde_json::Value::Array(vec![
                v(&FillResult::Completed {
                    position: "MySQL56/u:1-9".into(),
                    keyspace: "ks".into(),
                    shard: "-".into(),
                    row_count: 2,
                    duration_ms: 35,
                }),
                v(&FillResult::Failed {
                    error: EngineError::Internal { message: "m".into() },
                }),
            ]),
        ),
        ("EngineError", serde_json::Value::Array(errors.iter().map(v).collect())),
    ]
}

#[test]
fn fixtures_are_current_and_round_trip() {
    let dir = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../schema/fixtures"));
    let update = std::env::var("UPDATE_FIXTURES").is_ok();
    std::fs::create_dir_all(dir).unwrap();
    for (name, value) in fixtures() {
        let path = dir.join(format!("{name}.json"));
        let text = serde_json::to_string_pretty(&value).unwrap() + "\n";
        if update {
            std::fs::write(&path, &text).unwrap();
        } else {
            let on_disk = std::fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("{} missing; run with UPDATE_FIXTURES=1", path.display()));
            assert_eq!(on_disk, text, "{name} fixture is stale; run with UPDATE_FIXTURES=1");
        }
        // Rust round trip: every fixture deserializes back into its type and re-serializes identically.
        let round: serde_json::Value = match name {
            "SyncSchema" => serde_json::to_value(serde_json::from_value::<SyncSchema>(value.clone()).unwrap()).unwrap(),
            "IntrospectedSchema" => {
                serde_json::to_value(serde_json::from_value::<IntrospectedSchema>(value.clone()).unwrap()).unwrap()
            }
            "SourceTransaction" => {
                serde_json::to_value(serde_json::from_value::<SourceTransaction>(value.clone()).unwrap()).unwrap()
            }
            "CdcBatch" => serde_json::to_value(serde_json::from_value::<CdcBatch>(value.clone()).unwrap()).unwrap(),
            "CdcBatchAck" => {
                serde_json::to_value(serde_json::from_value::<Vec<CdcBatchAck>>(value.clone()).unwrap()).unwrap()
            }
            "FillRequest" => {
                serde_json::to_value(serde_json::from_value::<FillRequest>(value.clone()).unwrap()).unwrap()
            }
            "FillPollResponse" => {
                serde_json::to_value(serde_json::from_value::<FillPollResponse>(value.clone()).unwrap()).unwrap()
            }
            "FillChunk" => {
                serde_json::to_value(serde_json::from_value::<Vec<FillChunk>>(value.clone()).unwrap()).unwrap()
            }
            "FillResult" => {
                serde_json::to_value(serde_json::from_value::<Vec<FillResult>>(value.clone()).unwrap()).unwrap()
            }
            "EngineError" => {
                serde_json::to_value(serde_json::from_value::<Vec<EngineError>>(value.clone()).unwrap()).unwrap()
            }
            other => panic!("unhandled fixture {other}"),
        };
        assert_eq!(round, value, "{name} does not round trip in Rust");
    }
    // The sample schema hash is what TypeScript must reproduce.
    let schema = sample_schema();
    let hash_path = dir.join("SyncSchema.hash");
    if update {
        std::fs::write(&hash_path, format!("{}\n", schema.schema_hash)).unwrap();
    } else {
        assert_eq!(std::fs::read_to_string(&hash_path).unwrap().trim(), schema.schema_hash);
    }
}
