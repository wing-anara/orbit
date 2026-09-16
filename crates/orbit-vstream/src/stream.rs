//! Transaction assembly: turns the flat VEvent sequence into [`StreamItem`]s.
//!
//! vtgate delivers, per transaction: `BEGIN, FIELD*, ROW*, VGTID, COMMIT`. A transaction may
//! span several `VStreamResponse`s, so the assembler is a state machine over events, not over
//! responses. Events outside a transaction (`VGTID` at stream start, `DDL`, `OTHER`,
//! `HEARTBEAT`, `COPY_COMPLETED`) are surfaced as their own items.
//!
//! Every emitted transaction carries the exact set of transaction ids the step added (normally
//! one), computed from consecutive positions. A step that adds more than one id means vtgate
//! merged transactions, which the engine treats as malformed unless explicitly allowed.

use std::collections::HashMap;

use orbit_gtid::{GtidSet, StepError, diff_single};
use orbit_protocol::cdc::{RowChange, RowOp, SourceTransaction, TraceContext};
use orbit_protocol::schema::SyncSchema;
use tracing::{debug, warn};

use crate::checkpoint::ShardId;
use crate::decode::{TableFields, decode_row_change, strip_keyspace};
use crate::error::VStreamError;
use crate::normalize::TableProjection;
use crate::proto::binlogdata::{VEvent, VEventType, VGtid};

/// Something the assembler produced from the event stream.
#[derive(Debug, Clone)]
pub enum StreamItem {
    /// A committed transaction with its normalized changes (possibly empty).
    Transaction(SourceTransaction),
    /// The stream position advanced without a transaction (start of stream, DDL, OTHER).
    Position { shard: ShardId, position: String },
    /// A DDL statement was observed. Field projections are rebuilt on the next FIELD event.
    Ddl {
        shard: ShardId,
        position: String,
        statement: String,
    },
    /// vtgate heartbeat (only when `heartbeat_interval` is set).
    Heartbeat,
    /// Copy phase finished for `shard`, or for the whole request when `shard` is None.
    CopyCompleted { shard: Option<ShardId> },
}

/// Row changes accumulated while inside `BEGIN ... COMMIT`.
#[derive(Debug, Default)]
struct OpenTransaction {
    shard: Option<ShardId>,
    timestamp: i64,
    changes: Vec<RowChange>,
    position: Option<String>,
    received_at_ms: Option<i64>,
}

/// How consecutive positions are checked when a transaction commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepPolicy {
    /// Every commit must add exactly one transaction id. Anything else is malformed.
    Strict,
    /// Like `Strict`, but a step that adds several ids is accepted and rendered as a set.
    AllowMerged,
    /// No checks: used by the copy phase, where positions repeat and start unknown.
    Lenient,
}

pub struct Assembler<'a> {
    schema: &'a SyncSchema,
    /// Live field metadata per table.
    fields: HashMap<String, TableFields>,
    /// Projection per synced table, rebuilt whenever its FIELD event changes.
    projections: HashMap<String, TableProjection>,
    /// Last known position per shard, for gtid diffing.
    positions: HashMap<ShardId, GtidSet>,
    open: Option<OpenTransaction>,
    policy: StepPolicy,
}

impl<'a> Assembler<'a> {
    /// `start_positions` are the positions the stream was opened from, per shard. Shards
    /// started at `current` are learned from the first VGTID event.
    pub fn new(schema: &'a SyncSchema, start_positions: HashMap<ShardId, GtidSet>, policy: StepPolicy) -> Self {
        Self {
            schema,
            fields: HashMap::new(),
            projections: HashMap::new(),
            positions: start_positions,
            open: None,
            policy,
        }
    }

    pub fn position(&self, shard: &ShardId) -> Option<&GtidSet> {
        self.positions.get(shard)
    }

    /// Feeds one event. Returns zero or more items.
    pub fn push(&mut self, ev: &VEvent, received_at_ms: i64) -> Result<Vec<StreamItem>, VStreamError> {
        let ty = VEventType::try_from(ev.r#type)
            .map_err(|_| VStreamError::Unsupported(format!("unknown event type {}", ev.r#type)))?;
        let shard = || -> Result<ShardId, VStreamError> {
            if ev.keyspace.is_empty() || ev.shard.is_empty() {
                return Err(VStreamError::Malformed(format!("{ty:?} event without keyspace/shard")));
            }
            Ok(ShardId {
                keyspace: ev.keyspace.clone(),
                shard: ev.shard.clone(),
            })
        };
        match ty {
            VEventType::Begin => {
                match &self.open {
                    // The copy phase opens a transaction before it knows whether the table has
                    // rows, and never commits an empty one. Only tolerated in lenient mode.
                    Some(open) if self.policy == StepPolicy::Lenient && open.changes.is_empty() => {}
                    Some(_) => return Err(VStreamError::Malformed("BEGIN while a transaction is open".into())),
                    None => {}
                }
                self.open = Some(OpenTransaction {
                    shard: shard().ok(),
                    timestamp: ev.timestamp,
                    received_at_ms: Some(received_at_ms),
                    ..Default::default()
                });
                Ok(vec![])
            }
            VEventType::Field => {
                let fe = ev
                    .field_event
                    .as_ref()
                    .ok_or_else(|| VStreamError::Malformed("FIELD without field_event".into()))?;
                let tf = TableFields::from_event(fe);
                let table = tf.table.clone();
                if let Some(ts) = self.schema.table(&table) {
                    let projection = TableProjection::build(ts, &tf)?;
                    self.projections.insert(table.clone(), projection);
                } else {
                    debug!(table, "FIELD for a table not in the sync schema");
                }
                self.fields.insert(table, tf);
                Ok(vec![])
            }
            VEventType::Row => {
                let re = ev
                    .row_event
                    .as_ref()
                    .ok_or_else(|| VStreamError::Malformed("ROW without row_event".into()))?;
                let table = strip_keyspace(&re.table_name).to_string();
                let open = self
                    .open
                    .as_mut()
                    .ok_or_else(|| VStreamError::Malformed(format!("ROW for {table} outside a transaction")))?;
                let Some(projection) = self.projections.get(&table) else {
                    if self.schema.table(&table).is_some() {
                        return Err(VStreamError::Malformed(format!(
                            "ROW for synced table {table} before its FIELD event"
                        )));
                    }
                    return Ok(vec![]);
                };
                let fields = self.fields.get(&table).expect("projection implies fields");
                for change in &re.row_changes {
                    let raw = decode_row_change(change, fields)?;
                    let before = raw.before.as_ref().map(|r| projection.project(r)).transpose()?;
                    let after = raw.after.as_ref().map(|r| projection.project(r)).transpose()?;
                    let (op, key) = match (&before, &after) {
                        (None, Some(a)) => (RowOp::Insert, projection.key_of(a)),
                        (Some(_), Some(a)) => (RowOp::Update, projection.key_of(a)),
                        (Some(b), None) => (RowOp::Delete, projection.key_of(b)),
                        (None, None) => unreachable!("decode_row_change rejects empty changes"),
                    };
                    open.changes.push(RowChange {
                        table: table.clone(),
                        op,
                        key,
                        before,
                        after,
                    });
                }
                Ok(vec![])
            }
            VEventType::Vgtid => {
                let vgtid = ev
                    .vgtid
                    .as_ref()
                    .ok_or_else(|| VStreamError::Malformed("VGTID without vgtid".into()))?;
                let shard = shard()?;
                let pos = position_for(vgtid, &shard)?;
                match self.open.as_mut() {
                    Some(open) => {
                        open.position = Some(pos);
                        open.shard.get_or_insert(shard);
                        Ok(vec![])
                    }
                    None => {
                        self.positions.insert(shard.clone(), GtidSet::parse_position(&pos)?);
                        Ok(vec![StreamItem::Position { shard, position: pos }])
                    }
                }
            }
            VEventType::Commit => {
                let open = self
                    .open
                    .take()
                    .ok_or_else(|| VStreamError::Malformed("COMMIT without BEGIN".into()))?;
                let shard = open
                    .shard
                    .clone()
                    .or_else(|| shard().ok())
                    .ok_or_else(|| VStreamError::Malformed("COMMIT without shard".into()))?;
                let position = open
                    .position
                    .ok_or_else(|| VStreamError::Malformed("COMMIT without a VGTID inside the transaction".into()))?;
                let after = GtidSet::parse_position(&position)?;
                let gtid = self.advance(&shard, &after)?;
                Ok(vec![StreamItem::Transaction(SourceTransaction {
                    keyspace: shard.keyspace,
                    shard: shard.shard,
                    gtid,
                    position,
                    commit_timestamp: open.timestamp,
                    changes: open.changes,
                    trace: TraceContext {
                        subscriber_received_at_ms: open.received_at_ms,
                        distributor_dispatched_at_ms: None,
                    },
                })])
            }
            VEventType::Rollback => {
                self.open = None;
                Ok(vec![])
            }
            VEventType::Ddl => {
                if let Some(open) = self.open.take() {
                    // MySQL commits implicitly before DDL; vtgate does not emit COMMIT for it.
                    if !open.changes.is_empty() {
                        return Err(VStreamError::Malformed(
                            "DDL inside a transaction with row changes".into(),
                        ));
                    }
                }
                let shard = shard()?;
                let position = self.positions.get(&shard).map(|p| p.to_position()).unwrap_or_default();
                self.projections.clear();
                self.fields.clear();
                warn!(%shard, statement = %ev.statement, "DDL observed; projections will be rebuilt");
                Ok(vec![StreamItem::Ddl {
                    shard,
                    position,
                    statement: ev.statement.clone(),
                }])
            }
            VEventType::Other
            | VEventType::Gtid
            | VEventType::Savepoint
            | VEventType::Version
            | VEventType::PreviousGtids
            | VEventType::RowsQuery => Ok(vec![]),
            VEventType::Heartbeat => Ok(vec![StreamItem::Heartbeat]),
            VEventType::CopyCompleted => {
                if let Some(open) = &self.open
                    && open.changes.is_empty()
                {
                    self.open = None;
                }
                let shard = if ev.keyspace.is_empty() { None } else { Some(shard()?) };
                Ok(vec![StreamItem::CopyCompleted { shard }])
            }
            VEventType::Journal => Err(VStreamError::Unsupported(
                "reshard journal events are not supported; restart the subscriber after the reshard completes".into(),
            )),
            VEventType::Lastpk => Ok(vec![]),
            VEventType::Unknown
            | VEventType::Insert
            | VEventType::Replace
            | VEventType::Update
            | VEventType::Delete
            | VEventType::Set => Err(VStreamError::Unsupported(format!(
                "statement-level event {ty:?} (binlog_format must be ROW)"
            ))),
        }
    }

    /// Advances the shard position and returns the rendered set of added gtids.
    fn advance(&mut self, shard: &ShardId, after: &GtidSet) -> Result<String, VStreamError> {
        let before = self.positions.get(shard).cloned();
        let gtid = match (&before, self.policy) {
            (_, StepPolicy::Lenient) => {
                let mut set = GtidSet::empty();
                if let Some(before) = &before {
                    for g in after.difference(before) {
                        set.add(&g);
                    }
                }
                set.to_string()
            }
            (None, _) => {
                return Err(VStreamError::Malformed(format!(
                    "transaction on {shard} before any position was known"
                )));
            }
            (Some(before), policy) => match diff_single(before, after) {
                Ok(g) => g.to_string(),
                Err(StepError::MultipleAdded { count, .. }) if policy == StepPolicy::AllowMerged => {
                    warn!(%shard, count, "merged transactions in one step (allowed by configuration)");
                    let mut set = GtidSet::empty();
                    for g in after.difference(before) {
                        set.add(&g);
                    }
                    set.to_string()
                }
                Err(e) => return Err(e.into()),
            },
        };
        self.positions.insert(shard.clone(), after.clone());
        Ok(gtid)
    }
}

/// The position of `shard` inside a VGtid event.
pub fn position_for(vgtid: &VGtid, shard: &ShardId) -> Result<String, VStreamError> {
    vgtid
        .shard_gtids
        .iter()
        .find(|sg| sg.keyspace == shard.keyspace && sg.shard == shard.shard)
        .map(|sg| sg.gtid.clone())
        .ok_or_else(|| VStreamError::Malformed(format!("VGTID event lacks position for {shard}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::binlogdata::{FieldEvent, RowChange as PbRowChange, RowEvent, ShardGtid};
    use crate::proto::query::{Field, Row as PbRow, Type as PbType};
    use orbit_protocol::schema::{ColumnSchema, PartitionConfig, PlacementConfig, TableSchema, ValueKind};

    const U1: &str = "a2523813-adbe-11f1-b19c-0a2250a7ed6c";

    fn schema() -> SyncSchema {
        SyncSchema {
            format_version: 1,
            schema_hash: String::new(),
            app: "t".into(),
            keyspace: "ks".into(),
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
        }
    }

    fn sid() -> ShardId {
        ShardId {
            keyspace: "ks".into(),
            shard: "-".into(),
        }
    }

    fn ev(ty: VEventType) -> VEvent {
        VEvent {
            r#type: ty as i32,
            keyspace: "ks".into(),
            shard: "-".into(),
            timestamp: 1700000000,
            ..Default::default()
        }
    }

    fn vgtid_ev(gno: u64) -> VEvent {
        let mut e = ev(VEventType::Vgtid);
        e.vgtid = Some(VGtid {
            shard_gtids: vec![ShardGtid {
                keyspace: "ks".into(),
                shard: "-".into(),
                gtid: format!("MySQL56/{U1}:1-{gno}"),
                table_p_ks: vec![],
            }],
        });
        e
    }

    fn field_ev() -> VEvent {
        let mut e = ev(VEventType::Field);
        e.field_event = Some(FieldEvent {
            table_name: "ks.t".into(),
            fields: vec![
                Field {
                    name: "id".into(),
                    r#type: PbType::Varchar as i32,
                    column_type: "varchar(32)".into(),
                    ..Default::default()
                },
                Field {
                    name: "org".into(),
                    r#type: PbType::Varchar as i32,
                    column_type: "varchar(32)".into(),
                    ..Default::default()
                },
                Field {
                    name: "n".into(),
                    r#type: PbType::Int32 as i32,
                    column_type: "int".into(),
                    ..Default::default()
                },
            ],
            keyspace: "ks".into(),
            shard: "-".into(),
            enum_set_string_values: true,
            ..Default::default()
        });
        e
    }

    fn row(vals: &[Option<&str>]) -> PbRow {
        let mut lengths = vec![];
        let mut values = vec![];
        for v in vals {
            match v {
                None => lengths.push(-1),
                Some(s) => {
                    lengths.push(s.len() as i64);
                    values.extend_from_slice(s.as_bytes());
                }
            }
        }
        PbRow { lengths, values }
    }

    type Image<'a> = Option<&'a [Option<&'a str>]>;

    fn row_ev(changes: Vec<(Image<'_>, Image<'_>)>) -> VEvent {
        let mut e = ev(VEventType::Row);
        e.row_event = Some(RowEvent {
            table_name: "ks.t".into(),
            row_changes: changes
                .into_iter()
                .map(|(b, a)| PbRowChange {
                    before: b.map(row),
                    after: a.map(row),
                    ..Default::default()
                })
                .collect(),
            keyspace: "ks".into(),
            shard: "-".into(),
            ..Default::default()
        });
        e
    }

    fn assembler(schema: &SyncSchema, start: u64) -> Assembler<'_> {
        let mut pos = HashMap::new();
        pos.insert(sid(), GtidSet::parse_set(&format!("{U1}:1-{start}")).unwrap());
        Assembler::new(schema, pos, StepPolicy::Strict)
    }

    #[test]
    fn assembles_a_transaction_across_responses() {
        let s = schema();
        let mut a = assembler(&s, 10);
        assert!(a.push(&ev(VEventType::Begin), 1).unwrap().is_empty());
        assert!(a.push(&field_ev(), 1).unwrap().is_empty());
        assert!(
            a.push(&row_ev(vec![(None, Some(&[Some("a"), Some("o1"), Some("1")]))]), 1)
                .unwrap()
                .is_empty()
        );
        assert!(
            a.push(
                &row_ev(vec![
                    (
                        Some(&[Some("a"), Some("o1"), Some("1")]),
                        Some(&[Some("a"), Some("o1"), Some("2")])
                    ),
                    (Some(&[Some("b"), Some("o2"), None]), None)
                ]),
                1
            )
            .unwrap()
            .is_empty()
        );
        assert!(a.push(&vgtid_ev(11), 1).unwrap().is_empty());
        let items = a.push(&ev(VEventType::Commit), 1).unwrap();
        assert_eq!(items.len(), 1);
        let StreamItem::Transaction(tx) = &items[0] else {
            panic!("expected txn")
        };
        assert_eq!(tx.gtid, format!("{U1}:11"));
        assert_eq!(tx.position, format!("MySQL56/{U1}:1-11"));
        assert_eq!(tx.changes.len(), 3);
        assert_eq!(tx.changes[0].op, RowOp::Insert);
        assert_eq!(tx.changes[1].op, RowOp::Update);
        assert_eq!(tx.changes[2].op, RowOp::Delete);
        assert_eq!(tx.changes[2].key, vec![serde_json::json!("b")]);
        assert_eq!(tx.changes[1].after.as_ref().unwrap()["n"], 2);
    }

    #[test]
    fn empty_heartbeat_transactions_still_advance() {
        let s = schema();
        let mut a = assembler(&s, 10);
        a.push(&ev(VEventType::Begin), 1).unwrap();
        a.push(&vgtid_ev(11), 1).unwrap();
        let items = a.push(&ev(VEventType::Commit), 1).unwrap();
        let StreamItem::Transaction(tx) = &items[0] else {
            panic!()
        };
        assert!(tx.changes.is_empty());
        assert_eq!(tx.gtid, format!("{U1}:11"));
    }

    #[test]
    fn skipped_position_is_malformed_unless_allowed() {
        let s = schema();
        let mut a = assembler(&s, 10);
        a.push(&ev(VEventType::Begin), 1).unwrap();
        a.push(&vgtid_ev(13), 1).unwrap();
        assert!(matches!(
            a.push(&ev(VEventType::Commit), 1),
            Err(VStreamError::Step(StepError::MultipleAdded { count: 3, .. }))
        ));

        let mut pos = HashMap::new();
        pos.insert(sid(), GtidSet::parse_set(&format!("{U1}:1-10")).unwrap());
        let mut a = Assembler::new(&s, pos, StepPolicy::AllowMerged);
        a.push(&ev(VEventType::Begin), 1).unwrap();
        a.push(&vgtid_ev(13), 1).unwrap();
        let items = a.push(&ev(VEventType::Commit), 1).unwrap();
        let StreamItem::Transaction(tx) = &items[0] else {
            panic!()
        };
        assert_eq!(tx.gtid, format!("{U1}:11-13"));
    }

    #[test]
    fn malformed_sequences_are_rejected() {
        let s = schema();
        let mut a = assembler(&s, 10);
        assert!(matches!(
            a.push(&ev(VEventType::Commit), 1),
            Err(VStreamError::Malformed(_))
        ));
        a.push(&ev(VEventType::Begin), 1).unwrap();
        assert!(matches!(
            a.push(&ev(VEventType::Begin), 1),
            Err(VStreamError::Malformed(_))
        ));
        let mut a = assembler(&s, 10);
        a.push(&ev(VEventType::Begin), 1).unwrap();
        // ROW for a synced table before FIELD
        assert!(matches!(
            a.push(&row_ev(vec![(None, Some(&[Some("a"), Some("o"), None]))]), 1),
            Err(VStreamError::Malformed(_))
        ));
        let mut a = assembler(&s, 10);
        a.push(&ev(VEventType::Begin), 1).unwrap();
        // COMMIT without VGTID
        assert!(matches!(
            a.push(&ev(VEventType::Commit), 1),
            Err(VStreamError::Malformed(_))
        ));
        let mut a = assembler(&s, 10);
        assert!(matches!(
            a.push(&ev(VEventType::Journal), 1),
            Err(VStreamError::Unsupported(_))
        ));
    }

    #[test]
    fn initial_vgtid_outside_transaction_sets_position() {
        let s = schema();
        let mut a = Assembler::new(&s, HashMap::new(), StepPolicy::Strict);
        let items = a.push(&vgtid_ev(42), 1).unwrap();
        assert!(matches!(&items[0], StreamItem::Position { position, .. } if position.ends_with(":1-42")));
        a.push(&ev(VEventType::Begin), 1).unwrap();
        a.push(&vgtid_ev(43), 1).unwrap();
        let items = a.push(&ev(VEventType::Commit), 1).unwrap();
        let StreamItem::Transaction(tx) = &items[0] else {
            panic!()
        };
        assert_eq!(tx.gtid, format!("{U1}:43"));
    }

    #[test]
    fn ddl_clears_projections() {
        let s = schema();
        let mut a = assembler(&s, 10);
        a.push(&field_ev(), 1).unwrap();
        assert!(a.projections.contains_key("t"));
        let mut d = ev(VEventType::Ddl);
        d.statement = "alter table t add column x int".into();
        let items = a.push(&d, 1).unwrap();
        assert!(matches!(&items[0], StreamItem::Ddl { .. }));
        assert!(a.projections.is_empty());
    }
}
