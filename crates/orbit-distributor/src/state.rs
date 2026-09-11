//! Durable distributor state in SQLite.
//!
//! What is persisted, and why it is enough:
//! * `checkpoint`: the stream positions per shard and the epoch. Everything at or before it is
//!   durably applied by every partition's Durable Object.
//! * `partition_counters`: the next sequence number per partition *as of the checkpoint*.
//!   Replaying the stream from the checkpoint re-derives identical (partition, seq) assignments,
//!   because routing is deterministic. Durable Objects deduplicate by seq.
//! * `quarantine`: partition transactions whose delivery was rejected permanently. They are kept
//!   verbatim so an operator can replay them after fixing the target.
//! * `parent_index`: for every parent table (a table named as `partition_parent` by some other
//!   table), the partition of each parent row keyed by its rendered primary key. The router
//!   resolves the partition of a derived row through it. Entries are never deleted, not even
//!   when the parent row is deleted: a late child change (for example a delete that arrives
//!   after the parent is gone) still routes to the partition the parent had.
//! * `parent_index_ready`: one row per parent table whose bootstrap copy completed, with the
//!   stream position the copy was taken at.
//!
//! The checkpoint, the counters and the parent index updates of the checkpointed transactions
//! are written in one SQLite transaction, so a crash between them cannot desynchronize sequence
//! assignment or derived routing from the resume position.

use std::collections::BTreeMap;
use std::path::Path;

use orbit_protocol::cdc::PartitionTransaction;
use orbit_vstream::checkpoint::{Checkpoint, ShardId};
use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("state was created for schema {stored} but the running schema is {running}")]
    SchemaChanged { stored: String, running: String },
}

pub struct StateStore {
    conn: Connection,
}

/// One parent index entry: the partition of parent row `key` of table `table`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentEntry {
    pub table: String,
    pub key: String,
    pub partition: String,
}

const UPSERT_PARENT: &str = "INSERT INTO parent_index (tbl, key, partition) VALUES (?1, ?2, ?3) ON CONFLICT(tbl, key) DO UPDATE SET partition = excluded.partition";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedState {
    pub checkpoint: Checkpoint,
    pub counters: BTreeMap<String, u64>,
    pub schema_hash: String,
}

/// Epoch for a fresh state store: the current unix time in seconds. A Durable Object resets its
/// cache when it sees a higher epoch than it stored, so a state file created after any earlier
/// one always wins. A fixed initial epoch would make a recreated state file collide with the
/// sequence history the Durable Objects kept from the earlier file (`SequenceConflict`).
fn initial_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(1)
        .max(1)
}

impl StateStore {
    pub fn open(path: &Path) -> Result<Self, StateError> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        Self::init(conn)
    }

    pub fn open_in_memory() -> Result<Self, StateError> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self, StateError> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS partition_counters (partition TEXT PRIMARY KEY, next_seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS quarantine (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                partition TEXT NOT NULL,
                seq INTEGER NOT NULL,
                reason TEXT NOT NULL,
                transaction_json TEXT NOT NULL,
                quarantined_at INTEGER NOT NULL,
                UNIQUE (partition, seq)
            );
            CREATE TABLE IF NOT EXISTS parent_index (
                tbl TEXT NOT NULL,
                key TEXT NOT NULL,
                partition TEXT NOT NULL,
                PRIMARY KEY (tbl, key)
            );
            CREATE INDEX IF NOT EXISTS parent_index_by_partition ON parent_index (tbl, partition);
            CREATE TABLE IF NOT EXISTS parent_index_ready (tbl TEXT PRIMARY KEY, position TEXT NOT NULL);
            "#,
        )?;
        Ok(Self { conn })
    }

    /// Loads the persisted state, or initializes it for `schema_hash` with an empty checkpoint.
    /// A stored schema hash different from `schema_hash` is refused: sequence counters and
    /// routing are only meaningful for the schema they were produced with. Use
    /// [`StateStore::migrate_schema`] to accept a new schema deliberately.
    pub fn load_or_init(&self, schema_hash: &str) -> Result<PersistedState, StateError> {
        let stored: Option<String> = self.get_meta("schema_hash")?;
        match stored {
            None => {
                let tx = self.conn.unchecked_transaction()?;
                tx.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_hash', ?1)",
                    params![schema_hash],
                )?;
                tx.execute(
                    "INSERT INTO meta (key, value) VALUES ('epoch', ?1)",
                    params![initial_epoch().to_string()],
                )?;
                tx.execute("INSERT INTO meta (key, value) VALUES ('positions', '{}')", [])?;
                tx.commit()?;
            }
            Some(s) if s != schema_hash => {
                return Err(StateError::SchemaChanged {
                    stored: s,
                    running: schema_hash.to_string(),
                });
            }
            Some(_) => {}
        }
        self.load()
    }

    /// Accepts a new schema hash. Sequence counters are kept: partitions are identified by key,
    /// not by schema, and Durable Objects validate the hash on every batch.
    pub fn migrate_schema(&self, schema_hash: &str) -> Result<(), StateError> {
        self.conn.execute("INSERT INTO meta (key, value) VALUES ('schema_hash', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value", params![schema_hash])?;
        Ok(())
    }

    fn load(&self) -> Result<PersistedState, StateError> {
        let epoch: u64 = self
            .get_meta("epoch")?
            .unwrap_or_else(|| "1".into())
            .parse()
            .unwrap_or(1);
        let positions_json: String = self.get_meta("positions")?.unwrap_or_else(|| "{}".into());
        let positions: BTreeMap<String, String> = serde_json::from_str(&positions_json)?;
        let mut checkpoint = Checkpoint::empty(epoch);
        for (k, v) in positions {
            let (keyspace, shard) = k.split_once('/').unwrap_or((&k, ""));
            checkpoint.set(
                ShardId {
                    keyspace: keyspace.to_string(),
                    shard: shard.to_string(),
                },
                v,
            );
        }
        let mut stmt = self
            .conn
            .prepare("SELECT partition, next_seq FROM partition_counters")?;
        let counters = stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64)))?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let schema_hash = self.get_meta("schema_hash")?.unwrap_or_default();
        Ok(PersistedState {
            checkpoint,
            counters,
            schema_hash,
        })
    }

    /// Atomically persists a new checkpoint together with the counters of the partitions that
    /// advanced since the last persisted checkpoint and the parent index entries written by the
    /// transactions the checkpoint now covers (`parent_updates`, in stream order).
    pub fn commit_checkpoint(
        &self,
        checkpoint: &Checkpoint,
        counter_updates: &BTreeMap<String, u64>,
        parent_updates: &[ParentEntry],
    ) -> Result<(), StateError> {
        let positions: BTreeMap<String, String> = checkpoint
            .positions
            .iter()
            .map(|(k, v)| (format!("{}/{}", k.keyspace, k.shard), v.clone()))
            .collect();
        let tx = self.conn.unchecked_transaction()?;
        tx.execute("INSERT INTO meta (key, value) VALUES ('positions', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value", params![serde_json::to_string(&positions)?])?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('epoch', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![checkpoint.epoch.to_string()],
        )?;
        {
            let mut stmt = tx.prepare_cached("INSERT INTO partition_counters (partition, next_seq) VALUES (?1, ?2) ON CONFLICT(partition) DO UPDATE SET next_seq = excluded.next_seq")?;
            for (p, seq) in counter_updates {
                stmt.execute(params![p, *seq as i64])?;
            }
        }
        {
            let mut stmt = tx.prepare_cached(UPSERT_PARENT)?;
            for e in parent_updates {
                stmt.execute(params![e.table, e.key, e.partition])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Operator action: reset the checkpoint to `positions` and bump the epoch. Counters are
    /// preserved so sequence numbers keep increasing; Durable Objects discard cached scopes on
    /// an epoch change and refill on demand.
    pub fn reset_checkpoint(&self, checkpoint: &Checkpoint) -> Result<u64, StateError> {
        let current = self.load()?;
        let mut next = checkpoint.clone();
        next.epoch = (current.checkpoint.epoch + 1).max(initial_epoch());
        self.commit_checkpoint(&next, &BTreeMap::new(), &[])?;
        Ok(next.epoch)
    }

    /// The partition of parent row `key` of table `tbl`, as last recorded.
    pub fn parent_partition(&self, tbl: &str, key: &str) -> Result<Option<String>, StateError> {
        Ok(self
            .conn
            .query_row(
                "SELECT partition FROM parent_index WHERE tbl = ?1 AND key = ?2",
                params![tbl, key],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Records (or overwrites) the partition of one parent row.
    pub fn upsert_parent(&self, tbl: &str, key: &str, partition: &str) -> Result<(), StateError> {
        self.conn.execute(UPSERT_PARENT, params![tbl, key, partition])?;
        Ok(())
    }

    /// Records many parent rows of one table in one SQLite transaction. Used by the bootstrap
    /// copy. Returns the number of rows written.
    pub fn bulk_upsert_parents<'a>(
        &self,
        tbl: &str,
        rows: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<usize, StateError> {
        let tx = self.conn.unchecked_transaction()?;
        let mut n = 0;
        {
            let mut stmt = tx.prepare_cached(UPSERT_PARENT)?;
            for (key, partition) in rows {
                stmt.execute(params![tbl, key, partition])?;
                n += 1;
            }
        }
        tx.commit()?;
        Ok(n)
    }

    /// Rendered keys of every parent row of `tbl` recorded in `partition`.
    pub fn parent_keys_in_partition(&self, tbl: &str, partition: &str) -> Result<Vec<String>, StateError> {
        let mut stmt = self
            .conn
            .prepare("SELECT key FROM parent_index WHERE tbl = ?1 AND partition = ?2 ORDER BY key")?;
        let rows = stmt.query_map(params![tbl, partition], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// The position the bootstrap copy of `tbl` was taken at, or `None` when the index of
    /// `tbl` is not complete yet.
    pub fn parent_index_ready(&self, tbl: &str) -> Result<Option<String>, StateError> {
        Ok(self
            .conn
            .query_row(
                "SELECT position FROM parent_index_ready WHERE tbl = ?1",
                params![tbl],
                |r| r.get(0),
            )
            .optional()?)
    }

    /// Marks the index of `tbl` complete as of `position`.
    pub fn mark_parent_index_ready(&self, tbl: &str, position: &str) -> Result<(), StateError> {
        self.conn.execute(
            "INSERT INTO parent_index_ready (tbl, position) VALUES (?1, ?2) ON CONFLICT(tbl) DO UPDATE SET position = excluded.position",
            params![tbl, position],
        )?;
        Ok(())
    }

    pub fn quarantine(&self, tx: &PartitionTransaction, partition: &str, reason: &str) -> Result<(), StateError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        self.conn.execute(
            "INSERT OR IGNORE INTO quarantine (partition, seq, reason, transaction_json, quarantined_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![partition, tx.seq as i64, reason, serde_json::to_string(tx)?, now],
        )?;
        Ok(())
    }

    pub fn quarantined(&self) -> Result<Vec<(String, PartitionTransaction, String)>, StateError> {
        let mut stmt = self
            .conn
            .prepare("SELECT partition, transaction_json, reason FROM quarantine ORDER BY partition, seq")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })?;
        let mut out = Vec::new();
        for r in rows {
            let (p, json, reason) = r?;
            out.push((p, serde_json::from_str(&json)?, reason));
        }
        Ok(out)
    }

    pub fn quarantined_partitions(&self) -> Result<Vec<String>, StateError> {
        let mut stmt = self
            .conn
            .prepare("SELECT DISTINCT partition FROM quarantine ORDER BY partition")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn clear_quarantine(&self, partition: &str) -> Result<usize, StateError> {
        Ok(self
            .conn
            .execute("DELETE FROM quarantine WHERE partition = ?1", params![partition])?)
    }

    fn get_meta(&self, key: &str) -> Result<Option<String>, StateError> {
        Ok(self
            .conn
            .query_row("SELECT value FROM meta WHERE key = ?1", params![key], |r| r.get(0))
            .optional()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_persist_and_reload() {
        let dir = std::env::temp_dir().join(format!("orbit-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.sqlite");
        let _ = std::fs::remove_file(&path);
        {
            let store = StateStore::open(&path).unwrap();
            let st = store.load_or_init("h1").unwrap();
            let e0 = st.checkpoint.epoch;
            assert!(e0 > 1_600_000_000, "fresh epochs are unix seconds: {e0}");
            assert!(st.counters.is_empty());
            let mut cp = Checkpoint::empty(e0);
            cp.set(
                ShardId {
                    keyspace: "ks".into(),
                    shard: "0".into(),
                },
                "MySQL56/x:1-5".into(),
            );
            let mut updates = BTreeMap::new();
            updates.insert("org1".to_string(), 7u64);
            store
                .commit_checkpoint(
                    &cp,
                    &updates,
                    &[ParentEntry {
                        table: "p".into(),
                        key: "k1".into(),
                        partition: "org1".into(),
                    }],
                )
                .unwrap();
        }
        {
            let store = StateStore::open(&path).unwrap();
            assert!(matches!(
                store.load_or_init("h2"),
                Err(StateError::SchemaChanged { .. })
            ));
            let st = store.load_or_init("h1").unwrap();
            assert_eq!(
                st.checkpoint.position(&ShardId {
                    keyspace: "ks".into(),
                    shard: "0".into()
                }),
                Some("MySQL56/x:1-5")
            );
            assert_eq!(st.counters["org1"], 7);
            let before = store.load().unwrap().checkpoint.epoch;
            let epoch = store.reset_checkpoint(&Checkpoint::empty(0)).unwrap();
            assert!(epoch > before);
            let st = store.load_or_init("h1").unwrap();
            assert_eq!(st.checkpoint.epoch, epoch);
            assert!(st.checkpoint.positions.is_empty());
            assert_eq!(st.counters["org1"], 7, "counters survive a reset");
            assert_eq!(
                store.parent_partition("p", "k1").unwrap().as_deref(),
                Some("org1"),
                "parent index entries commit with the checkpoint"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parent_index_round_trip() {
        let store = StateStore::open_in_memory().unwrap();
        store.load_or_init("h").unwrap();
        assert_eq!(store.parent_index_ready("p").unwrap(), None);
        assert_eq!(store.parent_partition("p", "k1").unwrap(), None);
        let n = store
            .bulk_upsert_parents("p", [("k1", "A"), ("k2", "A"), ("k3", "B")])
            .unwrap();
        assert_eq!(n, 3);
        store.mark_parent_index_ready("p", "MySQL56/u:1-9").unwrap();
        assert_eq!(store.parent_index_ready("p").unwrap().as_deref(), Some("MySQL56/u:1-9"));
        assert_eq!(store.parent_partition("p", "k3").unwrap().as_deref(), Some("B"));
        // A later change moves the parent; the entry is overwritten, never removed.
        store.upsert_parent("p", "k3", "A").unwrap();
        assert_eq!(store.parent_partition("p", "k3").unwrap().as_deref(), Some("A"));
        assert_eq!(
            store.parent_keys_in_partition("p", "A").unwrap(),
            vec!["k1", "k2", "k3"]
        );
        assert!(store.parent_keys_in_partition("p", "B").unwrap().is_empty());
        assert_eq!(store.parent_partition("other", "k1").unwrap(), None);
    }

    #[test]
    fn quarantine_round_trip() {
        let store = StateStore::open_in_memory().unwrap();
        store.load_or_init("h").unwrap();
        let tx = PartitionTransaction {
            seq: 3,
            keyspace: "k".into(),
            shard: "0".into(),
            gtid: "u:3".into(),
            position: "MySQL56/u:1-3".into(),
            commit_timestamp: 0,
            changes: vec![],
            trace: Default::default(),
        };
        store.quarantine(&tx, "p1", "sequence_gap").unwrap();
        store.quarantine(&tx, "p1", "sequence_gap").unwrap();
        let q = store.quarantined().unwrap();
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].1, tx);
        assert_eq!(store.quarantined_partitions().unwrap(), vec!["p1"]);
        assert_eq!(store.clear_quarantine("p1").unwrap(), 1);
    }
}
