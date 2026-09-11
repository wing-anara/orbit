//! Checkpoints: per-shard positions plus the stream epoch.

use std::collections::BTreeMap;

use orbit_gtid::GtidSet;
use serde::{Deserialize, Serialize};

use crate::error::VStreamError;
use crate::proto::binlogdata::{ShardGtid, VGtid};

/// Identifies one shard stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardId {
    pub keyspace: String,
    pub shard: String,
}

impl std::fmt::Display for ShardId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.keyspace, self.shard)
    }
}

/// Where to resume. `positions` holds the `MySQL56/...` position per shard.
///
/// `epoch` increments every time an operator resets the checkpoint to a position that is not a
/// continuation of the previous one. Consumers use it to detect that history was skipped.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub epoch: u64,
    pub positions: BTreeMap<ShardId, String>,
}

impl Checkpoint {
    pub fn empty(epoch: u64) -> Self {
        Self {
            epoch,
            positions: BTreeMap::new(),
        }
    }

    pub fn position(&self, shard: &ShardId) -> Option<&str> {
        self.positions.get(shard).map(String::as_str)
    }

    pub fn set(&mut self, shard: ShardId, position: String) {
        self.positions.insert(shard, position);
    }

    /// Parsed positions, validating every entry.
    pub fn parsed(&self) -> Result<BTreeMap<ShardId, GtidSet>, VStreamError> {
        self.positions
            .iter()
            .map(|(k, v)| Ok((k.clone(), GtidSet::parse_position(v)?)))
            .collect()
    }

    /// True when every shard position in `self` is contained in `current`. Shards missing from
    /// `current` fail the check; shards missing from `self` are fine (they start at `current`).
    pub fn is_contained_in(&self, current: &Checkpoint) -> Result<bool, VStreamError> {
        let mine = self.parsed()?;
        let theirs = current.parsed()?;
        for (shard, pos) in &mine {
            match theirs.get(shard) {
                Some(cur) if pos.is_subset_of(cur) => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    /// Builds the `VGtid` for a stream request.
    ///
    /// * With explicit `shards`, each listed shard resumes from its checkpoint or `current`.
    /// * With `shards == [""]` ("all shards"), every shard that has a checkpoint resumes from it
    ///   and, if none has one, the request asks vtgate for all shards at `current`. Vitess has no
    ///   way to say "all shards, resuming these", so a keyspace that gains a shard after the
    ///   checkpoint was taken fails the checkpoint validation instead of streaming silently.
    pub fn to_vgtid(&self, keyspace: &str, shards: &[String]) -> VGtid {
        let all = shards.iter().all(|s| s.is_empty());
        let known: Vec<&ShardId> = self.positions.keys().filter(|id| id.keyspace == keyspace).collect();
        let shard_gtids = if all && !known.is_empty() {
            known
                .into_iter()
                .map(|id| ShardGtid {
                    keyspace: keyspace.to_string(),
                    shard: id.shard.clone(),
                    gtid: self.position(id).unwrap_or("current").to_string(),
                    table_p_ks: vec![],
                })
                .collect()
        } else {
            shards
                .iter()
                .map(|shard| {
                    let id = ShardId {
                        keyspace: keyspace.to_string(),
                        shard: shard.clone(),
                    };
                    ShardGtid {
                        keyspace: keyspace.to_string(),
                        shard: shard.clone(),
                        gtid: self.position(&id).unwrap_or("current").to_string(),
                        table_p_ks: vec![],
                    }
                })
                .collect()
        };
        VGtid { shard_gtids }
    }

    pub fn from_vgtid(epoch: u64, vgtid: &VGtid) -> Self {
        let mut cp = Checkpoint::empty(epoch);
        for sg in &vgtid.shard_gtids {
            cp.set(
                ShardId {
                    keyspace: sg.keyspace.clone(),
                    shard: sg.shard.clone(),
                },
                sg.gtid.clone(),
            );
        }
        cp
    }

    pub fn render(&self) -> String {
        self.positions
            .iter()
            .map(|(k, v)| format!("{k}@{v}"))
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const U1: &str = "a2523813-adbe-11f1-b19c-0a2250a7ed6c";

    fn sid(s: &str) -> ShardId {
        ShardId {
            keyspace: "ks".into(),
            shard: s.into(),
        }
    }

    #[test]
    fn containment_per_shard() {
        let mut cp = Checkpoint::empty(1);
        cp.set(sid("-"), format!("MySQL56/{U1}:1-10"));
        let mut cur = Checkpoint::empty(1);
        cur.set(sid("-"), format!("MySQL56/{U1}:1-12"));
        assert!(cp.is_contained_in(&cur).unwrap());
        assert!(!cur.is_contained_in(&cp).unwrap());
        let other = Checkpoint::empty(1);
        assert!(!cp.is_contained_in(&other).unwrap());
        assert!(other.is_contained_in(&cp).unwrap());
    }

    #[test]
    fn all_shards_request_uses_known_shard_names() {
        let mut cp = Checkpoint::empty(1);
        cp.set(sid("0"), format!("MySQL56/{U1}:1-103"));
        let v = cp.to_vgtid("ks", &[String::new()]);
        assert_eq!(v.shard_gtids.len(), 1);
        assert_eq!(v.shard_gtids[0].shard, "0");
        assert_eq!(v.shard_gtids[0].gtid, format!("MySQL56/{U1}:1-103"));
        let empty = Checkpoint::empty(1);
        let v = empty.to_vgtid("ks", &[String::new()]);
        assert_eq!(v.shard_gtids[0].shard, "");
        assert_eq!(v.shard_gtids[0].gtid, "current");
    }

    #[test]
    fn vgtid_round_trip_and_current_default() {
        let mut cp = Checkpoint::empty(3);
        cp.set(sid("-80"), format!("MySQL56/{U1}:1-10"));
        let v = cp.to_vgtid("ks", &["-80".into(), "80-".into()]);
        assert_eq!(v.shard_gtids[0].gtid, format!("MySQL56/{U1}:1-10"));
        assert_eq!(v.shard_gtids[1].gtid, "current");
        let back = Checkpoint::from_vgtid(3, &v);
        assert_eq!(back.position(&sid("80-")), Some("current"));
    }
}
