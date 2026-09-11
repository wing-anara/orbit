//! MySQL 5.6+ GTID sets, as carried inside Vitess positions.
//!
//! A Vitess position looks like `MySQL56/<uuid>:1-5:7,<uuid2>:1-3`. This crate parses the
//! `MySQL56/` flavor into an ordered set of `(uuid, intervals)` and offers the three operations
//! the sync engine needs:
//!
//! * `contains(gtid)`: "was this transaction already applied?"
//! * `is_subset_of(other)`: "is my checkpoint at or behind the server's current position?"
//! * `diff_single(before, after)`: recover the single transaction id that a stream step added.
//!
//! GTID sets are not totally ordered in general, so nothing here pretends they are. Ordering of
//! transactions inside one shard stream is given by delivery order, not by comparing sets.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

pub const MYSQL56_PREFIX: &str = "MySQL56/";

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum GtidError {
    #[error("unsupported position flavor: expected 'MySQL56/' prefix in {0:?}")]
    UnsupportedFlavor(String),
    #[error("invalid GTID set {0:?}: {1}")]
    Invalid(String, String),
}

/// A single transaction identifier `uuid:gno`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Gtid {
    pub uuid: String,
    pub gno: u64,
}

impl Gtid {
    pub fn parse(s: &str) -> Result<Self, GtidError> {
        let s = s.strip_prefix(MYSQL56_PREFIX).unwrap_or(s);
        let (uuid, gno) = s
            .split_once(':')
            .ok_or_else(|| GtidError::Invalid(s.to_string(), "expected uuid:gno".into()))?;
        validate_uuid(uuid).map_err(|m| GtidError::Invalid(s.to_string(), m))?;
        let gno = gno
            .parse::<u64>()
            .map_err(|_| GtidError::Invalid(s.to_string(), "gno must be an integer".into()))?;
        if gno == 0 {
            return Err(GtidError::Invalid(s.to_string(), "gno must be >= 1".into()));
        }
        Ok(Self {
            uuid: uuid.to_ascii_lowercase(),
            gno,
        })
    }
}

impl fmt::Display for Gtid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.uuid, self.gno)
    }
}

/// Closed interval of transaction numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Interval {
    pub start: u64,
    pub end: u64,
}

/// An ordered GTID set. Intervals per uuid are normalized: sorted, non-overlapping, merged.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct GtidSet {
    sets: BTreeMap<String, Vec<Interval>>,
}

impl GtidSet {
    pub fn empty() -> Self {
        Self::default()
    }

    /// Parses `MySQL56/uuid:1-5,uuid2:3`. The `MySQL56/` prefix is required unless `s` is empty.
    pub fn parse_position(s: &str) -> Result<Self, GtidError> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::empty());
        }
        let body = s
            .strip_prefix(MYSQL56_PREFIX)
            .ok_or_else(|| GtidError::UnsupportedFlavor(s.to_string()))?;
        Self::parse_set(body)
    }

    /// Parses the bare set form `uuid:1-5:7,uuid2:1-3` (no flavor prefix).
    pub fn parse_set(body: &str) -> Result<Self, GtidError> {
        let mut out = Self::empty();
        let body = body.trim();
        if body.is_empty() {
            return Ok(out);
        }
        for part in body.split(',') {
            let part = part.trim();
            if part.is_empty() {
                return Err(GtidError::Invalid(body.into(), "empty uuid set".into()));
            }
            let mut pieces = part.split(':');
            let uuid = pieces.next().unwrap_or_default();
            validate_uuid(uuid).map_err(|m| GtidError::Invalid(body.into(), m))?;
            let uuid = uuid.to_ascii_lowercase();
            let mut any = false;
            for iv in pieces {
                any = true;
                let (a, b) = match iv.split_once('-') {
                    Some((a, b)) => (a, b),
                    None => (iv, iv),
                };
                let start = a
                    .parse::<u64>()
                    .map_err(|_| GtidError::Invalid(body.into(), format!("bad interval start {a:?}")))?;
                let end = b
                    .parse::<u64>()
                    .map_err(|_| GtidError::Invalid(body.into(), format!("bad interval end {b:?}")))?;
                if start == 0 || end < start {
                    return Err(GtidError::Invalid(body.into(), format!("bad interval {iv:?}")));
                }
                out.add_interval(&uuid, Interval { start, end });
            }
            if !any {
                return Err(GtidError::Invalid(body.into(), "uuid without intervals".into()));
            }
        }
        Ok(out)
    }

    /// Renders as a Vitess position with the `MySQL56/` prefix. Empty set renders as "".
    pub fn to_position(&self) -> String {
        if self.sets.is_empty() {
            return String::new();
        }
        format!("{MYSQL56_PREFIX}{}", self)
    }

    pub fn is_empty(&self) -> bool {
        self.sets.is_empty()
    }

    pub fn add(&mut self, gtid: &Gtid) {
        self.add_interval(
            &gtid.uuid,
            Interval {
                start: gtid.gno,
                end: gtid.gno,
            },
        );
    }

    pub fn add_interval(&mut self, uuid: &str, iv: Interval) {
        let list = self.sets.entry(uuid.to_string()).or_default();
        list.push(iv);
        normalize(list);
    }

    pub fn contains(&self, gtid: &Gtid) -> bool {
        self.sets
            .get(&gtid.uuid)
            .map(|ivs| ivs.iter().any(|iv| iv.start <= gtid.gno && gtid.gno <= iv.end))
            .unwrap_or(false)
    }

    /// True when every transaction in `self` is also in `other`.
    pub fn is_subset_of(&self, other: &Self) -> bool {
        self.sets.iter().all(|(uuid, ivs)| {
            let Some(theirs) = other.sets.get(uuid) else {
                return false;
            };
            ivs.iter()
                .all(|iv| theirs.iter().any(|t| t.start <= iv.start && iv.end <= t.end))
        })
    }

    /// Set union.
    pub fn union(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for (uuid, ivs) in &other.sets {
            for iv in ivs {
                out.add_interval(uuid, *iv);
            }
        }
        out
    }

    /// Transactions in `self` that are not in `other`, as a set. Interval arithmetic: cost is
    /// proportional to the number of intervals, not to the number of transactions.
    pub fn difference_set(&self, other: &Self) -> GtidSet {
        let mut out = GtidSet::empty();
        for (uuid, ivs) in &self.sets {
            let theirs: &[Interval] = other.sets.get(uuid).map(Vec::as_slice).unwrap_or(&[]);
            for iv in ivs {
                let mut cursor = iv.start;
                for t in theirs {
                    if t.end < cursor {
                        continue;
                    }
                    if t.start > iv.end {
                        break;
                    }
                    if t.start > cursor {
                        out.add_interval(
                            uuid,
                            Interval {
                                start: cursor,
                                end: t.start - 1,
                            },
                        );
                    }
                    cursor = cursor.max(t.end.saturating_add(1));
                    if cursor > iv.end {
                        break;
                    }
                }
                if cursor <= iv.end {
                    out.add_interval(
                        uuid,
                        Interval {
                            start: cursor,
                            end: iv.end,
                        },
                    );
                }
            }
        }
        out
    }

    /// Transactions in `self` that are not in `other`, enumerated. Only use when the difference
    /// is known to be small (see [`diff_single`]).
    pub fn difference(&self, other: &Self) -> Vec<Gtid> {
        let mut out = Vec::new();
        for (uuid, ivs) in &self.difference_set(other).sets {
            for iv in ivs {
                for gno in iv.start..=iv.end {
                    out.push(Gtid {
                        uuid: uuid.clone(),
                        gno,
                    });
                }
            }
        }
        out
    }

    /// Total number of transactions in the set (sum of interval widths).
    pub fn count(&self) -> u64 {
        self.sets.values().flatten().map(|iv| iv.end - iv.start + 1).sum()
    }

    pub fn uuids(&self) -> impl Iterator<Item = &str> {
        self.sets.keys().map(String::as_str)
    }
}

impl fmt::Display for GtidSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (uuid, ivs) in &self.sets {
            if !first {
                f.write_str(",")?;
            }
            first = false;
            f.write_str(uuid)?;
            for iv in ivs {
                if iv.start == iv.end {
                    write!(f, ":{}", iv.start)?;
                } else {
                    write!(f, ":{}-{}", iv.start, iv.end)?;
                }
            }
        }
        Ok(())
    }
}

/// Given the positions before and after one stream step, return the single transaction the
/// step added. Errors when the step added zero or more than one transaction, which would mean the
/// stream skipped or merged transactions and the caller must not treat the step as one txn.
pub fn diff_single(before: &GtidSet, after: &GtidSet) -> Result<Gtid, StepError> {
    if !before.is_subset_of(after) {
        return Err(StepError::Regressed {
            before: before.to_string(),
            after: after.to_string(),
        });
    }
    let added = after.difference_set(before);
    match added.count() {
        1 => Ok(added
            .difference(&GtidSet::empty())
            .into_iter()
            .next()
            .expect("count checked")),
        0 => Err(StepError::NoProgress {
            position: after.to_string(),
        }),
        n => Err(StepError::MultipleAdded {
            count: n as usize,
            before: before.to_string(),
            after: after.to_string(),
        }),
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq, Clone)]
pub enum StepError {
    #[error("position regressed: before={before} after={after}")]
    Regressed { before: String, after: String },
    #[error("position did not advance: {position}")]
    NoProgress { position: String },
    #[error("position advanced by {count} transactions in one step: before={before} after={after}")]
    MultipleAdded {
        count: usize,
        before: String,
        after: String,
    },
}

fn validate_uuid(s: &str) -> Result<(), String> {
    let bytes = s.as_bytes();
    if bytes.len() != 36 {
        return Err(format!("uuid {s:?} must be 36 chars"));
    }
    for (i, b) in bytes.iter().enumerate() {
        let ok = match i {
            8 | 13 | 18 | 23 => *b == b'-',
            _ => b.is_ascii_hexdigit(),
        };
        if !ok {
            return Err(format!("uuid {s:?} is malformed"));
        }
    }
    Ok(())
}

fn normalize(list: &mut Vec<Interval>) {
    list.sort();
    let mut merged: Vec<Interval> = Vec::with_capacity(list.len());
    for iv in list.drain(..) {
        match merged.last_mut() {
            Some(last) if iv.start <= last.end.saturating_add(1) => {
                last.end = last.end.max(iv.end);
            }
            _ => merged.push(iv),
        }
    }
    *list = merged;
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const U1: &str = "a2523813-adbe-11f1-b19c-0a2250a7ed6c";
    const U2: &str = "1d3d5b28-0f4e-11ef-9d5b-0242ac120002";

    #[test]
    fn parses_and_renders_round_trip() {
        let s = format!("MySQL56/{U1}:1-209");
        let set = GtidSet::parse_position(&s).unwrap();
        assert_eq!(set.to_position(), s);
        assert_eq!(set.count(), 209);
    }

    #[test]
    fn merges_adjacent_and_overlapping_intervals() {
        let set = GtidSet::parse_set(&format!("{U1}:1-5:6-8:10:7")).unwrap();
        assert_eq!(set.to_string(), format!("{U1}:1-8:10"));
    }

    #[test]
    fn contains_and_subset() {
        let a = GtidSet::parse_set(&format!("{U1}:1-5,{U2}:1-2")).unwrap();
        let b = GtidSet::parse_set(&format!("{U1}:1-9,{U2}:1-3")).unwrap();
        assert!(a.is_subset_of(&b));
        assert!(!b.is_subset_of(&a));
        assert!(a.contains(&Gtid::parse(&format!("{U2}:2")).unwrap()));
        assert!(!a.contains(&Gtid::parse(&format!("{U2}:3")).unwrap()));
        assert!(!a.contains(&Gtid::parse(&format!("{U1}:0")).unwrap_or(Gtid {
            uuid: U1.into(),
            gno: 0
        })));
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(
            GtidSet::parse_position("MySQL56/garbage"),
            Err(GtidError::Invalid(..))
        ));
        assert!(matches!(
            GtidSet::parse_position("MariaDB/1-2-3"),
            Err(GtidError::UnsupportedFlavor(_))
        ));
        assert!(GtidSet::parse_set(&format!("{U1}:5-3")).is_err());
        assert!(GtidSet::parse_set(&format!("{U1}:0")).is_err());
        assert!(GtidSet::parse_set(U1).is_err());
    }

    #[test]
    fn empty_position_is_empty_set() {
        let set = GtidSet::parse_position("").unwrap();
        assert!(set.is_empty());
        assert_eq!(set.to_position(), "");
    }

    #[test]
    fn diff_single_finds_the_added_transaction() {
        let before = GtidSet::parse_set(&format!("{U1}:1-274")).unwrap();
        let after = GtidSet::parse_set(&format!("{U1}:1-275")).unwrap();
        assert_eq!(
            diff_single(&before, &after).unwrap(),
            Gtid {
                uuid: U1.into(),
                gno: 275
            }
        );
        assert_eq!(
            diff_single(&after, &before),
            Err(StepError::Regressed {
                before: after.to_string(),
                after: before.to_string()
            })
        );
        assert!(matches!(
            diff_single(&before, &before),
            Err(StepError::NoProgress { .. })
        ));
        let far = GtidSet::parse_set(&format!("{U1}:1-277")).unwrap();
        assert!(matches!(
            diff_single(&before, &far),
            Err(StepError::MultipleAdded { count: 3, .. })
        ));
    }

    #[test]
    fn diff_single_across_uuid_failover() {
        let before = GtidSet::parse_set(&format!("{U1}:1-10")).unwrap();
        let after = GtidSet::parse_set(&format!("{U1}:1-10,{U2}:1")).unwrap();
        assert_eq!(
            diff_single(&before, &after).unwrap(),
            Gtid {
                uuid: U2.into(),
                gno: 1
            }
        );
    }

    fn arb_intervals() -> impl Strategy<Value = Vec<(u64, u64)>> {
        prop::collection::vec((1u64..200, 0u64..20), 0..8)
            .prop_map(|v| v.into_iter().map(|(s, w)| (s, s + w)).collect())
    }

    #[test]
    fn difference_set_uses_intervals() {
        let a = GtidSet::parse_set(&format!("{U1}:1-100")).unwrap();
        let b = GtidSet::parse_set(&format!("{U1}:1-10:20-30:100")).unwrap();
        assert_eq!(a.difference_set(&b).to_string(), format!("{U1}:11-19:31-99"));
        assert!(b.difference_set(&a).is_empty());
        let big = GtidSet::parse_set(&format!("{U1}:1-4000000000")).unwrap();
        let big1 = GtidSet::parse_set(&format!("{U1}:1-4000000001")).unwrap();
        assert_eq!(diff_single(&big, &big1).unwrap().gno, 4000000001);
    }

    proptest! {
        #[test]
        fn difference_set_matches_enumeration(a in arb_intervals(), b in arb_intervals()) {
            let mut sa = GtidSet::empty();
            for (s, e) in &a { sa.add_interval(U1, Interval { start: *s, end: *e }); }
            let mut sb = GtidSet::empty();
            for (s, e) in &b { sb.add_interval(U1, Interval { start: *s, end: *e }); }
            let d = sa.difference_set(&sb);
            for gno in 1..260u64 {
                let g = Gtid { uuid: U1.into(), gno };
                prop_assert_eq!(d.contains(&g), sa.contains(&g) && !sb.contains(&g));
            }
        }

        #[test]
        fn union_contains_both_and_subset_holds(a in arb_intervals(), b in arb_intervals()) {
            let mut sa = GtidSet::empty();
            for (s, e) in &a { sa.add_interval(U1, Interval { start: *s, end: *e }); }
            let mut sb = GtidSet::empty();
            for (s, e) in &b { sb.add_interval(U1, Interval { start: *s, end: *e }); }
            let u = sa.union(&sb);
            prop_assert!(sa.is_subset_of(&u));
            prop_assert!(sb.is_subset_of(&u));
            for gno in 1..260u64 {
                let g = Gtid { uuid: U1.into(), gno };
                let expect = a.iter().any(|(s, e)| *s <= gno && gno <= *e) || b.iter().any(|(s, e)| *s <= gno && gno <= *e);
                prop_assert_eq!(u.contains(&g), expect);
            }
            // render/parse round trip is lossless
            let rendered = u.to_position();
            let reparsed = GtidSet::parse_position(&rendered).unwrap();
            prop_assert_eq!(reparsed, u);
        }

        #[test]
        fn adding_one_gtid_is_recoverable_by_diff(a in arb_intervals(), gno in 1u64..300) {
            let mut sa = GtidSet::empty();
            for (s, e) in &a { sa.add_interval(U1, Interval { start: *s, end: *e }); }
            let g = Gtid { uuid: U1.into(), gno };
            prop_assume!(!sa.contains(&g));
            let mut after = sa.clone();
            after.add(&g);
            prop_assert_eq!(diff_single(&sa, &after).unwrap(), g);
        }
    }
}
