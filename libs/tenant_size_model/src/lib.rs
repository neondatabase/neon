use std::borrow::Cow;
use std::collections::HashMap;

/// Pricing model or history size builder.
///
/// Maintains knowledge of the branches and their modifications. Generic over the branch name key
/// type.
pub struct Storage<K: 'static> {
    segments: Vec<Segment>,

    /// Mapping from the branch name to the index of a segment describing it's latest state.
    branches: HashMap<K, usize>,
}

/// Snapshot of a branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Segment {
    /// Previous segment index into ['Storage::segments`], if any.
    parent: Option<usize>,

    /// Description of how did we get to this state.
    ///
    /// Mainly used in the original scenarios 1..=4 with insert, delete and update. Not used when
    /// modifying a branch directly.
    pub op: Cow<'static, str>,

    /// LSN before this state
    start_lsn: u64,

    /// LSN at this state
    pub end_lsn: u64,

    /// Logical size before this state
    start_size: u64,

    /// Logical size at this state
    pub end_size: u64,

    /// Indices to [`Storage::segments`]
    ///
    /// FIXME: this could be an Option<usize>
    children_after: Vec<usize>,

    /// Determined by `retention_period` given to [`Storage::calculate`]
    pub needed: bool,
}

//
//
//
//
//                 *-g--*---D--->
//                /
//               /
//              /                 *---b----*-B--->
//             /                 /
//            /                 /
//      -----*--e---*-----f----* C
//           E                  \
//                               \
//                                *--a---*---A-->
//
// If A and B need to be retained, is it cheaper to store
// snapshot at C+a+b, or snapshots at A and B ?
//
// If D also needs to be retained, which is cheaper:
//
// 1. E+g+e+f+a+b
// 2. D+C+a+b
// 3. D+A+B

/// [`Segment`] which has had it's size calculated.
pub struct SegmentSize {
    pub seg_id: usize,

    pub method: SegmentMethod,

    this_size: u64,

    pub children: Vec<SegmentSize>,
}

impl SegmentSize {
    fn total(&self) -> u64 {
        self.this_size + self.children.iter().fold(0, |acc, x| acc + x.total())
    }

    pub fn total_children(&self) -> u64 {
        if self.method == SnapshotAfter {
            self.this_size + self.children.iter().fold(0, |acc, x| acc + x.total())
        } else {
            self.children.iter().fold(0, |acc, x| acc + x.total())
        }
    }
}

/// Different methods to retain history from a particular state
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SegmentMethod {
    SnapshotAfter,
    Wal,
    WalNeeded,
    Skipped,
}

use SegmentMethod::*;

impl<K: std::hash::Hash + Eq + 'static> Storage<K> {
    /// Creates a new storage with the given default branch name.
    pub fn new(initial_branch: K) -> Storage<K> {
        let init_segment = Segment {
            op: "".into(),
            needed: false,
            parent: None,
            start_lsn: 0,
            end_lsn: 0,
            start_size: 0,
            end_size: 0,
            children_after: Vec::new(),
        };

        Storage {
            segments: vec![init_segment],
            branches: HashMap::from([(initial_branch, 0)]),
        }
    }

    /// Advances the branch with the named operation, by the relative LSN and logical size bytes.
    pub fn modify_branch<Q: ?Sized>(
        &mut self,
        branch: &Q,
        op: Cow<'static, str>,
        lsn_bytes: u64,
        size_bytes: i64,
    ) where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        let lastseg_id = *self.branches.get(branch).unwrap();
        let newseg_id = self.segments.len();
        let lastseg = &mut self.segments[lastseg_id];

        let newseg = Segment {
            op,
            parent: Some(lastseg_id),
            start_lsn: lastseg.end_lsn,
            end_lsn: lastseg.end_lsn + lsn_bytes,
            start_size: lastseg.end_size,
            end_size: (lastseg.end_size as i64 + size_bytes) as u64,
            children_after: Vec::new(),
            needed: false,
        };
        lastseg.children_after.push(newseg_id);

        self.segments.push(newseg);
        *self.branches.get_mut(branch).expect("read already") = newseg_id;
    }

    pub fn insert<Q: ?Sized>(&mut self, branch: &Q, bytes: u64)
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        self.modify_branch(branch, "insert".into(), bytes, bytes as i64);
    }

    pub fn update<Q: ?Sized>(&mut self, branch: &Q, bytes: u64)
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        self.modify_branch(branch, "update".into(), bytes, 0i64);
    }

    pub fn delete<Q: ?Sized>(&mut self, branch: &Q, bytes: u64)
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        self.modify_branch(branch, "delete".into(), bytes, -(bytes as i64));
    }

    /// Panics if the parent branch cannot be found.
    pub fn branch<Q: ?Sized>(&mut self, parent: &Q, name: K)
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq,
    {
        // Find the right segment
        let branchseg_id = *self
            .branches
            .get(parent)
            .expect("should had found the parent by key");
        let _branchseg = &mut self.segments[branchseg_id];

        // Create branch name for it
        self.branches.insert(name, branchseg_id);
    }

    pub fn calculate(&mut self, retention_period: u64) -> SegmentSize {
        // Phase 1: Mark all the segments that need to be retained
        for (_branch, &last_seg_id) in self.branches.iter() {
            let last_seg = &self.segments[last_seg_id];
            let cutoff_lsn = last_seg.start_lsn.saturating_sub(retention_period);
            let mut seg_id = last_seg_id;
            loop {
                let seg = &mut self.segments[seg_id];
                if seg.end_lsn < cutoff_lsn {
                    break;
                }
                seg.needed = true;
                if let Some(prev_seg_id) = seg.parent {
                    seg_id = prev_seg_id;
                } else {
                    break;
                }
            }
        }

        // Phase 2: For each oldest segment in a chain that needs to be retained,
        // calculate if we should store snapshot or WAL
        self.size_from_snapshot_later(0)
    }

    fn size_from_wal(&self, seg_id: usize) -> SegmentSize {
        let seg = &self.segments[seg_id];

        let this_size = seg.end_lsn - seg.start_lsn;

        let mut children = Vec::new();

        // try both ways
        for &child_id in seg.children_after.iter() {
            // try each child both ways
            let child = &self.segments[child_id];
            let p1 = self.size_from_wal(child_id);

            let p = if !child.needed {
                let p2 = self.size_from_snapshot_later(child_id);
                if p1.total() < p2.total() {
                    p1
                } else {
                    p2
                }
            } else {
                p1
            };
            children.push(p);
        }
        SegmentSize {
            seg_id,
            method: if seg.needed { WalNeeded } else { Wal },
            this_size,
            children,
        }
    }

    fn size_from_snapshot_later(&self, seg_id: usize) -> SegmentSize {
        // If this is needed, then it's time to do the snapshot and continue
        // with wal method.
        let seg = &self.segments[seg_id];
        //eprintln!("snap: seg{}: {} needed: {}", seg_id, seg.children_after.len(), seg.needed);
        if seg.needed {
            let mut children = Vec::new();

            for &child_id in seg.children_after.iter() {
                // try each child both ways
                let child = &self.segments[child_id];
                let p1 = self.size_from_wal(child_id);

                let p = if !child.needed {
                    let p2 = self.size_from_snapshot_later(child_id);
                    if p1.total() < p2.total() {
                        p1
                    } else {
                        p2
                    }
                } else {
                    p1
                };
                children.push(p);
            }
            SegmentSize {
                seg_id,
                method: WalNeeded,
                this_size: seg.start_size,
                children,
            }
        } else {
            // If any of the direct children are "needed", need to be able to reconstruct here
            let mut children_needed = false;
            for &child in seg.children_after.iter() {
                let seg = &self.segments[child];
                if seg.needed {
                    children_needed = true;
                    break;
                }
            }

            let method1 = if !children_needed {
                let mut children = Vec::new();
                for child in seg.children_after.iter() {
                    children.push(self.size_from_snapshot_later(*child));
                }
                Some(SegmentSize {
                    seg_id,
                    method: Skipped,
                    this_size: 0,
                    children,
                })
            } else {
                None
            };

            // If this a junction, consider snapshotting here
            let method2 = if children_needed || seg.children_after.len() >= 2 {
                let mut children = Vec::new();
                for child in seg.children_after.iter() {
                    children.push(self.size_from_wal(*child));
                }
                Some(SegmentSize {
                    seg_id,
                    method: SnapshotAfter,
                    this_size: seg.end_size,
                    children,
                })
            } else {
                None
            };

            match (method1, method2) {
                (None, None) => panic!(),
                (Some(method), None) => method,
                (None, Some(method)) => method,
                (Some(method1), Some(method2)) => {
                    if method1.total() < method2.total() {
                        method1
                    } else {
                        method2
                    }
                }
            }
        }
    }

    pub fn into_segments(self) -> Vec<Segment> {
        self.segments
    }
}
