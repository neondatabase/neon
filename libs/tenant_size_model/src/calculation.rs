use crate::{SegmentMethod, SegmentSizeResult, SizeResult, StorageModel};

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

/// `Segment` which has had its size calculated.
#[derive(Clone, Debug)]
struct SegmentSize {
    method: SegmentMethod,

    // calculated size of this subtree, using this method
    accum_size: u64,

    seg_id: usize,
    children: Vec<SegmentSize>,
}

struct SizeAlternatives {
    /// cheapest alternative if parent is available.
    incremental: SegmentSize,

    /// cheapest alternative if parent node is not available
    non_incremental: Option<SegmentSize>,
}

impl StorageModel {
    pub fn calculate(&self) -> SizeResult {
        // Build adjacency list. 'child_list' is indexed by segment id. Each entry
        // contains a list of all child segments of the segment.
        let mut roots: Vec<usize> = Vec::new();
        let mut child_list: Vec<Vec<usize>> = Vec::new();
        child_list.resize(self.segments.len(), Vec::new());

        for (seg_id, seg) in self.segments.iter().enumerate() {
            if let Some(parent_id) = seg.parent {
                child_list[parent_id].push(seg_id);
            } else {
                roots.push(seg_id);
            }
        }

        let mut segment_results = Vec::new();
        segment_results.resize(
            self.segments.len(),
            SegmentSizeResult {
                method: SegmentMethod::Skipped,
                accum_size: 0,
            },
        );

        let mut total_size = 0;
        for root in roots {
            if let Some(selected) = self.size_here(root, &child_list).non_incremental {
                StorageModel::fill_selected_sizes(&selected, &mut segment_results);
                total_size += selected.accum_size;
            } else {
                // Couldn't find any way to get this root. Error?
            }
        }

        SizeResult {
            total_size,
            segments: segment_results,
        }
    }

    fn fill_selected_sizes(selected: &SegmentSize, result: &mut Vec<SegmentSizeResult>) {
        result[selected.seg_id] = SegmentSizeResult {
            method: selected.method,
            accum_size: selected.accum_size,
        };
        // recurse to children
        for child in selected.children.iter() {
            StorageModel::fill_selected_sizes(child, result);
        }
    }

    //
    // This is the core of the sizing calculation.
    //
    // This is a recursive function, that for each Segment calculates the best way
    // to reach all the Segments that are marked as needed in this subtree, under two
    // different conditions:
    // a) when the parent of this segment is available (as a snaphot or through WAL), and
    // b) when the parent of this segment is not available.
    //
    fn size_here(&self, seg_id: usize, child_list: &Vec<Vec<usize>>) -> SizeAlternatives {
        let seg = &self.segments[seg_id];
        // First figure out the best way to get each child
        let mut children = Vec::new();
        for child_id in &child_list[seg_id] {
            children.push(self.size_here(*child_id, child_list))
        }

        // Method 1. If this node is not needed, we can skip it as long as we
        // take snapshots later in each sub-tree
        let snapshot_later = if !seg.needed {
            let mut snapshot_later = SegmentSize {
                seg_id,
                method: SegmentMethod::Skipped,
                accum_size: 0,
                children: Vec::new(),
            };

            let mut possible = true;
            for child in children.iter() {
                if let Some(non_incremental) = &child.non_incremental {
                    snapshot_later.accum_size += non_incremental.accum_size;
                    snapshot_later.children.push(non_incremental.clone())
                } else {
                    possible = false;
                    break;
                }
            }
            if possible {
                Some(snapshot_later)
            } else {
                None
            }
        } else {
            None
        };

        // Method 2. Get a snapshot here. This assumed to be possible, if the 'size' of
        // this Segment was given.
        let snapshot_here = if !seg.needed || seg.parent.is_none() {
            if let Some(snapshot_size) = seg.size {
                let mut snapshot_here = SegmentSize {
                    seg_id,
                    method: SegmentMethod::SnapshotHere,
                    accum_size: snapshot_size,
                    children: Vec::new(),
                };
                for child in children.iter() {
                    snapshot_here.accum_size += child.incremental.accum_size;
                    snapshot_here.children.push(child.incremental.clone())
                }
                Some(snapshot_here)
            } else {
                None
            }
        } else {
            None
        };

        // Method 3. Use WAL to get here from parent
        let wal_here = {
            let mut wal_here = SegmentSize {
                seg_id,
                method: SegmentMethod::Wal,
                accum_size: if let Some(parent_id) = seg.parent {
                    seg.lsn - self.segments[parent_id].lsn
                } else {
                    0
                },
                children: Vec::new(),
            };
            for child in children {
                wal_here.accum_size += child.incremental.accum_size;
                wal_here.children.push(child.incremental)
            }
            wal_here
        };

        // If the parent is not available, what's the cheapest method involving
        // a snapshot here or later?
        let mut cheapest_non_incremental: Option<SegmentSize> = None;
        if let Some(snapshot_here) = snapshot_here {
            cheapest_non_incremental = Some(snapshot_here);
        }
        if let Some(snapshot_later) = snapshot_later {
            // Use <=, to prefer skipping if the size is equal
            if let Some(parent) = &cheapest_non_incremental {
                if snapshot_later.accum_size <= parent.accum_size {
                    cheapest_non_incremental = Some(snapshot_later);
                }
            } else {
                cheapest_non_incremental = Some(snapshot_later);
            }
        }

        // And what's the cheapest method, if the parent is available?
        let cheapest_incremental = if let Some(cheapest_non_incremental) = &cheapest_non_incremental
        {
            // Is it cheaper to use a snapshot here or later, anyway?
            // Use <, to prefer Wal over snapshot if the cost is the same
            if wal_here.accum_size < cheapest_non_incremental.accum_size {
                wal_here
            } else {
                cheapest_non_incremental.clone()
            }
        } else {
            wal_here
        };

        SizeAlternatives {
            incremental: cheapest_incremental,
            non_incremental: cheapest_non_incremental,
        }
    }
}
