use std::{
    collections::HashMap,
    ops::{Range, RangeBounds},
    slice,
};

use zenith_utils::{bin_ser::LeSer, lsn::Lsn, vec_map::VecMap};

use super::storage_layer::PageVersion;

const EMPTY_SLICE: &[(Lsn, Range<usize>)] = &[];

#[derive(Debug, Default)]
pub struct PageVersions {
    heap: Vec<u8>,
    ranges: HashMap<u32, VecMap<Lsn, Range<usize>>>,
}

impl PageVersions {
    pub fn append_or_update_last(
        &mut self,
        blknum: u32,
        lsn: Lsn,
        page_version: PageVersion,
    ) -> Option<PageVersion> {
        let mut new_bytes = PageVersion::ser(&page_version).unwrap();

        let map = self.ranges.entry(blknum).or_insert_with(VecMap::default);

        if let Some((last_lsn, last_range)) = map.as_slice().last() {
            if lsn == *last_lsn {
                let old_bytes = &self.heap[last_range.clone()];
                if old_bytes == new_bytes {
                    return Some(page_version);
                }
                // TODO optimize for case when old_bytes.len() >= new_bytes.len()
            }
        }

        let new_range = self.heap.len()..self.heap.len() + new_bytes.len();
        self.heap.append(&mut new_bytes);
        map.append_or_update_last(lsn, new_range)
            .unwrap()
            .map(|old_range| {
                let old_bytes = &self.heap[old_range];
                PageVersion::des(old_bytes).unwrap()
            })
    }

    /// Get all [`PageVersion`]s in a block
    pub fn iter_block(&self, blknum: u32) -> BlockVersionIter<'_> {
        let range_iter = self
            .ranges
            .get(&blknum)
            .map(VecMap::as_slice)
            .unwrap_or(EMPTY_SLICE)
            .iter();

        BlockVersionIter {
            heap: &self.heap,
            range_iter,
        }
    }

    /// Get a range of [`PageVersions`] in a block
    pub fn iter_block_lsn_range<R: RangeBounds<Lsn>>(
        &self,
        blknum: u32,
        range: R,
    ) -> BlockVersionIter<'_> {
        let range_iter = self
            .ranges
            .get(&blknum)
            .map(|vec_map| vec_map.slice_range(range))
            .unwrap_or(EMPTY_SLICE)
            .iter();

        BlockVersionIter {
            heap: &self.heap,
            range_iter,
        }
    }

    /// Iterate through [`PageVersion`]s in (block, lsn) order.
    /// If a [`cutoff_lsn`] is set, only show versions with `lsn < cutoff_lsn`
    pub fn ordered_page_version_iter(&self, cutoff_lsn: Option<Lsn>) -> OrderedPageVersionIter<'_> {
        let mut ordered_blocks: Vec<u32> = self.ranges.keys().cloned().collect();
        ordered_blocks.sort_unstable();

        let cur_block_iter = ordered_blocks
            .first()
            .map(|&blknum| self.iter_block(blknum))
            .unwrap_or_else(|| {
                let empty_iter = EMPTY_SLICE.iter();
                BlockVersionIter {
                    heap: &self.heap,
                    range_iter: empty_iter,
                }
            });

        OrderedPageVersionIter {
            page_versions: self,
            ordered_blocks,
            cur_block_idx: 0,
            cutoff_lsn,
            cur_block_iter,
        }
    }
}

pub struct BlockVersionIter<'a> {
    heap: &'a Vec<u8>,
    range_iter: slice::Iter<'a, (Lsn, Range<usize>)>,
}

impl BlockVersionIter<'_> {
    fn get_iter_result(&self, tuple: Option<&(Lsn, Range<usize>)>) -> Option<(Lsn, PageVersion)> {
        let (lsn, range) = tuple?;
        let range = range.clone();

        let pv_bytes = &self.heap[range];
        let page_version = PageVersion::des(pv_bytes).unwrap();

        Some((*lsn, page_version))
    }
}

impl Iterator for BlockVersionIter<'_> {
    type Item = (Lsn, PageVersion);

    fn next(&mut self) -> Option<Self::Item> {
        let tuple = self.range_iter.next();
        self.get_iter_result(tuple)
    }
}

impl DoubleEndedIterator for BlockVersionIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let tuple = self.range_iter.next_back();
        self.get_iter_result(tuple)
    }
}

pub struct OrderedPageVersionIter<'a> {
    page_versions: &'a PageVersions,

    ordered_blocks: Vec<u32>,
    cur_block_idx: usize,

    cutoff_lsn: Option<Lsn>,

    cur_block_iter: BlockVersionIter<'a>,
}

impl OrderedPageVersionIter<'_> {
    fn is_lsn_before_cutoff(&self, lsn: Lsn) -> bool {
        if let Some(cutoff_lsn) = self.cutoff_lsn.as_ref() {
            lsn < *cutoff_lsn
        } else {
            true
        }
    }
}

impl Iterator for OrderedPageVersionIter<'_> {
    type Item = (u32, Lsn, PageVersion);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((lsn, page_version)) = self.cur_block_iter.next() {
                if self.is_lsn_before_cutoff(lsn) {
                    let blknum = self.ordered_blocks[self.cur_block_idx];
                    return Some((blknum, lsn, page_version));
                }
            }

            let next_block_idx = self.cur_block_idx + 1;
            let blknum: u32 = *self.ordered_blocks.get(next_block_idx)?;
            self.cur_block_idx = next_block_idx;
            self.cur_block_iter = self.page_versions.iter_block(blknum);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_PAGE_VERSION: PageVersion = PageVersion {
        page_image: None,
        record: None,
    };

    #[test]
    fn test_ordered_iter() {
        let mut page_versions = PageVersions::default();
        const BLOCKS: u32 = 1000;
        const LSNS: u64 = 50;

        for blknum in 0..BLOCKS {
            for lsn in 0..LSNS {
                let old = page_versions.append_or_update_last(blknum, Lsn(lsn), EMPTY_PAGE_VERSION);
                assert!(old.is_none());
            }
        }

        let mut iter = page_versions.ordered_page_version_iter(None);
        for blknum in 0..BLOCKS {
            for lsn in 0..LSNS {
                let (actual_blknum, actual_lsn, _pv) = iter.next().unwrap();
                assert_eq!(actual_blknum, blknum);
                assert_eq!(Lsn(lsn), actual_lsn);
            }
        }
        assert!(iter.next().is_none());
        assert!(iter.next().is_none()); // should be robust against excessive next() calls

        const CUTOFF_LSN: Lsn = Lsn(30);
        let mut iter = page_versions.ordered_page_version_iter(Some(CUTOFF_LSN));
        for blknum in 0..BLOCKS {
            for lsn in 0..CUTOFF_LSN.0 {
                let (actual_blknum, actual_lsn, _pv) = iter.next().unwrap();
                assert_eq!(actual_blknum, blknum);
                assert_eq!(Lsn(lsn), actual_lsn);
            }
        }
        assert!(iter.next().is_none());
        assert!(iter.next().is_none()); // should be robust against excessive next() calls
    }
}
