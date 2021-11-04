use std::{collections::HashMap, ops::RangeBounds, slice};

use anyhow::Result;

use zenith_utils::chunked_buffer::{ChunkToken, ChunkedBuffer};
use zenith_utils::{lsn::Lsn, vec_map::VecMap};

use super::storage_layer::PageVersion;

const EMPTY_SLICE: &[(Lsn, ChunkToken)] = &[];

#[derive(Debug, Default)]
pub struct PageVersions {
    map: HashMap<u32, VecMap<Lsn, ChunkToken>>,

    /// The PageVersion structs are stored in a serialized format in this buffer.
    /// Each serialized PageVersion is preceded by a 'u32' length field.
    /// The 'map' stores offsets into this buffer.
    buffer: ChunkedBuffer,
}

impl PageVersions {
    pub fn append_or_update_last(
        &mut self,
        blknum: u32,
        lsn: Lsn,
        page_version: PageVersion,
    ) -> Option<ChunkToken> {
        let token = self.buffer.write(page_version.bytes());

        let map = self.map.entry(blknum).or_insert_with(VecMap::default);
        map.append_or_update_last(lsn, token).unwrap()
    }

    /// Get all [`PageVersion`]s in a block
    fn get_block_slice(&self, blknum: u32) -> &[(Lsn, ChunkToken)] {
        self.map
            .get(&blknum)
            .map(VecMap::as_slice)
            .unwrap_or(EMPTY_SLICE)
    }

    /// Get a range of [`PageVersions`] in a block
    pub fn get_block_lsn_range<R: RangeBounds<Lsn>>(
        &self,
        blknum: u32,
        range: R,
    ) -> &[(Lsn, ChunkToken)] {
        self.map
            .get(&blknum)
            .map(|vec_map| vec_map.slice_range(range))
            .unwrap_or(EMPTY_SLICE)
    }

    /// Iterate through [`PageVersion`]s in (block, lsn) order.
    /// If a [`cutoff_lsn`] is set, only show versions with `lsn < cutoff_lsn`
    pub fn ordered_page_version_iter(&self, cutoff_lsn: Option<Lsn>) -> OrderedPageVersionIter<'_> {
        let mut ordered_blocks: Vec<u32> = self.map.keys().cloned().collect();
        ordered_blocks.sort_unstable();

        let slice = ordered_blocks
            .first()
            .map(|&blknum| self.get_block_slice(blknum))
            .unwrap_or(EMPTY_SLICE);

        OrderedPageVersionIter {
            page_versions: self,
            ordered_blocks,
            cur_block_idx: 0,
            cutoff_lsn,
            cur_slice_iter: slice.iter(),
        }
    }

    pub fn get_page_version_bytes(&self, token: &ChunkToken) -> Vec<u8> {
        self.buffer.read(token)
    }

    pub fn get_page_version(&self, token: &ChunkToken) -> Result<PageVersion> {
        let buf = self.get_page_version_bytes(token).into_boxed_slice();
        Ok(PageVersion::from_bytes(buf))
    }
}

pub struct OrderedPageVersionIter<'a> {
    page_versions: &'a PageVersions,

    ordered_blocks: Vec<u32>,
    cur_block_idx: usize,

    cutoff_lsn: Option<Lsn>,

    cur_slice_iter: slice::Iter<'a, (Lsn, ChunkToken)>,
}

impl OrderedPageVersionIter<'_> {
    fn is_lsn_before_cutoff(&self, lsn: &Lsn) -> bool {
        if let Some(cutoff_lsn) = self.cutoff_lsn.as_ref() {
            lsn < cutoff_lsn
        } else {
            true
        }
    }
}

impl<'a> Iterator for OrderedPageVersionIter<'a> {
    type Item = (u32, Lsn, &'a ChunkToken);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((lsn, token)) = self.cur_slice_iter.next() {
                if self.is_lsn_before_cutoff(lsn) {
                    let blknum = self.ordered_blocks[self.cur_block_idx];
                    return Some((blknum, *lsn, token));
                }
            }

            let next_block_idx = self.cur_block_idx + 1;
            let blknum: u32 = *self.ordered_blocks.get(next_block_idx)?;
            self.cur_block_idx = next_block_idx;
            self.cur_slice_iter = self.page_versions.get_block_slice(blknum).iter();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::layered_repository::storage_layer::Page;

    use super::*;

    #[test]
    fn test_ordered_iter() {
        let mut page_versions = PageVersions::default();
        const BLOCKS: u32 = 1000;
        const LSNS: u64 = 50;

        let empty_page_version = PageVersion::Page(Page::zero_page());

        for blknum in 0..BLOCKS {
            for lsn in 0..LSNS {
                let old = page_versions.append_or_update_last(
                    blknum,
                    Lsn(lsn),
                    empty_page_version.clone(),
                );
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
