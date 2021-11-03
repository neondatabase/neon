use std::{collections::HashMap, ops::RangeBounds, slice};

use anyhow::Result;

use std::io::{Read, Write};

use zenith_utils::chunked_buffer::{ChunkedBuffer, ChunkedBufferReader};
use zenith_utils::{lsn::Lsn, vec_map::VecMap};

use super::storage_layer::PageVersion;

use zenith_utils::bin_ser::LeSer;

const EMPTY_SLICE: &[(Lsn, u64)] = &[];

#[derive(Debug, Default)]
pub struct PageVersions {
    map: HashMap<u32, VecMap<Lsn, u64>>,

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
    ) -> Option<u64> {
        // remember starting position
        let pos = self.buffer.len();

        // make room for the 'length' field by writing zeros as a placeholder.
        self.buffer.write_all(&[0u8; 4]).unwrap();

        page_version.ser_into(&mut self.buffer).unwrap();

        // write the 'length' field.
        let len = self.buffer.len() - pos - 4;
        let lenbuf = u32::to_ne_bytes(len as u32);
        self.buffer.write_all_at(&lenbuf, pos).unwrap();

        let map = self.map.entry(blknum).or_insert_with(VecMap::default);
        map.append_or_update_last(lsn, pos as u64).unwrap()
    }

    /// Get all [`PageVersion`]s in a block
    fn get_block_slice(&self, blknum: u32) -> &[(Lsn, u64)] {
        self.map
            .get(&blknum)
            .map(VecMap::as_slice)
            .unwrap_or(EMPTY_SLICE)
    }

    /// Get a range of [`PageVersions`] in a block
    pub fn get_block_lsn_range<R: RangeBounds<Lsn>>(&self, blknum: u32, range: R) -> &[(Lsn, u64)] {
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

    /// Returns a 'Read' that reads the page version at given offset.
    pub fn reader(&self, pos: u64) -> Result<std::io::Take<ChunkedBufferReader>, std::io::Error> {
        // read length
        let mut lenbuf = [0u8; 4];
        let mut reader = self.buffer.reader(pos);
        reader.read_exact(&mut lenbuf)?;
        let len = u32::from_ne_bytes(lenbuf);

        Ok(reader.take(len as u64))
    }

    pub fn get_page_version(&self, pos: u64) -> Result<PageVersion> {
        let mut reader = self.reader(pos)?;
        Ok(PageVersion::des_from(&mut reader)?)
    }
}

pub struct OrderedPageVersionIter<'a> {
    page_versions: &'a PageVersions,

    ordered_blocks: Vec<u32>,
    cur_block_idx: usize,

    cutoff_lsn: Option<Lsn>,

    cur_slice_iter: slice::Iter<'a, (Lsn, u64)>,
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
    type Item = (u32, Lsn, u64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((lsn, pos)) = self.cur_slice_iter.next() {
                if self.is_lsn_before_cutoff(lsn) {
                    let blknum = self.ordered_blocks[self.cur_block_idx];
                    return Some((blknum, *lsn, *pos));
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
