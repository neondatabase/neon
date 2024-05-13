//!
//! Utilities for vectored reading of variable-sized "blobs".
//!
//! The "blob" api is an abstraction on top of the "block" api,
//! with the main difference being that blobs do not have a fixed
//! size (each blob is prefixed with 1 or 4 byte length field)
//!
//! The vectored apis provided in this module allow for planning
//! and executing disk IO which covers multiple blobs.
//!
//! Reads are planned with [`VectoredReadPlanner`] which will coalesce
//! adjacent blocks into a single disk IO request and exectuted by
//! [`VectoredBlobReader`] which does all the required offset juggling
//! and returns a buffer housing all the blobs and a list of offsets.
//!
//! Note that the vectored blob api does *not* go through the page cache.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use bytes::BytesMut;
use pageserver_api::key::Key;
use utils::lsn::Lsn;
use utils::vec_map::VecMap;

use crate::virtual_file::VirtualFile;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MaxVectoredReadBytes(pub NonZeroUsize);

/// Metadata bundled with the start and end offset of a blob.
#[derive(Copy, Clone, Debug)]
pub struct BlobMeta {
    pub key: Key,
    pub lsn: Lsn,
}

/// Blob offsets into [`VectoredBlobsBuf::buf`]
pub struct VectoredBlob {
    pub start: usize,
    pub end: usize,
    pub meta: BlobMeta,
}

/// Return type of [`VectoredBlobReader::read_blobs`]
pub struct VectoredBlobsBuf {
    /// Buffer for all blobs in this read
    pub buf: BytesMut,
    /// Offsets into the buffer and metadata for all blobs in this read
    pub blobs: Vec<VectoredBlob>,
}

/// Description of one disk read for multiple blobs.
/// Used as the argument form [`VectoredBlobReader::read_blobs`]
#[derive(Debug)]
pub struct VectoredRead {
    pub start: u64,
    pub end: u64,
    /// Starting offsets and metadata for each blob in this read
    pub blobs_at: VecMap<u64, BlobMeta>,
}

impl VectoredRead {
    pub(crate) fn size(&self) -> usize {
        (self.end - self.start) as usize
    }
}

#[derive(Eq, PartialEq)]
pub(crate) enum VectoredReadExtended {
    Yes,
    No,
}

pub(crate) struct VectoredReadBuilder {
    start: u64,
    end: u64,
    blobs_at: VecMap<u64, BlobMeta>,
    max_read_size: usize,
}

impl VectoredReadBuilder {
    /// Start building a new vectored read.
    ///
    /// Note that by design, this does not check against reading more than `max_read_size` to
    /// support reading larger blobs than the configuration value. The builder will be single use
    /// however after that.
    pub(crate) fn new(
        start_offset: u64,
        end_offset: u64,
        meta: BlobMeta,
        max_read_size: usize,
    ) -> Self {
        let mut blobs_at = VecMap::default();
        blobs_at
            .append(start_offset, meta)
            .expect("First insertion always succeeds");

        Self {
            start: start_offset,
            end: end_offset,
            blobs_at,
            max_read_size,
        }
    }

    /// Attempt to extend the current read with a new blob if the start
    /// offset matches with the current end of the vectored read
    /// and the resuting size is below the max read size
    pub(crate) fn extend(&mut self, start: u64, end: u64, meta: BlobMeta) -> VectoredReadExtended {
        tracing::trace!(start, end, "trying to extend");
        let size = (end - start) as usize;
        if self.end == start && self.size() + size <= self.max_read_size {
            self.end = end;
            self.blobs_at
                .append(start, meta)
                .expect("LSNs are ordered within vectored reads");

            return VectoredReadExtended::Yes;
        }

        VectoredReadExtended::No
    }

    pub(crate) fn size(&self) -> usize {
        (self.end - self.start) as usize
    }

    pub(crate) fn build(self) -> VectoredRead {
        VectoredRead {
            start: self.start,
            end: self.end,
            blobs_at: self.blobs_at,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum BlobFlag {
    None,
    Ignore,
    ReplaceAll,
}

/// Planner for vectored blob reads.
///
/// Blob offsets are received via [`VectoredReadPlanner::handle`]
/// and coalesced into disk reads.
///
/// The implementation is very simple:
/// * Collect all blob offsets in an ordered structure
/// * Iterate over the collected blobs and coalesce them into reads at the end
pub struct VectoredReadPlanner {
    // Track all the blob offsets. Start offsets must be ordered.
    blobs: BTreeMap<Key, Vec<(Lsn, u64, u64)>>,
    // Arguments for previous blob passed into [`VectoredReadPlanner::handle`]
    prev: Option<(Key, Lsn, u64, BlobFlag)>,

    max_read_size: usize,
}

impl VectoredReadPlanner {
    pub fn new(max_read_size: usize) -> Self {
        Self {
            blobs: BTreeMap::new(),
            prev: None,
            max_read_size,
        }
    }

    /// Include a new blob in the read plan.
    ///
    /// This function is called from a B-Tree index visitor (see `DeltaLayerInner::plan_reads`
    /// and `ImageLayerInner::plan_reads`). Said visitor wants to collect blob offsets for all
    /// keys in a given keyspace. This function must be called for each key in the desired
    /// keyspace (monotonically continuous). [`Self::handle_range_end`] must
    /// be called after every range in the offset.
    ///
    /// In the event that keys are skipped, the behaviour is undefined and can lead to an
    /// incorrect read plan. We can end up asserting, erroring in wal redo or returning
    /// incorrect data to the user.
    ///
    /// The `flag` argument has two interesting values:
    /// * [`BlobFlag::ReplaceAll`]: The blob for this key should replace all existing blobs.
    /// This is used for WAL records that `will_init`.
    /// * [`BlobFlag::Ignore`]: This blob should not be included in the read. This happens
    /// if the blob is cached.
    pub fn handle(&mut self, key: Key, lsn: Lsn, offset: u64, flag: BlobFlag) {
        // Implementation note: internally lag behind by one blob such that
        // we have a start and end offset when initialising [`VectoredRead`]
        let (prev_key, prev_lsn, prev_offset, prev_flag) = match self.prev {
            None => {
                self.prev = Some((key, lsn, offset, flag));
                return;
            }
            Some(prev) => prev,
        };

        self.add_blob(prev_key, prev_lsn, prev_offset, offset, prev_flag);

        self.prev = Some((key, lsn, offset, flag));
    }

    pub fn handle_range_end(&mut self, offset: u64) {
        if let Some((prev_key, prev_lsn, prev_offset, prev_flag)) = self.prev {
            self.add_blob(prev_key, prev_lsn, prev_offset, offset, prev_flag);
        }

        self.prev = None;
    }

    fn add_blob(&mut self, key: Key, lsn: Lsn, start_offset: u64, end_offset: u64, flag: BlobFlag) {
        match flag {
            BlobFlag::None => {
                let blobs_for_key = self.blobs.entry(key).or_default();
                blobs_for_key.push((lsn, start_offset, end_offset));
            }
            BlobFlag::ReplaceAll => {
                let blobs_for_key = self.blobs.entry(key).or_default();
                blobs_for_key.clear();
                blobs_for_key.push((lsn, start_offset, end_offset));
            }
            BlobFlag::Ignore => {}
        }
    }

    pub fn finish(self) -> Vec<VectoredRead> {
        let mut current_read_builder: Option<VectoredReadBuilder> = None;
        let mut reads = Vec::new();

        for (key, blobs_for_key) in self.blobs {
            for (lsn, start_offset, end_offset) in blobs_for_key {
                let extended = match &mut current_read_builder {
                    Some(read_builder) => {
                        read_builder.extend(start_offset, end_offset, BlobMeta { key, lsn })
                    }
                    None => VectoredReadExtended::No,
                };

                if extended == VectoredReadExtended::No {
                    let next_read_builder = VectoredReadBuilder::new(
                        start_offset,
                        end_offset,
                        BlobMeta { key, lsn },
                        self.max_read_size,
                    );

                    let prev_read_builder = current_read_builder.replace(next_read_builder);

                    // `current_read_builder` is None in the first iteration of the outer loop
                    if let Some(read_builder) = prev_read_builder {
                        reads.push(read_builder.build());
                    }
                }
            }
        }

        if let Some(read_builder) = current_read_builder {
            reads.push(read_builder.build());
        }

        reads
    }
}

/// Disk reader for vectored blob spans (does not go through the page cache)
pub struct VectoredBlobReader<'a> {
    file: &'a VirtualFile,
}

impl<'a> VectoredBlobReader<'a> {
    pub fn new(file: &'a VirtualFile) -> Self {
        Self { file }
    }

    /// Read the requested blobs into the buffer.
    ///
    /// We have to deal with the fact that blobs are not fixed size.
    /// Each blob is prefixed by a size header.
    ///
    /// The success return value is a struct which contains the buffer
    /// filled from disk and a list of offsets at which each blob lies
    /// in the buffer.
    pub async fn read_blobs(
        &self,
        read: &VectoredRead,
        buf: BytesMut,
    ) -> Result<VectoredBlobsBuf, std::io::Error> {
        assert!(read.size() > 0);
        assert!(
            read.size() <= buf.capacity(),
            "{} > {}",
            read.size(),
            buf.capacity()
        );
        let buf = self
            .file
            .read_exact_at_n(buf, read.start, read.size())
            .await?;

        let blobs_at = read.blobs_at.as_slice();
        let start_offset = blobs_at.first().expect("VectoredRead is never empty").0;

        let mut metas = Vec::with_capacity(blobs_at.len());

        // Blobs in `read` only provide their starting offset. The end offset
        // of a blob is implicit: the start of the next blob if one exists
        // or the end of the read.
        let pairs = blobs_at.iter().zip(
            blobs_at
                .iter()
                .map(Some)
                .skip(1)
                .chain(std::iter::once(None)),
        );

        for ((offset, meta), next) in pairs {
            let offset_in_buf = offset - start_offset;
            let first_len_byte = buf[offset_in_buf as usize];

            // Each blob is prefixed by a header containing it's size.
            // Extract the size and skip that header to find the start of the data.
            // The size can be 1 or 4 bytes. The most significant bit is 0 in the
            // 1 byte case and 1 in the 4 byte case.
            let (size_length, blob_size) = if first_len_byte < 0x80 {
                (1, first_len_byte as u64)
            } else {
                let mut blob_size_buf = [0u8; 4];
                let offset_in_buf = offset_in_buf as usize;

                blob_size_buf.copy_from_slice(&buf[offset_in_buf..offset_in_buf + 4]);
                blob_size_buf[0] &= 0x7f;
                (4, u32::from_be_bytes(blob_size_buf) as u64)
            };

            let start = offset_in_buf + size_length;
            let end = match next {
                Some((next_blob_start_offset, _)) => next_blob_start_offset - start_offset,
                None => start + blob_size,
            };

            assert_eq!(end - start, blob_size);

            metas.push(VectoredBlob {
                start: start as usize,
                end: end as usize,
                meta: *meta,
            })
        }

        Ok(VectoredBlobsBuf { buf, blobs: metas })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validate_read(read: &VectoredRead, offset_range: &[(Key, Lsn, u64, BlobFlag)]) {
        assert_eq!(read.start, offset_range.first().unwrap().2);

        let expected_offsets_in_read: Vec<_> = offset_range.iter().map(|o| o.2).collect();

        let offsets_in_read: Vec<_> = read
            .blobs_at
            .as_slice()
            .iter()
            .map(|(offset, _)| *offset)
            .collect();

        assert_eq!(expected_offsets_in_read, offsets_in_read);
    }

    #[test]
    fn planner_max_read_size_test() {
        let max_read_size = 128 * 1024;
        let key = Key::MIN;
        let lsn = Lsn(0);

        let blob_descriptions = vec![
            (key, lsn, 0, BlobFlag::None),
            (key, lsn, 32 * 1024, BlobFlag::None),
            (key, lsn, 96 * 1024, BlobFlag::None), // Last in read 1
            (key, lsn, 128 * 1024, BlobFlag::None), // Last in read 2
            (key, lsn, 198 * 1024, BlobFlag::None), // Last in read 3
            (key, lsn, 268 * 1024, BlobFlag::None), // Last in read 4
            (key, lsn, 396 * 1024, BlobFlag::None), // Last in read 5
            (key, lsn, 652 * 1024, BlobFlag::None), // Last in read 6
        ];

        let ranges = [
            &blob_descriptions[0..3],
            &blob_descriptions[3..4],
            &blob_descriptions[4..5],
            &blob_descriptions[5..6],
            &blob_descriptions[6..7],
            &blob_descriptions[7..],
        ];

        let mut planner = VectoredReadPlanner::new(max_read_size);
        for (key, lsn, offset, flag) in blob_descriptions.clone() {
            planner.handle(key, lsn, offset, flag);
        }

        planner.handle_range_end(652 * 1024);

        let reads = planner.finish();
        assert_eq!(reads.len(), 6);

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }

    #[test]
    fn planner_replacement_test() {
        let max_read_size = 128 * 1024;
        let first_key = Key::MIN;
        let second_key = first_key.next();
        let lsn = Lsn(0);

        let blob_descriptions = vec![
            (first_key, lsn, 0, BlobFlag::None),    // First in read 1
            (first_key, lsn, 1024, BlobFlag::None), // Last in read 1
            (second_key, lsn, 2 * 1024, BlobFlag::ReplaceAll),
            (second_key, lsn, 3 * 1024, BlobFlag::None),
            (second_key, lsn, 4 * 1024, BlobFlag::ReplaceAll), // First in read 2
            (second_key, lsn, 5 * 1024, BlobFlag::None),       // Last in read 2
        ];

        let ranges = [&blob_descriptions[0..2], &blob_descriptions[4..]];

        let mut planner = VectoredReadPlanner::new(max_read_size);
        for (key, lsn, offset, flag) in blob_descriptions.clone() {
            planner.handle(key, lsn, offset, flag);
        }

        planner.handle_range_end(6 * 1024);

        let reads = planner.finish();
        assert_eq!(reads.len(), 2);

        for (idx, read) in reads.iter().enumerate() {
            validate_read(read, ranges[idx]);
        }
    }
}
