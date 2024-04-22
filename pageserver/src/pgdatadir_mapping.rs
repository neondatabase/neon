//!
//! This provides an abstraction to store PostgreSQL relations and other files
//! in the key-value store that implements the Repository interface.
//!
//! (TODO: The line between PUT-functions here and walingest.rs is a bit blurry, as
//! walingest.rs handles a few things like implicit relation creation and extension.
//! Clarify that)
//!
use super::tenant::{PageReconstructError, Timeline};
use crate::context::RequestContext;
use crate::keyspace::{KeySpace, KeySpaceAccum};
use crate::repository::*;
use crate::span::debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id;
use crate::walrecord::NeonWalRecord;
use anyhow::{ensure, Context};
use bytes::{Buf, Bytes, BytesMut};
use enum_map::Enum;
use itertools::Itertools;
use pageserver_api::key::{
    dbdir_key_range, is_rel_block_key, is_slru_block_key, rel_block_to_key, rel_dir_to_key,
    rel_key_range, rel_size_to_key, relmap_file_key, slru_block_to_key, slru_dir_to_key,
    slru_segment_key_range, slru_segment_size_to_key, twophase_file_key, twophase_key_range,
    AUX_FILES_KEY, CHECKPOINT_KEY, CONTROLFILE_KEY, DBDIR_KEY, TWOPHASEDIR_KEY,
};
use pageserver_api::reltag::{BlockNumber, RelTag, SlruKind};
use postgres_ffi::relfile_utils::{FSM_FORKNUM, VISIBILITYMAP_FORKNUM};
use postgres_ffi::BLCKSZ;
use postgres_ffi::{Oid, TimestampTz, TransactionId};
use serde::{Deserialize, Serialize};
use std::collections::{hash_map, HashMap, HashSet};
use std::ops::ControlFlow;
use std::ops::Range;
use strum::IntoEnumIterator;
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};
use utils::bin_ser::DeserializeError;
use utils::vec_map::{VecMap, VecMapOrdering};
use utils::{bin_ser::BeSer, lsn::Lsn};

const MAX_AUX_FILE_DELTAS: usize = 1024;

#[derive(Debug)]
pub enum LsnForTimestamp {
    /// Found commits both before and after the given timestamp
    Present(Lsn),

    /// Found no commits after the given timestamp, this means
    /// that the newest data in the branch is older than the given
    /// timestamp.
    ///
    /// All commits <= LSN happened before the given timestamp
    Future(Lsn),

    /// The queried timestamp is past our horizon we look back at (PITR)
    ///
    /// All commits > LSN happened after the given timestamp,
    /// but any commits < LSN might have happened before or after
    /// the given timestamp. We don't know because no data before
    /// the given lsn is available.
    Past(Lsn),

    /// We have found no commit with a timestamp,
    /// so we can't return anything meaningful.
    ///
    /// The associated LSN is the lower bound value we can safely
    /// create branches on, but no statement is made if it is
    /// older or newer than the timestamp.
    ///
    /// This variant can e.g. be returned right after a
    /// cluster import.
    NoData(Lsn),
}

#[derive(Debug, thiserror::Error)]
pub enum CalculateLogicalSizeError {
    #[error("cancelled")]
    Cancelled,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CollectKeySpaceError {
    #[error(transparent)]
    Decode(#[from] DeserializeError),
    #[error(transparent)]
    PageRead(PageReconstructError),
    #[error("cancelled")]
    Cancelled,
}

impl From<PageReconstructError> for CollectKeySpaceError {
    fn from(err: PageReconstructError) -> Self {
        match err {
            PageReconstructError::Cancelled => Self::Cancelled,
            err => Self::PageRead(err),
        }
    }
}

impl From<PageReconstructError> for CalculateLogicalSizeError {
    fn from(pre: PageReconstructError) -> Self {
        match pre {
            PageReconstructError::AncestorStopping(_) | PageReconstructError::Cancelled => {
                Self::Cancelled
            }
            _ => Self::Other(pre.into()),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RelationError {
    #[error("Relation Already Exists")]
    AlreadyExists,
    #[error("invalid relnode")]
    InvalidRelnode,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

///
/// This impl provides all the functionality to store PostgreSQL relations, SLRUs,
/// and other special kinds of files, in a versioned key-value store. The
/// Timeline struct provides the key-value store.
///
/// This is a separate impl, so that we can easily include all these functions in a Timeline
/// implementation, and might be moved into a separate struct later.
impl Timeline {
    /// Start ingesting a WAL record, or other atomic modification of
    /// the timeline.
    ///
    /// This provides a transaction-like interface to perform a bunch
    /// of modifications atomically.
    ///
    /// To ingest a WAL record, call begin_modification(lsn) to get a
    /// DatadirModification object. Use the functions in the object to
    /// modify the repository state, updating all the pages and metadata
    /// that the WAL record affects. When you're done, call commit() to
    /// commit the changes.
    ///
    /// Lsn stored in modification is advanced by `ingest_record` and
    /// is used by `commit()` to update `last_record_lsn`.
    ///
    /// Calling commit() will flush all the changes and reset the state,
    /// so the `DatadirModification` struct can be reused to perform the next modification.
    ///
    /// Note that any pending modifications you make through the
    /// modification object won't be visible to calls to the 'get' and list
    /// functions of the timeline until you finish! And if you update the
    /// same page twice, the last update wins.
    ///
    pub fn begin_modification(&self, lsn: Lsn) -> DatadirModification
    where
        Self: Sized,
    {
        DatadirModification {
            tline: self,
            pending_lsns: Vec::new(),
            pending_updates: HashMap::new(),
            pending_deletions: Vec::new(),
            pending_nblocks: 0,
            pending_directory_entries: Vec::new(),
            lsn,
        }
    }

    //------------------------------------------------------------------------------
    // Public GET functions
    //------------------------------------------------------------------------------

    /// Look up given page version.
    pub(crate) async fn get_rel_page_at_lsn(
        &self,
        tag: RelTag,
        blknum: BlockNumber,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        if tag.relnode == 0 {
            return Err(PageReconstructError::Other(
                RelationError::InvalidRelnode.into(),
            ));
        }

        let nblocks = self.get_rel_size(tag, version, ctx).await?;
        if blknum >= nblocks {
            debug!(
                "read beyond EOF at {} blk {} at {}, size is {}: returning all-zeros page",
                tag,
                blknum,
                version.get_lsn(),
                nblocks
            );
            return Ok(ZERO_PAGE.clone());
        }

        let key = rel_block_to_key(tag, blknum);
        version.get(self, key, ctx).await
    }

    // Get size of a database in blocks
    pub(crate) async fn get_db_size(
        &self,
        spcnode: Oid,
        dbnode: Oid,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<usize, PageReconstructError> {
        let mut total_blocks = 0;

        let rels = self.list_rels(spcnode, dbnode, version, ctx).await?;

        for rel in rels {
            let n_blocks = self.get_rel_size(rel, version, ctx).await?;
            total_blocks += n_blocks as usize;
        }
        Ok(total_blocks)
    }

    /// Get size of a relation file
    pub(crate) async fn get_rel_size(
        &self,
        tag: RelTag,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<BlockNumber, PageReconstructError> {
        if tag.relnode == 0 {
            return Err(PageReconstructError::Other(
                RelationError::InvalidRelnode.into(),
            ));
        }

        if let Some(nblocks) = self.get_cached_rel_size(&tag, version.get_lsn()) {
            return Ok(nblocks);
        }

        if (tag.forknum == FSM_FORKNUM || tag.forknum == VISIBILITYMAP_FORKNUM)
            && !self.get_rel_exists(tag, version, ctx).await?
        {
            // FIXME: Postgres sometimes calls smgrcreate() to create
            // FSM, and smgrnblocks() on it immediately afterwards,
            // without extending it.  Tolerate that by claiming that
            // any non-existent FSM fork has size 0.
            return Ok(0);
        }

        let key = rel_size_to_key(tag);
        let mut buf = version.get(self, key, ctx).await?;
        let nblocks = buf.get_u32_le();

        self.update_cached_rel_size(tag, version.get_lsn(), nblocks);

        Ok(nblocks)
    }

    /// Does relation exist?
    pub(crate) async fn get_rel_exists(
        &self,
        tag: RelTag,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<bool, PageReconstructError> {
        if tag.relnode == 0 {
            return Err(PageReconstructError::Other(
                RelationError::InvalidRelnode.into(),
            ));
        }

        // first try to lookup relation in cache
        if let Some(_nblocks) = self.get_cached_rel_size(&tag, version.get_lsn()) {
            return Ok(true);
        }
        // fetch directory listing
        let key = rel_dir_to_key(tag.spcnode, tag.dbnode);
        let buf = version.get(self, key, ctx).await?;

        match RelDirectory::des(&buf).context("deserialization failure") {
            Ok(dir) => {
                let exists = dir.rels.get(&(tag.relnode, tag.forknum)).is_some();
                Ok(exists)
            }
            Err(e) => Err(PageReconstructError::from(e)),
        }
    }

    /// Get a list of all existing relations in given tablespace and database.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    pub(crate) async fn list_rels(
        &self,
        spcnode: Oid,
        dbnode: Oid,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<HashSet<RelTag>, PageReconstructError> {
        // fetch directory listing
        let key = rel_dir_to_key(spcnode, dbnode);
        let buf = version.get(self, key, ctx).await?;

        match RelDirectory::des(&buf).context("deserialization failure") {
            Ok(dir) => {
                let rels: HashSet<RelTag> =
                    HashSet::from_iter(dir.rels.iter().map(|(relnode, forknum)| RelTag {
                        spcnode,
                        dbnode,
                        relnode: *relnode,
                        forknum: *forknum,
                    }));

                Ok(rels)
            }
            Err(e) => Err(PageReconstructError::from(e)),
        }
    }

    /// Get the whole SLRU segment
    pub(crate) async fn get_slru_segment(
        &self,
        kind: SlruKind,
        segno: u32,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        let n_blocks = self
            .get_slru_segment_size(kind, segno, Version::Lsn(lsn), ctx)
            .await?;
        let mut segment = BytesMut::with_capacity(n_blocks as usize * BLCKSZ as usize);
        for blkno in 0..n_blocks {
            let block = self
                .get_slru_page_at_lsn(kind, segno, blkno, lsn, ctx)
                .await?;
            segment.extend_from_slice(&block[..BLCKSZ as usize]);
        }
        Ok(segment.freeze())
    }

    /// Look up given SLRU page version.
    pub(crate) async fn get_slru_page_at_lsn(
        &self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        let key = slru_block_to_key(kind, segno, blknum);
        self.get(key, lsn, ctx).await
    }

    /// Get size of an SLRU segment
    pub(crate) async fn get_slru_segment_size(
        &self,
        kind: SlruKind,
        segno: u32,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<BlockNumber, PageReconstructError> {
        let key = slru_segment_size_to_key(kind, segno);
        let mut buf = version.get(self, key, ctx).await?;
        Ok(buf.get_u32_le())
    }

    /// Get size of an SLRU segment
    pub(crate) async fn get_slru_segment_exists(
        &self,
        kind: SlruKind,
        segno: u32,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<bool, PageReconstructError> {
        // fetch directory listing
        let key = slru_dir_to_key(kind);
        let buf = version.get(self, key, ctx).await?;

        match SlruSegmentDirectory::des(&buf).context("deserialization failure") {
            Ok(dir) => {
                let exists = dir.segments.get(&segno).is_some();
                Ok(exists)
            }
            Err(e) => Err(PageReconstructError::from(e)),
        }
    }

    /// Locate LSN, such that all transactions that committed before
    /// 'search_timestamp' are visible, but nothing newer is.
    ///
    /// This is not exact. Commit timestamps are not guaranteed to be ordered,
    /// so it's not well defined which LSN you get if there were multiple commits
    /// "in flight" at that point in time.
    ///
    pub(crate) async fn find_lsn_for_timestamp(
        &self,
        search_timestamp: TimestampTz,
        cancel: &CancellationToken,
        ctx: &RequestContext,
    ) -> Result<LsnForTimestamp, PageReconstructError> {
        let gc_cutoff_lsn_guard = self.get_latest_gc_cutoff_lsn();
        // We use this method to figure out the branching LSN for the new branch, but the
        // GC cutoff could be before the branching point and we cannot create a new branch
        // with LSN < `ancestor_lsn`. Thus, pick the maximum of these two to be
        // on the safe side.
        let min_lsn = std::cmp::max(*gc_cutoff_lsn_guard, self.get_ancestor_lsn());
        let max_lsn = self.get_last_record_lsn();

        // LSNs are always 8-byte aligned. low/mid/high represent the
        // LSN divided by 8.
        let mut low = min_lsn.0 / 8;
        let mut high = max_lsn.0 / 8 + 1;

        let mut found_smaller = false;
        let mut found_larger = false;
        while low < high {
            if cancel.is_cancelled() {
                return Err(PageReconstructError::Cancelled);
            }
            // cannot overflow, high and low are both smaller than u64::MAX / 2
            let mid = (high + low) / 2;

            let cmp = self
                .is_latest_commit_timestamp_ge_than(
                    search_timestamp,
                    Lsn(mid * 8),
                    &mut found_smaller,
                    &mut found_larger,
                    ctx,
                )
                .await?;

            if cmp {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        // If `found_smaller == true`, `low = t + 1` where `t` is the target LSN,
        // so the LSN of the last commit record before or at `search_timestamp`.
        // Remove one from `low` to get `t`.
        //
        // FIXME: it would be better to get the LSN of the previous commit.
        // Otherwise, if you restore to the returned LSN, the database will
        // include physical changes from later commits that will be marked
        // as aborted, and will need to be vacuumed away.
        let commit_lsn = Lsn((low - 1) * 8);
        match (found_smaller, found_larger) {
            (false, false) => {
                // This can happen if no commit records have been processed yet, e.g.
                // just after importing a cluster.
                Ok(LsnForTimestamp::NoData(min_lsn))
            }
            (false, true) => {
                // Didn't find any commit timestamps smaller than the request
                Ok(LsnForTimestamp::Past(min_lsn))
            }
            (true, false) => {
                // Only found commits with timestamps smaller than the request.
                // It's still a valid case for branch creation, return it.
                // And `update_gc_info()` ignores LSN for a `LsnForTimestamp::Future`
                // case, anyway.
                Ok(LsnForTimestamp::Future(commit_lsn))
            }
            (true, true) => Ok(LsnForTimestamp::Present(commit_lsn)),
        }
    }

    /// Subroutine of find_lsn_for_timestamp(). Returns true, if there are any
    /// commits that committed after 'search_timestamp', at LSN 'probe_lsn'.
    ///
    /// Additionally, sets 'found_smaller'/'found_Larger, if encounters any commits
    /// with a smaller/larger timestamp.
    ///
    pub(crate) async fn is_latest_commit_timestamp_ge_than(
        &self,
        search_timestamp: TimestampTz,
        probe_lsn: Lsn,
        found_smaller: &mut bool,
        found_larger: &mut bool,
        ctx: &RequestContext,
    ) -> Result<bool, PageReconstructError> {
        self.map_all_timestamps(probe_lsn, ctx, |timestamp| {
            if timestamp >= search_timestamp {
                *found_larger = true;
                return ControlFlow::Break(true);
            } else {
                *found_smaller = true;
            }
            ControlFlow::Continue(())
        })
        .await
    }

    /// Obtain the possible timestamp range for the given lsn.
    ///
    /// If the lsn has no timestamps, returns None. returns `(min, max, median)` if it has timestamps.
    pub(crate) async fn get_timestamp_for_lsn(
        &self,
        probe_lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Option<TimestampTz>, PageReconstructError> {
        let mut max: Option<TimestampTz> = None;
        self.map_all_timestamps(probe_lsn, ctx, |timestamp| {
            if let Some(max_prev) = max {
                max = Some(max_prev.max(timestamp));
            } else {
                max = Some(timestamp);
            }
            ControlFlow::Continue(())
        })
        .await?;

        Ok(max)
    }

    /// Runs the given function on all the timestamps for a given lsn
    ///
    /// The return value is either given by the closure, or set to the `Default`
    /// impl's output.
    async fn map_all_timestamps<T: Default>(
        &self,
        probe_lsn: Lsn,
        ctx: &RequestContext,
        mut f: impl FnMut(TimestampTz) -> ControlFlow<T>,
    ) -> Result<T, PageReconstructError> {
        for segno in self
            .list_slru_segments(SlruKind::Clog, Version::Lsn(probe_lsn), ctx)
            .await?
        {
            let nblocks = self
                .get_slru_segment_size(SlruKind::Clog, segno, Version::Lsn(probe_lsn), ctx)
                .await?;
            for blknum in (0..nblocks).rev() {
                let clog_page = self
                    .get_slru_page_at_lsn(SlruKind::Clog, segno, blknum, probe_lsn, ctx)
                    .await?;

                if clog_page.len() == BLCKSZ as usize + 8 {
                    let mut timestamp_bytes = [0u8; 8];
                    timestamp_bytes.copy_from_slice(&clog_page[BLCKSZ as usize..]);
                    let timestamp = TimestampTz::from_be_bytes(timestamp_bytes);

                    match f(timestamp) {
                        ControlFlow::Break(b) => return Ok(b),
                        ControlFlow::Continue(()) => (),
                    }
                }
            }
        }
        Ok(Default::default())
    }

    pub(crate) async fn get_slru_keyspace(
        &self,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<KeySpace, PageReconstructError> {
        let mut accum = KeySpaceAccum::new();

        for kind in SlruKind::iter() {
            let mut segments: Vec<u32> = self
                .list_slru_segments(kind, version, ctx)
                .await?
                .into_iter()
                .collect();
            segments.sort_unstable();

            for seg in segments {
                let block_count = self.get_slru_segment_size(kind, seg, version, ctx).await?;

                accum.add_range(
                    slru_block_to_key(kind, seg, 0)..slru_block_to_key(kind, seg, block_count),
                );
            }
        }

        Ok(accum.to_keyspace())
    }

    /// Get a list of SLRU segments
    pub(crate) async fn list_slru_segments(
        &self,
        kind: SlruKind,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<HashSet<u32>, PageReconstructError> {
        // fetch directory entry
        let key = slru_dir_to_key(kind);

        let buf = version.get(self, key, ctx).await?;
        match SlruSegmentDirectory::des(&buf).context("deserialization failure") {
            Ok(dir) => Ok(dir.segments),
            Err(e) => Err(PageReconstructError::from(e)),
        }
    }

    pub(crate) async fn get_relmap_file(
        &self,
        spcnode: Oid,
        dbnode: Oid,
        version: Version<'_>,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        let key = relmap_file_key(spcnode, dbnode);

        let buf = version.get(self, key, ctx).await?;
        Ok(buf)
    }

    pub(crate) async fn list_dbdirs(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashMap<(Oid, Oid), bool>, PageReconstructError> {
        // fetch directory entry
        let buf = self.get(DBDIR_KEY, lsn, ctx).await?;

        match DbDirectory::des(&buf).context("deserialization failure") {
            Ok(dir) => Ok(dir.dbdirs),
            Err(e) => Err(PageReconstructError::from(e)),
        }
    }

    pub(crate) async fn get_twophase_file(
        &self,
        xid: TransactionId,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        let key = twophase_file_key(xid);
        let buf = self.get(key, lsn, ctx).await?;
        Ok(buf)
    }

    pub(crate) async fn list_twophase_files(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashSet<TransactionId>, PageReconstructError> {
        // fetch directory entry
        let buf = self.get(TWOPHASEDIR_KEY, lsn, ctx).await?;

        match TwoPhaseDirectory::des(&buf).context("deserialization failure") {
            Ok(dir) => Ok(dir.xids),
            Err(e) => Err(PageReconstructError::from(e)),
        }
    }

    pub(crate) async fn get_control_file(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        self.get(CONTROLFILE_KEY, lsn, ctx).await
    }

    pub(crate) async fn get_checkpoint(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        self.get(CHECKPOINT_KEY, lsn, ctx).await
    }

    pub(crate) async fn list_aux_files(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<HashMap<String, Bytes>, PageReconstructError> {
        match self.get(AUX_FILES_KEY, lsn, ctx).await {
            Ok(buf) => match AuxFilesDirectory::des(&buf).context("deserialization failure") {
                Ok(dir) => Ok(dir.files),
                Err(e) => Err(PageReconstructError::from(e)),
            },
            Err(e) => {
                // This is expected: historical databases do not have the key.
                debug!("Failed to get info about AUX files: {}", e);
                Ok(HashMap::new())
            }
        }
    }

    /// Does the same as get_current_logical_size but counted on demand.
    /// Used to initialize the logical size tracking on startup.
    ///
    /// Only relation blocks are counted currently. That excludes metadata,
    /// SLRUs, twophase files etc.
    ///
    /// # Cancel-Safety
    ///
    /// This method is cancellation-safe.
    pub async fn get_current_logical_size_non_incremental(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<u64, CalculateLogicalSizeError> {
        debug_assert_current_span_has_tenant_and_timeline_id_no_shard_id();

        // Fetch list of database dirs and iterate them
        let buf = self.get(DBDIR_KEY, lsn, ctx).await?;
        let dbdir = DbDirectory::des(&buf).context("deserialize db directory")?;

        let mut total_size: u64 = 0;
        for (spcnode, dbnode) in dbdir.dbdirs.keys() {
            for rel in self
                .list_rels(*spcnode, *dbnode, Version::Lsn(lsn), ctx)
                .await?
            {
                if self.cancel.is_cancelled() {
                    return Err(CalculateLogicalSizeError::Cancelled);
                }
                let relsize_key = rel_size_to_key(rel);
                let mut buf = self.get(relsize_key, lsn, ctx).await?;
                let relsize = buf.get_u32_le();

                total_size += relsize as u64;
            }
        }
        Ok(total_size * BLCKSZ as u64)
    }

    ///
    /// Get a KeySpace that covers all the Keys that are in use at the given LSN.
    /// Anything that's not listed maybe removed from the underlying storage (from
    /// that LSN forwards).
    pub(crate) async fn collect_keyspace(
        &self,
        lsn: Lsn,
        ctx: &RequestContext,
    ) -> Result<KeySpace, CollectKeySpaceError> {
        // Iterate through key ranges, greedily packing them into partitions
        let mut result = KeySpaceAccum::new();

        // The dbdir metadata always exists
        result.add_key(DBDIR_KEY);

        // Fetch list of database dirs and iterate them
        let buf = self.get(DBDIR_KEY, lsn, ctx).await?;
        let dbdir = DbDirectory::des(&buf)?;

        let mut dbs: Vec<(Oid, Oid)> = dbdir.dbdirs.keys().cloned().collect();
        dbs.sort_unstable();
        for (spcnode, dbnode) in dbs {
            result.add_key(relmap_file_key(spcnode, dbnode));
            result.add_key(rel_dir_to_key(spcnode, dbnode));

            let mut rels: Vec<RelTag> = self
                .list_rels(spcnode, dbnode, Version::Lsn(lsn), ctx)
                .await?
                .into_iter()
                .collect();
            rels.sort_unstable();
            for rel in rels {
                let relsize_key = rel_size_to_key(rel);
                let mut buf = self.get(relsize_key, lsn, ctx).await?;
                let relsize = buf.get_u32_le();

                result.add_range(rel_block_to_key(rel, 0)..rel_block_to_key(rel, relsize));
                result.add_key(relsize_key);
            }
        }

        // Iterate SLRUs next
        for kind in [
            SlruKind::Clog,
            SlruKind::MultiXactMembers,
            SlruKind::MultiXactOffsets,
        ] {
            let slrudir_key = slru_dir_to_key(kind);
            result.add_key(slrudir_key);
            let buf = self.get(slrudir_key, lsn, ctx).await?;
            let dir = SlruSegmentDirectory::des(&buf)?;
            let mut segments: Vec<u32> = dir.segments.iter().cloned().collect();
            segments.sort_unstable();
            for segno in segments {
                let segsize_key = slru_segment_size_to_key(kind, segno);
                let mut buf = self.get(segsize_key, lsn, ctx).await?;
                let segsize = buf.get_u32_le();

                result.add_range(
                    slru_block_to_key(kind, segno, 0)..slru_block_to_key(kind, segno, segsize),
                );
                result.add_key(segsize_key);
            }
        }

        // Then pg_twophase
        result.add_key(TWOPHASEDIR_KEY);
        let buf = self.get(TWOPHASEDIR_KEY, lsn, ctx).await?;
        let twophase_dir = TwoPhaseDirectory::des(&buf)?;
        let mut xids: Vec<TransactionId> = twophase_dir.xids.iter().cloned().collect();
        xids.sort_unstable();
        for xid in xids {
            result.add_key(twophase_file_key(xid));
        }

        result.add_key(CONTROLFILE_KEY);
        result.add_key(CHECKPOINT_KEY);
        if self.get(AUX_FILES_KEY, lsn, ctx).await.is_ok() {
            result.add_key(AUX_FILES_KEY);
        }
        Ok(result.to_keyspace())
    }

    /// Get cached size of relation if it not updated after specified LSN
    pub fn get_cached_rel_size(&self, tag: &RelTag, lsn: Lsn) -> Option<BlockNumber> {
        let rel_size_cache = self.rel_size_cache.read().unwrap();
        if let Some((cached_lsn, nblocks)) = rel_size_cache.map.get(tag) {
            if lsn >= *cached_lsn {
                return Some(*nblocks);
            }
        }
        None
    }

    /// Update cached relation size if there is no more recent update
    pub fn update_cached_rel_size(&self, tag: RelTag, lsn: Lsn, nblocks: BlockNumber) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();

        if lsn < rel_size_cache.complete_as_of {
            // Do not cache old values. It's safe to cache the size on read, as long as
            // the read was at an LSN since we started the WAL ingestion. Reasoning: we
            // never evict values from the cache, so if the relation size changed after
            // 'lsn', the new value is already in the cache.
            return;
        }

        match rel_size_cache.map.entry(tag) {
            hash_map::Entry::Occupied(mut entry) => {
                let cached_lsn = entry.get_mut();
                if lsn >= cached_lsn.0 {
                    *cached_lsn = (lsn, nblocks);
                }
            }
            hash_map::Entry::Vacant(entry) => {
                entry.insert((lsn, nblocks));
            }
        }
    }

    /// Store cached relation size
    pub fn set_cached_rel_size(&self, tag: RelTag, lsn: Lsn, nblocks: BlockNumber) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        rel_size_cache.map.insert(tag, (lsn, nblocks));
    }

    /// Remove cached relation size
    pub fn remove_cached_rel_size(&self, tag: &RelTag) {
        let mut rel_size_cache = self.rel_size_cache.write().unwrap();
        rel_size_cache.map.remove(tag);
    }
}

/// DatadirModification represents an operation to ingest an atomic set of
/// updates to the repository. It is created by the 'begin_record'
/// function. It is called for each WAL record, so that all the modifications
/// by a one WAL record appear atomic.
pub struct DatadirModification<'a> {
    /// The timeline this modification applies to. You can access this to
    /// read the state, but note that any pending updates are *not* reflected
    /// in the state in 'tline' yet.
    pub tline: &'a Timeline,

    /// Current LSN of the modification
    lsn: Lsn,

    // The modifications are not applied directly to the underlying key-value store.
    // The put-functions add the modifications here, and they are flushed to the
    // underlying key-value store by the 'finish' function.
    pending_lsns: Vec<Lsn>,
    pending_updates: HashMap<Key, Vec<(Lsn, Value)>>,
    pending_deletions: Vec<(Range<Key>, Lsn)>,
    pending_nblocks: i64,

    /// For special "directory" keys that store key-value maps, track the size of the map
    /// if it was updated in this modification.
    pending_directory_entries: Vec<(DirectoryKind, usize)>,
}

impl<'a> DatadirModification<'a> {
    /// Get the current lsn
    pub(crate) fn get_lsn(&self) -> Lsn {
        self.lsn
    }

    /// Set the current lsn
    pub(crate) fn set_lsn(&mut self, lsn: Lsn) -> anyhow::Result<()> {
        ensure!(
            lsn >= self.lsn,
            "setting an older lsn {} than {} is not allowed",
            lsn,
            self.lsn
        );
        if lsn > self.lsn {
            self.pending_lsns.push(self.lsn);
            self.lsn = lsn;
        }
        Ok(())
    }

    /// Initialize a completely new repository.
    ///
    /// This inserts the directory metadata entries that are assumed to
    /// always exist.
    pub fn init_empty(&mut self) -> anyhow::Result<()> {
        let buf = DbDirectory::ser(&DbDirectory {
            dbdirs: HashMap::new(),
        })?;
        self.pending_directory_entries.push((DirectoryKind::Db, 0));
        self.put(DBDIR_KEY, Value::Image(buf.into()));

        // Create AuxFilesDirectory
        self.init_aux_dir()?;

        let buf = TwoPhaseDirectory::ser(&TwoPhaseDirectory {
            xids: HashSet::new(),
        })?;
        self.pending_directory_entries
            .push((DirectoryKind::TwoPhase, 0));
        self.put(TWOPHASEDIR_KEY, Value::Image(buf.into()));

        let buf: Bytes = SlruSegmentDirectory::ser(&SlruSegmentDirectory::default())?.into();
        let empty_dir = Value::Image(buf);
        self.put(slru_dir_to_key(SlruKind::Clog), empty_dir.clone());
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(SlruKind::Clog), 0));
        self.put(
            slru_dir_to_key(SlruKind::MultiXactMembers),
            empty_dir.clone(),
        );
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(SlruKind::Clog), 0));
        self.put(slru_dir_to_key(SlruKind::MultiXactOffsets), empty_dir);
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(SlruKind::MultiXactOffsets), 0));

        Ok(())
    }

    #[cfg(test)]
    pub fn init_empty_test_timeline(&mut self) -> anyhow::Result<()> {
        self.init_empty()?;
        self.put_control_file(bytes::Bytes::from_static(
            b"control_file contents do not matter",
        ))
        .context("put_control_file")?;
        self.put_checkpoint(bytes::Bytes::from_static(
            b"checkpoint_file contents do not matter",
        ))
        .context("put_checkpoint_file")?;
        Ok(())
    }

    /// Put a new page version that can be constructed from a WAL record
    ///
    /// NOTE: this will *not* implicitly extend the relation, if the page is beyond the
    /// current end-of-file. It's up to the caller to check that the relation size
    /// matches the blocks inserted!
    pub fn put_rel_wal_record(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
        rec: NeonWalRecord,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        self.put(rel_block_to_key(rel, blknum), Value::WalRecord(rec));
        Ok(())
    }

    // Same, but for an SLRU.
    pub fn put_slru_wal_record(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        rec: NeonWalRecord,
    ) -> anyhow::Result<()> {
        self.put(
            slru_block_to_key(kind, segno, blknum),
            Value::WalRecord(rec),
        );
        Ok(())
    }

    /// Like put_wal_record, but with ready-made image of the page.
    pub fn put_rel_page_image(
        &mut self,
        rel: RelTag,
        blknum: BlockNumber,
        img: Bytes,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        self.put(rel_block_to_key(rel, blknum), Value::Image(img));
        Ok(())
    }

    pub fn put_slru_page_image(
        &mut self,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        img: Bytes,
    ) -> anyhow::Result<()> {
        self.put(slru_block_to_key(kind, segno, blknum), Value::Image(img));
        Ok(())
    }

    /// Store a relmapper file (pg_filenode.map) in the repository
    pub async fn put_relmap_file(
        &mut self,
        spcnode: Oid,
        dbnode: Oid,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Add it to the directory (if it doesn't exist already)
        let buf = self.get(DBDIR_KEY, ctx).await?;
        let mut dbdir = DbDirectory::des(&buf)?;

        let r = dbdir.dbdirs.insert((spcnode, dbnode), true);
        if r.is_none() || r == Some(false) {
            // The dbdir entry didn't exist, or it contained a
            // 'false'. The 'insert' call already updated it with
            // 'true', now write the updated 'dbdirs' map back.
            let buf = DbDirectory::ser(&dbdir)?;
            self.put(DBDIR_KEY, Value::Image(buf.into()));

            // Create AuxFilesDirectory as well
            self.init_aux_dir()?;
        }
        if r.is_none() {
            // Create RelDirectory
            let buf = RelDirectory::ser(&RelDirectory {
                rels: HashSet::new(),
            })?;
            self.pending_directory_entries.push((DirectoryKind::Rel, 0));
            self.put(
                rel_dir_to_key(spcnode, dbnode),
                Value::Image(Bytes::from(buf)),
            );
        }

        self.put(relmap_file_key(spcnode, dbnode), Value::Image(img));
        Ok(())
    }

    pub async fn put_twophase_file(
        &mut self,
        xid: TransactionId,
        img: Bytes,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Add it to the directory entry
        let buf = self.get(TWOPHASEDIR_KEY, ctx).await?;
        let mut dir = TwoPhaseDirectory::des(&buf)?;
        if !dir.xids.insert(xid) {
            anyhow::bail!("twophase file for xid {} already exists", xid);
        }
        self.pending_directory_entries
            .push((DirectoryKind::TwoPhase, dir.xids.len()));
        self.put(
            TWOPHASEDIR_KEY,
            Value::Image(Bytes::from(TwoPhaseDirectory::ser(&dir)?)),
        );

        self.put(twophase_file_key(xid), Value::Image(img));
        Ok(())
    }

    pub fn put_control_file(&mut self, img: Bytes) -> anyhow::Result<()> {
        self.put(CONTROLFILE_KEY, Value::Image(img));
        Ok(())
    }

    pub fn put_checkpoint(&mut self, img: Bytes) -> anyhow::Result<()> {
        self.put(CHECKPOINT_KEY, Value::Image(img));
        Ok(())
    }

    pub async fn drop_dbdir(
        &mut self,
        spcnode: Oid,
        dbnode: Oid,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let total_blocks = self
            .tline
            .get_db_size(spcnode, dbnode, Version::Modified(self), ctx)
            .await?;

        // Remove entry from dbdir
        let buf = self.get(DBDIR_KEY, ctx).await?;
        let mut dir = DbDirectory::des(&buf)?;
        if dir.dbdirs.remove(&(spcnode, dbnode)).is_some() {
            let buf = DbDirectory::ser(&dir)?;
            self.pending_directory_entries
                .push((DirectoryKind::Db, dir.dbdirs.len()));
            self.put(DBDIR_KEY, Value::Image(buf.into()));
        } else {
            warn!(
                "dropped dbdir for spcnode {} dbnode {} did not exist in db directory",
                spcnode, dbnode
            );
        }

        // Update logical database size.
        self.pending_nblocks -= total_blocks as i64;

        // Delete all relations and metadata files for the spcnode/dnode
        self.delete(dbdir_key_range(spcnode, dbnode));
        Ok(())
    }

    /// Create a relation fork.
    ///
    /// 'nblocks' is the initial size.
    pub async fn put_rel_creation(
        &mut self,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> Result<(), RelationError> {
        if rel.relnode == 0 {
            return Err(RelationError::InvalidRelnode);
        }
        // It's possible that this is the first rel for this db in this
        // tablespace.  Create the reldir entry for it if so.
        let mut dbdir = DbDirectory::des(&self.get(DBDIR_KEY, ctx).await.context("read db")?)
            .context("deserialize db")?;
        let rel_dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let mut rel_dir = if dbdir.dbdirs.get(&(rel.spcnode, rel.dbnode)).is_none() {
            // Didn't exist. Update dbdir
            dbdir.dbdirs.insert((rel.spcnode, rel.dbnode), false);
            let buf = DbDirectory::ser(&dbdir).context("serialize db")?;
            self.pending_directory_entries
                .push((DirectoryKind::Db, dbdir.dbdirs.len()));
            self.put(DBDIR_KEY, Value::Image(buf.into()));

            // and create the RelDirectory
            RelDirectory::default()
        } else {
            // reldir already exists, fetch it
            RelDirectory::des(&self.get(rel_dir_key, ctx).await.context("read db")?)
                .context("deserialize db")?
        };

        // Add the new relation to the rel directory entry, and write it back
        if !rel_dir.rels.insert((rel.relnode, rel.forknum)) {
            return Err(RelationError::AlreadyExists);
        }

        self.pending_directory_entries
            .push((DirectoryKind::Rel, rel_dir.rels.len()));

        self.put(
            rel_dir_key,
            Value::Image(Bytes::from(
                RelDirectory::ser(&rel_dir).context("serialize")?,
            )),
        );

        // Put size
        let size_key = rel_size_to_key(rel);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

        self.pending_nblocks += nblocks as i64;

        // Update relation size cache
        self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

        // Even if nblocks > 0, we don't insert any actual blocks here. That's up to the
        // caller.
        Ok(())
    }

    /// Truncate relation
    pub async fn put_rel_truncation(
        &mut self,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);
        if self
            .tline
            .get_rel_exists(rel, Version::Modified(self), ctx)
            .await?
        {
            let size_key = rel_size_to_key(rel);
            // Fetch the old size first
            let old_size = self.get(size_key, ctx).await?.get_u32_le();

            // Update the entry with the new size.
            let buf = nblocks.to_le_bytes();
            self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            // Update logical database size.
            self.pending_nblocks -= old_size as i64 - nblocks as i64;
        }
        Ok(())
    }

    /// Extend relation
    /// If new size is smaller, do nothing.
    pub async fn put_rel_extend(
        &mut self,
        rel: RelTag,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);

        // Put size
        let size_key = rel_size_to_key(rel);
        let old_size = self.get(size_key, ctx).await?.get_u32_le();

        // only extend relation here. never decrease the size
        if nblocks > old_size {
            let buf = nblocks.to_le_bytes();
            self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

            // Update relation size cache
            self.tline.set_cached_rel_size(rel, self.lsn, nblocks);

            self.pending_nblocks += nblocks as i64 - old_size as i64;
        }
        Ok(())
    }

    /// Drop a relation.
    pub async fn put_rel_drop(&mut self, rel: RelTag, ctx: &RequestContext) -> anyhow::Result<()> {
        anyhow::ensure!(rel.relnode != 0, RelationError::InvalidRelnode);

        // Remove it from the directory entry
        let dir_key = rel_dir_to_key(rel.spcnode, rel.dbnode);
        let buf = self.get(dir_key, ctx).await?;
        let mut dir = RelDirectory::des(&buf)?;

        self.pending_directory_entries
            .push((DirectoryKind::Rel, dir.rels.len()));

        if dir.rels.remove(&(rel.relnode, rel.forknum)) {
            self.put(dir_key, Value::Image(Bytes::from(RelDirectory::ser(&dir)?)));
        } else {
            warn!("dropped rel {} did not exist in rel directory", rel);
        }

        // update logical size
        let size_key = rel_size_to_key(rel);
        let old_size = self.get(size_key, ctx).await?.get_u32_le();
        self.pending_nblocks -= old_size as i64;

        // Remove enty from relation size cache
        self.tline.remove_cached_rel_size(&rel);

        // Delete size entry, as well as all blocks
        self.delete(rel_key_range(rel));

        Ok(())
    }

    pub async fn put_slru_segment_creation(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Add it to the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key, ctx).await?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.insert(segno) {
            anyhow::bail!("slru segment {kind:?}/{segno} already exists");
        }
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(kind), dir.segments.len()));
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));

        // even if nblocks > 0, we don't insert any actual blocks here

        Ok(())
    }

    /// Extend SLRU segment
    pub fn put_slru_extend(
        &mut self,
        kind: SlruKind,
        segno: u32,
        nblocks: BlockNumber,
    ) -> anyhow::Result<()> {
        // Put size
        let size_key = slru_segment_size_to_key(kind, segno);
        let buf = nblocks.to_le_bytes();
        self.put(size_key, Value::Image(Bytes::from(buf.to_vec())));
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub async fn drop_slru_segment(
        &mut self,
        kind: SlruKind,
        segno: u32,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Remove it from the directory entry
        let dir_key = slru_dir_to_key(kind);
        let buf = self.get(dir_key, ctx).await?;
        let mut dir = SlruSegmentDirectory::des(&buf)?;

        if !dir.segments.remove(&segno) {
            warn!("slru segment {:?}/{} does not exist", kind, segno);
        }
        self.pending_directory_entries
            .push((DirectoryKind::SlruSegment(kind), dir.segments.len()));
        self.put(
            dir_key,
            Value::Image(Bytes::from(SlruSegmentDirectory::ser(&dir)?)),
        );

        // Delete size entry, as well as all blocks
        self.delete(slru_segment_key_range(kind, segno));

        Ok(())
    }

    /// Drop a relmapper file (pg_filenode.map)
    pub fn drop_relmap_file(&mut self, _spcnode: Oid, _dbnode: Oid) -> anyhow::Result<()> {
        // TODO
        Ok(())
    }

    /// This method is used for marking truncated SLRU files
    pub async fn drop_twophase_file(
        &mut self,
        xid: TransactionId,
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        // Remove it from the directory entry
        let buf = self.get(TWOPHASEDIR_KEY, ctx).await?;
        let mut dir = TwoPhaseDirectory::des(&buf)?;

        if !dir.xids.remove(&xid) {
            warn!("twophase file for xid {} does not exist", xid);
        }
        self.pending_directory_entries
            .push((DirectoryKind::TwoPhase, dir.xids.len()));
        self.put(
            TWOPHASEDIR_KEY,
            Value::Image(Bytes::from(TwoPhaseDirectory::ser(&dir)?)),
        );

        // Delete it
        self.delete(twophase_key_range(xid));

        Ok(())
    }

    pub fn init_aux_dir(&mut self) -> anyhow::Result<()> {
        let buf = AuxFilesDirectory::ser(&AuxFilesDirectory {
            files: HashMap::new(),
        })?;
        self.pending_directory_entries
            .push((DirectoryKind::AuxFiles, 0));
        self.put(AUX_FILES_KEY, Value::Image(Bytes::from(buf)));
        Ok(())
    }

    pub async fn put_file(
        &mut self,
        path: &str,
        content: &[u8],
        ctx: &RequestContext,
    ) -> anyhow::Result<()> {
        let file_path = path.to_string();
        let content = if content.is_empty() {
            None
        } else {
            Some(Bytes::copy_from_slice(content))
        };

        let n_files;
        let mut aux_files = self.tline.aux_files.lock().await;
        if let Some(mut dir) = aux_files.dir.take() {
            // We already updated aux files in `self`: emit a delta and update our latest value
            dir.upsert(file_path.clone(), content.clone());
            n_files = dir.files.len();
            if aux_files.n_deltas == MAX_AUX_FILE_DELTAS {
                self.put(
                    AUX_FILES_KEY,
                    Value::Image(Bytes::from(
                        AuxFilesDirectory::ser(&dir).context("serialize")?,
                    )),
                );
                aux_files.n_deltas = 0;
            } else {
                self.put(
                    AUX_FILES_KEY,
                    Value::WalRecord(NeonWalRecord::AuxFile { file_path, content }),
                );
                aux_files.n_deltas += 1;
            }
            aux_files.dir = Some(dir);
        } else {
            // Check if the AUX_FILES_KEY is initialized
            match self.get(AUX_FILES_KEY, ctx).await {
                Ok(dir_bytes) => {
                    let mut dir = AuxFilesDirectory::des(&dir_bytes)?;
                    // Key is already set, we may append a delta
                    self.put(
                        AUX_FILES_KEY,
                        Value::WalRecord(NeonWalRecord::AuxFile {
                            file_path: file_path.clone(),
                            content: content.clone(),
                        }),
                    );
                    dir.upsert(file_path, content);
                    n_files = dir.files.len();
                    aux_files.dir = Some(dir);
                }
                Err(
                    e @ (PageReconstructError::AncestorStopping(_)
                    | PageReconstructError::Cancelled
                    | PageReconstructError::AncestorLsnTimeout(_)),
                ) => {
                    // Important that we do not interpret a shutdown error as "not found" and thereby
                    // reset the map.
                    return Err(e.into());
                }
                // FIXME: PageReconstructError doesn't have an explicit variant for key-not-found, so
                // we are assuming that all _other_ possible errors represents a missing key.  If some
                // other error occurs, we may incorrectly reset the map of aux files.
                Err(PageReconstructError::Other(_) | PageReconstructError::WalRedo(_)) => {
                    // Key is missing, we must insert an image as the basis for subsequent deltas.

                    let mut dir = AuxFilesDirectory {
                        files: HashMap::new(),
                    };
                    dir.upsert(file_path, content);
                    self.put(
                        AUX_FILES_KEY,
                        Value::Image(Bytes::from(
                            AuxFilesDirectory::ser(&dir).context("serialize")?,
                        )),
                    );
                    n_files = 1;
                    aux_files.dir = Some(dir);
                }
            }
        }

        self.pending_directory_entries
            .push((DirectoryKind::AuxFiles, n_files));

        Ok(())
    }

    ///
    /// Flush changes accumulated so far to the underlying repository.
    ///
    /// Usually, changes made in DatadirModification are atomic, but this allows
    /// you to flush them to the underlying repository before the final `commit`.
    /// That allows to free up the memory used to hold the pending changes.
    ///
    /// Currently only used during bulk import of a data directory. In that
    /// context, breaking the atomicity is OK. If the import is interrupted, the
    /// whole import fails and the timeline will be deleted anyway.
    /// (Or to be precise, it will be left behind for debugging purposes and
    /// ignored, see <https://github.com/neondatabase/neon/pull/1809>)
    ///
    /// Note: A consequence of flushing the pending operations is that they
    /// won't be visible to subsequent operations until `commit`. The function
    /// retains all the metadata, but data pages are flushed. That's again OK
    /// for bulk import, where you are just loading data pages and won't try to
    /// modify the same pages twice.
    pub async fn flush(&mut self, ctx: &RequestContext) -> anyhow::Result<()> {
        // Unless we have accumulated a decent amount of changes, it's not worth it
        // to scan through the pending_updates list.
        let pending_nblocks = self.pending_nblocks;
        if pending_nblocks < 10000 {
            return Ok(());
        }

        let mut writer = self.tline.writer().await;

        // Flush relation and  SLRU data blocks, keep metadata.
        let mut retained_pending_updates = HashMap::<_, Vec<_>>::new();
        for (key, values) in self.pending_updates.drain() {
            for (lsn, value) in values {
                if is_rel_block_key(&key) || is_slru_block_key(key) {
                    // This bails out on first error without modifying pending_updates.
                    // That's Ok, cf this function's doc comment.
                    writer.put(key, lsn, &value, ctx).await?;
                } else {
                    retained_pending_updates
                        .entry(key)
                        .or_default()
                        .push((lsn, value));
                }
            }
        }

        self.pending_updates = retained_pending_updates;

        if pending_nblocks != 0 {
            writer.update_current_logical_size(pending_nblocks * i64::from(BLCKSZ));
            self.pending_nblocks = 0;
        }

        for (kind, count) in std::mem::take(&mut self.pending_directory_entries) {
            writer.update_directory_entries_count(kind, count as u64);
        }

        Ok(())
    }

    ///
    /// Finish this atomic update, writing all the updated keys to the
    /// underlying timeline.
    /// All the modifications in this atomic update are stamped by the specified LSN.
    ///
    pub async fn commit(&mut self, ctx: &RequestContext) -> anyhow::Result<()> {
        let mut writer = self.tline.writer().await;

        let pending_nblocks = self.pending_nblocks;
        self.pending_nblocks = 0;

        if !self.pending_updates.is_empty() {
            // The put_batch call below expects expects the inputs to be sorted by Lsn,
            // so we do that first.
            let lsn_ordered_batch: VecMap<Lsn, (Key, Value)> = VecMap::from_iter(
                self.pending_updates
                    .drain()
                    .map(|(key, vals)| vals.into_iter().map(move |(lsn, val)| (lsn, (key, val))))
                    .kmerge_by(|lhs, rhs| lhs.0 < rhs.0),
                VecMapOrdering::GreaterOrEqual,
            );

            writer.put_batch(lsn_ordered_batch, ctx).await?;
        }

        if !self.pending_deletions.is_empty() {
            writer.delete_batch(&self.pending_deletions).await?;
            self.pending_deletions.clear();
        }

        self.pending_lsns.push(self.lsn);
        for pending_lsn in self.pending_lsns.drain(..) {
            // Ideally, we should be able to call writer.finish_write() only once
            // with the highest LSN. However, the last_record_lsn variable in the
            // timeline keeps track of the latest LSN and the immediate previous LSN
            // so we need to record every LSN to not leave a gap between them.
            writer.finish_write(pending_lsn);
        }

        if pending_nblocks != 0 {
            writer.update_current_logical_size(pending_nblocks * i64::from(BLCKSZ));
        }

        for (kind, count) in std::mem::take(&mut self.pending_directory_entries) {
            writer.update_directory_entries_count(kind, count as u64);
        }

        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.pending_updates.len() + self.pending_deletions.len()
    }

    // Internal helper functions to batch the modifications

    async fn get(&self, key: Key, ctx: &RequestContext) -> Result<Bytes, PageReconstructError> {
        // Have we already updated the same key? Read the latest pending updated
        // version in that case.
        //
        // Note: we don't check pending_deletions. It is an error to request a
        // value that has been removed, deletion only avoids leaking storage.
        if let Some(values) = self.pending_updates.get(&key) {
            if let Some((_, value)) = values.last() {
                return if let Value::Image(img) = value {
                    Ok(img.clone())
                } else {
                    // Currently, we never need to read back a WAL record that we
                    // inserted in the same "transaction". All the metadata updates
                    // work directly with Images, and we never need to read actual
                    // data pages. We could handle this if we had to, by calling
                    // the walredo manager, but let's keep it simple for now.
                    Err(PageReconstructError::from(anyhow::anyhow!(
                        "unexpected pending WAL record"
                    )))
                };
            }
        }
        let lsn = Lsn::max(self.tline.get_last_record_lsn(), self.lsn);
        self.tline.get(key, lsn, ctx).await
    }

    fn put(&mut self, key: Key, val: Value) {
        let values = self.pending_updates.entry(key).or_default();
        // Replace the previous value if it exists at the same lsn
        if let Some((last_lsn, last_value)) = values.last_mut() {
            if *last_lsn == self.lsn {
                *last_value = val;
                return;
            }
        }
        values.push((self.lsn, val));
    }

    fn delete(&mut self, key_range: Range<Key>) {
        trace!("DELETE {}-{}", key_range.start, key_range.end);
        self.pending_deletions.push((key_range, self.lsn));
    }
}

/// This struct facilitates accessing either a committed key from the timeline at a
/// specific LSN, or the latest uncommitted key from a pending modification.
/// During WAL ingestion, the records from multiple LSNs may be batched in the same
/// modification before being flushed to the timeline. Hence, the routines in WalIngest
/// need to look up the keys in the modification first before looking them up in the
/// timeline to not miss the latest updates.
#[derive(Clone, Copy)]
pub enum Version<'a> {
    Lsn(Lsn),
    Modified(&'a DatadirModification<'a>),
}

impl<'a> Version<'a> {
    async fn get(
        &self,
        timeline: &Timeline,
        key: Key,
        ctx: &RequestContext,
    ) -> Result<Bytes, PageReconstructError> {
        match self {
            Version::Lsn(lsn) => timeline.get(key, *lsn, ctx).await,
            Version::Modified(modification) => modification.get(key, ctx).await,
        }
    }

    fn get_lsn(&self) -> Lsn {
        match self {
            Version::Lsn(lsn) => *lsn,
            Version::Modified(modification) => modification.lsn,
        }
    }
}

//--- Metadata structs stored in key-value pairs in the repository.

#[derive(Debug, Serialize, Deserialize)]
struct DbDirectory {
    // (spcnode, dbnode) -> (do relmapper and PG_VERSION files exist)
    dbdirs: HashMap<(Oid, Oid), bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TwoPhaseDirectory {
    xids: HashSet<TransactionId>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RelDirectory {
    // Set of relations that exist. (relfilenode, forknum)
    //
    // TODO: Store it as a btree or radix tree or something else that spans multiple
    // key-value pairs, if you have a lot of relations
    rels: HashSet<(Oid, u8)>,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub(crate) struct AuxFilesDirectory {
    pub(crate) files: HashMap<String, Bytes>,
}

impl AuxFilesDirectory {
    pub(crate) fn upsert(&mut self, key: String, value: Option<Bytes>) {
        if let Some(value) = value {
            self.files.insert(key, value);
        } else {
            self.files.remove(&key);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RelSizeEntry {
    nblocks: u32,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct SlruSegmentDirectory {
    // Set of SLRU segments that exist.
    segments: HashSet<u32>,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, enum_map::Enum)]
#[repr(u8)]
pub(crate) enum DirectoryKind {
    Db,
    TwoPhase,
    Rel,
    AuxFiles,
    SlruSegment(SlruKind),
}

impl DirectoryKind {
    pub(crate) const KINDS_NUM: usize = <DirectoryKind as Enum>::LENGTH;
    pub(crate) fn offset(&self) -> usize {
        self.into_usize()
    }
}

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; BLCKSZ as usize]);

#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use hex_literal::hex;
    use utils::id::TimelineId;

    use super::*;

    use crate::{tenant::harness::TenantHarness, DEFAULT_PG_VERSION};

    /// Test a round trip of aux file updates, from DatadirModification to reading back from the Timeline
    #[tokio::test]
    async fn aux_files_round_trip() -> anyhow::Result<()> {
        let name = "aux_files_round_trip";
        let harness = TenantHarness::create(name)?;

        pub const TIMELINE_ID: TimelineId =
            TimelineId::from_array(hex!("11223344556677881122334455667788"));

        let (tenant, ctx) = harness.load().await;
        let tline = tenant
            .create_empty_timeline(TIMELINE_ID, Lsn(0), DEFAULT_PG_VERSION, &ctx)
            .await?;
        let tline = tline.raw_timeline().unwrap();

        // First modification: insert two keys
        let mut modification = tline.begin_modification(Lsn(0x1000));
        modification.put_file("foo/bar1", b"content1", &ctx).await?;
        modification.set_lsn(Lsn(0x1008))?;
        modification.put_file("foo/bar2", b"content2", &ctx).await?;
        modification.commit(&ctx).await?;
        let expect_1008 = HashMap::from([
            ("foo/bar1".to_string(), Bytes::from_static(b"content1")),
            ("foo/bar2".to_string(), Bytes::from_static(b"content2")),
        ]);

        let readback = tline.list_aux_files(Lsn(0x1008), &ctx).await?;
        assert_eq!(readback, expect_1008);

        // Second modification: update one key, remove the other
        let mut modification = tline.begin_modification(Lsn(0x2000));
        modification.put_file("foo/bar1", b"content3", &ctx).await?;
        modification.set_lsn(Lsn(0x2008))?;
        modification.put_file("foo/bar2", b"", &ctx).await?;
        modification.commit(&ctx).await?;
        let expect_2008 =
            HashMap::from([("foo/bar1".to_string(), Bytes::from_static(b"content3"))]);

        let readback = tline.list_aux_files(Lsn(0x2008), &ctx).await?;
        assert_eq!(readback, expect_2008);

        // Reading back in time works
        let readback = tline.list_aux_files(Lsn(0x1008), &ctx).await?;
        assert_eq!(readback, expect_1008);

        Ok(())
    }

    /*
        fn assert_current_logical_size<R: Repository>(timeline: &DatadirTimeline<R>, lsn: Lsn) {
            let incremental = timeline.get_current_logical_size();
            let non_incremental = timeline
                .get_current_logical_size_non_incremental(lsn)
                .unwrap();
            assert_eq!(incremental, non_incremental);
        }
    */

    /*
    ///
    /// Test list_rels() function, with branches and dropped relations
    ///
    #[test]
    fn test_list_rels_drop() -> Result<()> {
        let repo = RepoHarness::create("test_list_rels_drop")?.load();
        let tline = create_empty_timeline(repo, TIMELINE_ID)?;
        const TESTDB: u32 = 111;

        // Import initial dummy checkpoint record, otherwise the get_timeline() call
        // after branching fails below
        let mut writer = tline.begin_record(Lsn(0x10));
        writer.put_checkpoint(ZERO_CHECKPOINT.clone())?;
        writer.finish()?;

        // Create a relation on the timeline
        let mut writer = tline.begin_record(Lsn(0x20));
        writer.put_rel_page_image(TESTREL_A, 0, TEST_IMG("foo blk 0 at 2"))?;
        writer.finish()?;

        let writer = tline.begin_record(Lsn(0x00));
        writer.finish()?;

        // Check that list_rels() lists it after LSN 2, but no before it
        assert!(!tline.list_rels(0, TESTDB, Lsn(0x10))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x20))?.contains(&TESTREL_A));
        assert!(tline.list_rels(0, TESTDB, Lsn(0x30))?.contains(&TESTREL_A));

        // Create a branch, check that the relation is visible there
        repo.branch_timeline(&tline, NEW_TIMELINE_ID, Lsn(0x30))?;
        let newtline = match repo.get_timeline(NEW_TIMELINE_ID)?.local_timeline() {
            Some(timeline) => timeline,
            None => panic!("Should have a local timeline"),
        };
        let newtline = DatadirTimelineImpl::new(newtline);
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));

        // Drop it on the branch
        let mut new_writer = newtline.begin_record(Lsn(0x40));
        new_writer.drop_relation(TESTREL_A)?;
        new_writer.finish()?;

        // Check that it's no longer listed on the branch after the point where it was dropped
        assert!(newtline
            .list_rels(0, TESTDB, Lsn(0x30))?
            .contains(&TESTREL_A));
        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        // Run checkpoint and garbage collection and check that it's still not visible
        newtline.checkpoint(CheckpointConfig::Forced)?;
        repo.gc_iteration(Some(NEW_TIMELINE_ID), 0, true)?;

        assert!(!newtline
            .list_rels(0, TESTDB, Lsn(0x40))?
            .contains(&TESTREL_A));

        Ok(())
    }
     */

    /*
    #[test]
    fn test_read_beyond_eof() -> Result<()> {
        let repo = RepoHarness::create("test_read_beyond_eof")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;

        make_some_layers(&tline, Lsn(0x20))?;
        let mut writer = tline.begin_record(Lsn(0x60));
        walingest.put_rel_page_image(
            &mut writer,
            TESTREL_A,
            0,
            TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x60))),
        )?;
        writer.finish()?;

        // Test read before rel creation. Should error out.
        assert!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x10), false).is_err());

        // Read block beyond end of relation at different points in time.
        // These reads should fall into different delta, image, and in-memory layers.
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x20), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x25), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x30), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x35), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x40), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x45), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x50), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x55), false)?, ZERO_PAGE);
        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x60), false)?, ZERO_PAGE);

        // Test on an in-memory layer with no preceding layer
        let mut writer = tline.begin_record(Lsn(0x70));
        walingest.put_rel_page_image(
            &mut writer,
            TESTREL_B,
            0,
            TEST_IMG(&format!("foo blk 0 at {}", Lsn(0x70))),
        )?;
        writer.finish()?;

        assert_eq!(tline.get_rel_page_at_lsn(TESTREL_B, 1, Lsn(0x70), false)?6, ZERO_PAGE);

        Ok(())
    }
     */
}
