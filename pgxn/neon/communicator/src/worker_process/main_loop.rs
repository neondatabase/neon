use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::str::FromStr as _;

use crate::backend_comms::NeonIORequestSlot;
use crate::file_cache::FileCache;
use crate::global_allocator::MyAllocatorCollector;
use crate::init::CommunicatorInitStruct;
use crate::integrated_cache::{CacheResult, IntegratedCacheWriteAccess};
use crate::neon_request::{CGetPageVRequest, CPrefetchVRequest};
use crate::neon_request::{NeonIORequest, NeonIOResult};
use crate::worker_process::in_progress_ios::{RequestInProgressKey, RequestInProgressTable};
use pageserver_client_grpc::{PageserverClient, ShardSpec, ShardStripeSize};
use pageserver_page_api as page_api;

use metrics::{IntCounter, IntCounterVec};

use tokio::io::AsyncReadExt;
use tokio_pipe::PipeRead;
use uring_common::buf::IoBuf;
use utils::id::{TenantId, TimelineId};

use super::callbacks::{get_request_lsn, notify_proc};

use tracing::{error, info, info_span, trace};

use utils::lsn::Lsn;

pub struct CommunicatorWorkerProcessStruct<'a> {
    /// Tokio runtime that the main loop and any other related tasks runs in.
    runtime: tokio::runtime::Handle,

    /// Client to communicate with the pageserver
    client: PageserverClient,

    /// Request slots that backends use to send IO requests to the communicator.
    neon_request_slots: &'a [NeonIORequestSlot],

    /// Notification pipe. Backends use this to notify the communicator that a request is waiting to
    /// be processed in one of the request slots.
    submission_pipe_read_fd: OwnedFd,

    /// Locking table for all in-progress IO requests.
    in_progress_table: RequestInProgressTable,

    /// Local File Cache, relation size tracking, last-written LSN tracking
    pub(crate) cache: IntegratedCacheWriteAccess<'a>,

    /*** Static configuration ***/
    /// Stripe size doesn't change after startup. (The shard map is not stored here, it's passed
    /// directly to the client)
    stripe_size: Option<ShardStripeSize>,

    /*** Metrics ***/
    request_counters: IntCounterVec,
    request_rel_exists_counter: IntCounter,
    request_rel_size_counter: IntCounter,
    request_get_pagev_counter: IntCounter,
    request_read_slru_segment_counter: IntCounter,
    request_prefetchv_counter: IntCounter,
    request_db_size_counter: IntCounter,
    request_write_page_counter: IntCounter,
    request_rel_extend_counter: IntCounter,
    request_rel_zero_extend_counter: IntCounter,
    request_rel_create_counter: IntCounter,
    request_rel_truncate_counter: IntCounter,
    request_rel_unlink_counter: IntCounter,

    getpage_cache_misses_counter: IntCounter,
    getpage_cache_hits_counter: IntCounter,

    request_nblocks_counters: IntCounterVec,
    request_get_pagev_nblocks_counter: IntCounter,
    request_prefetchv_nblocks_counter: IntCounter,
    request_rel_zero_extend_nblocks_counter: IntCounter,

    allocator_metrics: MyAllocatorCollector,
}

pub(super) async fn init(
    cis: Box<CommunicatorInitStruct>,
    tenant_id: String,
    timeline_id: String,
    auth_token: Option<String>,
    shard_map: HashMap<utils::shard::ShardIndex, String>,
    stripe_size: Option<ShardStripeSize>,
    initial_file_cache_size: u64,
    file_cache_path: Option<PathBuf>,
) -> CommunicatorWorkerProcessStruct<'static> {
    info!("Test log message");
    let last_lsn = get_request_lsn();

    let file_cache = if let Some(path) = file_cache_path {
        Some(FileCache::new(&path, initial_file_cache_size).expect("could not create cache file"))
    } else {
        // FIXME: temporarily for testing, use LFC even if disabled
        Some(
            FileCache::new(&PathBuf::from("new_filecache"), 1000)
                .expect("could not create cache file"),
        )
    };

    // Initialize subsystems
    let cache = cis
        .integrated_cache_init_struct
        .worker_process_init(last_lsn, file_cache);

    info!("Initialised integrated cache: {cache:?}");

    // TODO: plumb through the stripe size.
    let tenant_id = TenantId::from_str(&tenant_id).expect("invalid tenant ID");
    let timeline_id = TimelineId::from_str(&timeline_id).expect("invalid timeline ID");
    let shard_spec = ShardSpec::new(shard_map, stripe_size).expect("invalid shard spec");
    let client = PageserverClient::new(tenant_id, timeline_id, shard_spec, auth_token, None)
        .expect("could not create client");

    let request_counters = IntCounterVec::new(
        metrics::core::Opts::new(
            "backend_requests_total",
            "Number of requests from backends.",
        ),
        &["request_kind"],
    )
    .unwrap();
    let request_rel_exists_counter = request_counters.with_label_values(&["rel_exists"]);
    let request_rel_size_counter = request_counters.with_label_values(&["rel_size"]);
    let request_get_pagev_counter = request_counters.with_label_values(&["get_pagev"]);
    let request_read_slru_segment_counter =
        request_counters.with_label_values(&["read_slru_segment"]);
    let request_prefetchv_counter = request_counters.with_label_values(&["prefetchv"]);
    let request_db_size_counter = request_counters.with_label_values(&["db_size"]);
    let request_write_page_counter = request_counters.with_label_values(&["write_page"]);
    let request_rel_extend_counter = request_counters.with_label_values(&["rel_extend"]);
    let request_rel_zero_extend_counter = request_counters.with_label_values(&["rel_zero_extend"]);
    let request_rel_create_counter = request_counters.with_label_values(&["rel_create"]);
    let request_rel_truncate_counter = request_counters.with_label_values(&["rel_truncate"]);
    let request_rel_unlink_counter = request_counters.with_label_values(&["rel_unlink"]);

    let getpage_cache_misses_counter = IntCounter::new(
        "getpage_cache_misses",
        "Number of file cache misses in get_pagev requests.",
    )
    .unwrap();
    let getpage_cache_hits_counter = IntCounter::new(
        "getpage_cache_hits",
        "Number of file cache hits in get_pagev requests.",
    )
    .unwrap();

    // For the requests that affect multiple blocks, have separate counters for the # of blocks affected
    let request_nblocks_counters = IntCounterVec::new(
        metrics::core::Opts::new(
            "request_nblocks_total",
            "Number of blocks in backend requests.",
        ),
        &["request_kind"],
    )
    .unwrap();
    let request_get_pagev_nblocks_counter =
        request_nblocks_counters.with_label_values(&["get_pagev"]);
    let request_prefetchv_nblocks_counter =
        request_nblocks_counters.with_label_values(&["prefetchv"]);
    let request_rel_zero_extend_nblocks_counter =
        request_nblocks_counters.with_label_values(&["rel_zero_extend"]);

    CommunicatorWorkerProcessStruct {
        runtime: tokio::runtime::Handle::current(),
        stripe_size,
        neon_request_slots: cis.neon_request_slots,
        client,
        cache,
        submission_pipe_read_fd: cis.submission_pipe_read_fd,
        in_progress_table: RequestInProgressTable::new(),

        // metrics
        request_counters,
        request_rel_exists_counter,
        request_rel_size_counter,
        request_get_pagev_counter,
        request_read_slru_segment_counter,
        request_prefetchv_counter,
        request_db_size_counter,
        request_write_page_counter,
        request_rel_extend_counter,
        request_rel_zero_extend_counter,
        request_rel_create_counter,
        request_rel_truncate_counter,
        request_rel_unlink_counter,

        getpage_cache_misses_counter,
        getpage_cache_hits_counter,

        request_nblocks_counters,
        request_get_pagev_nblocks_counter,
        request_prefetchv_nblocks_counter,
        request_rel_zero_extend_nblocks_counter,

        allocator_metrics: MyAllocatorCollector::new(),
    }
}

impl<'t> CommunicatorWorkerProcessStruct<'t> {
    /// Update the configuration
    pub(super) fn update_shard_map(
        &self,
        new_shard_map: HashMap<utils::shard::ShardIndex, String>,
    ) {
        let shard_spec =
            ShardSpec::new(new_shard_map, self.stripe_size.clone()).expect("invalid shard spec");

        {
            let _in_runtime = self.runtime.enter();
            if let Err(err) = self.client.update_shards(shard_spec) {
                tracing::error!("could not update shard map: {err:?}");
            }
        }
    }

    /// Main loop of the worker process. Receive requests from the backends and process them.
    pub(super) async fn run(&'static self) {
        let mut idxbuf: [u8; 4] = [0; 4];

        let mut submission_pipe_read =
            PipeRead::try_from(self.submission_pipe_read_fd.as_raw_fd()).expect("invalid pipe fd");

        loop {
            // Wait for a backend to ring the doorbell
            match submission_pipe_read.read(&mut idxbuf).await {
                Ok(4) => {}
                Ok(nbytes) => panic!("short read ({nbytes} bytes) on communicator pipe"),
                Err(e) => panic!("error reading from communicator pipe: {e}"),
            }
            let slot_idx = u32::from_ne_bytes(idxbuf) as usize;

            // Read the IO request from the slot indicated in the wakeup
            let Some(slot) = self.neon_request_slots[slot_idx].start_processing_request() else {
                // This currently should not happen. But if we had multiple threads picking up
                // requests, and without waiting for the notifications, it could.
                panic!("no request in slot");
            };

            // Ok, we have ownership of this request now. We must process it now, there's no going
            // back.
            //
            // Spawn a separate task for every request. That's a little excessive for requests that
            // can be quickly satisfied from the cache, but we expect that to be rare, because the
            // requesting backend would have already checked the cache.
            tokio::spawn(async move {
                use tracing::Instrument;

                let request_id = slot.get_request().request_id();
                let owner_procno = slot.get_owner_procno();

                let span = info_span!(
                    "processing",
                    request_id = request_id,
                    slot_idx = slot_idx,
                    procno = owner_procno,
                );
                async {
                    // FIXME: as a temporary hack, abort the request if we don't get a response
                    // promptly.
                    //
                    // Lots of regression tests are getting stuck and failing at the moment,
                    // this makes them fail a little faster, which it faster to iterate.
                    // This needs to be removed once more regression tests are passing.
                    // See also similar hack in the backend code, in wait_request_completion()
                    let result = tokio::time::timeout(
                        tokio::time::Duration::from_secs(30),
                        self.handle_request(slot.get_request()),
                    )
                    .await
                    .unwrap_or_else(|_elapsed| {
                        info!("request {request_id} timed out");
                        NeonIOResult::Error(libc::ETIMEDOUT)
                    });
                    trace!("request {request_id} at slot {slot_idx} completed");

                    // Ok, we have completed the IO. Mark the request as completed. After that,
                    // we no longer have ownership of the slot, and must not modify it.
                    slot.completed(result);

                    // Notify the backend about the completion. (Note that the backend might see
                    // the completed status even before this; this is just a wakeup)
                    notify_proc(owner_procno);
                }
                .instrument(span)
                .await
            });
        }
    }

    /// Compute the 'request_lsn' to use for a pageserver request
    fn request_lsns(&self, not_modified_since_lsn: Lsn) -> page_api::ReadLsn {
        let mut request_lsn = get_request_lsn();

        // Is it possible that the last-written LSN is ahead of last flush LSN? Generally not, we
        // shouldn't evict a page from the buffer cache before all its modifications have been
        // safely flushed. That's the "WAL before data" rule. However, there are a few exceptions:
        //
        // - when creation an index: _bt_blwritepage logs the full page without flushing WAL before
        // smgrextend (files are fsynced before build ends).
        //
        // XXX: If we make a request LSN greater than the current WAL flush LSN, the pageserver would
        // block waiting for the WAL arrive, until we flush it and it propagates through the
        // safekeepers to the pageserver. If there's nothing that forces the WAL to be flushed,
        // the pageserver would get stuck waiting forever. To avoid that, all the write-
        // functions in communicator_new.c call XLogSetAsyncXactLSN(). That nudges the WAL writer to
        // perform the flush relatively soon.
        //
        // It would perhaps be nicer to do the WAL flush here, but it's tricky to call back into
        // Postgres code to do that from here. That's why we rely on communicator_new.c to do the
        // calls "pre-emptively".
        //
        // FIXME: Because of the above, it can still happen that the flush LSN is ahead of
        // not_modified_since, if the WAL writer hasn't done the flush yet. It would be nice to know
        // if there are other cases like that that we have mised, but unfortunately we cannot turn
        // this into an assertion because of that legit case.
        //
        // See also the old logic in neon_get_request_lsns() C function
        if not_modified_since_lsn > request_lsn {
            tracing::info!(
                "not_modified_since_lsn {} is ahead of last flushed LSN {}",
                not_modified_since_lsn,
                request_lsn
            );
            request_lsn = not_modified_since_lsn;
        }

        page_api::ReadLsn {
            request_lsn,
            not_modified_since_lsn: Some(not_modified_since_lsn),
        }
    }

    /// Handle one IO request
    async fn handle_request(&'static self, req: &'_ NeonIORequest) -> NeonIOResult {
        match req {
            NeonIORequest::Empty => {
                error!("unexpected Empty IO request");
                NeonIOResult::Error(0)
            }
            NeonIORequest::RelExists(req) => {
                self.request_rel_exists_counter.inc();
                let rel = req.reltag();

                let _in_progress_guard = self
                    .in_progress_table
                    .lock(RequestInProgressKey::Rel(rel), req.request_id)
                    .await;

                // Check the cache first
                let not_modified_since = match self.cache.get_rel_exists(&rel) {
                    CacheResult::Found(exists) => return NeonIOResult::RelExists(exists),
                    CacheResult::NotFound(lsn) => lsn,
                };

                match self
                    .client
                    .check_rel_exists(page_api::CheckRelExistsRequest {
                        read_lsn: self.request_lsns(not_modified_since),
                        rel,
                    })
                    .await
                {
                    Ok(exists) => NeonIOResult::RelExists(exists),
                    Err(err) => {
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(0)
                    }
                }
            }

            NeonIORequest::RelSize(req) => {
                self.request_rel_size_counter.inc();
                let rel = req.reltag();

                let _in_progress_guard = self
                    .in_progress_table
                    .lock(RequestInProgressKey::Rel(rel), req.request_id)
                    .await;

                // Check the cache first
                let not_modified_since = match self.cache.get_rel_size(&rel) {
                    CacheResult::Found(nblocks) => {
                        tracing::trace!("found relsize for {:?} in cache: {}", rel, nblocks);
                        return NeonIOResult::RelSize(nblocks);
                    }
                    CacheResult::NotFound(lsn) => lsn,
                };

                let read_lsn = self.request_lsns(not_modified_since);
                match self
                    .client
                    .get_rel_size(page_api::GetRelSizeRequest { read_lsn, rel })
                    .await
                {
                    Ok(nblocks) => {
                        // update the cache
                        tracing::info!(
                            "updated relsize for {:?} in cache: {}, lsn {}",
                            rel,
                            nblocks,
                            read_lsn
                        );
                        self.cache
                            .remember_rel_size(&rel, nblocks, not_modified_since);

                        NeonIOResult::RelSize(nblocks)
                    }
                    Err(err) => {
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(0)
                    }
                }
            }
            NeonIORequest::GetPageV(req) => {
                self.request_get_pagev_counter.inc();
                self.request_get_pagev_nblocks_counter
                    .inc_by(req.nblocks as u64);
                match self.handle_get_pagev_request(req).await {
                    Ok(()) => NeonIOResult::GetPageV,
                    Err(errno) => NeonIOResult::Error(errno),
                }
            }
            NeonIORequest::ReadSlruSegment(req) => {
                self.request_read_slru_segment_counter.inc();
                let lsn = self.cache.get_lsn();

                match self
                    .client
                    .get_slru_segment(page_api::GetSlruSegmentRequest {
                        read_lsn: self.request_lsns(lsn),
                        kind: req.slru_kind,
                        segno: req.segment_number,
                    })
                    .await
                {
                    Ok(slru_bytes) => {
                        let src: &[u8] = &slru_bytes.as_ref();
                        let dest = req.dest;
                        let len = std::cmp::min(src.len(), dest.bytes_total());

                        unsafe {
                            std::ptr::copy_nonoverlapping(src.as_ptr(), dest.as_mut_ptr(), len);
                        };

                        let blocks_count = len / (crate::BLCKSZ * crate::SLRU_PAGES_PER_SEGMENT);

                        NeonIOResult::ReadSlruSegment(blocks_count as _)
                    }
                    Err(err) => {
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(0)
                    }
                }
            }
            NeonIORequest::PrefetchV(req) => {
                self.request_prefetchv_counter.inc();
                self.request_prefetchv_nblocks_counter
                    .inc_by(req.nblocks as u64);
                let req = *req;
                tokio::spawn(async move { self.handle_prefetchv_request(&req).await });
                NeonIOResult::PrefetchVLaunched
            }
            NeonIORequest::DbSize(req) => {
                self.request_db_size_counter.inc();
                let _in_progress_guard = self
                    .in_progress_table
                    .lock(RequestInProgressKey::Db(req.db_oid), req.request_id)
                    .await;

                // Check the cache first
                let not_modified_since = match self.cache.get_db_size(req.db_oid) {
                    CacheResult::Found(db_size) => {
                        // get_page already copied the block content to the destination
                        return NeonIOResult::DbSize(db_size);
                    }
                    CacheResult::NotFound(lsn) => lsn,
                };

                match self
                    .client
                    .get_db_size(page_api::GetDbSizeRequest {
                        read_lsn: self.request_lsns(not_modified_since),
                        db_oid: req.db_oid,
                    })
                    .await
                {
                    Ok(db_size) => NeonIOResult::DbSize(db_size),
                    Err(err) => {
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(0)
                    }
                }
            }

            // Write requests
            NeonIORequest::WritePage(req) => {
                self.request_write_page_counter.inc();

                let rel = req.reltag();
                let _in_progress_guard = self
                    .in_progress_table
                    .lock(
                        RequestInProgressKey::Block(rel, req.block_number),
                        req.request_id,
                    )
                    .await;

                // We must at least update the last-written LSN on the page, but also store the page
                // image in the LFC while we still have it
                self.cache
                    .remember_page(&rel, req.block_number, req.src, Lsn(req.lsn), true)
                    .await;
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelExtend(req) => {
                self.request_rel_extend_counter.inc();

                let rel = req.reltag();
                let _in_progress_guard = self
                    .in_progress_table
                    .lock(
                        RequestInProgressKey::Block(rel, req.block_number),
                        req.request_id,
                    )
                    .await;

                // We must at least update the last-written LSN on the page and the relation size,
                // but also store the page image in the LFC while we still have it
                self.cache
                    .remember_page(&rel, req.block_number, req.src, Lsn(req.lsn), true)
                    .await;
                self.cache
                    .remember_rel_size(&req.reltag(), req.block_number + 1, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelZeroExtend(req) => {
                self.request_rel_zero_extend_counter.inc();
                self.request_rel_zero_extend_nblocks_counter
                    .inc_by(req.nblocks as u64);

                // TODO: need to grab an io-in-progress lock for this? I guess not
                // TODO: We could put the empty pages to the cache. Maybe have
                // a marker on the block entries for all-zero pages, instead of
                // actually storing the empty pages.
                self.cache.remember_rel_size(
                    &req.reltag(),
                    req.block_number + req.nblocks,
                    Lsn(req.lsn),
                );
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelCreate(req) => {
                self.request_rel_create_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache.remember_rel_size(&req.reltag(), 0, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelTruncate(req) => {
                self.request_rel_truncate_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache
                    .remember_rel_size(&req.reltag(), req.nblocks, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelUnlink(req) => {
                self.request_rel_unlink_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache.forget_rel(&req.reltag(), None, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
            NeonIORequest::UpdateCachedRelSize(req) => {
                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache
                    .remember_rel_size(&req.reltag(), req.nblocks, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
        }
    }

    /// Subroutine to handle a GetPageV request, since it's a little more complicated than
    /// others.
    async fn handle_get_pagev_request(&'t self, req: &CGetPageVRequest) -> Result<(), i32> {
        let rel = req.reltag();

        // Check the cache first
        //
        // Note: Because the backends perform a direct lookup in the cache before sending
        // the request to the communicator process, we expect the pages to almost never
        // be already in cache. It could happen if:
        // 1. two backends try to read the same page at the same time, but that should never
        //    happen because there's higher level locking in the Postgres buffer manager, or
        // 2. a prefetch request finished at the same time as a backend requested the
        //    page. That's much more likely.
        let mut cache_misses = Vec::with_capacity(req.nblocks as usize);
        for i in 0..req.nblocks {
            let blkno = req.block_number + i as u32;

            // note: this is deadlock-safe even though we hold multiple locks at the same time,
            // because they're always acquired in the same order.
            let in_progress_guard = self
                .in_progress_table
                .lock(RequestInProgressKey::Block(rel, blkno), req.request_id)
                .await;

            let dest = req.dest[i as usize];
            let not_modified_since = match self.cache.get_page(&rel, blkno, dest).await {
                Ok(CacheResult::Found(_)) => {
                    // get_page already copied the block content to the destination
                    trace!("found blk {} in rel {:?} in LFC", blkno, rel);
                    continue;
                }
                Ok(CacheResult::NotFound(lsn)) => lsn,
                Err(_io_error) => return Err(-1), // FIXME errno?
            };
            cache_misses.push((blkno, not_modified_since, dest, in_progress_guard));
        }
        self.getpage_cache_misses_counter
            .inc_by(cache_misses.len() as u64);
        self.getpage_cache_hits_counter
            .inc_by(req.nblocks as u64 - cache_misses.len() as u64);

        if cache_misses.is_empty() {
            return Ok(());
        }
        let not_modified_since = cache_misses
            .iter()
            .map(|(_blkno, lsn, _dest, _guard)| *lsn)
            .max()
            .unwrap();

        // Construct a pageserver request for the cache misses
        let block_numbers: Vec<u32> = cache_misses
            .iter()
            .map(|(blkno, _lsn, _dest, _guard)| *blkno)
            .collect();
        let read_lsn = self.request_lsns(not_modified_since);
        info!(
            "sending getpage request for blocks {:?} in rel {:?} lsns {}",
            block_numbers, rel, read_lsn
        );
        match self
            .client
            .get_page(page_api::GetPageRequest {
                request_id: req.request_id.into(),
                request_class: page_api::GetPageClass::Normal,
                read_lsn,
                rel,
                block_numbers: block_numbers.clone(),
            })
            .await
        {
            Ok(resp) => {
                // Write the received page images directly to the shared memory location
                // that the backend requested.
                if resp.pages.len() != block_numbers.len() {
                    error!(
                        "received unexpected response with {} page images from pageserver for a request for {} pages",
                        resp.pages.len(),
                        block_numbers.len(),
                    );
                    return Err(-1);
                }

                info!(
                    "received getpage response for blocks {:?} in rel {:?} lsns {}",
                    block_numbers, rel, read_lsn
                );

                for (page, (blkno, _lsn, dest, _guard)) in resp.pages.into_iter().zip(cache_misses)
                {
                    let src: &[u8] = page.image.as_ref();
                    let len = std::cmp::min(src.len(), dest.bytes_total());
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dest.as_mut_ptr(), len);
                    };

                    // Also store it in the LFC while we have it
                    self.cache
                        .remember_page(
                            &rel,
                            blkno,
                            page.image,
                            read_lsn.not_modified_since_lsn.unwrap(),
                            false,
                        )
                        .await;
                }
            }
            Err(err) => {
                info!("tonic error: {err:?}");
                return Err(-1);
            }
        }
        Ok(())
    }

    /// Subroutine to handle a PrefetchV request, since it's a little more complicated than
    /// others.
    ///
    /// This is very similar to a GetPageV request, but the results are only stored in the cache.
    async fn handle_prefetchv_request(&'static self, req: &CPrefetchVRequest) -> Result<(), i32> {
        let rel = req.reltag();

        // Check the cache first
        let mut cache_misses = Vec::with_capacity(req.nblocks as usize);
        for i in 0..req.nblocks {
            let blkno = req.block_number + i as u32;

            // note: this is deadlock-safe even though we hold multiple locks at the same time,
            // because they're always acquired in the same order.
            let in_progress_guard = self
                .in_progress_table
                .lock(RequestInProgressKey::Block(rel, blkno), req.request_id)
                .await;

            let not_modified_since = match self.cache.page_is_cached(&rel, blkno).await {
                Ok(CacheResult::Found(_)) => {
                    trace!("found blk {} in rel {:?} in LFC", blkno, rel);
                    continue;
                }
                Ok(CacheResult::NotFound(lsn)) => lsn,
                Err(_io_error) => return Err(-1), // FIXME errno?
            };
            cache_misses.push((blkno, not_modified_since, in_progress_guard));
        }
        if cache_misses.is_empty() {
            return Ok(());
        }
        let not_modified_since = cache_misses
            .iter()
            .map(|(_blkno, lsn, _guard)| *lsn)
            .max()
            .unwrap();
        let block_numbers: Vec<u32> = cache_misses
            .iter()
            .map(|(blkno, _lsn, _guard)| *blkno)
            .collect();

        // TODO: spawn separate tasks for these. Use the integrated cache to keep track of the
        // in-flight requests

        match self
            .client
            .get_page(page_api::GetPageRequest {
                request_id: req.request_id.into(),
                request_class: page_api::GetPageClass::Prefetch,
                read_lsn: self.request_lsns(not_modified_since),
                rel,
                block_numbers: block_numbers.clone(),
            })
            .await
        {
            Ok(resp) => {
                trace!(
                    "prefetch completed, remembering blocks {:?} in rel {:?} in LFC",
                    block_numbers, rel
                );
                if resp.pages.len() != block_numbers.len() {
                    error!(
                        "received unexpected response with {} page images from pageserver for a request for {} pages",
                        resp.pages.len(),
                        block_numbers.len(),
                    );
                    return Err(-1);
                }

                for (page, (blkno, _lsn, _guard)) in resp.pages.into_iter().zip(cache_misses) {
                    self.cache
                        .remember_page(&rel, blkno, page.image, not_modified_since, false)
                        .await;
                }
            }
            Err(err) => {
                info!("tonic error: {err:?}");
                return Err(-1);
            }
        }
        Ok(())
    }
}

impl<'t> metrics::core::Collector for CommunicatorWorkerProcessStruct<'t> {
    fn desc(&self) -> Vec<&metrics::core::Desc> {
        let mut descs = Vec::new();

        descs.append(&mut self.request_counters.desc());
        descs.append(&mut self.getpage_cache_misses_counter.desc());
        descs.append(&mut self.getpage_cache_hits_counter.desc());
        descs.append(&mut self.request_nblocks_counters.desc());

        if let Some(file_cache) = &self.cache.file_cache {
            descs.append(&mut file_cache.desc());
        }
        descs.append(&mut self.cache.desc());
        descs.append(&mut self.allocator_metrics.desc());

        descs
    }
    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        let mut values = Vec::new();

        values.append(&mut self.request_counters.collect());
        values.append(&mut self.getpage_cache_misses_counter.collect());
        values.append(&mut self.getpage_cache_hits_counter.collect());
        values.append(&mut self.request_nblocks_counters.collect());

        if let Some(file_cache) = &self.cache.file_cache {
            values.append(&mut file_cache.collect());
        }
        values.append(&mut self.cache.collect());
        values.append(&mut self.allocator_metrics.collect());

        values
    }
}
