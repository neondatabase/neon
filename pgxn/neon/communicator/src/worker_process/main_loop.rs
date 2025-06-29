use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend_comms::NeonIOHandle;
use crate::file_cache::FileCache;
use crate::global_allocator::MyAllocatorCollector;
use crate::init::CommunicatorInitStruct;
use crate::integrated_cache::{CacheResult, IntegratedCacheWriteAccess};
use crate::neon_request::{CGetPageVRequest, CPrefetchVRequest};
use crate::neon_request::{NeonIORequest, NeonIOResult};
use crate::worker_process::in_progress_ios::{RequestInProgressKey, RequestInProgressTable};
use pageserver_client_grpc::request_tracker::ShardedRequestTracker;
use pageserver_page_api as page_api;

use metrics::{IntCounter, IntCounterVec};

use tokio::io::AsyncReadExt;
use tokio_pipe::PipeRead;
use uring_common::buf::IoBuf;

use super::callbacks::{get_request_lsn, notify_proc};

use tracing::{error, info, trace};

use utils::lsn::Lsn;

pub struct CommunicatorWorkerProcessStruct<'a> {
    neon_request_slots: &'a [NeonIOHandle],

    request_tracker: ShardedRequestTracker,

    pub(crate) cache: IntegratedCacheWriteAccess<'a>,

    submission_pipe_read_fd: OwnedFd,

    next_request_id: AtomicU64,

    in_progress_table: RequestInProgressTable,

    // Metrics
    request_counters: IntCounterVec,
    request_rel_exists_counter: IntCounter,
    request_rel_size_counter: IntCounter,
    request_get_pagev_counter: IntCounter,
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

    let request_tracker = ShardedRequestTracker::new();
    request_tracker
        .update_shard_map(
            shard_map,
            None,
            tenant_id,
            timeline_id,
            auth_token.as_deref(),
        )
        .await;

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
        neon_request_slots: cis.neon_request_slots,
        request_tracker,
        cache,
        submission_pipe_read_fd: cis.submission_pipe_read_fd,
        next_request_id: AtomicU64::new(1),
        in_progress_table: RequestInProgressTable::new(),

        // metrics
        request_counters,
        request_rel_exists_counter,
        request_rel_size_counter,
        request_get_pagev_counter,
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
    /// Main loop of the worker process. Receive requests from the backends and process them.
    pub(super) async fn run(self: &'static Self) {
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
            let request_idx = u32::from_ne_bytes(idxbuf);

            // Read the IO request from the slot indicated in the wakeup
            let Some(slot) =
                self.neon_request_slots[request_idx as usize].start_processing_request()
            else {
                // This currently should not happen. But if we have multiple threads picking up
                // requests, and without waiting for the notifications, it could.
                panic!("no request in slot");
            };

            // Ok, we have ownership of this request now. We must process
            // it now, there's no going back.

            //trace!("processing request {request_idx}: {request:?}");

            // Spawn a separate task for every request. That's a little excessive for requests that
            // can be quickly satisfied from the cache, but we expect that to be rare, because the
            // requesting backend would have already checked the cache.
            tokio::spawn(async {
                let result = self.handle_request(slot.get_request()).await;
                let owner_procno = slot.get_owner_procno();

                // Ok, we have completed the IO. Mark the request as completed. After that,
                // we no longer have ownership of the slot, and must not modify it.
                slot.completed(result);

                // Notify the backend about the completion. (Note that the backend might see
                // the completed status even before this; this is just a wakeup)
                notify_proc(owner_procno);
            });
        }
    }

    fn request_lsns(&self, not_modified_since_lsn: Lsn) -> page_api::ReadLsn {
        page_api::ReadLsn {
            request_lsn: get_request_lsn(),
            not_modified_since_lsn: Some(not_modified_since_lsn),
        }
    }

    async fn handle_request<'x>(self: &'static Self, req: &'x NeonIORequest) -> NeonIOResult {
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
                    .lock(RequestInProgressKey::Rel(rel.clone()));

                let not_modified_since = match self.cache.get_rel_exists(&rel) {
                    CacheResult::Found(exists) => return NeonIOResult::RelExists(exists),
                    CacheResult::NotFound(lsn) => lsn,
                };

                match self
                    .request_tracker
                    .process_check_rel_exists_request(page_api::CheckRelExistsRequest {
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
                    .lock(RequestInProgressKey::Rel(rel.clone()));

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
                    .request_tracker
                    .process_get_rel_size_request(page_api::GetRelSizeRequest {
                        read_lsn,
                        rel: rel.clone(),
                    })
                    .await
                {
                    Ok(nblocks) => {
                        // update the cache
                        tracing::info!("updated relsize for {:?} in cache: {}", rel, nblocks);
                        self.cache.remember_rel_size(&rel, nblocks);

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
            NeonIORequest::PrefetchV(req) => {
                self.request_prefetchv_counter.inc();
                self.request_prefetchv_nblocks_counter
                    .inc_by(req.nblocks as u64);
                let req = req.clone();
                tokio::spawn(async move { self.handle_prefetchv_request(&req).await });
                NeonIOResult::PrefetchVLaunched
            }
            NeonIORequest::DbSize(req) => {
                self.request_db_size_counter.inc();
                let _in_progress_guard = self
                    .in_progress_table
                    .lock(RequestInProgressKey::Db(req.db_oid));

                // Check the cache first
                let not_modified_since = match self.cache.get_db_size(req.db_oid) {
                    CacheResult::Found(db_size) => {
                        // get_page already copied the block content to the destination
                        return NeonIOResult::DbSize(db_size);
                    }
                    CacheResult::NotFound(lsn) => lsn,
                };

                match self
                    .request_tracker
                    .process_get_dbsize_request(page_api::GetDbSizeRequest {
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

                // Also store it in the LFC while we still have it
                let rel = req.reltag();
                let _in_progress_guard = self
                    .in_progress_table
                    .lock(RequestInProgressKey::Block(rel.clone(), req.block_number));
                self.cache
                    .remember_page(&rel, req.block_number, req.src, Lsn(req.lsn), true)
                    .await;
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelExtend(req) => {
                self.request_rel_extend_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache
                    .remember_rel_size(&req.reltag(), req.block_number + 1);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelZeroExtend(req) => {
                self.request_rel_zero_extend_counter.inc();
                self.request_rel_zero_extend_nblocks_counter
                    .inc_by(req.nblocks as u64);

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache
                    .remember_rel_size(&req.reltag(), req.block_number + req.nblocks);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelCreate(req) => {
                self.request_rel_create_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache.remember_rel_size(&req.reltag(), 0);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelTruncate(req) => {
                self.request_rel_truncate_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache.remember_rel_size(&req.reltag(), req.nblocks);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelUnlink(req) => {
                self.request_rel_unlink_counter.inc();

                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache.forget_rel(&req.reltag());
                NeonIOResult::WriteOK
            }
        }
    }

    async fn handle_get_pagev_request(&'t self, req: &CGetPageVRequest) -> Result<(), i32> {
        let rel = req.reltag();

        // Check the cache first
        //
        // Note: Because the backends perform a direct lookup in the cache before sending
        // the request to the communicator process, we expect the pages to almost never
        // be already in cache. It could happen when:
        // 1. two backends try to read the same page at the same time, but that should never
        //    happen because there's higher level locking in the Postgres buffer manager, or
        // 2. if a prefetch request finished at the same time as a backend requested the
        //    page. That's much more likely.
        let mut cache_misses = Vec::with_capacity(req.nblocks as usize);
        for i in 0..req.nblocks {
            let blkno = req.block_number + i as u32;

            // note: this is deadlock-safe even though we hold multiple locks at the same time,
            // because they're always acquired in the same order.
            let in_progress_guard = self
                .in_progress_table
                .lock(RequestInProgressKey::Block(rel.clone(), blkno))
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

        // TODO: Use batched protocol
        for (blkno, _lsn, dest, _guard) in cache_misses.iter() {
            match self
                .request_tracker
                .get_page(page_api::GetPageRequest {
                    request_id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
                    request_class: page_api::GetPageClass::Normal,
                    read_lsn: self.request_lsns(not_modified_since),
                    rel: rel.clone(),
                    block_numbers: vec![*blkno],
                })
                .await
            {
                Ok(resp) => {
                    // Write the received page image directly to the shared memory location
                    // that the backend requested.
                    assert!(resp.page_images.len() == 1);
                    let page_image = resp.page_images[0].clone();
                    let src: &[u8] = page_image.as_ref();
                    let len = std::cmp::min(src.len(), dest.bytes_total() as usize);
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dest.as_mut_ptr(), len);
                    };

                    // Also store it in the LFC while we have it
                    self.cache
                        .remember_page(&rel, *blkno, page_image, not_modified_since, false)
                        .await;
                }
                Err(err) => {
                    info!("tonic error: {err:?}");
                    return Err(-1);
                }
            }
        }
        Ok(())
    }

    async fn handle_prefetchv_request(
        self: &'static Self,
        req: &CPrefetchVRequest,
    ) -> Result<(), i32> {
        let rel = req.reltag();

        // Check the cache first
        let mut cache_misses = Vec::with_capacity(req.nblocks as usize);
        for i in 0..req.nblocks {
            let blkno = req.block_number + i as u32;

            // note: this is deadlock-safe even though we hold multiple locks at the same time,
            // because they're always acquired in the same order.
            let in_progress_guard = self
                .in_progress_table
                .lock(RequestInProgressKey::Block(rel.clone(), blkno))
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

        // TODO: spawn separate tasks for these. Use the integrated cache to keep track of the
        // in-flight requests

        // TODO: Use batched protocol
        for (blkno, _lsn, _guard) in cache_misses.iter() {
            match self
                .request_tracker
                .get_page(page_api::GetPageRequest {
                    request_id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
                    request_class: page_api::GetPageClass::Prefetch,
                    read_lsn: self.request_lsns(not_modified_since),
                    rel: rel.clone(),
                    block_numbers: vec![*blkno],
                })
                .await
            {
                Ok(resp) => {
                    trace!(
                        "prefetch completed, remembering blk {} in rel {:?} in LFC",
                        *blkno, rel
                    );
                    assert!(resp.page_images.len() == 1);
                    let page_image = resp.page_images[0].clone();
                    self.cache
                        .remember_page(&rel, *blkno, page_image, not_modified_since, false)
                        .await;
                }
                Err(err) => {
                    info!("tonic error: {err:?}");
                    return Err(-1);
                }
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
