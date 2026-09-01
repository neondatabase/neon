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
use crate::neon_request::{CGetPageVRequest, CGetPageVUncachedRequest, CPrefetchVRequest};
use crate::neon_request::{INVALID_BLOCK_NUMBER, NeonIORequest, NeonIOResult};
use crate::worker_process::control_socket;
use crate::worker_process::in_progress_ios::{RequestInProgressKey, RequestInProgressTable};
use crate::worker_process::lfc_metrics::LfcMetricsCollector;
use pageserver_client_grpc::{PageserverClient, ShardSpec, ShardStripeSize};
use pageserver_page_api as page_api;

use tokio::io::AsyncReadExt;
use tokio_pipe::PipeRead;
use uring_common::buf::IoBuf;

use measured::MetricGroup;
use measured::metric::MetricEncoding;
use measured::metric::counter::CounterState;
use measured::metric::gauge::GaugeState;
use measured::metric::group::Encoding;
use measured::{Gauge, GaugeVec};
use utils::id::{TenantId, TimelineId};

use super::callbacks::{get_request_lsn, notify_proc};

use tracing::{error, info, info_span, trace};

use utils::lsn::Lsn;

pub struct CommunicatorWorkerProcessStruct<'a> {
    /// Tokio runtime that the main loop and any other related tasks runs in.
    runtime: tokio::runtime::Runtime,

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

    /*** Metrics ***/
    pub(crate) lfc_metrics: LfcMetricsCollector,

    request_counters: GaugeVec<RequestTypeLabelGroupSet>,

    getpage_cache_misses_counter: Gauge,
    getpage_cache_hits_counter: Gauge,

    // For the requests that affect multiple blocks, have separate counters for the # of blocks affected
    request_nblocks_counters: GaugeVec<RequestTypeLabelGroupSet>,

    allocator_metrics: MyAllocatorCollector,
}

// Define a label group, consisting of 1 or more label values
#[derive(measured::LabelGroup)]
#[label(set = RequestTypeLabelGroupSet)]
struct RequestTypeLabelGroup {
    request_type: crate::neon_request::NeonIORequestDiscriminants,
}

impl RequestTypeLabelGroup {
    fn from_req(req: &NeonIORequest) -> Self {
        RequestTypeLabelGroup {
            request_type: req.into(),
        }
    }
}

/// Launch the communicator process's Rust subsystems
#[allow(clippy::too_many_arguments)]
pub(super) fn init_legacy() -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("communicator thread")
        .build()
        .unwrap();

    // Start the listener on the control socket
    runtime
        .block_on(control_socket::launch_listener(None))
        .map_err(|e| e.to_string())?;

    Box::leak(Box::new(runtime));

    Ok(())
}

/// Launch the communicator process's Rust subsystems
#[allow(clippy::too_many_arguments)]
pub(super) fn init(
    cis: CommunicatorInitStruct,
    tenant_id: &str,
    timeline_id: &str,
    auth_token: Option<&str>,
    shard_map: HashMap<utils::shard::ShardIndex, String>,
    stripe_size: Option<ShardStripeSize>,
    initial_file_cache_size: u64,
    file_cache_path: Option<PathBuf>,
) -> Result<&'static CommunicatorWorkerProcessStruct<'static>, String> {
    // The caller validated these already
    let tenant_id = TenantId::from_str(tenant_id).map_err(|e| format!("invalid tenant ID: {e}"))?;
    let timeline_id =
        TimelineId::from_str(timeline_id).map_err(|e| format!("invalid timeline ID: {e}"))?;
    let shard_spec =
        ShardSpec::new(shard_map, stripe_size).map_err(|e| format!("invalid shard spec: {e}:"))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("communicator thread")
        .build()
        .unwrap();

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

    let client = {
        let _guard = runtime.enter();
        PageserverClient::new(
            tenant_id,
            timeline_id,
            shard_spec,
            auth_token.map(|s| s.to_string()),
            None,
        )
        .expect("could not create client")
    };

    let worker_struct = CommunicatorWorkerProcessStruct {
        // Note: it's important to not drop the runtime, or all the tasks are dropped
        // too. Including it in the returned struct is one way to keep it around.
        runtime,
        neon_request_slots: cis.neon_request_slots,
        client,
        cache,
        submission_pipe_read_fd: cis.submission_pipe_read_fd,
        in_progress_table: RequestInProgressTable::new(),

        // metrics
        lfc_metrics: LfcMetricsCollector,

        request_counters: GaugeVec::new(),

        getpage_cache_misses_counter: Gauge::new(),
        getpage_cache_hits_counter: Gauge::new(),

        request_nblocks_counters: GaugeVec::new(),

        allocator_metrics: MyAllocatorCollector::new(),
    };

    let worker_struct = Box::leak(Box::new(worker_struct));

    let main_loop_handle = worker_struct.runtime.spawn(worker_struct.run());
    worker_struct.runtime.spawn(async {
        let err = main_loop_handle.await.unwrap_err();
        error!("error: {err:?}");
    });

    // Start the listener on the control socket
    worker_struct
        .runtime
        .block_on(control_socket::launch_listener(Some(worker_struct)))
        .map_err(|e| e.to_string())?;

    Ok(worker_struct)
}

impl<'t> CommunicatorWorkerProcessStruct<'t> {
    /// Update the configuration
    pub(super) fn update_shard_map(
        &self,
        new_shard_map: HashMap<utils::shard::ShardIndex, String>,
        stripe_size: Option<ShardStripeSize>,
    ) {
        let shard_spec = ShardSpec::new(new_shard_map, stripe_size).expect("invalid shard spec");

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
                        tokio::time::Duration::from_secs(60),
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
    async fn handle_request(&'static self, request: &'_ NeonIORequest) -> NeonIOResult {
        self.request_counters
            .inc(RequestTypeLabelGroup::from_req(request));
        match request {
            NeonIORequest::Empty => {
                error!("unexpected Empty IO request");
                NeonIOResult::Error(0)
            }
            NeonIORequest::RelSize(req) => {
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
                    // XXX: we don't cache negative entries, so if there's no entry in the cache, it could mean
                    // that the relation doesn't exist or that we don't have it cached.
                    CacheResult::NotFound(lsn) => lsn,
                };

                let read_lsn = self.request_lsns(not_modified_since);
                match self
                    .client
                    .get_rel_size(page_api::GetRelSizeRequest {
                        read_lsn,
                        rel,
                        allow_missing: req.allow_missing,
                    })
                    .await
                {
                    Ok(Some(nblocks)) => {
                        // update the cache
                        tracing::trace!(
                            "updated relsize for {:?} in cache: {}, lsn {}",
                            rel,
                            nblocks,
                            read_lsn
                        );
                        self.cache
                            .remember_rel_size(&rel, nblocks, not_modified_since);

                        NeonIOResult::RelSize(nblocks)
                    }
                    Ok(None) => {
                        // TODO: cache negative entry?
                        NeonIOResult::RelSize(INVALID_BLOCK_NUMBER)
                    }
                    Err(err) => {
                        // FIXME: Could we map the tonic StatusCode to a libc errno in a more fine-grained way? Or pass the error message to the backend
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(libc::EIO)
                    }
                }
            }
            NeonIORequest::GetPageV(req) => match self.handle_get_pagev_request(req).await {
                Ok(()) => NeonIOResult::GetPageV,
                Err(errno) => NeonIOResult::Error(errno),
            },
            NeonIORequest::GetPageVUncached(req) => {
                match self.handle_get_pagev_uncached_request(req).await {
                    Ok(()) => NeonIOResult::GetPageV,
                    Err(errno) => NeonIOResult::Error(errno),
                }
            }
            NeonIORequest::ReadSlruSegment(req) => {
                let lsn = Lsn(req.request_lsn);
                let file_path = req.destination_file_path();

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
                        if let Err(e) = tokio::fs::write(&file_path, &slru_bytes).await {
                            error!("could not write slru segment to file {file_path}: {e}");
                            return NeonIOResult::Error(e.raw_os_error().unwrap_or(libc::EIO));
                        }

                        let blocks_count = slru_bytes.len() / crate::BLCKSZ;

                        NeonIOResult::ReadSlruSegment(blocks_count as _)
                    }
                    Err(err) => {
                        // FIXME: Could we map the tonic StatusCode to a libc errno in a more fine-grained way? Or pass the error message to the backend
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(libc::EIO)
                    }
                }
            }
            NeonIORequest::PrefetchV(req) => {
                self.request_nblocks_counters
                    .inc_by(RequestTypeLabelGroup::from_req(request), req.nblocks as i64);
                let req = *req;
                // FIXME: handle_request() runs in a separate task already, do we really need to spawn a new one here?
                tokio::spawn(async move { self.handle_prefetchv_request(&req).await });
                NeonIOResult::PrefetchVLaunched
            }
            NeonIORequest::DbSize(req) => {
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
                        // FIXME: Could we map the tonic StatusCode to a libc errno in a more fine-grained way? Or pass the error message to the backend
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(libc::EIO)
                    }
                }
            }

            // Write requests
            NeonIORequest::WritePage(req) => {
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
                self.request_nblocks_counters
                    .inc_by(RequestTypeLabelGroup::from_req(request), req.nblocks as i64);

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
                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache.remember_rel_size(&req.reltag(), 0, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelTruncate(req) => {
                // TODO: need to grab an io-in-progress lock for this? I guess not
                self.cache
                    .remember_rel_size(&req.reltag(), req.nblocks, Lsn(req.lsn));
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelUnlink(req) => {
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
                Err(_io_error) => return Err(libc::EIO), // FIXME print the error?
            };
            cache_misses.push((blkno, not_modified_since, dest, in_progress_guard));
        }
        self.getpage_cache_misses_counter
            .inc_by(cache_misses.len() as i64);
        self.getpage_cache_hits_counter
            .inc_by(req.nblocks as i64 - cache_misses.len() as i64);

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
        trace!(
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
                    return Err(libc::EIO);
                }

                trace!(
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
                // FIXME: Could we map the tonic StatusCode to a libc errno in a more fine-grained way? Or pass the error message to the backend
                info!("tonic error: {err:?}");
                return Err(libc::EIO);
            }
        }
        Ok(())
    }

    /// Subroutine to handle an GetPageVUncached request.
    ///
    /// Note: this bypasses the cache, in-progress IO locking, and all other side-effects.
    /// This request type is only used in tests.
    async fn handle_get_pagev_uncached_request(
        &'t self,
        req: &CGetPageVUncachedRequest,
    ) -> Result<(), i32> {
        let rel = req.reltag();

        // Construct a pageserver request
        let block_numbers: Vec<u32> =
            (req.block_number..(req.block_number + (req.nblocks as u32))).collect();
        let read_lsn = page_api::ReadLsn {
            request_lsn: Lsn(req.request_lsn),
            not_modified_since_lsn: Some(Lsn(req.not_modified_since)),
        };
        trace!(
            "sending (uncached) getpage request for blocks {:?} in rel {:?} lsns {}",
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
                    return Err(libc::EIO);
                }

                trace!(
                    "received getpage response for blocks {:?} in rel {:?} lsns {}",
                    block_numbers, rel, read_lsn
                );

                for (page, dest) in resp.pages.into_iter().zip(req.dest) {
                    let src: &[u8] = page.image.as_ref();
                    let len = std::cmp::min(src.len(), dest.bytes_total());
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dest.as_mut_ptr(), len);
                    };
                }
            }
            Err(err) => {
                // FIXME: Could we map the tonic StatusCode to a libc errno in a more fine-grained way? Or pass the error message to the backend
                info!("tonic error: {err:?}");
                return Err(libc::EIO);
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
                Err(_io_error) => return Err(libc::EIO), // FIXME print the error?
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
                    return Err(libc::EIO);
                }

                for (page, (blkno, _lsn, _guard)) in resp.pages.into_iter().zip(cache_misses) {
                    self.cache
                        .remember_page(&rel, blkno, page.image, not_modified_since, false)
                        .await;
                }
            }
            Err(err) => {
                // FIXME: Could we map the tonic StatusCode to a libc errno in a more fine-grained way? Or pass the error message to the backend
                info!("tonic error: {err:?}");
                return Err(libc::EIO);
            }
        }
        Ok(())
    }
}

impl<T> MetricGroup<T> for CommunicatorWorkerProcessStruct<'_>
where
    T: Encoding,
    CounterState: MetricEncoding<T>,
    GaugeState: MetricEncoding<T>,
{
    fn collect_group_into(&self, enc: &mut T) -> Result<(), T::Err> {
        use measured::metric::MetricFamilyEncoding;
        use measured::metric::name::MetricName;

        self.lfc_metrics.collect_group_into(enc)?;
        self.cache.collect_group_into(enc)?;
        self.request_counters
            .collect_family_into(MetricName::from_str("request_counters"), enc)?;
        self.request_nblocks_counters
            .collect_family_into(MetricName::from_str("request_nblocks_counters"), enc)?;
        self.allocator_metrics.collect_group_into(enc)?;

        Ok(())
    }
}
