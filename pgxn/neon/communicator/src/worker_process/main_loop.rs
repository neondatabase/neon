use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend_comms::NeonIOHandle;
use crate::file_cache::FileCache;
use crate::init::CommunicatorInitStruct;
use crate::integrated_cache::{CacheResult, IntegratedCacheWriteAccess};
use crate::neon_request::{CGetPageVRequest, CPrefetchVRequest};
use crate::neon_request::{NeonIORequest, NeonIOResult};
use pageserver_client_grpc::PageserverClient;
use pageserver_page_api::model;

use tokio::io::AsyncReadExt;
use tokio_epoll_uring::IoBuf;
use tokio_pipe::PipeRead;

use super::callbacks::{get_request_lsn, notify_proc};

use tracing::{error, info, trace};

use utils::lsn::Lsn;

pub struct CommunicatorWorkerProcessStruct<'a> {
    neon_request_slots: &'a [NeonIOHandle],

    pageserver_client: PageserverClient,

    cache: IntegratedCacheWriteAccess<'a>,

    submission_pipe_read_raw_fd: i32,

    next_request_id: AtomicU64,
}

pub(super) async fn init(
    cis: Box<CommunicatorInitStruct>,
    tenant_id: String,
    timeline_id: String,
    auth_token: Option<String>,
    shard_map: HashMap<utils::shard::ShardIndex, String>,
    file_cache_size: u64,
    file_cache_path: Option<PathBuf>,
) -> CommunicatorWorkerProcessStruct<'static> {
    let last_lsn = get_request_lsn();

    let uring_system = tokio_epoll_uring::System::launch().await.unwrap();

    let file_cache = if let Some(path) = file_cache_path {
        Some(
            FileCache::new(&path, file_cache_size, uring_system)
                .expect("could not create cache file"),
        )
    } else {
        // FIXME: temporarily for testing, use LFC even if disabled
        Some(
            FileCache::new(&PathBuf::from("new_filecache"), 1000, uring_system)
                .expect("could not create cache file"),
        )
    };

    // Initialize subsystems
    let cache = cis
        .integrated_cache_init_struct
        .worker_process_init(last_lsn, file_cache);

    let pageserver_client = PageserverClient::new(&tenant_id, &timeline_id, &auth_token, shard_map);

    let this = CommunicatorWorkerProcessStruct {
        neon_request_slots: cis.neon_request_slots,
        pageserver_client,
        cache,
        submission_pipe_read_raw_fd: cis.submission_pipe_read_fd,
        next_request_id: AtomicU64::new(1),
    };

    this
}

impl<'t> CommunicatorWorkerProcessStruct<'t> {
    /// Main loop of the worker process. Receive requests from the backends and process them.
    pub(super) async fn run(self: &'static Self) {
        let mut idxbuf: [u8; 4] = [0; 4];

        let mut submission_pipe_read =
            PipeRead::from_raw_fd_checked(self.submission_pipe_read_raw_fd)
                .expect("invalid pipe fd");

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

    fn request_common(&self, not_modified_since_lsn: Lsn) -> model::RequestCommon {
        model::RequestCommon {
            request_lsn: get_request_lsn(),
            not_modified_since_lsn,
        }
    }

    async fn handle_request<'x>(self: &'static Self, req: &'x NeonIORequest) -> NeonIOResult {
        match req {
            NeonIORequest::Empty => {
                error!("unexpected Empty IO request");
                NeonIOResult::Error(-1)
            }
            NeonIORequest::RelExists(req) => {
                let rel = req.reltag();

                let not_modified_since = match self.cache.get_rel_exists(&rel) {
                    CacheResult::Found(exists) => return NeonIOResult::RelExists(exists),
                    CacheResult::NotFound(lsn) => lsn,
                };

                match self
                    .pageserver_client
                    .process_rel_exists_request(&model::RelExistsRequest {
                        common: self.request_common(not_modified_since),
                        rel,
                    })
                    .await
                {
                    Ok(exists) => NeonIOResult::RelExists(exists),
                    Err(err) => {
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(-1)
                    }
                }
            }

            NeonIORequest::RelSize(req) => {
                let rel = req.reltag();

                // Check the cache first
                let not_modified_since = match self.cache.get_rel_size(&rel) {
                    CacheResult::Found(nblocks) => {
                        tracing::trace!("found relsize for {:?} in cache: {}", rel, nblocks);
                        return NeonIOResult::RelSize(nblocks);
                    }
                    CacheResult::NotFound(lsn) => lsn,
                };

                let common = self.request_common(not_modified_since);
                match self
                    .pageserver_client
                    .process_rel_size_request(&model::RelSizeRequest {
                        common: common.clone(),
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
                        NeonIOResult::Error(-1)
                    }
                }
            }
            NeonIORequest::GetPageV(req) => match self.handle_get_pagev_request(req).await {
                Ok(()) => NeonIOResult::GetPageV,
                Err(errno) => NeonIOResult::Error(errno),
            },
            NeonIORequest::PrefetchV(req) => {
                let req = req.clone();
                tokio::spawn(async move { self.handle_prefetchv_request(&req).await });
                NeonIOResult::PrefetchVLaunched
            }
            NeonIORequest::DbSize(req) => {
                // Check the cache first
                let not_modified_since = match self.cache.get_db_size(req.db_oid) {
                    CacheResult::Found(db_size) => {
                        // get_page already copied the block content to the destination
                        return NeonIOResult::DbSize(db_size);
                    }
                    CacheResult::NotFound(lsn) => lsn,
                };

                match self
                    .pageserver_client
                    .process_dbsize_request(&model::DbSizeRequest {
                        common: self.request_common(not_modified_since),
                        db_oid: req.db_oid,
                    })
                    .await
                {
                    Ok(db_size) => NeonIOResult::DbSize(db_size),
                    Err(err) => {
                        info!("tonic error: {err:?}");
                        NeonIOResult::Error(-1)
                    }
                }
            }

            // Write requests
            NeonIORequest::WritePage(req) => {
                // Also store it in the LFC while we still have it
                let rel = req.reltag();
                self.cache
                    .remember_page(&rel, req.block_number, req.src, Lsn(req.lsn))
                    .await;
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelExtend(req) => {
                self.cache
                    .remember_rel_size(&req.reltag(), req.block_number + 1);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelZeroExtend(req) => {
                self.cache
                    .remember_rel_size(&req.reltag(), req.block_number + req.nblocks);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelCreate(req) => {
                self.cache.remember_rel_size(&req.reltag(), 0);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelTruncate(req) => {
                self.cache.remember_rel_size(&req.reltag(), req.nblocks);
                NeonIOResult::WriteOK
            }
            NeonIORequest::RelUnlink(req) => {
                self.cache.forget_rel(&req.reltag());
                NeonIOResult::WriteOK
            }
        }
    }

    async fn handle_get_pagev_request(&'t self, req: &CGetPageVRequest) -> Result<(), i32> {
        let rel = req.reltag();

        // Check the cache first
        let mut cache_misses = Vec::new();
        for i in 0..req.nblocks {
            let blkno = req.block_number + i as u32;
            let dest = req.dest[i as usize];
            let not_modified_since = match self.cache.get_page(&rel, blkno, dest).await {
                Ok(CacheResult::Found(_)) => {
                    // get_page already copied the block content to the destination
                    trace!("found blk {} in rel {:?} in LFC ", blkno, rel);
                    continue;
                }
                Ok(CacheResult::NotFound(lsn)) => lsn,
                Err(_io_error) => return Err(-1), // FIXME errno?
            };
            cache_misses.push((blkno, not_modified_since, dest));
        }
        if cache_misses.is_empty() {
            return Ok(());
        }
        let not_modified_since = cache_misses
            .iter()
            .map(|(_blkno, lsn, _dest)| *lsn)
            .max()
            .unwrap();

        // TODO: Use batched protocol
        for (blkno, _lsn, dest) in cache_misses.iter() {
            match self
                .pageserver_client
                .get_page(&model::GetPageRequest {
                    id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
                    common: self.request_common(not_modified_since),
                    rel: rel.clone(),
                    block_number: *blkno,
                    class: model::GetPageClass::Normal,
                })
                .await
            {
                Ok(page_image) => {
                    // Write the received page image directly to the shared memory location
                    // that the backend requested.
                    let src: &[u8] = page_image.as_ref();
                    let len = std::cmp::min(src.len(), dest.bytes_total() as usize);
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dest.as_mut_ptr(), len);
                    };

                    trace!("remembering blk {} in rel {:?} in LFC", blkno, rel);

                    // Also store it in the LFC while we have it
                    self.cache
                        .remember_page(&rel, *blkno, page_image, not_modified_since)
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
        let mut cache_misses = Vec::new();
        for i in 0..req.nblocks {
            let blkno = req.block_number + i as u32;
            let not_modified_since = match self.cache.page_is_cached(&rel, blkno).await {
                Ok(CacheResult::Found(_)) => {
                    trace!("found blk {} in rel {:?} in LFC ", req.block_number, rel);
                    continue;
                }
                Ok(CacheResult::NotFound(lsn)) => lsn,
                Err(_io_error) => return Err(-1), // FIXME errno?
            };
            cache_misses.push((req.block_number, not_modified_since));
        }
        if cache_misses.is_empty() {
            return Ok(());
        }
        let not_modified_since = cache_misses.iter().map(|(_blkno, lsn)| *lsn).max().unwrap();

        // TODO: spawn separate tasks for these. Use the integrated cache to keep track of the
        // in-flight requests

        // TODO: Use batched protocol
        for (blkno, _lsn) in cache_misses.iter() {
            match self
                .pageserver_client
                .get_page(&model::GetPageRequest {
                    id: self.next_request_id.fetch_add(1, Ordering::Relaxed),
                    common: self.request_common(not_modified_since),
                    rel: rel.clone(),
                    block_number: *blkno,
                    class: model::GetPageClass::Prefetch,
                })
                .await
            {
                Ok(page_image) => {
                    trace!(
                        "prefetch completed, remembering blk {} in rel {:?} in LFC",
                        req.block_number, rel
                    );
                    self.cache
                        .remember_page(&rel, req.block_number, page_image, not_modified_since)
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
        if let Some(file_cache) = &self.cache.file_cache {
            descs.append(&mut file_cache.desc());
        }
        descs
    }
    fn collect(&self) -> Vec<metrics::proto::MetricFamily> {
        let mut values = Vec::new();
        if let Some(file_cache) = &self.cache.file_cache {
            values.append(&mut file_cache.collect());
        }
        values
    }
}
