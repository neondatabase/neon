//! This code runs in the communicator worker process. This provides
//! the glue code to:
//!
//! - launch the 'processor',
//! - receive IO requests from backends and pass them to the processor,
//! - write results back to backends.

mod callbacks;

use std::collections::HashMap;
use std::ffi::{CStr, c_char};
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;

use crate::CommunicatorInitStruct;
use crate::backend_comms::NeonIOHandleState;
use crate::neon_request::{NeonIORequest, NeonIOResult};
use crate::processor::CommunicatorProcessor;

use tokio::io::AsyncReadExt;
use tokio_pipe::PipeRead;

use callbacks::{elog_log, notify_proc};

///
/// Main entry point for the communicator process.
///
/// The caller has initialized the process as a regular PostgreSQL
/// background worker process. The shared memory segment used to
/// communicate with the backends has been allocated and initialized
/// earlier already (TODO: by which function?).
///
#[unsafe(no_mangle)]
pub extern "C" fn communicator_worker_process_launch(
    cis: *const CommunicatorInitStruct,
    tenant_id: *const c_char,
    timeline_id: *const c_char,
    auth_token: *const c_char,
    shard_map: *mut *mut c_char,
    nshards: u32,
) {
    elog_log("In communicator_worker_process_launch");

    let cis = unsafe { &*cis };
    let tenant_id = {
        let c_str = unsafe { CStr::from_ptr(tenant_id) };
        c_str.to_str().unwrap()
    };
    let timeline_id = {
        let c_str = unsafe { CStr::from_ptr(timeline_id) };
        c_str.to_str().unwrap()
    };
    let auth_token = {
        if let Some(c_str) = unsafe { auth_token.as_ref() } {
            Some(c_str.to_string())
        } else {
            None
        }
    };

    let shard_map = parse_shard_map(nshards, shard_map);

    // start main loop
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("communicator thread")
        .on_thread_start(|| {
            elog_log("thread started");
        })
        .on_thread_stop(|| {
            elog_log("thread stopping");
        })
        .build()
        .unwrap();

    let main_loop_handle = runtime.spawn(communicator_process_main_loop(
        cis,
        tenant_id,
        timeline_id,
        auth_token,
        shard_map,
    ));

    runtime.spawn(async {
        let err = main_loop_handle.await.unwrap_err();
        elog_log(&format!("error: {err:?}"));
    });

    // keep the runtime running after we exit this function
    Box::leak(Box::new(runtime));
}

/// Main loop of the worker process. Receive requests from the backends and process them.
async fn communicator_process_main_loop(
    cis: &CommunicatorInitStruct,
    tenant_id: &str,
    timeline_id: &str,
    auth_token: Option<String>,
    shard_map: HashMap<u16, String>,
) {
    let mut submission_pipe_read =
        PipeRead::from_raw_fd_checked(cis.submission_pipe_read_fd).expect("invalid pipe fd");

    elog_log("In communicator_process_main_loop");

    let processor = CommunicatorProcessor::new(tenant_id, timeline_id, &auth_token, shard_map);

    let mut idxbuf: [u8; 4] = [0; 4];
    loop {
        // Wait for a backend to ring the doorbell
        let res = submission_pipe_read.read(&mut idxbuf).await;
        if res.is_err() {
            panic!("reading from communicator pipe failed: {res:?}");
        }
        let request_idx = u32::from_ne_bytes(idxbuf);

        elog_log(&format!("received request {request_idx}"));

        // Read the IO request from the slot indicated in the wakeup
        let slot = cis.get_request_slot(request_idx);
        if let Err(_s) = slot.state.compare_exchange(
            NeonIOHandleState::Submitted,
            NeonIOHandleState::Processing,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            // FIXME surprising state. This is unexpected at the moment, but if we
            // started to process requests more aggressively, without waiting for the
            // read from the pipe, then this could happen
            continue;
        }
        fence(Ordering::Acquire);

        // Ok, we have ownership of this request now. We must process
        // it now, there's no going back.
        //
        // TODO: possible to have a guard object for that? That would protect from
        // dropped futures.

        let request = slot.request.clone();

        elog_log(&format!("processing request {request_idx}: {request:?}"));
        match request {
            NeonIORequest::Empty => {
                panic!("unexpected Empty IO request");
            }
            NeonIORequest::RelExists(req) => match processor.process_rel_exists_request(&req).await
            {
                Ok(exists) => {
                    slot.result = NeonIOResult::RelExists(exists);
                }
                Err(err) => {
                    elog_log(&format!("tonic error: {err:?}"));
                    slot.result = NeonIOResult::Error(-1);
                }
            },
            NeonIORequest::RelSize(req) => match processor.process_rel_size_request(&req).await {
                Ok(nblocks) => {
                    slot.result = NeonIOResult::RelSize(nblocks);
                }
                Err(err) => {
                    elog_log(&format!("tonic error: {err:?}"));
                    slot.result = NeonIOResult::Error(-1);
                }
            },
            NeonIORequest::GetPage(req) => match processor.process_get_page_request(&req).await {
                Ok(page_image) => {
                    // Write the received page image directly to the shared memory location
                    // that the backend requested.
                    let src: &[u8] = page_image.as_slice();
                    let dst = cis.shmem_ptr.with_addr(req.dest_ptr);
                    let len = std::cmp::min(src.len(), req.dest_size as usize);
                    unsafe {
                        std::ptr::copy_nonoverlapping(src.as_ptr(), dst, len);
                    };
                    slot.result = NeonIOResult::GetPage;
                }
                Err(err) => {
                    elog_log(&format!("tonic error: {err:?}"));
                    slot.result = NeonIOResult::Error(-1);
                }
            },
            NeonIORequest::DbSize(req) => match processor.process_dbsize_request(&req).await {
                Ok(db_size) => {
                    slot.result = NeonIOResult::DbSize(db_size);
                }
                Err(err) => {
                    elog_log(&format!("tonic error: {err:?}"));
                    slot.result = NeonIOResult::Error(-1);
                }
            },
        };

        let owner_procno = slot.owner_procno;

        // Ok, we have completed the IO. Mark the request as completed. After that,
        // we no longer have ownership of the slot, and must not modify it.
        let old_state = slot
            .state
            .swap(NeonIOHandleState::Completed, Ordering::Release);
        assert!(old_state == NeonIOHandleState::Processing);

        // Notify the backend about the completion. (Note that the backend might see
        // the completed status even before this; this is just a wakeup)
        notify_proc(owner_procno);
    }
}

fn parse_shard_map(nshards: u32, shard_map: *mut *mut c_char) -> HashMap<u16, String> {
    let mut result: HashMap<u16, String> = HashMap::new();
    let mut p = shard_map;

    for i in 0..nshards {
        let c_str = unsafe { CStr::from_ptr(*p) };

        p = unsafe { p.add(1) };

        let s = c_str.to_str().unwrap();
        result.insert(i as u16, s.into());
    }
    result
}
