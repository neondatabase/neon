use std::ffi::CString;

use postgres_ffi::WAL_SEGMENT_SIZE;
use utils::id::TenantTimelineId;

use crate::{bindings::{Safekeeper, WalProposer, WalProposerConfig, WalProposerCreate}, api_bindings::create_api};

/// Rust high-level wrapper for C walproposer API.
pub trait ApiImpl {
    fn strong_random(&self, buf: &mut [u8]) -> bool;
    fn wal_reader_allocate(&self, sk: &mut Safekeeper);
    fn init_event_set(&self, wp: &mut WalProposer);
}

pub struct Config {
    pub ttid: TenantTimelineId,
    pub safekeepers_list: Vec<String>,
    pub safekeeper_reconnect_timeout: i32,
    pub safekeeper_connection_timeout: i32,
    pub sync_safekeepers: bool,
}

pub fn new(api: Box<dyn ApiImpl>, config: Config) -> *mut WalProposer {
    let neon_tenant = CString::new(config.ttid.tenant_id.to_string()).unwrap().into_raw();
    let neon_timeline = CString::new(config.ttid.timeline_id.to_string()).unwrap().into_raw();

    let mut safekeepers_list = CString::new(config.safekeepers_list.join(",")).unwrap().into_bytes_with_nul().into_boxed_slice();
    let safekeepers_list = safekeepers_list.as_mut_ptr() as *mut i8;
    let boxed_impl = Box::new(api);

    let callback_data = Box::into_raw(boxed_impl) as *mut ::std::os::raw::c_void;

    let c_config = WalProposerConfig {
        neon_tenant,
        neon_timeline,
        safekeepers_list,
        safekeeper_reconnect_timeout: config.safekeeper_reconnect_timeout,
        safekeeper_connection_timeout: config.safekeeper_connection_timeout,
        wal_segment_size: WAL_SEGMENT_SIZE as i32, // default 16MB
        syncSafekeepers: config.sync_safekeepers,
        systemId: 0,
        pgTimeline: 1,
        callback_data,
    };
    let c_config = Box::into_raw(Box::new(c_config));

    let api = create_api();
    unsafe { WalProposerCreate(c_config, api) }
}
