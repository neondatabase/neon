use std::ffi::CString;

use crate::{
    api_bindings::{create_api, take_vec_u8, Level},
    bindings::{
        NeonWALReadResult, Safekeeper, WalProposer, WalProposerBroadcast, WalProposerConfig,
        WalProposerCreate, WalProposerFree, WalProposerPoll, WalProposerStart,
    },
};
use postgres_ffi::WAL_SEGMENT_SIZE;
use utils::{id::TenantTimelineId, lsn::Lsn};

/// Rust high-level wrapper for C walproposer API. Many methods are not required
/// for simple cases, hence todo!() in default implementations.
///
/// Refer to `pgxn/neon/walproposer.h` for documentation.
pub trait ApiImpl {
    fn get_shmem_state(&self) -> *mut crate::bindings::WalproposerShmemState {
        todo!()
    }

    fn start_streaming(&self, _startpos: u64, _callback: &StreamingCallback) {
        todo!()
    }

    fn get_flush_rec_ptr(&self) -> u64 {
        todo!()
    }

    fn update_donor(&self, _donor: &mut Safekeeper, _donor_lsn: u64) {
        todo!()
    }

    fn get_current_timestamp(&self) -> i64 {
        todo!()
    }

    fn conn_error_message(&self, _sk: &mut Safekeeper) -> String {
        todo!()
    }

    fn conn_status(&self, _sk: &mut Safekeeper) -> crate::bindings::WalProposerConnStatusType {
        todo!()
    }

    fn conn_connect_start(&self, _sk: &mut Safekeeper) {
        todo!()
    }

    fn conn_connect_poll(
        &self,
        _sk: &mut Safekeeper,
    ) -> crate::bindings::WalProposerConnectPollStatusType {
        todo!()
    }

    fn conn_send_query(&self, _sk: &mut Safekeeper, _query: &str) -> bool {
        todo!()
    }

    fn conn_get_query_result(
        &self,
        _sk: &mut Safekeeper,
    ) -> crate::bindings::WalProposerExecStatusType {
        todo!()
    }

    fn conn_flush(&self, _sk: &mut Safekeeper) -> i32 {
        todo!()
    }

    fn conn_finish(&self, _sk: &mut Safekeeper) {
        todo!()
    }

    fn conn_async_read(
        &self,
        _sk: &mut Safekeeper,
        _vec: &mut Vec<u8>,
    ) -> crate::bindings::PGAsyncReadResult {
        todo!()
    }

    fn conn_async_write(
        &self,
        _sk: &mut Safekeeper,
        _buf: &[u8],
    ) -> crate::bindings::PGAsyncWriteResult {
        todo!()
    }

    fn conn_blocking_write(&self, _sk: &mut Safekeeper, _buf: &[u8]) -> bool {
        todo!()
    }

    fn recovery_download(&self, _wp: &mut WalProposer, _sk: &mut Safekeeper) -> bool {
        todo!()
    }

    fn wal_reader_allocate(&self, _sk: &mut Safekeeper) -> NeonWALReadResult {
        todo!()
    }

    fn wal_read(&self, _sk: &mut Safekeeper, _buf: &mut [u8], _startpos: u64) -> NeonWALReadResult {
        todo!()
    }

    fn wal_reader_events(&self, _sk: &mut Safekeeper) -> u32 {
        todo!()
    }

    fn init_event_set(&self, _wp: &mut WalProposer) {
        todo!()
    }

    fn update_event_set(&self, _sk: &mut Safekeeper, _events_mask: u32) {
        todo!()
    }

    fn active_state_update_event_set(&self, _sk: &mut Safekeeper) {
        todo!()
    }

    fn add_safekeeper_event_set(&self, _sk: &mut Safekeeper, _events_mask: u32) {
        todo!()
    }

    fn rm_safekeeper_event_set(&self, _sk: &mut Safekeeper) {
        todo!()
    }

    fn wait_event_set(&self, _wp: &mut WalProposer, _timeout_millis: i64) -> WaitResult {
        todo!()
    }

    fn strong_random(&self, _buf: &mut [u8]) -> bool {
        todo!()
    }

    fn get_redo_start_lsn(&self) -> u64 {
        todo!()
    }

    fn finish_sync_safekeepers(&self, _lsn: u64) {
        todo!()
    }

    fn process_safekeeper_feedback(&mut self, _wp: &mut WalProposer, _sk: &mut Safekeeper) {
        todo!()
    }

    fn log_internal(&self, _wp: &mut WalProposer, _level: Level, _msg: &str) {
        todo!()
    }

    fn after_election(&self, _wp: &mut WalProposer) {
        todo!()
    }
}

#[derive(Debug)]
pub enum WaitResult {
    Latch,
    Timeout,
    Network(*mut Safekeeper, u32),
}

#[derive(Clone)]
pub struct Config {
    /// Tenant and timeline id
    pub ttid: TenantTimelineId,
    /// List of safekeepers in format `host:port`
    pub safekeepers_list: Vec<String>,
    /// Safekeeper reconnect timeout in milliseconds
    pub safekeeper_reconnect_timeout: i32,
    /// Safekeeper connection timeout in milliseconds
    pub safekeeper_connection_timeout: i32,
    /// walproposer mode, finish when all safekeepers are synced or subscribe
    /// to WAL streaming
    pub sync_safekeepers: bool,
}

/// WalProposer main struct. C methods are reexported as Rust functions.
pub struct Wrapper {
    wp: *mut WalProposer,
    _safekeepers_list_vec: Vec<u8>,
}

impl Wrapper {
    pub fn new(api: Box<dyn ApiImpl>, config: Config) -> Wrapper {
        let neon_tenant = CString::new(config.ttid.tenant_id.to_string())
            .unwrap()
            .into_raw();
        let neon_timeline = CString::new(config.ttid.timeline_id.to_string())
            .unwrap()
            .into_raw();

        let mut safekeepers_list_vec = CString::new(config.safekeepers_list.join(","))
            .unwrap()
            .into_bytes_with_nul();
        assert!(safekeepers_list_vec.len() == safekeepers_list_vec.capacity());
        let safekeepers_list = safekeepers_list_vec.as_mut_ptr() as *mut std::ffi::c_char;

        let callback_data = Box::into_raw(Box::new(api)) as *mut ::std::os::raw::c_void;

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
        let wp = unsafe { WalProposerCreate(c_config, api) };
        Wrapper {
            wp,
            _safekeepers_list_vec: safekeepers_list_vec,
        }
    }

    pub fn start(&self) {
        unsafe { WalProposerStart(self.wp) }
    }
}

impl Drop for Wrapper {
    fn drop(&mut self) {
        unsafe {
            let config = (*self.wp).config;
            drop(Box::from_raw(
                (*config).callback_data as *mut Box<dyn ApiImpl>,
            ));
            drop(CString::from_raw((*config).neon_tenant));
            drop(CString::from_raw((*config).neon_timeline));
            drop(Box::from_raw(config));

            for i in 0..(*self.wp).n_safekeepers {
                let sk = &mut (*self.wp).safekeeper[i as usize];
                take_vec_u8(&mut sk.inbuf);
            }

            WalProposerFree(self.wp);
        }
    }
}

pub struct StreamingCallback {
    wp: *mut WalProposer,
}

impl StreamingCallback {
    pub fn new(wp: *mut WalProposer) -> StreamingCallback {
        StreamingCallback { wp }
    }

    pub fn broadcast(&self, startpos: Lsn, endpos: Lsn) {
        unsafe { WalProposerBroadcast(self.wp, startpos.0, endpos.0) }
    }

    pub fn poll(&self) {
        unsafe { WalProposerPoll(self.wp) }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{
        cell::Cell,
        sync::{atomic::AtomicUsize, mpsc::sync_channel},
    };

    use std::cell::UnsafeCell;
    use utils::id::TenantTimelineId;

    use crate::{api_bindings::Level, bindings::NeonWALReadResult, walproposer::Wrapper};

    use super::ApiImpl;

    #[derive(Clone, Copy, Debug)]
    struct WaitEventsData {
        sk: *mut crate::bindings::Safekeeper,
        event_mask: u32,
    }

    struct MockImpl {
        // data to return from wait_event_set
        wait_events: Cell<WaitEventsData>,
        // walproposer->safekeeper messages
        expected_messages: Vec<Vec<u8>>,
        expected_ptr: AtomicUsize,
        // safekeeper->walproposer messages
        safekeeper_replies: Vec<Vec<u8>>,
        replies_ptr: AtomicUsize,
        // channel to send LSN to the main thread
        sync_channel: std::sync::mpsc::SyncSender<u64>,
        // Shmem state, used for storing donor info
        shmem: UnsafeCell<crate::bindings::WalproposerShmemState>,
    }

    impl MockImpl {
        fn check_walproposer_msg(&self, msg: &[u8]) {
            let ptr = self
                .expected_ptr
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            if ptr >= self.expected_messages.len() {
                panic!("unexpected message from walproposer");
            }

            let expected_msg = &self.expected_messages[ptr];
            assert_eq!(msg, expected_msg.as_slice());
        }

        fn next_safekeeper_reply(&self) -> &[u8] {
            let ptr = self
                .replies_ptr
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            if ptr >= self.safekeeper_replies.len() {
                panic!("no more safekeeper replies");
            }

            &self.safekeeper_replies[ptr]
        }
    }

    impl ApiImpl for MockImpl {
        fn get_shmem_state(&self) -> *mut crate::bindings::WalproposerShmemState {
            self.shmem.get()
        }

        fn get_current_timestamp(&self) -> i64 {
            println!("get_current_timestamp");
            0
        }

        fn update_donor(&self, donor: &mut crate::bindings::Safekeeper, donor_lsn: u64) {
            let mut shmem = unsafe { *self.get_shmem_state() };
            shmem.propEpochStartLsn.value = donor_lsn;
            shmem.donor_conninfo = donor.conninfo;
            shmem.donor_lsn = donor_lsn;
        }

        fn conn_status(
            &self,
            _: &mut crate::bindings::Safekeeper,
        ) -> crate::bindings::WalProposerConnStatusType {
            println!("conn_status");
            crate::bindings::WalProposerConnStatusType_WP_CONNECTION_OK
        }

        fn conn_connect_start(&self, _: &mut crate::bindings::Safekeeper) {
            println!("conn_connect_start");
        }

        fn conn_connect_poll(
            &self,
            _: &mut crate::bindings::Safekeeper,
        ) -> crate::bindings::WalProposerConnectPollStatusType {
            println!("conn_connect_poll");
            crate::bindings::WalProposerConnectPollStatusType_WP_CONN_POLLING_OK
        }

        fn conn_send_query(&self, _: &mut crate::bindings::Safekeeper, query: &str) -> bool {
            println!("conn_send_query: {}", query);
            true
        }

        fn conn_get_query_result(
            &self,
            _: &mut crate::bindings::Safekeeper,
        ) -> crate::bindings::WalProposerExecStatusType {
            println!("conn_get_query_result");
            crate::bindings::WalProposerExecStatusType_WP_EXEC_SUCCESS_COPYBOTH
        }

        fn conn_async_read(
            &self,
            _: &mut crate::bindings::Safekeeper,
            vec: &mut Vec<u8>,
        ) -> crate::bindings::PGAsyncReadResult {
            println!("conn_async_read");
            let reply = self.next_safekeeper_reply();
            println!("conn_async_read result: {:?}", reply);
            vec.extend_from_slice(reply);
            crate::bindings::PGAsyncReadResult_PG_ASYNC_READ_SUCCESS
        }

        fn conn_blocking_write(&self, _: &mut crate::bindings::Safekeeper, buf: &[u8]) -> bool {
            println!("conn_blocking_write: {:?}", buf);
            self.check_walproposer_msg(buf);
            true
        }

        fn recovery_download(
            &self,
            _wp: &mut crate::bindings::WalProposer,
            _sk: &mut crate::bindings::Safekeeper,
        ) -> bool {
            true
        }

        fn wal_reader_allocate(&self, _: &mut crate::bindings::Safekeeper) -> NeonWALReadResult {
            println!("wal_reader_allocate");
            crate::bindings::NeonWALReadResult_NEON_WALREAD_SUCCESS
        }

        fn init_event_set(&self, _: &mut crate::bindings::WalProposer) {
            println!("init_event_set")
        }

        fn update_event_set(&self, sk: &mut crate::bindings::Safekeeper, event_mask: u32) {
            println!(
                "update_event_set, sk={:?}, events_mask={:#b}",
                sk as *mut crate::bindings::Safekeeper, event_mask
            );
            self.wait_events.set(WaitEventsData { sk, event_mask });
        }

        fn add_safekeeper_event_set(&self, sk: &mut crate::bindings::Safekeeper, event_mask: u32) {
            println!(
                "add_safekeeper_event_set, sk={:?}, events_mask={:#b}",
                sk as *mut crate::bindings::Safekeeper, event_mask
            );
            self.wait_events.set(WaitEventsData { sk, event_mask });
        }

        fn rm_safekeeper_event_set(&self, sk: &mut crate::bindings::Safekeeper) {
            println!(
                "rm_safekeeper_event_set, sk={:?}",
                sk as *mut crate::bindings::Safekeeper
            );
        }

        fn wait_event_set(
            &self,
            _: &mut crate::bindings::WalProposer,
            timeout_millis: i64,
        ) -> super::WaitResult {
            let data = self.wait_events.get();
            println!(
                "wait_event_set, timeout_millis={}, res={:?}",
                timeout_millis, data
            );
            super::WaitResult::Network(data.sk, data.event_mask)
        }

        fn strong_random(&self, buf: &mut [u8]) -> bool {
            println!("strong_random");
            buf.fill(0);
            true
        }

        fn finish_sync_safekeepers(&self, lsn: u64) {
            self.sync_channel.send(lsn).unwrap();
            panic!("sync safekeepers finished at lsn={}", lsn);
        }

        fn log_internal(&self, _wp: &mut crate::bindings::WalProposer, level: Level, msg: &str) {
            println!("wp_log[{}] {}", level, msg);
        }

        fn after_election(&self, _wp: &mut crate::bindings::WalProposer) {
            println!("after_election");
        }
    }

    /// Test that walproposer can successfully connect to safekeeper and finish
    /// sync_safekeepers. API is mocked in MockImpl.
    ///
    /// Run this test with valgrind to detect leaks:
    /// `valgrind --leak-check=full target/debug/deps/walproposer-<build>`
    #[test]
    fn test_simple_sync_safekeepers() -> anyhow::Result<()> {
        let ttid = TenantTimelineId::new(
            "9e4c8f36063c6c6e93bc20d65a820f3d".parse()?,
            "9e4c8f36063c6c6e93bc20d65a820f3d".parse()?,
        );

        let (sender, receiver) = sync_channel(1);

        let my_impl: Box<dyn ApiImpl> = Box::new(MockImpl {
            wait_events: Cell::new(WaitEventsData {
                sk: std::ptr::null_mut(),
                event_mask: 0,
            }),
            expected_messages: vec![
                // TODO: When updating Postgres versions, this test will cause
                // problems. Postgres version in message needs updating.
                //
                // Greeting(ProposerGreeting { protocol_version: 2, pg_version: 160003, proposer_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], system_id: 0, timeline_id: 9e4c8f36063c6c6e93bc20d65a820f3d, tenant_id: 9e4c8f36063c6c6e93bc20d65a820f3d, tli: 1, wal_seg_size: 16777216 })
                vec![
                    103, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 3, 113, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 158, 76, 143, 54, 6, 60, 108, 110,
                    147, 188, 32, 214, 90, 130, 15, 61, 158, 76, 143, 54, 6, 60, 108, 110, 147,
                    188, 32, 214, 90, 130, 15, 61, 1, 0, 0, 0, 0, 0, 0, 1,
                ],
                // VoteRequest(VoteRequest { term: 3 })
                vec![
                    118, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0,
                ],
            ],
            expected_ptr: AtomicUsize::new(0),
            safekeeper_replies: vec![
                // Greeting(AcceptorGreeting { term: 2, node_id: NodeId(1) })
                vec![
                    103, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
                ],
                // VoteResponse(VoteResponse { term: 3, vote_given: 1, flush_lsn: 0/539, truncate_lsn: 0/539, term_history: [(2, 0/539)], timeline_start_lsn: 0/539 })
                vec![
                    118, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 57,
                    5, 0, 0, 0, 0, 0, 0, 57, 5, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0,
                    0, 57, 5, 0, 0, 0, 0, 0, 0, 57, 5, 0, 0, 0, 0, 0, 0,
                ],
            ],
            replies_ptr: AtomicUsize::new(0),
            sync_channel: sender,
            shmem: UnsafeCell::new(crate::api_bindings::empty_shmem()),
        });
        let config = crate::walproposer::Config {
            ttid,
            safekeepers_list: vec!["localhost:5000".to_string()],
            safekeeper_reconnect_timeout: 1000,
            safekeeper_connection_timeout: 10000,
            sync_safekeepers: true,
        };

        let wp = Wrapper::new(my_impl, config);

        // walproposer will panic when it finishes sync_safekeepers
        std::panic::catch_unwind(|| wp.start()).unwrap_err();
        // validate the resulting LSN
        assert_eq!(receiver.try_recv(), Ok(1337));
        Ok(())
        // drop() will free up resources here
    }
}
