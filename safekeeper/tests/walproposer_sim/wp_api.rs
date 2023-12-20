use std::{
    cell::{RefCell, RefMut, UnsafeCell},
    ffi::CStr,
    sync::Arc,
};

use bytes::Bytes;
use desim::{
    network::TCP,
    node_os::NodeOs,
    proto::AnyMessage,
    world::{NodeEvent, NodeId},
};
use tracing::debug;
use utils::lsn::Lsn;
use walproposer::{
    api_bindings::Level,
    bindings::{pg_atomic_uint64, PageserverFeedback, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE},
    walproposer::{ApiImpl, Config},
};

use super::disk_walproposer::DiskWalProposer;

struct SafekeeperConn {
    host: String,
    port: String,
    node_id: NodeId,
    socket: Option<TCP>,
    // connection is in progress
    is_connecting: bool,
    // START_WAL_PUSH is in progress
    is_start_wal_push: bool,
    raw_ptr: *mut walproposer::bindings::Safekeeper,
}

impl SafekeeperConn {
    pub fn new(host: String, port: String) -> Self {
        // port number is the same as NodeId
        let port_num = port.parse::<u32>().unwrap();
        Self {
            host,
            port,
            node_id: port_num,
            socket: None,
            is_connecting: false,
            is_start_wal_push: false,
            raw_ptr: std::ptr::null_mut(),
        }
    }
}

pub struct SimulationApi {
    os: NodeOs,
    safekeepers: RefCell<Vec<SafekeeperConn>>,
    disk: Arc<DiskWalProposer>,
    redo_start_lsn: Option<Lsn>,
    shmem: UnsafeCell<walproposer::bindings::WalproposerShmemState>,
    config: Config,
}

pub struct Args {
    pub os: NodeOs,
    pub config: Config,
    pub disk: Arc<DiskWalProposer>,
    pub redo_start_lsn: Option<Lsn>,
}

impl SimulationApi {
    pub fn new(args: Args) -> Self {
        // initialize connection state for each safekeeper
        let sk_conns = args
            .config
            .safekeepers_list
            .iter()
            .map(|s| {
                SafekeeperConn::new(
                    s.split(':').next().unwrap().to_string(),
                    s.split(':').nth(1).unwrap().to_string(),
                )
            })
            .collect::<Vec<_>>();

        Self {
            os: args.os,
            safekeepers: RefCell::new(sk_conns),
            disk: args.disk,
            redo_start_lsn: args.redo_start_lsn,
            shmem: UnsafeCell::new(walproposer::bindings::WalproposerShmemState {
                mutex: 0,
                feedback: PageserverFeedback {
                    currentClusterSize: 0,
                    last_received_lsn: 0,
                    disk_consistent_lsn: 0,
                    remote_consistent_lsn: 0,
                    replytime: 0,
                },
                mineLastElectedTerm: 0,
                backpressureThrottlingTime: pg_atomic_uint64 { value: 0 },
            }),
            config: args.config,
        }
    }

    /// Get SafekeeperConn for the given Safekeeper.
    fn get_conn(&self, sk: &mut walproposer::bindings::Safekeeper) -> RefMut<'_, SafekeeperConn> {
        let sk_port = unsafe { CStr::from_ptr(sk.port).to_str().unwrap() };
        let state = self.safekeepers.borrow_mut();
        RefMut::map(state, |v| {
            v.iter_mut()
                .find(|conn| conn.port == sk_port)
                .expect("safekeeper conn not found by port")
        })
    }

    /// Find existing connection by TCP object.
    fn find_conn(&self, tcp: TCP) -> Option<RefMut<'_, SafekeeperConn>> {
        let state = self.safekeepers.borrow_mut();
        RefMut::filter_map(state, |v| {
            v.iter_mut()
                .find(|conn| conn.socket.as_ref().is_some_and(|s| s.connection_id() == tcp.connection_id()))
        })
        .ok()
    }
}

impl ApiImpl for SimulationApi {
    fn get_current_timestamp(&self) -> i64 {
        debug!("get_current_timestamp");
        0
    }

    fn conn_status(
        &self,
        _: &mut walproposer::bindings::Safekeeper,
    ) -> walproposer::bindings::WalProposerConnStatusType {
        debug!("conn_status");
        if self.os.random(100) < 10 {
            walproposer::bindings::WalProposerConnStatusType_WP_CONNECTION_BAD
        } else {
            walproposer::bindings::WalProposerConnStatusType_WP_CONNECTION_OK
        }
    }

    fn conn_connect_start(&self, sk: &mut walproposer::bindings::Safekeeper) {
        debug!("conn_connect_start");
        let mut conn = self.get_conn(sk);

        assert!(conn.socket.is_none());
        let socket = self.os.open_tcp(conn.node_id);
        conn.socket = Some(socket);
        conn.raw_ptr = sk;
        conn.is_connecting = true;
    }

    fn conn_connect_poll(
        &self,
        _: &mut walproposer::bindings::Safekeeper,
    ) -> walproposer::bindings::WalProposerConnectPollStatusType {
        debug!("conn_connect_poll");
        walproposer::bindings::WalProposerConnectPollStatusType_WP_CONN_POLLING_OK
    }

    fn conn_send_query(&self, sk: &mut walproposer::bindings::Safekeeper, query: &str) -> bool {
        debug!("conn_send_query: {}", query);
        self.get_conn(sk).is_start_wal_push = true;
        true
    }

    fn conn_get_query_result(
        &self,
        _: &mut walproposer::bindings::Safekeeper,
    ) -> walproposer::bindings::WalProposerExecStatusType {
        debug!("conn_get_query_result");
        walproposer::bindings::WalProposerExecStatusType_WP_EXEC_SUCCESS_COPYBOTH
    }

    fn conn_async_read(
        &self,
        sk: &mut walproposer::bindings::Safekeeper,
        vec: &mut Vec<u8>,
    ) -> walproposer::bindings::PGAsyncReadResult {
        debug!("conn_async_read");
        let conn = self.get_conn(sk);
        if conn.socket.is_none() {
            // socket is already closed
            return walproposer::bindings::PGAsyncReadResult_PG_ASYNC_READ_FAIL;
        }
        let peeked = self.os.epoll_peek(0);
        match peeked {
            Some(NodeEvent::Message((_, tcp))) => {
                if tcp.connection_id() != conn.socket.as_ref().unwrap().connection_id() {
                    return walproposer::bindings::PGAsyncReadResult_PG_ASYNC_READ_TRY_AGAIN;
                }
            }
            _ => return walproposer::bindings::PGAsyncReadResult_PG_ASYNC_READ_TRY_AGAIN,
        }

        let event = self
            .os
            .epoll_recv(0)
            .expect("message from safekeeper is ready");
        let msg = match event {
            desim::world::NodeEvent::Message((msg, tcp)) => {
                assert!(tcp.id() == conn.socket.as_ref().unwrap().id());
                msg
            }
            _ => unreachable!(),
        };

        let b = match msg {
            desim::proto::AnyMessage::Bytes(b) => b,
            _ => unreachable!(),
        };

        vec.extend_from_slice(&b);

        walproposer::bindings::PGAsyncReadResult_PG_ASYNC_READ_SUCCESS
    }

    fn conn_blocking_write(&self, sk: &mut walproposer::bindings::Safekeeper, buf: &[u8]) -> bool {
        let mut conn = self.get_conn(sk);
        debug!("conn_blocking_write to {}: {:?}", conn.node_id, buf);
        let socket = conn.socket.as_mut().unwrap();
        socket.send(desim::proto::AnyMessage::Bytes(Bytes::copy_from_slice(
            buf,
        )));
        true
    }

    fn conn_async_write(
        &self,
        sk: &mut walproposer::bindings::Safekeeper,
        buf: &[u8],
    ) -> walproposer::bindings::PGAsyncWriteResult {
        let mut conn = self.get_conn(sk);
        debug!("conn_async_write to {}: {:?}", conn.node_id, buf);
        if let Some(socket) = conn.socket.as_mut() {
            socket.send(desim::proto::AnyMessage::Bytes(Bytes::copy_from_slice(
                buf,
            )));
        } else {
            // connection is already closed
            debug!("conn_async_write: writing to a closed socket!");
            // TODO: maybe we should return error here?
        }
        walproposer::bindings::PGAsyncWriteResult_PG_ASYNC_WRITE_SUCCESS
    }

    fn wal_reader_allocate(&self, _: &mut walproposer::bindings::Safekeeper) {
        debug!("wal_reader_allocate")
    }

    fn wal_read(&self, _sk: &mut walproposer::bindings::Safekeeper, buf: &mut [u8], startpos: u64) {
        self.disk.lock().read(startpos, buf);
    }

    fn free_event_set(&self, _: &mut walproposer::bindings::WalProposer) {
        debug!("free_event_set")
    }

    fn init_event_set(&self, _: &mut walproposer::bindings::WalProposer) {
        debug!("init_event_set")
    }

    fn update_event_set(&self, sk: &mut walproposer::bindings::Safekeeper, event_mask: u32) {
        debug!(
            "update_event_set, sk={:?}, events_mask={:#b}",
            sk as *mut walproposer::bindings::Safekeeper, event_mask
        );
    }

    fn add_safekeeper_event_set(
        &self,
        sk: &mut walproposer::bindings::Safekeeper,
        event_mask: u32,
    ) {
        debug!(
            "add_safekeeper_event_set, sk={:?}, events_mask={:#b}",
            sk as *mut walproposer::bindings::Safekeeper, event_mask
        );
    }

    fn wait_event_set(
        &self,
        _: &mut walproposer::bindings::WalProposer,
        timeout_millis: i64,
    ) -> walproposer::walproposer::WaitResult {
        let mut conns = self.safekeepers.borrow_mut();
        for conn in conns.iter_mut() {
            if conn.socket.is_some() && conn.is_connecting {
                conn.is_connecting = false;
                debug!("wait_event_set, connecting to {}:{}", conn.host, conn.port);
                return walproposer::walproposer::WaitResult::Network(
                    conn.raw_ptr,
                    WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE,
                );
            }
            if conn.socket.is_some() && conn.is_start_wal_push {
                conn.is_start_wal_push = false;
                debug!(
                    "wait_event_set, start wal push to {}:{}",
                    conn.host, conn.port
                );
                return walproposer::walproposer::WaitResult::Network(
                    conn.raw_ptr,
                    WL_SOCKET_READABLE,
                );
            }
        }
        drop(conns);

        let peek = self.os.epoll_peek(timeout_millis);
        debug!(
            "wait_event_set, timeout_millis={}, peek={:?}",
            timeout_millis, peek
        );

        let event: desim::world::NodeEvent = match peek {
            Some(event) => event,
            None => return walproposer::walproposer::WaitResult::Timeout,
        };

        let res = match event {
            desim::world::NodeEvent::Closed(tcp) => {
                let ev2 = self.os.epoll_recv(0);
                assert!(ev2.is_some());
                let mut sk = self.find_conn(tcp);
                if let Some(sk) = sk.as_mut() {
                    sk.socket = None;
                    walproposer::walproposer::WaitResult::Network(sk.raw_ptr, WL_SOCKET_READABLE)
                } else {
                    // connection is already closed
                    // TODO!
                    walproposer::walproposer::WaitResult::Latch
                }
            }
            desim::world::NodeEvent::Message((msg, tcp)) => {
                let _ = match msg {
                    desim::proto::AnyMessage::Bytes(b) => b,
                    _ => unreachable!(),
                };
                // walproposer must read the message
                walproposer::walproposer::WaitResult::Network(
                    self.find_conn(tcp).unwrap().raw_ptr,
                    WL_SOCKET_READABLE,
                )
            }
            desim::world::NodeEvent::Internal(_) => {
                let ev2 = self.os.epoll_recv(0);
                assert!(ev2.is_some());
                // TODO: distinguish different types?
                walproposer::walproposer::WaitResult::Latch
            }
            desim::world::NodeEvent::Accept(_) => unreachable!(),
            desim::world::NodeEvent::WakeTimeout(_) => unreachable!(),
        };
        debug!(
            "wait_event_set, timeout_millis={}, res={:?}",
            timeout_millis, res,
        );
        res
    }

    fn strong_random(&self, buf: &mut [u8]) -> bool {
        debug!("strong_random");
        buf.fill(0);
        true
    }

    fn finish_sync_safekeepers(&self, lsn: u64) {
        debug!("finish_sync_safekeepers, lsn={}", lsn);
        self.os.set_result(0, Lsn(lsn).to_string());
        self.os
            .exit(format!("sync safekeepers finished at lsn={}", lsn));
    }

    fn log_internal(&self, _wp: &mut walproposer::bindings::WalProposer, level: Level, msg: &str) {
        debug!("walprop_log[{}] {}", level, msg);
        if level == Level::Fatal || level == Level::Panic {
            if msg == "Failed to recover state" {
                // Recovery connection broken in the middle of recovery
                self.os.exit(msg.to_owned());
            }
            if msg.contains("rejects our connection request with term") {
                // collected quorum with lower term, then got rejected by next connected safekeeper
                self.os.exit(msg.to_owned());
            }
            if msg.contains("collected propEpochStartLsn") && msg.contains(", but basebackup LSN ")
            {
                // sync-safekeepers collected wrong quorum, walproposer collected another quorum
                self.os.exit(msg.to_owned());
            }
            panic!("unknown FATAL error from walproposer: {}", msg);
        }
    }

    fn after_election(&self, wp: &mut walproposer::bindings::WalProposer) {
        let prop_lsn = wp.propEpochStartLsn;
        let prop_term = wp.propTerm;

        let mut prev_lsn: u64 = 0;
        let mut prev_term: u64 = 0;

        unsafe {
            let history = wp.propTermHistory.entries;
            let len = wp.propTermHistory.n_entries as usize;
            if len > 1 {
                let entry = *history.wrapping_add(len - 2);
                prev_lsn = entry.lsn;
                prev_term = entry.term;
            }
        }

        let msg = format!(
            "prop_elected;{};{};{};{}",
            prop_lsn, prop_term, prev_lsn, prev_term
        );

        debug!(msg);
        self.os.log_event(msg);
    }

    fn get_redo_start_lsn(&self) -> u64 {
        debug!("get_redo_start_lsn -> {:?}", self.redo_start_lsn);
        self.redo_start_lsn.expect("redo_start_lsn is not set").0
    }

    fn get_shmem_state(&self) -> *mut walproposer::bindings::WalproposerShmemState {
        self.shmem.get()
    }

    fn start_streaming(
        &self,
        startpos: u64,
        callback: &walproposer::walproposer::StreamingCallback,
    ) {
        let disk = &self.disk;
        let disk_lsn = disk.lock().flush_rec_ptr().0;
        debug!("start_streaming at {} (disk_lsn={})", startpos, disk_lsn);
        if startpos < disk_lsn {
            debug!("startpos < disk_lsn, it means we wrote some transaction even before streaming started");
        }
        assert!(startpos <= disk_lsn);
        let mut broadcasted = Lsn(startpos);

        loop {
            let available = disk.lock().flush_rec_ptr();
            assert!(available >= broadcasted);
            callback.broadcast(broadcasted, available);
            broadcasted = available;
            callback.poll();
        }
    }

    fn process_safekeeper_feedback(
        &self,
        wp: &mut walproposer::bindings::WalProposer,
        commit_lsn: u64,
    ) {
        debug!("process_safekeeper_feedback, commit_lsn={}", commit_lsn);
        if commit_lsn > wp.lastSentCommitLsn {
            self.os.log_event(format!("commit_lsn;{}", commit_lsn));
        }
    }

    fn get_flush_rec_ptr(&self) -> u64 {
        let lsn = self.disk.lock().flush_rec_ptr();
        debug!("get_flush_rec_ptr: {}", lsn);
        lsn.0
    }

    fn confirm_wal_streamed(&self, _wp: &mut walproposer::bindings::WalProposer, lsn: u64) {
        debug!("confirm_wal_streamed: {}", Lsn(lsn))
    }

    fn recovery_download(
        &self,
        sk: &mut walproposer::bindings::Safekeeper,
        mut startpos: u64,
        endpos: u64,
    ) -> bool {
        let replication_prompt = format!(
            "START_REPLICATION {} {} {} {}",
            self.config.ttid.tenant_id, self.config.ttid.timeline_id, startpos, endpos,
        );
        let async_conn = self.get_conn(sk);
        debug!(
            "recovery_download from {} to {}, sk={}",
            startpos, endpos, async_conn.node_id
        );

        let conn = self.os.open_tcp_nopoll(async_conn.node_id);
        conn.send(desim::proto::AnyMessage::Bytes(replication_prompt.into()));

        while startpos < endpos {
            let event = conn.recv();
            match event {
                NodeEvent::Closed(_) => {
                    debug!("connection closed in recovery");
                    break;
                }
                NodeEvent::Message((AnyMessage::Bytes(b), _)) => {
                    debug!("got recovery bytes from safekeeper");
                    self.disk.lock().write(startpos, &b);
                    startpos += b.len() as u64;
                }
                NodeEvent::Message(_) => unreachable!(),
                NodeEvent::Accept(_) => unreachable!(),
                NodeEvent::Internal(_) => unreachable!(),
                NodeEvent::WakeTimeout(_) => unreachable!(),
            }
        }

        debug!("recovery finished at {}", startpos);

        startpos == endpos
    }

    fn conn_finish(&self, sk: &mut walproposer::bindings::Safekeeper) {
        let mut conn = self.get_conn(sk);
        debug!("conn_finish to {}", conn.node_id);
        if let Some(socket) = conn.socket.as_mut() {
            socket.close();
        } else {
            // connection is already closed
        }
        conn.socket = None;
    }

    fn conn_error_message(&self, _sk: &mut walproposer::bindings::Safekeeper) -> String {
        "connection is closed, probably".into()
    }
}
