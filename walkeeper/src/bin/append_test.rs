//
// Performance test for append handling in safekeeper.
//
use anyhow::Result;
use clap::{App, Arg};
use const_format::formatcp;
use daemonize::Daemonize;
use log::*;
use std::path::{Path, PathBuf};
use std::thread;
use walkeeper::defaults::{DEFAULT_HTTP_LISTEN_ADDR, DEFAULT_PG_LISTEN_ADDR};
use walkeeper::http;
use walkeeper::s3_offload;
use walkeeper::wal_service;
use walkeeper::SafeKeeperConf;
use walkeeper::timeline::{GlobalTimelines, Timeline};
use walkeeper::timeline::CreateControlFile;
use walkeeper::json_ctrl;
use zenith_utils::http::endpoint;
use zenith_utils::shutdown::exit_now;
use zenith_utils::signals;
use zenith_utils::{logging, tcp_listener, GIT_VERSION};
use zenith_utils::zid::{self, ZTimelineId, ZTenantId};
use std::sync::Arc;

use walkeeper::safekeeper::{AcceptorProposerMessage, AppendResponse};
use walkeeper::safekeeper::{
    AppendRequest, AppendRequestHeader, ProposerAcceptorMessage, ProposerElected, ProposerGreeting,
};
use walkeeper::safekeeper::{SafeKeeperState, Term, TermHistory, TermSwitchEntry};
use walkeeper::send_wal::SendWalHandler;
use walkeeper::timeline::TimelineTools;
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils;
use postgres_ffi::{uint32, uint64, Oid, XLogRecord};
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, RowDescriptor, TEXT_OID};

fn main() -> Result<()> {
    zenith_metrics::set_common_metrics_prefix("safekeeper");
    let log_file = logging::init("safekeeper.log", false)?;
    info!("version: {}", GIT_VERSION);
    
    let timeline_id = ZTimelineId::generate();
    let tenant_id = ZTenantId::generate();
    let create = CreateControlFile::True;
    let conf = SafeKeeperConf::default();

    let mut timeline = GlobalTimelines::get(&conf, tenant_id, timeline_id, create)?;
    
    json_ctrl::prepare_safekeeper(tenant_id ,timeline_id, &mut timeline)?;

    let term = 1;
    let epoch_start_lsn = Lsn::from(0x16B9188);
    let mut lsn = epoch_start_lsn;

    json_ctrl::send_proposer_elected(&timeline, term, lsn)?;

    let message = "a".repeat(1024 * 8);

    info!("Starting test");

    let now = std::time::Instant::now();
    let test_duration = std::time::Duration::from_secs(10);
    let timeout = now.checked_add(test_duration).unwrap();

    let mut total_count = 0;

    while std::time::Instant::now() < timeout {
        let result = json_ctrl::append_logical_message(&timeline, json_ctrl::AppendLogicalMessage{
            lm_prefix: "".to_string(),
            lm_message: message.clone(),
            set_commit_lsn: true,
            send_proposer_elected: false,
            term,
            epoch_start_lsn,
            begin_lsn: lsn,
            truncate_lsn: lsn,
        })?;

        lsn = result.end_lsn;
        total_count += 1;
    }

    info!("Total count: {}", total_count);
    info!("Test duration: {}s", test_duration.as_secs_f64());
    info!("Appends per second: {}", total_count as f64 / test_duration.as_secs_f64());

    Ok(())
}
