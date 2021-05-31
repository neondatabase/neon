//! Part of WAL acceptor pretending to be Postgres, streaming xlog to
//! pageserver/any other consumer.
//!

use crate::replication::ReplicationConn;
use crate::timeline::{Timeline, TimelineTools};
use crate::WalAcceptorConf;
use anyhow::{bail, Result};
use bytes::Bytes;
use pageserver::ZTimelineId;
use std::str::FromStr;
use std::sync::Arc;
use zenith_utils::postgres_backend;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeStartupMessage, RowDescriptor};

/// Handler for streaming WAL from acceptor
pub struct SendWalHandler {
    /// wal acceptor configuration
    pub conf: WalAcceptorConf,
    /// assigned application name
    pub appname: Option<String>,
    pub timeline: Option<Arc<Timeline>>,
}

impl postgres_backend::Handler for SendWalHandler {
    fn startup(&mut self, _pgb: &mut PostgresBackend, sm: &FeStartupMessage) -> Result<()> {
        match sm.params.get("ztimelineid") {
            Some(ref ztimelineid) => {
                let ztlid = ZTimelineId::from_str(ztimelineid)?;
                self.timeline.set(ztlid)?;
            }
            _ => bail!("timelineid is required"),
        }
        if let Some(app_name) = sm.params.get("application_name") {
            self.appname = Some(app_name.clone());
        }
        Ok(())
    }

    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: Bytes) -> Result<()> {
        if query_string.starts_with(b"IDENTIFY_SYSTEM") {
            self.handle_identify_system(pgb)?;
            Ok(())
        } else if query_string.starts_with(b"START_REPLICATION") {
            ReplicationConn::new(pgb).run(self, pgb, &query_string)?;
            Ok(())
        } else {
            bail!("Unexpected command {:?}", query_string);
        }
    }
}

impl SendWalHandler {
    pub fn new(conf: WalAcceptorConf) -> Self {
        SendWalHandler {
            conf,
            appname: None,
            timeline: None,
        }
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self, pgb: &mut PostgresBackend) -> Result<()> {
        let (start_pos, timeline) = self.timeline.find_end_of_wal(&self.conf.data_dir, false);
        let lsn = start_pos.to_string();
        let tli = timeline.to_string();
        let sysid = self.timeline.get().get_info().server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli_bytes = tli.as_bytes();
        let sysid_bytes = sysid.as_bytes();

        pgb.write_message_noflush(&BeMessage::RowDescription(&[
            RowDescriptor {
                name: b"systemid",
                typoid: 25,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"timeline",
                typoid: 23,
                typlen: 4,
                ..Default::default()
            },
            RowDescriptor {
                name: b"xlogpos",
                typoid: 25,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"dbname",
                typoid: 25,
                typlen: -1,
                ..Default::default()
            },
        ]))?
        .write_message_noflush(&BeMessage::DataRow(&[
            Some(sysid_bytes),
            Some(tli_bytes),
            Some(lsn_bytes),
            None,
        ]))?
        .write_message(&BeMessage::CommandComplete(b"IDENTIFY_SYSTEM"))?;
        Ok(())
    }
}
