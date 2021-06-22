//! Part of WAL acceptor pretending to be Postgres, streaming xlog to
//! pageserver/any other consumer and answering to some utility queries.
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
use zenith_utils::pq_proto::{BeMessage, FeStartupMessage, RowDescriptor, JSON_OID, TEXT_OID};

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
            Some(ref ztimelineid) => match ZTimelineId::from_str(ztimelineid) {
                Ok(ztlid) => {
                    self.timeline.set(&self.conf, ztlid, false)?;
                }
                Err(e) => {
                    bail!("failed to parse ztimelineid: {}", e)
                }
            },
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
        } else if query_string.starts_with(b"START_REPLICATION") {
            ReplicationConn::new(pgb).run(self, pgb, &query_string)?;
        } else if query_string.starts_with(b"state") {
            self.handle_state(pgb)?;
        } else if query_string.to_ascii_lowercase().starts_with(b"set ") {
            // have it because psycopg2 executes "SET datestyle TO 'ISO'"
            // on connect
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else {
            bail!("Unexpected command {:?}", query_string);
        }
        Ok(())
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
                typoid: TEXT_OID,
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
                typoid: TEXT_OID,
                typlen: -1,
                ..Default::default()
            },
            RowDescriptor {
                name: b"dbname",
                typoid: TEXT_OID,
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

    /// Send current state of safekeeper
    fn handle_state(&mut self, pgb: &mut PostgresBackend) -> Result<()> {
        let my_info = self.timeline.get().get_info();
        pgb.write_message_noflush(&BeMessage::RowDescription(&[RowDescriptor {
            name: b"state",
            typoid: JSON_OID,
            typlen: -1,
            ..Default::default()
        }]))?
        .write_message_noflush(&BeMessage::DataRow(&[Some(
            serde_json::to_string(&my_info).unwrap().as_bytes(),
        )]))?
        .write_message(&BeMessage::CommandComplete(b"IDENTIFY_SYSTEM"))?;

        Ok(())
    }
}
