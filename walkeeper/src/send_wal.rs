//! Part of Safekeeper pretending to be Postgres, streaming xlog to
//! pageserver/any other consumer.
//!

use crate::json_ctrl::handle_json_ctrl;
use crate::receive_wal::ReceiveWalConn;
use crate::replication::ReplicationConn;
use crate::timeline::{Timeline, TimelineTools};
use crate::SafeKeeperConf;
use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use postgres_ffi::xlog_utils::PG_TLI;
use std::str::FromStr;
use std::sync::Arc;
use zenith_utils::postgres_backend;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeStartupMessage, RowDescriptor, INT4_OID, TEXT_OID};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::timeline::CreateControlFile;

/// Handler for streaming WAL from acceptor
pub struct SendWalHandler {
    pub conf: SafeKeeperConf,
    /// assigned application name
    pub appname: Option<String>,
    pub tenantid: Option<ZTenantId>,
    pub timelineid: Option<ZTimelineId>,
    pub timeline: Option<Arc<Timeline>>,
}

impl postgres_backend::Handler for SendWalHandler {
    fn startup(&mut self, _pgb: &mut PostgresBackend, sm: &FeStartupMessage) -> Result<()> {
        let ztimelineid = sm
            .params
            .get("ztimelineid")
            .ok_or_else(|| anyhow!("timelineid is required"))?;
        self.timelineid = Some(ZTimelineId::from_str(ztimelineid)?);

        let ztenantid = sm
            .params
            .get("ztenantid")
            .ok_or_else(|| anyhow!("tenantid is required"))?;
        self.tenantid = Some(ZTenantId::from_str(ztenantid)?);

        if let Some(app_name) = sm.params.get("application_name") {
            self.appname = Some(app_name.clone());
        }

        Ok(())
    }

    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: Bytes) -> Result<()> {
        // START_WAL_PUSH is the only command that initializes the timeline in production.
        // There is also JSON_CTRL command, which should initialize the timeline for testing.
        if self.timeline.is_none() {
            if query_string.starts_with(b"START_WAL_PUSH") || query_string.starts_with(b"JSON_CTRL")
            {
                self.timeline.set(
                    &self.conf,
                    self.tenantid.unwrap(),
                    self.timelineid.unwrap(),
                    CreateControlFile::True,
                )?;
            } else {
                self.timeline.set(
                    &self.conf,
                    self.tenantid.unwrap(),
                    self.timelineid.unwrap(),
                    CreateControlFile::False,
                )?;
            }
        }
        if query_string.starts_with(b"IDENTIFY_SYSTEM") {
            self.handle_identify_system(pgb)?;
        } else if query_string.starts_with(b"START_REPLICATION") {
            ReplicationConn::new(pgb).run(self, pgb, &query_string)?;
        } else if query_string.starts_with(b"START_WAL_PUSH") {
            // TODO: this repeats query decoding logic from page_service so it is probably
            // a good idea to refactor it in pgbackend and pass string to process query instead of bytes
            let decoded_query_string = match query_string.last() {
                Some(0) => std::str::from_utf8(&query_string[..query_string.len() - 1])?,
                _ => std::str::from_utf8(&query_string)?,
            };
            let pageserver_connstr = decoded_query_string
                .split_whitespace()
                .nth(1)
                .map(|s| s.to_owned());
            ReceiveWalConn::new(pgb, pageserver_connstr)
                .run(self)
                .with_context(|| "failed to run ReceiveWalConn")?;
        } else if query_string.starts_with(b"JSON_CTRL") {
            handle_json_ctrl(self, pgb, &query_string)?;
        } else {
            bail!("Unexpected command {:?}", query_string);
        }
        Ok(())
    }
}

impl SendWalHandler {
    pub fn new(conf: SafeKeeperConf) -> Self {
        SendWalHandler {
            conf,
            appname: None,
            tenantid: None,
            timelineid: None,
            timeline: None,
        }
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self, pgb: &mut PostgresBackend) -> Result<()> {
        let start_pos = self.timeline.get().get_end_of_wal();
        let lsn = start_pos.to_string();
        let sysid = self.timeline.get().get_info().server.system_id.to_string();
        let lsn_bytes = lsn.as_bytes();
        let tli = PG_TLI.to_string();
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
                typoid: INT4_OID,
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
}
