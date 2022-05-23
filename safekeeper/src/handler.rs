//! Part of Safekeeper pretending to be Postgres, i.e. handling Postgres
//! protocol commands.

use crate::json_ctrl::{handle_json_ctrl, AppendLogicalMessage};
use crate::receive_wal::ReceiveWalConn;
use crate::safekeeper::{AcceptorProposerMessage, ProposerAcceptorMessage};
use crate::send_wal::ReplicationConn;
use crate::timeline::{Timeline, TimelineTools};
use crate::SafeKeeperConf;
use anyhow::{bail, Context, Result};

use postgres_ffi::xlog_utils::PG_TLI;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;
use utils::{
    lsn::Lsn,
    postgres_backend::{self, PostgresBackend},
    pq_proto::{BeMessage, FeStartupPacket, RowDescriptor, INT4_OID, TEXT_OID},
    zid::{TenantId, ZTenantTimelineId, ZTimelineId},
};

/// Safekeeper handler of postgres commands
pub struct SafekeeperPostgresHandler {
    pub conf: SafeKeeperConf,
    /// assigned application name
    pub appname: Option<String>,
    pub TenantId: Option<TenantId>,
    pub ztimelineid: Option<ZTimelineId>,
    pub timeline: Option<Arc<Timeline>>,
    pageserver_connstr: Option<String>,
}

/// Parsed Postgres command.
enum SafekeeperPostgresCommand {
    StartWalPush { pageserver_connstr: Option<String> },
    StartReplication { start_lsn: Lsn },
    IdentifySystem,
    JSONCtrl { cmd: AppendLogicalMessage },
}

fn parse_cmd(cmd: &str) -> Result<SafekeeperPostgresCommand> {
    if cmd.starts_with("START_WAL_PUSH") {
        let re = Regex::new(r"START_WAL_PUSH(?: (.+))?").unwrap();

        let caps = re.captures(cmd).unwrap();
        let pageserver_connstr = caps.get(1).map(|m| m.as_str().to_owned());
        Ok(SafekeeperPostgresCommand::StartWalPush { pageserver_connstr })
    } else if cmd.starts_with("START_REPLICATION") {
        let re =
            Regex::new(r"START_REPLICATION(?: PHYSICAL)? ([[:xdigit:]]+/[[:xdigit:]]+)").unwrap();
        let mut caps = re.captures_iter(cmd);
        let start_lsn = caps
            .next()
            .map(|cap| cap[1].parse::<Lsn>())
            .context("failed to parse start LSN from START_REPLICATION command")??;
        Ok(SafekeeperPostgresCommand::StartReplication { start_lsn })
    } else if cmd.starts_with("IDENTIFY_SYSTEM") {
        Ok(SafekeeperPostgresCommand::IdentifySystem)
    } else if cmd.starts_with("JSON_CTRL") {
        let cmd = cmd.strip_prefix("JSON_CTRL").context("invalid prefix")?;
        Ok(SafekeeperPostgresCommand::JSONCtrl {
            cmd: serde_json::from_str(cmd)?,
        })
    } else {
        bail!("unsupported command {}", cmd);
    }
}

impl postgres_backend::Handler for SafekeeperPostgresHandler {
    // ztenant id and ztimeline id are passed in connection string params
    fn startup(&mut self, _pgb: &mut PostgresBackend, sm: &FeStartupPacket) -> Result<()> {
        if let FeStartupPacket::StartupMessage { params, .. } = sm {
            self.TenantId = match params.get("TenantId") {
                Some(z) => Some(TenantId::from_str(z)?), // just curious, can I do that from .map?
                _ => None,
            };

            self.ztimelineid = match params.get("ztimelineid") {
                Some(z) => Some(ZTimelineId::from_str(z)?),
                _ => None,
            };

            if let Some(app_name) = params.get("application_name") {
                self.appname = Some(app_name.clone());
            }

            self.pageserver_connstr = params.get("pageserver_connstr").cloned();

            Ok(())
        } else {
            bail!("Safekeeper received unexpected initial message: {:?}", sm);
        }
    }

    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: &str) -> Result<()> {
        let cmd = parse_cmd(query_string)?;

        info!("got query {:?}", query_string);

        let create = !(matches!(cmd, SafekeeperPostgresCommand::StartReplication { .. })
            || matches!(cmd, SafekeeperPostgresCommand::IdentifySystem));

        let tenantid = self.TenantId.context("tenantid is required")?;
        let timelineid = self.ztimelineid.context("timelineid is required")?;
        if self.timeline.is_none() {
            self.timeline.set(
                &self.conf,
                ZTenantTimelineId::new(tenantid, timelineid),
                create,
            )?;
        }

        match cmd {
            SafekeeperPostgresCommand::StartWalPush { pageserver_connstr } => {
                ReceiveWalConn::new(pgb, pageserver_connstr)
                    .run(self)
                    .context("failed to run ReceiveWalConn")?;
            }
            SafekeeperPostgresCommand::StartReplication { start_lsn } => {
                ReplicationConn::new(pgb)
                    .run(self, pgb, start_lsn, self.pageserver_connstr.clone())
                    .context("failed to run ReplicationConn")?;
            }
            SafekeeperPostgresCommand::IdentifySystem => {
                self.handle_identify_system(pgb)?;
            }
            SafekeeperPostgresCommand::JSONCtrl { ref cmd } => {
                handle_json_ctrl(self, pgb, cmd)?;
            }
        }
        Ok(())
    }
}

impl SafekeeperPostgresHandler {
    pub fn new(conf: SafeKeeperConf) -> Self {
        SafekeeperPostgresHandler {
            conf,
            appname: None,
            TenantId: None,
            ztimelineid: None,
            timeline: None,
            pageserver_connstr: None,
        }
    }

    /// Shortcut for calling `process_msg` in the timeline.
    pub fn process_safekeeper_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        self.timeline
            .get()
            .process_msg(msg)
            .context("failed to process ProposerAcceptorMessage")
    }

    ///
    /// Handle IDENTIFY_SYSTEM replication command
    ///
    fn handle_identify_system(&mut self, pgb: &mut PostgresBackend) -> Result<()> {
        let start_pos = self.timeline.get().get_end_of_wal();
        let lsn = start_pos.to_string();
        let sysid = self
            .timeline
            .get()
            .get_state()
            .1
            .server
            .system_id
            .to_string();
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
