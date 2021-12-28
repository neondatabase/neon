//! Part of Safekeeper pretending to be Postgres, i.e. handling Postgres
//! protocol commands.

use crate::json_ctrl::{handle_json_ctrl, AppendLogicalMessage};
use crate::receive_wal::ReceiveWalConn;
use crate::send_wal::ReplicationConn;
use crate::timeline::{Timeline, TimelineTools};
use crate::SafeKeeperConf;
use anyhow::{anyhow, bail, Context, Result};

use postgres_ffi::xlog_utils::PG_TLI;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::pq_proto::{BeMessage, FeStartupPacket, RowDescriptor, INT4_OID, TEXT_OID};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::callmemaybe::CallmeEvent;
use crate::timeline::CreateControlFile;
use tokio::sync::mpsc::UnboundedSender;

/// Safekeeper handler of postgres commands
pub struct SafekeeperPostgresHandler {
    pub conf: SafeKeeperConf,
    /// assigned application name
    pub appname: Option<String>,
    pub ztenantid: Option<ZTenantId>,
    pub ztimelineid: Option<ZTimelineId>,
    pub timeline: Option<Arc<Timeline>>,
    //sender to communicate with callmemaybe thread
    pub tx: UnboundedSender<CallmeEvent>,
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
            .ok_or_else(|| anyhow!("failed to parse start LSN from START_REPLICATION command"))??;
        Ok(SafekeeperPostgresCommand::StartReplication { start_lsn })
    } else if cmd.starts_with("IDENTIFY_SYSTEM") {
        Ok(SafekeeperPostgresCommand::IdentifySystem)
    } else if cmd.starts_with("JSON_CTRL") {
        let cmd = cmd
            .strip_prefix("JSON_CTRL")
            .ok_or_else(|| anyhow!("invalid prefix"))?;
        let parsed_cmd: AppendLogicalMessage = serde_json::from_str(cmd)?;
        Ok(SafekeeperPostgresCommand::JSONCtrl { cmd: parsed_cmd })
    } else {
        bail!("unsupported command {}", cmd);
    }
}

impl postgres_backend::Handler for SafekeeperPostgresHandler {
    // ztenant id and ztimeline id are passed in connection string params
    fn startup(&mut self, _pgb: &mut PostgresBackend, sm: &FeStartupPacket) -> Result<()> {
        if let FeStartupPacket::StartupMessage { params, .. } = sm {
            self.ztenantid = match params.get("ztenantid") {
                Some(z) => Some(ZTenantId::from_str(z)?), // just curious, can I do that from .map?
                _ => None,
            };

            self.ztimelineid = match params.get("ztimelineid") {
                Some(z) => Some(ZTimelineId::from_str(z)?),
                _ => None,
            };

            if let Some(app_name) = params.get("application_name") {
                self.appname = Some(app_name.clone());
            }

            Ok(())
        } else {
            bail!("Walkeeper received unexpected initial message: {:?}", sm);
        }
    }

    fn process_query(&mut self, pgb: &mut PostgresBackend, query_string: &str) -> Result<()> {
        let cmd = parse_cmd(query_string)?;

        // Is this command is ztimeline scoped?
        match cmd {
            SafekeeperPostgresCommand::StartWalPush { .. }
            | SafekeeperPostgresCommand::StartReplication { .. }
            | SafekeeperPostgresCommand::IdentifySystem
            | SafekeeperPostgresCommand::JSONCtrl { .. } => {
                let tenantid = self
                    .ztenantid
                    .ok_or_else(|| anyhow!("tenantid is required"))?;
                let timelineid = self
                    .ztimelineid
                    .ok_or_else(|| anyhow!("timelineid is required"))?;
                if self.timeline.is_none() {
                    // START_WAL_PUSH is the only command that initializes the timeline in production.
                    // There is also JSON_CTRL command, which should initialize the timeline for testing.
                    let create_control_file = match cmd {
                        SafekeeperPostgresCommand::StartWalPush { .. }
                        | SafekeeperPostgresCommand::JSONCtrl { .. } => CreateControlFile::True,
                        _ => CreateControlFile::False,
                    };
                    self.timeline
                        .set(&self.conf, tenantid, timelineid, create_control_file)?;
                }
            }
        }

        match cmd {
            SafekeeperPostgresCommand::StartWalPush { pageserver_connstr } => {
                ReceiveWalConn::new(pgb, pageserver_connstr)
                    .run(self)
                    .with_context(|| "failed to run ReceiveWalConn")?;
            }
            SafekeeperPostgresCommand::StartReplication { start_lsn } => {
                ReplicationConn::new(pgb)
                    .run(self, pgb, start_lsn)
                    .with_context(|| "failed to run ReplicationConn")?;
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
    pub fn new(conf: SafeKeeperConf, tx: UnboundedSender<CallmeEvent>) -> Self {
        SafekeeperPostgresHandler {
            conf,
            appname: None,
            ztenantid: None,
            ztimelineid: None,
            timeline: None,
            tx,
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
