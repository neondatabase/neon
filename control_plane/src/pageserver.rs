//! Code to manage pageservers
//!
//! In the local test environment, the pageserver stores its data directly in
//!
//!   .neon/
//!
use std::borrow::Cow;
use std::collections::HashMap;

use std::io;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{bail, Context};
use camino::Utf8PathBuf;
use futures::SinkExt;
use pageserver_api::models::{
    self, LocationConfig, ShardParameters, TenantHistorySize, TenantInfo, TimelineInfo,
};
use pageserver_api::shard::TenantShardId;
use pageserver_client::mgmt_api;
use postgres_backend::AuthType;
use postgres_connection::{parse_host_port, PgConnectionConfig};
use utils::auth::{Claims, Scope};
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::local_env::PageServerConf;
use crate::{background_process, local_env::LocalEnv};

/// Directory within .neon which will be used by default for LocalFs remote storage.
pub const PAGESERVER_REMOTE_STORAGE_DIR: &str = "local_fs_remote_storage/pageserver";

//
// Control routines for pageserver.
//
// Used in CLI and tests.
//
#[derive(Debug)]
pub struct PageServerNode {
    pub pg_connection_config: PgConnectionConfig,
    pub conf: PageServerConf,
    pub env: LocalEnv,
    pub http_client: mgmt_api::Client,
}

impl PageServerNode {
    pub fn from_env(env: &LocalEnv, conf: &PageServerConf) -> PageServerNode {
        let (host, port) =
            parse_host_port(&conf.listen_pg_addr).expect("Unable to parse listen_pg_addr");
        let port = port.unwrap_or(5432);
        Self {
            pg_connection_config: PgConnectionConfig::new_host_port(host, port),
            conf: conf.clone(),
            env: env.clone(),
            http_client: mgmt_api::Client::new(
                format!("http://{}", conf.listen_http_addr),
                {
                    match conf.http_auth_type {
                        AuthType::Trust => None,
                        AuthType::NeonJWT => Some(
                            env.generate_auth_token(&Claims::new(None, Scope::PageServerApi))
                                .unwrap(),
                        ),
                    }
                }
                .as_deref(),
            ),
        }
    }

    /// Merge overrides provided by the user on the command line with our default overides derived from neon_local configuration.
    ///
    /// These all end up on the command line of the `pageserver` binary.
    fn neon_local_overrides(&self, cli_overrides: &[&str]) -> Vec<String> {
        // FIXME: the paths should be shell-escaped to handle paths with spaces, quotas etc.
        let pg_distrib_dir_param = format!(
            "pg_distrib_dir='{}'",
            self.env.pg_distrib_dir_raw().display()
        );

        let PageServerConf {
            id,
            listen_pg_addr,
            listen_http_addr,
            pg_auth_type,
            http_auth_type,
            virtual_file_io_engine,
            get_vectored_impl,
        } = &self.conf;

        let id = format!("id={}", id);

        let http_auth_type_param = format!("http_auth_type='{}'", http_auth_type);
        let listen_http_addr_param = format!("listen_http_addr='{}'", listen_http_addr);

        let pg_auth_type_param = format!("pg_auth_type='{}'", pg_auth_type);
        let listen_pg_addr_param = format!("listen_pg_addr='{}'", listen_pg_addr);
        let virtual_file_io_engine = if let Some(virtual_file_io_engine) = virtual_file_io_engine {
            format!("virtual_file_io_engine='{virtual_file_io_engine}'")
        } else {
            String::new()
        };
        let get_vectored_impl = if let Some(get_vectored_impl) = get_vectored_impl {
            format!("get_vectored_impl='{get_vectored_impl}'")
        } else {
            String::new()
        };

        let broker_endpoint_param = format!("broker_endpoint='{}'", self.env.broker.client_url());

        let mut overrides = vec![
            id,
            pg_distrib_dir_param,
            http_auth_type_param,
            pg_auth_type_param,
            listen_http_addr_param,
            listen_pg_addr_param,
            broker_endpoint_param,
            virtual_file_io_engine,
            get_vectored_impl,
        ];

        if let Some(control_plane_api) = &self.env.control_plane_api {
            overrides.push(format!(
                "control_plane_api='{}'",
                control_plane_api.as_str()
            ));

            // Storage controller uses the same auth as pageserver: if JWT is enabled
            // for us, we will also need it to talk to them.
            if matches!(http_auth_type, AuthType::NeonJWT) {
                let jwt_token = self
                    .env
                    .generate_auth_token(&Claims::new(None, Scope::GenerationsApi))
                    .unwrap();
                overrides.push(format!("control_plane_api_token='{}'", jwt_token));
            }
        }

        if !cli_overrides
            .iter()
            .any(|c| c.starts_with("remote_storage"))
        {
            overrides.push(format!(
                "remote_storage={{local_path='../{PAGESERVER_REMOTE_STORAGE_DIR}'}}"
            ));
        }

        if *http_auth_type != AuthType::Trust || *pg_auth_type != AuthType::Trust {
            // Keys are generated in the toplevel repo dir, pageservers' workdirs
            // are one level below that, so refer to keys with ../
            overrides.push("auth_validation_public_key_path='../auth_public_key.pem'".to_owned());
        }

        // Apply the user-provided overrides
        overrides.extend(cli_overrides.iter().map(|&c| c.to_owned()));

        overrides
    }

    /// Initializes a pageserver node by creating its config with the overrides provided.
    pub fn initialize(&self, config_overrides: &[&str]) -> anyhow::Result<()> {
        // First, run `pageserver --init` and wait for it to write a config into FS and exit.
        self.pageserver_init(config_overrides)
            .with_context(|| format!("Failed to run init for pageserver node {}", self.conf.id))
    }

    pub fn repo_path(&self) -> PathBuf {
        self.env.pageserver_data_dir(self.conf.id)
    }

    /// The pid file is created by the pageserver process, with its pid stored inside.
    /// Other pageservers cannot lock the same file and overwrite it for as long as the current
    /// pageserver runs. (Unless someone removes the file manually; never do that!)
    fn pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.repo_path().join("pageserver.pid"))
            .expect("non-Unicode path")
    }

    pub async fn start(&self, config_overrides: &[&str]) -> anyhow::Result<()> {
        self.start_node(config_overrides, false).await
    }

    fn pageserver_init(&self, config_overrides: &[&str]) -> anyhow::Result<()> {
        let datadir = self.repo_path();
        let node_id = self.conf.id;
        println!(
            "Initializing pageserver node {} at '{}' in {:?}",
            node_id,
            self.pg_connection_config.raw_address(),
            datadir
        );
        io::stdout().flush()?;

        if !datadir.exists() {
            std::fs::create_dir(&datadir)?;
        }

        let datadir_path_str = datadir.to_str().with_context(|| {
            format!("Cannot start pageserver node {node_id} in path that has no string representation: {datadir:?}")
        })?;
        let mut args = self.pageserver_basic_args(config_overrides, datadir_path_str);
        args.push(Cow::Borrowed("--init"));

        let init_output = Command::new(self.env.pageserver_bin())
            .args(args.iter().map(Cow::as_ref))
            .envs(self.pageserver_env_variables()?)
            .output()
            .with_context(|| format!("Failed to run pageserver init for node {node_id}"))?;

        anyhow::ensure!(
            init_output.status.success(),
            "Pageserver init for node {} did not finish successfully, stdout: {}, stderr: {}",
            node_id,
            String::from_utf8_lossy(&init_output.stdout),
            String::from_utf8_lossy(&init_output.stderr),
        );

        // Write metadata file, used by pageserver on startup to register itself with
        // the storage controller
        let metadata_path = datadir.join("metadata.json");

        let (_http_host, http_port) =
            parse_host_port(&self.conf.listen_http_addr).expect("Unable to parse listen_http_addr");
        let http_port = http_port.unwrap_or(9898);
        // Intentionally hand-craft JSON: this acts as an implicit format compat test
        // in case the pageserver-side structure is edited, and reflects the real life
        // situation: the metadata is written by some other script.
        std::fs::write(
            metadata_path,
            serde_json::to_vec(&serde_json::json!({
                "host": "localhost",
                "port": self.pg_connection_config.port(),
                "http_host": "localhost",
                "http_port": http_port,
            }))
            .unwrap(),
        )
        .expect("Failed to write metadata file");

        Ok(())
    }

    async fn start_node(
        &self,
        config_overrides: &[&str],
        update_config: bool,
    ) -> anyhow::Result<()> {
        // TODO: using a thread here because start_process() is not async but we need to call check_status()
        let datadir = self.repo_path();
        print!(
            "Starting pageserver node {} at '{}' in {:?}",
            self.conf.id,
            self.pg_connection_config.raw_address(),
            datadir
        );
        io::stdout().flush().context("flush stdout")?;

        let datadir_path_str = datadir.to_str().with_context(|| {
            format!(
                "Cannot start pageserver node {} in path that has no string representation: {:?}",
                self.conf.id, datadir,
            )
        })?;
        let mut args = self.pageserver_basic_args(config_overrides, datadir_path_str);
        if update_config {
            args.push(Cow::Borrowed("--update-config"));
        }
        background_process::start_process(
            "pageserver",
            &datadir,
            &self.env.pageserver_bin(),
            args.iter().map(Cow::as_ref),
            self.pageserver_env_variables()?,
            background_process::InitialPidFile::Expect(self.pid_file()),
            || async {
                let st = self.check_status().await;
                match st {
                    Ok(()) => Ok(true),
                    Err(mgmt_api::Error::ReceiveBody(_)) => Ok(false),
                    Err(e) => Err(anyhow::anyhow!("Failed to check node status: {e}")),
                }
            },
        )
        .await?;

        Ok(())
    }

    fn pageserver_basic_args<'a>(
        &self,
        config_overrides: &'a [&'a str],
        datadir_path_str: &'a str,
    ) -> Vec<Cow<'a, str>> {
        let mut args = vec![Cow::Borrowed("-D"), Cow::Borrowed(datadir_path_str)];

        let overrides = self.neon_local_overrides(config_overrides);
        for config_override in overrides {
            args.push(Cow::Borrowed("-c"));
            args.push(Cow::Owned(config_override));
        }

        args
    }

    fn pageserver_env_variables(&self) -> anyhow::Result<Vec<(String, String)>> {
        // FIXME: why is this tied to pageserver's auth type? Whether or not the safekeeper
        // needs a token, and how to generate that token, seems independent to whether
        // the pageserver requires a token in incoming requests.
        Ok(if self.conf.http_auth_type != AuthType::Trust {
            // Generate a token to connect from the pageserver to a safekeeper
            let token = self
                .env
                .generate_auth_token(&Claims::new(None, Scope::SafekeeperData))?;
            vec![("NEON_AUTH_TOKEN".to_owned(), token)]
        } else {
            Vec::new()
        })
    }

    ///
    /// Stop the server.
    ///
    /// If 'immediate' is true, we use SIGQUIT, killing the process immediately.
    /// Otherwise we use SIGTERM, triggering a clean shutdown
    ///
    /// If the server is not running, returns success
    ///
    pub fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        background_process::stop_process(immediate, "pageserver", &self.pid_file())
    }

    pub async fn page_server_psql_client(
        &self,
    ) -> anyhow::Result<(
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    )> {
        let mut config = self.pg_connection_config.clone();
        if self.conf.pg_auth_type == AuthType::NeonJWT {
            let token = self
                .env
                .generate_auth_token(&Claims::new(None, Scope::PageServerApi))?;
            config = config.set_password(Some(token));
        }
        Ok(config.connect_no_tls().await?)
    }

    pub async fn check_status(&self) -> mgmt_api::Result<()> {
        self.http_client.status().await
    }

    pub async fn tenant_list(&self) -> mgmt_api::Result<Vec<TenantInfo>> {
        self.http_client.list_tenants().await
    }
    pub fn parse_config(mut settings: HashMap<&str, &str>) -> anyhow::Result<models::TenantConfig> {
        let result = models::TenantConfig {
            checkpoint_distance: settings
                .remove("checkpoint_distance")
                .map(|x| x.parse::<u64>())
                .transpose()?,
            checkpoint_timeout: settings.remove("checkpoint_timeout").map(|x| x.to_string()),
            compaction_target_size: settings
                .remove("compaction_target_size")
                .map(|x| x.parse::<u64>())
                .transpose()?,
            compaction_period: settings.remove("compaction_period").map(|x| x.to_string()),
            compaction_threshold: settings
                .remove("compaction_threshold")
                .map(|x| x.parse::<usize>())
                .transpose()?,
            compaction_algorithm: settings
                .remove("compaction_algorithm")
                .map(serde_json::from_str)
                .transpose()
                .context("Failed to parse 'compaction_algorithm' json")?,
            gc_horizon: settings
                .remove("gc_horizon")
                .map(|x| x.parse::<u64>())
                .transpose()?,
            gc_period: settings.remove("gc_period").map(|x| x.to_string()),
            image_creation_threshold: settings
                .remove("image_creation_threshold")
                .map(|x| x.parse::<usize>())
                .transpose()?,
            image_layer_creation_check_threshold: settings
                .remove("image_layer_creation_check_threshold")
                .map(|x| x.parse::<u8>())
                .transpose()?,
            pitr_interval: settings.remove("pitr_interval").map(|x| x.to_string()),
            walreceiver_connect_timeout: settings
                .remove("walreceiver_connect_timeout")
                .map(|x| x.to_string()),
            lagging_wal_timeout: settings
                .remove("lagging_wal_timeout")
                .map(|x| x.to_string()),
            max_lsn_wal_lag: settings
                .remove("max_lsn_wal_lag")
                .map(|x| x.parse::<NonZeroU64>())
                .transpose()
                .context("Failed to parse 'max_lsn_wal_lag' as non zero integer")?,
            trace_read_requests: settings
                .remove("trace_read_requests")
                .map(|x| x.parse::<bool>())
                .transpose()
                .context("Failed to parse 'trace_read_requests' as bool")?,
            eviction_policy: settings
                .remove("eviction_policy")
                .map(serde_json::from_str)
                .transpose()
                .context("Failed to parse 'eviction_policy' json")?,
            min_resident_size_override: settings
                .remove("min_resident_size_override")
                .map(|x| x.parse::<u64>())
                .transpose()
                .context("Failed to parse 'min_resident_size_override' as integer")?,
            evictions_low_residence_duration_metric_threshold: settings
                .remove("evictions_low_residence_duration_metric_threshold")
                .map(|x| x.to_string()),
            heatmap_period: settings.remove("heatmap_period").map(|x| x.to_string()),
            lazy_slru_download: settings
                .remove("lazy_slru_download")
                .map(|x| x.parse::<bool>())
                .transpose()
                .context("Failed to parse 'lazy_slru_download' as bool")?,
            timeline_get_throttle: settings
                .remove("timeline_get_throttle")
                .map(serde_json::from_str)
                .transpose()
                .context("parse `timeline_get_throttle` from json")?,
            try_enable_aux_file_v2: settings
                .remove("try_enable_aux_file_v2")
                .map(|x| x.parse::<bool>())
                .transpose()
                .context("Failed to parse 'try_enable_aux_file_v2' as bool")?,
        };
        if !settings.is_empty() {
            bail!("Unrecognized tenant settings: {settings:?}")
        } else {
            Ok(result)
        }
    }

    pub async fn tenant_create(
        &self,
        new_tenant_id: TenantId,
        generation: Option<u32>,
        settings: HashMap<&str, &str>,
    ) -> anyhow::Result<TenantId> {
        let config = Self::parse_config(settings.clone())?;

        let request = models::TenantCreateRequest {
            new_tenant_id: TenantShardId::unsharded(new_tenant_id),
            generation,
            config,
            shard_parameters: ShardParameters::default(),
            // Placement policy is not meaningful for creations not done via storage controller
            placement_policy: None,
        };
        if !settings.is_empty() {
            bail!("Unrecognized tenant settings: {settings:?}")
        }
        Ok(self.http_client.tenant_create(&request).await?)
    }

    pub async fn tenant_config(
        &self,
        tenant_id: TenantId,
        mut settings: HashMap<&str, &str>,
    ) -> anyhow::Result<()> {
        let config = {
            // Braces to make the diff easier to read
            models::TenantConfig {
                checkpoint_distance: settings
                    .remove("checkpoint_distance")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'checkpoint_distance' as an integer")?,
                checkpoint_timeout: settings.remove("checkpoint_timeout").map(|x| x.to_string()),
                compaction_target_size: settings
                    .remove("compaction_target_size")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'compaction_target_size' as an integer")?,
                compaction_period: settings.remove("compaction_period").map(|x| x.to_string()),
                compaction_threshold: settings
                    .remove("compaction_threshold")
                    .map(|x| x.parse::<usize>())
                    .transpose()
                    .context("Failed to parse 'compaction_threshold' as an integer")?,
                compaction_algorithm: settings
                    .remove("compactin_algorithm")
                    .map(serde_json::from_str)
                    .transpose()
                    .context("Failed to parse 'compaction_algorithm' json")?,
                gc_horizon: settings
                    .remove("gc_horizon")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'gc_horizon' as an integer")?,
                gc_period: settings.remove("gc_period").map(|x| x.to_string()),
                image_creation_threshold: settings
                    .remove("image_creation_threshold")
                    .map(|x| x.parse::<usize>())
                    .transpose()
                    .context("Failed to parse 'image_creation_threshold' as non zero integer")?,
                image_layer_creation_check_threshold: settings
                    .remove("image_layer_creation_check_threshold")
                    .map(|x| x.parse::<u8>())
                    .transpose()
                    .context("Failed to parse 'image_creation_check_threshold' as integer")?,

                pitr_interval: settings.remove("pitr_interval").map(|x| x.to_string()),
                walreceiver_connect_timeout: settings
                    .remove("walreceiver_connect_timeout")
                    .map(|x| x.to_string()),
                lagging_wal_timeout: settings
                    .remove("lagging_wal_timeout")
                    .map(|x| x.to_string()),
                max_lsn_wal_lag: settings
                    .remove("max_lsn_wal_lag")
                    .map(|x| x.parse::<NonZeroU64>())
                    .transpose()
                    .context("Failed to parse 'max_lsn_wal_lag' as non zero integer")?,
                trace_read_requests: settings
                    .remove("trace_read_requests")
                    .map(|x| x.parse::<bool>())
                    .transpose()
                    .context("Failed to parse 'trace_read_requests' as bool")?,
                eviction_policy: settings
                    .remove("eviction_policy")
                    .map(serde_json::from_str)
                    .transpose()
                    .context("Failed to parse 'eviction_policy' json")?,
                min_resident_size_override: settings
                    .remove("min_resident_size_override")
                    .map(|x| x.parse::<u64>())
                    .transpose()
                    .context("Failed to parse 'min_resident_size_override' as an integer")?,
                evictions_low_residence_duration_metric_threshold: settings
                    .remove("evictions_low_residence_duration_metric_threshold")
                    .map(|x| x.to_string()),
                heatmap_period: settings.remove("heatmap_period").map(|x| x.to_string()),
                lazy_slru_download: settings
                    .remove("lazy_slru_download")
                    .map(|x| x.parse::<bool>())
                    .transpose()
                    .context("Failed to parse 'lazy_slru_download' as bool")?,
                timeline_get_throttle: settings
                    .remove("timeline_get_throttle")
                    .map(serde_json::from_str)
                    .transpose()
                    .context("parse `timeline_get_throttle` from json")?,
                try_enable_aux_file_v2: settings
                    .remove("try_enable_aux_file_v2")
                    .map(|x| x.parse::<bool>())
                    .transpose()
                    .context("Failed to parse 'try_enable_aux_file_v2' as bool")?,
            }
        };

        if !settings.is_empty() {
            bail!("Unrecognized tenant settings: {settings:?}")
        }

        self.http_client
            .tenant_config(&models::TenantConfigRequest { tenant_id, config })
            .await?;

        Ok(())
    }

    pub async fn location_config(
        &self,
        tenant_shard_id: TenantShardId,
        config: LocationConfig,
        flush_ms: Option<Duration>,
        lazy: bool,
    ) -> anyhow::Result<()> {
        Ok(self
            .http_client
            .location_config(tenant_shard_id, config, flush_ms, lazy)
            .await?)
    }

    pub async fn timeline_list(
        &self,
        tenant_shard_id: &TenantShardId,
    ) -> anyhow::Result<Vec<TimelineInfo>> {
        Ok(self.http_client.list_timelines(*tenant_shard_id).await?)
    }

    pub async fn timeline_create(
        &self,
        tenant_shard_id: TenantShardId,
        new_timeline_id: TimelineId,
        ancestor_start_lsn: Option<Lsn>,
        ancestor_timeline_id: Option<TimelineId>,
        pg_version: Option<u32>,
        existing_initdb_timeline_id: Option<TimelineId>,
    ) -> anyhow::Result<TimelineInfo> {
        let req = models::TimelineCreateRequest {
            new_timeline_id,
            ancestor_start_lsn,
            ancestor_timeline_id,
            pg_version,
            existing_initdb_timeline_id,
        };
        Ok(self
            .http_client
            .timeline_create(tenant_shard_id, &req)
            .await?)
    }

    /// Import a basebackup prepared using either:
    /// a) `pg_basebackup -F tar`, or
    /// b) The `fullbackup` pageserver endpoint
    ///
    /// # Arguments
    /// * `tenant_id` - tenant to import into. Created if not exists
    /// * `timeline_id` - id to assign to imported timeline
    /// * `base` - (start lsn of basebackup, path to `base.tar` file)
    /// * `pg_wal` - if there's any wal to import: (end lsn, path to `pg_wal.tar`)
    pub async fn timeline_import(
        &self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
        base: (Lsn, PathBuf),
        pg_wal: Option<(Lsn, PathBuf)>,
        pg_version: u32,
    ) -> anyhow::Result<()> {
        let (client, conn) = self.page_server_psql_client().await?;
        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });
        let client = std::pin::pin!(client);

        // Init base reader
        let (start_lsn, base_tarfile_path) = base;
        let base_tarfile = tokio::fs::File::open(base_tarfile_path).await?;
        let base_tarfile = tokio_util::io::ReaderStream::new(base_tarfile);

        // Init wal reader if necessary
        let (end_lsn, wal_reader) = if let Some((end_lsn, wal_tarfile_path)) = pg_wal {
            let wal_tarfile = tokio::fs::File::open(wal_tarfile_path).await?;
            let wal_reader = tokio_util::io::ReaderStream::new(wal_tarfile);
            (end_lsn, Some(wal_reader))
        } else {
            (start_lsn, None)
        };

        let copy_in = |reader, cmd| {
            let client = &client;
            async move {
                let writer = client.copy_in(&cmd).await?;
                let writer = std::pin::pin!(writer);
                let mut writer = writer.sink_map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, format!("{e}"))
                });
                let mut reader = std::pin::pin!(reader);
                writer.send_all(&mut reader).await?;
                writer.into_inner().finish().await?;
                anyhow::Ok(())
            }
        };

        // Import base
        copy_in(
            base_tarfile,
            format!(
                "import basebackup {tenant_id} {timeline_id} {start_lsn} {end_lsn} {pg_version}"
            ),
        )
        .await?;
        // Import wal if necessary
        if let Some(wal_reader) = wal_reader {
            copy_in(
                wal_reader,
                format!("import wal {tenant_id} {timeline_id} {start_lsn} {end_lsn}"),
            )
            .await?;
        }

        Ok(())
    }

    pub async fn tenant_synthetic_size(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> anyhow::Result<TenantHistorySize> {
        Ok(self
            .http_client
            .tenant_synthetic_size(tenant_shard_id)
            .await?)
    }
}
