use crate::{
    background_process,
    local_env::{LocalEnv, NeonStorageControllerConf},
};
use camino::{Utf8Path, Utf8PathBuf};
use pageserver_api::{
    controller_api::{
        NodeConfigureRequest, NodeRegisterRequest, TenantCreateRequest, TenantCreateResponse,
        TenantLocateResponse, TenantShardMigrateRequest, TenantShardMigrateResponse,
    },
    models::{
        TenantShardSplitRequest, TenantShardSplitResponse, TimelineCreateRequest, TimelineInfo,
    },
    shard::{ShardStripeSize, TenantShardId},
};
use pageserver_client::mgmt_api::ResponseErrorMessageExt;
use postgres_backend::AuthType;
use reqwest::Method;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{fs, str::FromStr, time::Duration};
use tokio::process::Command;
use tracing::instrument;
use url::Url;
use utils::{
    auth::{encode_from_key_file, Claims, Scope},
    id::{NodeId, TenantId},
};

pub struct StorageController {
    env: LocalEnv,
    listen: String,
    private_key: Option<Vec<u8>>,
    public_key: Option<String>,
    postgres_port: u16,
    client: reqwest::Client,
    config: NeonStorageControllerConf,
}

const COMMAND: &str = "storage_controller";

const STORAGE_CONTROLLER_POSTGRES_VERSION: u32 = 16;

#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: Option<NodeId>,
    pub generation_override: Option<i32>,
}

#[derive(Serialize, Deserialize)]
pub struct AttachHookResponse {
    pub gen: Option<u32>,
}

#[derive(Serialize, Deserialize)]
pub struct InspectRequest {
    pub tenant_shard_id: TenantShardId,
}

#[derive(Serialize, Deserialize)]
pub struct InspectResponse {
    pub attachment: Option<(u32, NodeId)>,
}

impl StorageController {
    pub fn from_env(env: &LocalEnv) -> Self {
        // Makes no sense to construct this if pageservers aren't going to use it: assume
        // pageservers have control plane API set
        let listen_url = env.control_plane_api.clone().unwrap();

        let listen = format!(
            "{}:{}",
            listen_url.host_str().unwrap(),
            listen_url.port().unwrap()
        );

        // Convention: NeonEnv in python tests reserves the next port after the control_plane_api
        // port, for use by our captive postgres.
        let postgres_port = listen_url
            .port()
            .expect("Control plane API setting should always have a port")
            + 1;

        // Assume all pageservers have symmetric auth configuration: this service
        // expects to use one JWT token to talk to all of them.
        let ps_conf = env
            .pageservers
            .first()
            .expect("Config is validated to contain at least one pageserver");
        let (private_key, public_key) = match ps_conf.http_auth_type {
            AuthType::Trust => (None, None),
            AuthType::NeonJWT => {
                let private_key_path = env.get_private_key_path();
                let private_key = fs::read(private_key_path).expect("failed to read private key");

                // If pageserver auth is enabled, this implicitly enables auth for this service,
                // using the same credentials.
                let public_key_path =
                    camino::Utf8PathBuf::try_from(env.base_data_dir.join("auth_public_key.pem"))
                        .unwrap();

                // This service takes keys as a string rather than as a path to a file/dir: read the key into memory.
                let public_key = if std::fs::metadata(&public_key_path)
                    .expect("Can't stat public key")
                    .is_dir()
                {
                    // Our config may specify a directory: this is for the pageserver's ability to handle multiple
                    // keys.  We only use one key at a time, so, arbitrarily load the first one in the directory.
                    let mut dir =
                        std::fs::read_dir(&public_key_path).expect("Can't readdir public key path");
                    let dent = dir
                        .next()
                        .expect("Empty key dir")
                        .expect("Error reading key dir");

                    std::fs::read_to_string(dent.path()).expect("Can't read public key")
                } else {
                    std::fs::read_to_string(&public_key_path).expect("Can't read public key")
                };
                (Some(private_key), Some(public_key))
            }
        };

        Self {
            env: env.clone(),
            listen,
            private_key,
            public_key,
            postgres_port,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
            config: env.storage_controller.clone(),
        }
    }

    fn pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.env.base_data_dir.join("storage_controller.pid"))
            .expect("non-Unicode path")
    }

    /// PIDFile for the postgres instance used to store storage controller state
    fn postgres_pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            self.env
                .base_data_dir
                .join("storage_controller_postgres.pid"),
        )
        .expect("non-Unicode path")
    }

    /// Find the directory containing postgres subdirectories, such `bin` and `lib`
    ///
    /// This usually uses STORAGE_CONTROLLER_POSTGRES_VERSION of postgres, but will fall back
    /// to other versions if that one isn't found.  Some automated tests create circumstances
    /// where only one version is available in pg_distrib_dir, such as `test_remote_extensions`.
    async fn get_pg_dir(&self, dir_name: &str) -> anyhow::Result<Utf8PathBuf> {
        let prefer_versions = [STORAGE_CONTROLLER_POSTGRES_VERSION, 15, 14];

        for v in prefer_versions {
            let path = Utf8PathBuf::from_path_buf(self.env.pg_dir(v, dir_name)?).unwrap();
            if tokio::fs::try_exists(&path).await? {
                return Ok(path);
            }
        }

        // Fall through
        anyhow::bail!(
            "Postgres directory '{}' not found in {}",
            dir_name,
            self.env.pg_distrib_dir.display(),
        );
    }

    pub async fn get_pg_bin_dir(&self) -> anyhow::Result<Utf8PathBuf> {
        self.get_pg_dir("bin").await
    }

    pub async fn get_pg_lib_dir(&self) -> anyhow::Result<Utf8PathBuf> {
        self.get_pg_dir("lib").await
    }

    /// Readiness check for our postgres process
    async fn pg_isready(&self, pg_bin_dir: &Utf8Path) -> anyhow::Result<bool> {
        let bin_path = pg_bin_dir.join("pg_isready");
        let args = ["-h", "localhost", "-p", &format!("{}", self.postgres_port)];
        let exitcode = Command::new(bin_path).args(args).spawn()?.wait().await?;

        Ok(exitcode.success())
    }

    /// Create our database if it doesn't exist, and run migrations.
    ///
    /// This function is equivalent to the `diesel setup` command in the diesel CLI.  We implement
    /// the same steps by hand to avoid imposing a dependency on installing diesel-cli for developers
    /// who just want to run `cargo neon_local` without knowing about diesel.
    ///
    /// Returns the database url
    pub async fn setup_database(&self) -> anyhow::Result<String> {
        const DB_NAME: &str = "storage_controller";
        let database_url = format!("postgresql://localhost:{}/{DB_NAME}", self.postgres_port);

        let pg_bin_dir = self.get_pg_bin_dir().await?;
        let createdb_path = pg_bin_dir.join("createdb");
        let output = Command::new(&createdb_path)
            .args([
                "-h",
                "localhost",
                "-p",
                &format!("{}", self.postgres_port),
                DB_NAME,
            ])
            .output()
            .await
            .expect("Failed to spawn createdb");

        if !output.status.success() {
            let stderr = String::from_utf8(output.stderr).expect("Non-UTF8 output from createdb");
            if stderr.contains("already exists") {
                tracing::info!("Database {DB_NAME} already exists");
            } else {
                anyhow::bail!("createdb failed with status {}: {stderr}", output.status);
            }
        }

        Ok(database_url)
    }

    pub async fn start(&self, retry_timeout: &Duration) -> anyhow::Result<()> {
        // Start a vanilla Postgres process used by the storage controller for persistence.
        let pg_data_path = Utf8PathBuf::from_path_buf(self.env.base_data_dir.clone())
            .unwrap()
            .join("storage_controller_db");
        let pg_bin_dir = self.get_pg_bin_dir().await?;
        let pg_lib_dir = self.get_pg_lib_dir().await?;
        let pg_log_path = pg_data_path.join("postgres.log");

        if !tokio::fs::try_exists(&pg_data_path).await? {
            // Initialize empty database
            let initdb_path = pg_bin_dir.join("initdb");
            let mut child = Command::new(&initdb_path)
                .envs(vec![
                    ("LD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                    ("DYLD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                ])
                .args(["-D", pg_data_path.as_ref()])
                .spawn()
                .expect("Failed to spawn initdb");
            let status = child.wait().await?;
            if !status.success() {
                anyhow::bail!("initdb failed with status {status}");
            }

            // Write a minimal config file:
            // - Specify the port, since this is chosen dynamically
            // - Switch off fsync, since we're running on lightweight test environments and when e.g. scale testing
            //   the storage controller we don't want a slow local disk to interfere with that.
            tokio::fs::write(
                &pg_data_path.join("postgresql.conf"),
                format!("port = {}\nfsync=off\n", self.postgres_port),
            )
            .await?;
        };

        println!("Starting storage controller database...");
        let db_start_args = [
            "-w",
            "-D",
            pg_data_path.as_ref(),
            "-l",
            pg_log_path.as_ref(),
            "start",
        ];

        background_process::start_process(
            "storage_controller_db",
            &self.env.base_data_dir,
            pg_bin_dir.join("pg_ctl").as_std_path(),
            db_start_args,
            vec![
                ("LD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                ("DYLD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
            ],
            background_process::InitialPidFile::Create(self.postgres_pid_file()),
            retry_timeout,
            || self.pg_isready(&pg_bin_dir),
        )
        .await?;

        // Run migrations on every startup, in case something changed.
        let database_url = self.setup_database().await?;

        let mut args = vec![
            "-l",
            &self.listen,
            "--dev",
            "--database-url",
            &database_url,
            "--max-unavailable-interval",
            &humantime::Duration::from(self.config.max_unavailable).to_string(),
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
        if let Some(private_key) = &self.private_key {
            let claims = Claims::new(None, Scope::PageServerApi);
            let jwt_token =
                encode_from_key_file(&claims, private_key).expect("failed to generate jwt token");
            args.push(format!("--jwt-token={jwt_token}"));
        }

        if let Some(public_key) = &self.public_key {
            args.push(format!("--public-key=\"{public_key}\""));
        }

        if let Some(control_plane_compute_hook_api) = &self.env.control_plane_compute_hook_api {
            args.push(format!(
                "--compute-hook-url={control_plane_compute_hook_api}"
            ));
        }

        if let Some(split_threshold) = self.config.split_threshold.as_ref() {
            args.push(format!("--split-threshold={split_threshold}"))
        }

        args.push(format!(
            "--neon-local-repo-dir={}",
            self.env.base_data_dir.display()
        ));

        background_process::start_process(
            COMMAND,
            &self.env.base_data_dir,
            &self.env.storage_controller_bin(),
            args,
            vec![
                ("LD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
                ("DYLD_LIBRARY_PATH".to_owned(), pg_lib_dir.to_string()),
            ],
            background_process::InitialPidFile::Create(self.pid_file()),
            retry_timeout,
            || async {
                match self.ready().await {
                    Ok(_) => Ok(true),
                    Err(_) => Ok(false),
                }
            },
        )
        .await?;

        Ok(())
    }

    pub async fn stop(&self, immediate: bool) -> anyhow::Result<()> {
        background_process::stop_process(immediate, COMMAND, &self.pid_file())?;

        let pg_data_path = self.env.base_data_dir.join("storage_controller_db");
        let pg_bin_dir = self.get_pg_bin_dir().await?;

        println!("Stopping storage controller database...");
        let pg_stop_args = ["-D", &pg_data_path.to_string_lossy(), "stop"];
        let stop_status = Command::new(pg_bin_dir.join("pg_ctl"))
            .args(pg_stop_args)
            .spawn()?
            .wait()
            .await?;
        if !stop_status.success() {
            let pg_status_args = ["-D", &pg_data_path.to_string_lossy(), "status"];
            let status_exitcode = Command::new(pg_bin_dir.join("pg_ctl"))
                .args(pg_status_args)
                .spawn()?
                .wait()
                .await?;

            // pg_ctl status returns this exit code if postgres is not running: in this case it is
            // fine that stop failed.  Otherwise it is an error that stop failed.
            const PG_STATUS_NOT_RUNNING: i32 = 3;
            if Some(PG_STATUS_NOT_RUNNING) == status_exitcode.code() {
                println!("Storage controller database is already stopped");
                return Ok(());
            } else {
                anyhow::bail!("Failed to stop storage controller database: {stop_status}")
            }
        }

        Ok(())
    }

    fn get_claims_for_path(path: &str) -> anyhow::Result<Option<Claims>> {
        let category = match path.find('/') {
            Some(idx) => &path[..idx],
            None => path,
        };

        match category {
            "status" | "ready" => Ok(None),
            "control" | "debug" => Ok(Some(Claims::new(None, Scope::Admin))),
            "v1" => Ok(Some(Claims::new(None, Scope::PageServerApi))),
            _ => Err(anyhow::anyhow!("Failed to determine claims for {}", path)),
        }
    }

    /// Simple HTTP request wrapper for calling into storage controller
    async fn dispatch<RQ, RS>(
        &self,
        method: reqwest::Method,
        path: String,
        body: Option<RQ>,
    ) -> anyhow::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        // The configured URL has the /upcall path prefix for pageservers to use: we will strip that out
        // for general purpose API access.
        let listen_url = self.env.control_plane_api.clone().unwrap();
        let url = Url::from_str(&format!(
            "http://{}:{}/{path}",
            listen_url.host_str().unwrap(),
            listen_url.port().unwrap()
        ))
        .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(private_key) = &self.private_key {
            println!("Getting claims for path {}", path);
            if let Some(required_claims) = Self::get_claims_for_path(&path)? {
                println!("Got claims {:?} for path {}", required_claims, path);
                let jwt_token = encode_from_key_file(&required_claims, private_key)?;
                builder = builder.header(
                    reqwest::header::AUTHORIZATION,
                    format!("Bearer {jwt_token}"),
                );
            }
        }

        let response = builder.send().await?;
        let response = response.error_from_body().await?;

        Ok(response
            .json()
            .await
            .map_err(pageserver_client::mgmt_api::Error::ReceiveBody)?)
    }

    /// Call into the attach_hook API, for use before handing out attachments to pageservers
    #[instrument(skip(self))]
    pub async fn attach_hook(
        &self,
        tenant_shard_id: TenantShardId,
        pageserver_id: NodeId,
    ) -> anyhow::Result<Option<u32>> {
        let request = AttachHookRequest {
            tenant_shard_id,
            node_id: Some(pageserver_id),
            generation_override: None,
        };

        let response = self
            .dispatch::<_, AttachHookResponse>(
                Method::POST,
                "debug/v1/attach-hook".to_string(),
                Some(request),
            )
            .await?;

        Ok(response.gen)
    }

    #[instrument(skip(self))]
    pub async fn inspect(
        &self,
        tenant_shard_id: TenantShardId,
    ) -> anyhow::Result<Option<(u32, NodeId)>> {
        let request = InspectRequest { tenant_shard_id };

        let response = self
            .dispatch::<_, InspectResponse>(
                Method::POST,
                "debug/v1/inspect".to_string(),
                Some(request),
            )
            .await?;

        Ok(response.attachment)
    }

    #[instrument(skip(self))]
    pub async fn tenant_create(
        &self,
        req: TenantCreateRequest,
    ) -> anyhow::Result<TenantCreateResponse> {
        self.dispatch(Method::POST, "v1/tenant".to_string(), Some(req))
            .await
    }

    #[instrument(skip(self))]
    pub async fn tenant_import(&self, tenant_id: TenantId) -> anyhow::Result<TenantCreateResponse> {
        self.dispatch::<(), TenantCreateResponse>(
            Method::POST,
            format!("debug/v1/tenant/{tenant_id}/import"),
            None,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn tenant_locate(&self, tenant_id: TenantId) -> anyhow::Result<TenantLocateResponse> {
        self.dispatch::<(), _>(
            Method::GET,
            format!("debug/v1/tenant/{tenant_id}/locate"),
            None,
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn tenant_migrate(
        &self,
        tenant_shard_id: TenantShardId,
        node_id: NodeId,
    ) -> anyhow::Result<TenantShardMigrateResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_shard_id}/migrate"),
            Some(TenantShardMigrateRequest {
                tenant_shard_id,
                node_id,
            }),
        )
        .await
    }

    #[instrument(skip(self), fields(%tenant_id, %new_shard_count))]
    pub async fn tenant_split(
        &self,
        tenant_id: TenantId,
        new_shard_count: u8,
        new_stripe_size: Option<ShardStripeSize>,
    ) -> anyhow::Result<TenantShardSplitResponse> {
        self.dispatch(
            Method::PUT,
            format!("control/v1/tenant/{tenant_id}/shard_split"),
            Some(TenantShardSplitRequest {
                new_shard_count,
                new_stripe_size,
            }),
        )
        .await
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub async fn node_register(&self, req: NodeRegisterRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(Method::POST, "control/v1/node".to_string(), Some(req))
            .await
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub async fn node_configure(&self, req: NodeConfigureRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(
            Method::PUT,
            format!("control/v1/node/{}/config", req.node_id),
            Some(req),
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn ready(&self) -> anyhow::Result<()> {
        self.dispatch::<(), ()>(Method::GET, "ready".to_string(), None)
            .await
    }

    #[instrument(skip_all, fields(%tenant_id, timeline_id=%req.new_timeline_id))]
    pub async fn tenant_timeline_create(
        &self,
        tenant_id: TenantId,
        req: TimelineCreateRequest,
    ) -> anyhow::Result<TimelineInfo> {
        self.dispatch(
            Method::POST,
            format!("v1/tenant/{tenant_id}/timeline"),
            Some(req),
        )
        .await
    }
}
