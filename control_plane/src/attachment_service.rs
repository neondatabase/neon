use crate::{background_process, local_env::LocalEnv};
use camino::Utf8PathBuf;
use diesel::{
    backend::Backend,
    query_builder::{AstPass, QueryFragment, QueryId},
    Connection, PgConnection, QueryResult, RunQueryDsl,
};
use diesel_migrations::{HarnessWithOutput, MigrationHarness};
use hyper::Method;
use pageserver_api::{
    models::{ShardParameters, TenantCreateRequest, TimelineCreateRequest, TimelineInfo},
    shard::TenantShardId,
};
use pageserver_client::mgmt_api::ResponseErrorMessageExt;
use postgres_backend::AuthType;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    env,
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::process::Command;
use tracing::instrument;
use utils::{
    auth::{Claims, Scope},
    id::{NodeId, TenantId},
};

pub struct AttachmentService {
    env: LocalEnv,
    listen: String,
    path: PathBuf,
    jwt_token: Option<String>,
    public_key_path: Option<Utf8PathBuf>,
    postgres_port: u16,
    client: reqwest::Client,
}

const COMMAND: &str = "attachment_service";

const ATTACHMENT_SERVICE_POSTGRES_VERSION: u32 = 16;

#[derive(Serialize, Deserialize)]
pub struct AttachHookRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: Option<NodeId>,
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

#[derive(Serialize, Deserialize)]
pub struct TenantCreateResponseShard {
    pub node_id: NodeId,
    pub generation: u32,
}

#[derive(Serialize, Deserialize)]
pub struct TenantCreateResponse {
    pub shards: Vec<TenantCreateResponseShard>,
}

#[derive(Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct NodeConfigureRequest {
    pub node_id: NodeId,

    pub availability: Option<NodeAvailability>,
    pub scheduling: Option<NodeSchedulingPolicy>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantLocateResponseShard {
    pub shard_id: TenantShardId,
    pub node_id: NodeId,

    pub listen_pg_addr: String,
    pub listen_pg_port: u16,

    pub listen_http_addr: String,
    pub listen_http_port: u16,
}

#[derive(Serialize, Deserialize)]
pub struct TenantLocateResponse {
    pub shards: Vec<TenantLocateResponseShard>,
    pub shard_params: ShardParameters,
}

/// Explicitly migrating a particular shard is a low level operation
/// TODO: higher level "Reschedule tenant" operation where the request
/// specifies some constraints, e.g. asking it to get off particular node(s)
#[derive(Serialize, Deserialize, Debug)]
pub struct TenantShardMigrateRequest {
    pub tenant_shard_id: TenantShardId,
    pub node_id: NodeId,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum NodeAvailability {
    // Normal, happy state
    Active,
    // Offline: Tenants shouldn't try to attach here, but they may assume that their
    // secondary locations on this node still exist.  Newly added nodes are in this
    // state until we successfully contact them.
    Offline,
}

impl FromStr for NodeAvailability {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "offline" => Ok(Self::Offline),
            _ => Err(anyhow::anyhow!("Unknown availability state '{s}'")),
        }
    }
}

/// FIXME: this is a duplicate of the type in the attachment_service crate, because the
/// type needs to be defined with diesel traits in there.
#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum NodeSchedulingPolicy {
    Active,
    Filling,
    Pause,
    Draining,
}

impl FromStr for NodeSchedulingPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "active" => Ok(Self::Active),
            "filling" => Ok(Self::Filling),
            "pause" => Ok(Self::Pause),
            "draining" => Ok(Self::Draining),
            _ => Err(anyhow::anyhow!("Unknown scheduling state '{s}'")),
        }
    }
}

impl From<NodeSchedulingPolicy> for String {
    fn from(value: NodeSchedulingPolicy) -> String {
        use NodeSchedulingPolicy::*;
        match value {
            Active => "active",
            Filling => "filling",
            Pause => "pause",
            Draining => "draining",
        }
        .to_string()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TenantShardMigrateResponse {}

impl AttachmentService {
    pub fn from_env(env: &LocalEnv) -> Self {
        let path = env.base_data_dir.join("attachments.json");

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
        let (jwt_token, public_key_path) = match ps_conf.http_auth_type {
            AuthType::Trust => (None, None),
            AuthType::NeonJWT => {
                let jwt_token = env
                    .generate_auth_token(&Claims::new(None, Scope::PageServerApi))
                    .unwrap();

                // If pageserver auth is enabled, this implicitly enables auth for this service,
                // using the same credentials.
                let public_key_path =
                    camino::Utf8PathBuf::try_from(env.base_data_dir.join("auth_public_key.pem"))
                        .unwrap();
                (Some(jwt_token), Some(public_key_path))
            }
        };

        Self {
            env: env.clone(),
            path,
            listen,
            jwt_token,
            public_key_path,
            postgres_port,
            client: reqwest::ClientBuilder::new()
                .build()
                .expect("Failed to construct http client"),
        }
    }

    fn pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(self.env.base_data_dir.join("attachment_service.pid"))
            .expect("non-Unicode path")
    }

    /// PIDFile for the postgres instance used to store attachment service state
    fn postgres_pid_file(&self) -> Utf8PathBuf {
        Utf8PathBuf::from_path_buf(
            self.env
                .base_data_dir
                .join("attachment_service_postgres.pid"),
        )
        .expect("non-Unicode path")
    }

    /// In order to access database migrations, we need to find the Neon source tree
    async fn find_source_root(&self) -> anyhow::Result<Utf8PathBuf> {
        // We assume that either prd or our binary is in the source tree. The former is usually
        // true for automated test runners, the latter is usually true for developer workstations. Often
        // both are true, which is fine.
        let candidate_start_points: Vec<Utf8PathBuf> = vec![
            // Current working directory
            Utf8PathBuf::from_path_buf(std::env::current_dir()?).unwrap(),
            // Directory containing the binary we're running inside
            Utf8PathBuf::from_path_buf(env::current_exe()?.parent().unwrap().to_owned()).unwrap(),
            // Source dir on github action workers
            Utf8PathBuf::from_str("/__w/neon").unwrap(),
        ];

        // For each candidate start point, search through ancestors looking for a neon.git source tree root
        for start_point in &candidate_start_points {
            // Start from the build dir: assumes we are running out of a built neon source tree
            let mut cursor = start_point.clone();
            loop {
                // A crude approximation: the root of the source tree is whatever contains a "control_plane"
                // subdirectory.
                let control_plane = cursor.join("control_plane");
                if tokio::fs::try_exists(&control_plane).await? {
                    return Ok(cursor);
                } else {
                    cursor = match cursor.parent() {
                        Some(d) => d.to_owned(),
                        None => {
                            // Give up searching from this starting point, try the next one
                            break;
                        }
                    };
                }
            }
        }

        // Fall-through
        Err(anyhow::anyhow!(
            "Could not find control_plane src dir, after searching ancestors of {candidate_start_points:?}"
        ))
    }

    /// Find the directory containing postgres binaries, such as `initdb` and `pg_ctl`
    ///
    /// This usually uses ATTACHMENT_SERVICE_POSTGRES_VERSION of postgres, but will fall back
    /// to other versions if that one isn't found.  Some automated tests create circumstances
    /// where only one version is available in pg_distrib_dir, such as `test_remote_extensions`.
    pub async fn get_pg_bin_path(&self) -> anyhow::Result<PathBuf> {
        let prefer_versions = vec![ATTACHMENT_SERVICE_POSTGRES_VERSION, 15, 14];

        for v in prefer_versions {
            let path = self.env.pg_bin_dir(v)?;
            if tokio::fs::try_exists(&path).await? {
                return Ok(path);
            }
        }

        // Fall through
        anyhow::bail!(
            "Postgres binaries not found in {}",
            self.env.pg_distrib_dir.display()
        );
    }

    /// Readiness check for our postgres process
    async fn pg_isready(&self, pg_bin_path: &Path) -> anyhow::Result<bool> {
        let bin_path = pg_bin_path.join("pg_isready");
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
    /// Returns value for DATABASE_URL environment variable.
    pub async fn setup_database(&self) -> anyhow::Result<String> {
        let database_url = format!(
            "postgresql://localhost:{}/attachment_service",
            self.postgres_port
        );
        println!("Running attachment service database setup...");
        fn change_database_of_url(database_url: &str, default_database: &str) -> (String, String) {
            let base = ::url::Url::parse(database_url).unwrap();
            let database = base.path_segments().unwrap().last().unwrap().to_owned();
            let mut new_url = base.join(default_database).unwrap();
            new_url.set_query(base.query());
            (database, new_url.into())
        }

        #[derive(Debug, Clone)]
        pub struct CreateDatabaseStatement {
            db_name: String,
        }

        impl CreateDatabaseStatement {
            pub fn new(db_name: &str) -> Self {
                CreateDatabaseStatement {
                    db_name: db_name.to_owned(),
                }
            }
        }

        impl<DB: Backend> QueryFragment<DB> for CreateDatabaseStatement {
            fn walk_ast<'b>(&'b self, mut out: AstPass<'_, 'b, DB>) -> QueryResult<()> {
                out.push_sql("CREATE DATABASE ");
                out.push_identifier(&self.db_name)?;
                Ok(())
            }
        }

        impl<Conn> RunQueryDsl<Conn> for CreateDatabaseStatement {}

        impl QueryId for CreateDatabaseStatement {
            type QueryId = ();

            const HAS_STATIC_QUERY_ID: bool = false;
        }
        if PgConnection::establish(&database_url).is_err() {
            let (database, postgres_url) = change_database_of_url(&database_url, "postgres");
            println!("Creating database: {database}");
            let mut conn = PgConnection::establish(&postgres_url)?;
            CreateDatabaseStatement::new(&database).execute(&mut conn)?;
        }
        let mut conn = PgConnection::establish(&database_url)?;

        let migrations_dir = self
            .find_source_root()
            .await?
            .join("control_plane/attachment_service/migrations");

        let migrations = diesel_migrations::FileBasedMigrations::from_path(migrations_dir)?;
        println!("Running migrations in {}", migrations.path().display());
        HarnessWithOutput::write_to_stdout(&mut conn)
            .run_pending_migrations(migrations)
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!(e))?;

        println!("Migrations complete");

        Ok(database_url)
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        // Start a vanilla Postgres process used by the attachment service for persistence.
        let pg_data_path = self.env.base_data_dir.join("attachment_service_db");
        let pg_bin_path = self.get_pg_bin_path().await?;
        let pg_log_path = pg_data_path.join("postgres.log");

        if !tokio::fs::try_exists(&pg_data_path).await? {
            // Initialize empty database
            let initdb_path = pg_bin_path.join("initdb");
            let mut child = Command::new(&initdb_path)
                .args(["-D", &pg_data_path.to_string_lossy()])
                .spawn()
                .expect("Failed to spawn initdb");
            let status = child.wait().await?;
            if !status.success() {
                anyhow::bail!("initdb failed with status {status}");
            }

            tokio::fs::write(
                &pg_data_path.join("postgresql.conf"),
                format!("port = {}", self.postgres_port),
            )
            .await?;
        };

        println!("Starting attachment service database...");
        let db_start_args = [
            "-w",
            "-D",
            &pg_data_path.to_string_lossy(),
            "-l",
            &pg_log_path.to_string_lossy(),
            "start",
        ];

        background_process::start_process(
            "attachment_service_db",
            &self.env.base_data_dir,
            &pg_bin_path.join("pg_ctl"),
            db_start_args,
            [],
            background_process::InitialPidFile::Create(self.postgres_pid_file()),
            || self.pg_isready(&pg_bin_path),
        )
        .await?;

        // Run migrations on every startup, in case something changed.
        let database_url = self.setup_database().await?;

        let path_str = self.path.to_string_lossy();
        let mut args = vec!["-l", &self.listen, "-p", &path_str]
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        if let Some(jwt_token) = &self.jwt_token {
            args.push(format!("--jwt-token={jwt_token}"));
        }

        if let Some(public_key_path) = &self.public_key_path {
            args.push(format!("--public-key={public_key_path}"));
        }

        background_process::start_process(
            COMMAND,
            &self.env.base_data_dir,
            &self.env.attachment_service_bin(),
            args,
            [
                (
                    "NEON_REPO_DIR".to_string(),
                    self.env.base_data_dir.to_string_lossy().to_string(),
                ),
                ("DATABASE_URL".to_string(), database_url),
            ],
            background_process::InitialPidFile::Create(self.pid_file()),
            || async {
                match self.status().await {
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

        let pg_data_path = self.env.base_data_dir.join("attachment_service_db");
        let pg_bin_path = self.get_pg_bin_path().await?;

        println!("Stopping attachment service database...");
        let pg_stop_args = ["-D", &pg_data_path.to_string_lossy(), "stop"];
        let stop_status = Command::new(pg_bin_path.join("pg_ctl"))
            .args(pg_stop_args)
            .spawn()?
            .wait()
            .await?;
        if !stop_status.success() {
            let pg_status_args = ["-D", &pg_data_path.to_string_lossy(), "status"];
            let status_exitcode = Command::new(pg_bin_path.join("pg_ctl"))
                .args(pg_status_args)
                .spawn()?
                .wait()
                .await?;

            // pg_ctl status returns the magic number 3 if postgres is not running: in this case it is
            // fine that stop failed.  Otherwise it is an error that stop failed.
            if Some(3) == status_exitcode.code() {
                println!("Attachment service data base is already stopped");
                return Ok(());
            } else {
                anyhow::bail!("Failed to stop attachment service database: {stop_status}")
            }
        }

        Ok(())
    }

    /// Simple HTTP request wrapper for calling into attachment service
    async fn dispatch<RQ, RS>(
        &self,
        method: hyper::Method,
        path: String,
        body: Option<RQ>,
    ) -> anyhow::Result<RS>
    where
        RQ: Serialize + Sized,
        RS: DeserializeOwned + Sized,
    {
        let url = self
            .env
            .control_plane_api
            .clone()
            .unwrap()
            .join(&path)
            .unwrap();

        let mut builder = self.client.request(method, url);
        if let Some(body) = body {
            builder = builder.json(&body)
        }
        if let Some(jwt_token) = &self.jwt_token {
            builder = builder.header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {jwt_token}"),
            );
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
        };

        let response = self
            .dispatch::<_, AttachHookResponse>(
                Method::POST,
                "attach-hook".to_string(),
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
            .dispatch::<_, InspectResponse>(Method::POST, "inspect".to_string(), Some(request))
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
    pub async fn tenant_locate(&self, tenant_id: TenantId) -> anyhow::Result<TenantLocateResponse> {
        self.dispatch::<(), _>(Method::GET, format!("tenant/{tenant_id}/locate"), None)
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
            format!("tenant/{tenant_shard_id}/migrate"),
            Some(TenantShardMigrateRequest {
                tenant_shard_id,
                node_id,
            }),
        )
        .await
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub async fn node_register(&self, req: NodeRegisterRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(Method::POST, "node".to_string(), Some(req))
            .await
    }

    #[instrument(skip_all, fields(node_id=%req.node_id))]
    pub async fn node_configure(&self, req: NodeConfigureRequest) -> anyhow::Result<()> {
        self.dispatch::<_, ()>(
            Method::PUT,
            format!("node/{}/config", req.node_id),
            Some(req),
        )
        .await
    }

    #[instrument(skip(self))]
    pub async fn status(&self) -> anyhow::Result<()> {
        self.dispatch::<(), ()>(Method::GET, "status".to_string(), None)
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
