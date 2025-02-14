//! This program dumps a remote Postgres database into a local Postgres database
//! and uploads the resulting PGDATA into object storage for import into a Timeline.
//!
//! # Context, Architecture, Design
//!
//! See cloud.git Fast Imports RFC (<https://github.com/neondatabase/cloud/pull/19799>)
//! for the full picture.
//! The RFC describing the storage pieces of importing the PGDATA dump into a Timeline
//! is publicly accessible at <https://github.com/neondatabase/neon/pull/9538>.
//!
//! # This is a Prototype!
//!
//! This program is part of a prototype feature and not yet used in production.
//!
//! The cloud.git RFC contains lots of suggestions for improving e2e throughput
//! of this step of the timeline import process.
//!
//! # Local Testing
//!
//! - Comment out most of the pgxns in compute-node.Dockerfile to speed up the build.
//! - Build the image with the following command:
//!
//! ```bash
//! docker buildx build --platform linux/amd64 --build-arg DEBIAN_VERSION=bullseye --build-arg GIT_VERSION=local --build-arg PG_VERSION=v14 --build-arg BUILD_TAG="$(date --iso-8601=s -u)" -t localhost:3030/localregistry/compute-node-v14:latest -f compute/compute-node.Dockerfile .
//! docker push localhost:3030/localregistry/compute-node-v14:latest
//! ```

use anyhow::{bail, Context};
use aws_config::BehaviorVersion;
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use compute_tools::extension_server::{get_pg_version, PostgresMajorVersion};
use nix::unistd::Pid;
use tracing::{error, info, info_span, warn, Instrument};
use utils::fs_ext::is_directory_empty;

#[path = "fast_import/aws_s3_sync.rs"]
mod aws_s3_sync;
#[path = "fast_import/child_stdio_to_log.rs"]
mod child_stdio_to_log;
#[path = "fast_import/s3_uri.rs"]
mod s3_uri;

const PG_WAIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
const PG_WAIT_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(300);

#[derive(Subcommand, Debug)]
enum Command {
    /// Runs local postgres (neon binary), restores into it,
    /// uploads pgdata to s3 to be consumed by pageservers
    Pgdata {
        /// Raw connection string to the source database. Used only in tests,
        /// real scenario uses encrypted connection string in spec.json from s3.
        #[clap(long)]
        source_connection_string: Option<String>,
        /// If specified, will not shut down the local postgres after the import. Used in local testing
        #[clap(short, long)]
        interactive: bool,
        /// Port to run postgres on. Default is 5432.
        #[clap(long, default_value_t = 5432)]
        pg_port: u16, // port to run postgres on, 5432 is default

        /// Number of CPUs in the system. This is used to configure # of
        /// parallel worker processes, for index creation.
        #[clap(long, env = "NEON_IMPORTER_NUM_CPUS")]
        num_cpus: Option<usize>,

        /// Amount of RAM in the system. This is used to configure shared_buffers
        /// and maintenance_work_mem.
        #[clap(long, env = "NEON_IMPORTER_MEMORY_MB")]
        memory_mb: Option<usize>,
    },

    /// Runs pg_dump-pg_restore from source to destination without running local postgres.
    DumpRestore {
        /// Raw connection string to the source database. Used only in tests,
        /// real scenario uses encrypted connection string in spec.json from s3.
        #[clap(long)]
        source_connection_string: Option<String>,
        /// Raw connection string to the destination database. Used only in tests,
        /// real scenario uses encrypted connection string in spec.json from s3.
        #[clap(long)]
        destination_connection_string: Option<String>,
    },
}

#[derive(clap::Parser)]
struct Args {
    #[clap(long, env = "NEON_IMPORTER_WORKDIR")]
    working_directory: Utf8PathBuf,
    #[clap(long, env = "NEON_IMPORTER_S3_PREFIX")]
    s3_prefix: Option<s3_uri::S3Uri>,
    #[clap(long, env = "NEON_IMPORTER_PG_BIN_DIR")]
    pg_bin_dir: Utf8PathBuf,
    #[clap(long, env = "NEON_IMPORTER_PG_LIB_DIR")]
    pg_lib_dir: Utf8PathBuf,

    #[clap(subcommand)]
    command: Command,
}

#[serde_with::serde_as]
#[derive(serde::Deserialize)]
struct Spec {
    encryption_secret: EncryptionSecret,
    #[serde_as(as = "serde_with::base64::Base64")]
    source_connstring_ciphertext_base64: Vec<u8>,
    #[serde_as(as = "Option<serde_with::base64::Base64>")]
    destination_connstring_ciphertext_base64: Option<Vec<u8>>,
}

#[derive(serde::Deserialize)]
enum EncryptionSecret {
    #[allow(clippy::upper_case_acronyms)]
    KMS { key_id: String },
}

// copied from pageserver_api::config::defaults::DEFAULT_LOCALE to avoid dependency just for a constant
const DEFAULT_LOCALE: &str = if cfg!(target_os = "macos") {
    "C"
} else {
    "C.UTF-8"
};

async fn decode_connstring(
    kms_client: &aws_sdk_kms::Client,
    key_id: &String,
    connstring_ciphertext_base64: Vec<u8>,
) -> Result<String, anyhow::Error> {
    let mut output = kms_client
        .decrypt()
        .key_id(key_id)
        .ciphertext_blob(aws_sdk_s3::primitives::Blob::new(
            connstring_ciphertext_base64,
        ))
        .send()
        .await
        .context("decrypt connection string")?;

    let plaintext = output
        .plaintext
        .take()
        .context("get plaintext connection string")?;

    String::from_utf8(plaintext.into_inner()).context("parse connection string as utf8")
}

struct PostgresProcess {
    pgdata_dir: Utf8PathBuf,
    pg_bin_dir: Utf8PathBuf,
    pgbin: Utf8PathBuf,
    pg_lib_dir: Utf8PathBuf,
    postgres_proc: Option<tokio::process::Child>,
}

impl PostgresProcess {
    fn new(pgdata_dir: Utf8PathBuf, pg_bin_dir: Utf8PathBuf, pg_lib_dir: Utf8PathBuf) -> Self {
        Self {
            pgdata_dir,
            pgbin: pg_bin_dir.join("postgres"),
            pg_bin_dir,
            pg_lib_dir,
            postgres_proc: None,
        }
    }

    async fn prepare(&self, initdb_user: &str) -> Result<(), anyhow::Error> {
        tokio::fs::create_dir(&self.pgdata_dir)
            .await
            .context("create pgdata directory")?;

        let pg_version = match get_pg_version(self.pgbin.as_ref()) {
            PostgresMajorVersion::V14 => 14,
            PostgresMajorVersion::V15 => 15,
            PostgresMajorVersion::V16 => 16,
            PostgresMajorVersion::V17 => 17,
        };
        postgres_initdb::do_run_initdb(postgres_initdb::RunInitdbArgs {
            superuser: initdb_user,
            locale: DEFAULT_LOCALE, // XXX: this shouldn't be hard-coded,
            pg_version,
            initdb_bin: self.pg_bin_dir.join("initdb").as_ref(),
            library_search_path: &self.pg_lib_dir, // TODO: is this right? Prob works in compute image, not sure about neon_local.
            pgdata: &self.pgdata_dir,
        })
        .await
        .context("initdb")
    }

    async fn start(
        &mut self,
        initdb_user: &str,
        port: u16,
        nproc: usize,
        memory_mb: usize,
    ) -> Result<&tokio::process::Child, anyhow::Error> {
        self.prepare(initdb_user).await?;

        // Somewhat arbitrarily, use 10 % of memory for shared buffer cache, 70% for
        // maintenance_work_mem (i.e. for sorting during index creation), and leave the rest
        // available for misc other stuff that PostgreSQL uses memory for.
        let shared_buffers_mb = ((memory_mb as f32) * 0.10) as usize;
        let maintenance_work_mem_mb = ((memory_mb as f32) * 0.70) as usize;

        //
        // Launch postgres process
        //
        let mut proc = tokio::process::Command::new(&self.pgbin)
            .arg("-D")
            .arg(&self.pgdata_dir)
            .args(["-p", &format!("{port}")])
            .args(["-c", "wal_level=minimal"])
            .args(["-c", &format!("shared_buffers={shared_buffers_mb}MB")])
            .args(["-c", "shared_buffers=10GB"])
            .args(["-c", "max_wal_senders=0"])
            .args(["-c", "fsync=off"])
            .args(["-c", "full_page_writes=off"])
            .args(["-c", "synchronous_commit=off"])
            .args([
                "-c",
                &format!("maintenance_work_mem={maintenance_work_mem_mb}MB"),
            ])
            .args(["-c", &format!("max_parallel_maintenance_workers={nproc}")])
            .args(["-c", &format!("max_parallel_workers={nproc}")])
            .args(["-c", &format!("max_parallel_workers_per_gather={nproc}")])
            .args(["-c", &format!("max_worker_processes={nproc}")])
            .args(["-c", "effective_io_concurrency=100"])
            .env_clear()
            .env("LD_LIBRARY_PATH", &self.pg_lib_dir)
            .env(
                "ASAN_OPTIONS",
                std::env::var("ASAN_OPTIONS").unwrap_or_default(),
            )
            .env(
                "UBSAN_OPTIONS",
                std::env::var("UBSAN_OPTIONS").unwrap_or_default(),
            )
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn postgres")?;

        info!("spawned postgres, waiting for it to become ready");
        tokio::spawn(
            child_stdio_to_log::relay_process_output(proc.stdout.take(), proc.stderr.take())
                .instrument(info_span!("postgres")),
        );

        self.postgres_proc = Some(proc);
        Ok(self.postgres_proc.as_ref().unwrap())
    }

    async fn shutdown(&mut self) -> Result<(), anyhow::Error> {
        let proc: &mut tokio::process::Child = self.postgres_proc.as_mut().unwrap();
        info!("shutdown postgres");
        nix::sys::signal::kill(
            Pid::from_raw(i32::try_from(proc.id().unwrap()).expect("convert child pid to i32")),
            nix::sys::signal::SIGTERM,
        )
        .context("signal postgres to shut down")?;
        proc.wait()
            .await
            .context("wait for postgres to shut down")
            .map(|_| ())
    }
}

async fn wait_until_ready(connstring: String, create_dbname: String) {
    // Create neondb database in the running postgres
    let start_time = std::time::Instant::now();

    loop {
        if start_time.elapsed() > PG_WAIT_TIMEOUT {
            error!(
                "timeout exceeded: failed to poll postgres and create database within 10 minutes"
            );
            std::process::exit(1);
        }

        match tokio_postgres::connect(
            &connstring.replace("dbname=neondb", "dbname=postgres"),
            tokio_postgres::NoTls,
        )
        .await
        {
            Ok((client, connection)) => {
                // Spawn the connection handling task to maintain the connection
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        warn!("connection error: {}", e);
                    }
                });

                match client
                    .simple_query(format!("CREATE DATABASE {create_dbname};").as_str())
                    .await
                {
                    Ok(_) => {
                        info!("created {} database", create_dbname);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "failed to create database: {}, retying in {}s",
                            e,
                            PG_WAIT_RETRY_INTERVAL.as_secs_f32()
                        );
                        tokio::time::sleep(PG_WAIT_RETRY_INTERVAL).await;
                        continue;
                    }
                }
            }
            Err(_) => {
                info!(
                    "postgres not ready yet, retrying in {}s",
                    PG_WAIT_RETRY_INTERVAL.as_secs_f32()
                );
                tokio::time::sleep(PG_WAIT_RETRY_INTERVAL).await;
                continue;
            }
        }
    }
}

async fn run_dump_restore(
    workdir: Utf8PathBuf,
    pg_bin_dir: Utf8PathBuf,
    pg_lib_dir: Utf8PathBuf,
    source_connstring: String,
    destination_connstring: String,
) -> Result<(), anyhow::Error> {
    let dumpdir = workdir.join("dumpdir");

    let common_args = [
        // schema mapping (prob suffices to specify them on one side)
        "--no-owner".to_string(),
        "--no-privileges".to_string(),
        "--no-publications".to_string(),
        "--no-security-labels".to_string(),
        "--no-subscriptions".to_string(),
        "--no-tablespaces".to_string(),
        // format
        "--format".to_string(),
        "directory".to_string(),
        // concurrency
        "--jobs".to_string(),
        num_cpus::get().to_string(),
        // progress updates
        "--verbose".to_string(),
    ];

    info!("dump into the working directory");
    {
        let mut pg_dump = tokio::process::Command::new(pg_bin_dir.join("pg_dump"))
            .args(&common_args)
            .arg("-f")
            .arg(&dumpdir)
            .arg("--no-sync")
            // POSITIONAL args
            // source db (db name included in connection string)
            .arg(&source_connstring)
            // how we run it
            .env_clear()
            .env("LD_LIBRARY_PATH", &pg_lib_dir)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn pg_dump")?;

        info!(pid=%pg_dump.id().unwrap(), "spawned pg_dump");

        tokio::spawn(
            child_stdio_to_log::relay_process_output(pg_dump.stdout.take(), pg_dump.stderr.take())
                .instrument(info_span!("pg_dump")),
        );

        let st = pg_dump.wait().await.context("wait for pg_dump")?;
        info!(status=?st, "pg_dump exited");
        if !st.success() {
            error!(status=%st, "pg_dump failed, restore will likely fail as well");
            bail!("pg_dump failed");
        }
    }

    // TODO: maybe do it in a streaming way, plenty of internal research done on this already
    // TODO: do the unlogged table trick
    {
        let mut pg_restore = tokio::process::Command::new(pg_bin_dir.join("pg_restore"))
            .args(&common_args)
            .arg("-d")
            .arg(&destination_connstring)
            // POSITIONAL args
            .arg(&dumpdir)
            // how we run it
            .env_clear()
            .env("LD_LIBRARY_PATH", &pg_lib_dir)
            .kill_on_drop(true)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("spawn pg_restore")?;

        info!(pid=%pg_restore.id().unwrap(), "spawned pg_restore");
        tokio::spawn(
            child_stdio_to_log::relay_process_output(
                pg_restore.stdout.take(),
                pg_restore.stderr.take(),
            )
            .instrument(info_span!("pg_restore")),
        );
        let st = pg_restore.wait().await.context("wait for pg_restore")?;
        info!(status=?st, "pg_restore exited");
        if !st.success() {
            error!(status=%st, "pg_restore failed, restore will likely fail as well");
            bail!("pg_restore failed");
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn cmd_pgdata(
    kms_client: Option<aws_sdk_kms::Client>,
    maybe_s3_prefix: Option<s3_uri::S3Uri>,
    maybe_spec: Option<Spec>,
    source_connection_string: Option<String>,
    interactive: bool,
    pg_port: u16,
    workdir: Utf8PathBuf,
    pg_bin_dir: Utf8PathBuf,
    pg_lib_dir: Utf8PathBuf,
    num_cpus: Option<usize>,
    memory_mb: Option<usize>,
) -> Result<(), anyhow::Error> {
    if maybe_spec.is_none() && source_connection_string.is_none() {
        bail!("spec must be provided for pgdata command");
    }
    if maybe_spec.is_some() && source_connection_string.is_some() {
        bail!("only one of spec or source_connection_string can be provided");
    }

    let source_connection_string = if let Some(spec) = maybe_spec {
        match spec.encryption_secret {
            EncryptionSecret::KMS { key_id } => {
                decode_connstring(
                    kms_client.as_ref().unwrap(),
                    &key_id,
                    spec.source_connstring_ciphertext_base64,
                )
                .await?
            }
        }
    } else {
        source_connection_string.unwrap()
    };

    let superuser = "cloud_admin";
    let destination_connstring = format!(
        "host=localhost port={} user={} dbname=neondb",
        pg_port, superuser
    );

    let pgdata_dir = workdir.join("pgdata");
    let mut proc = PostgresProcess::new(pgdata_dir.clone(), pg_bin_dir.clone(), pg_lib_dir.clone());
    let nproc = num_cpus.unwrap_or_else(num_cpus::get);
    let memory_mb = memory_mb.unwrap_or(256);
    proc.start(superuser, pg_port, nproc, memory_mb).await?;
    wait_until_ready(destination_connstring.clone(), "neondb".to_string()).await;

    run_dump_restore(
        workdir.clone(),
        pg_bin_dir,
        pg_lib_dir,
        source_connection_string,
        destination_connstring,
    )
    .await?;

    // If interactive mode, wait for Ctrl+C
    if interactive {
        info!("Running in interactive mode. Press Ctrl+C to shut down.");
        tokio::signal::ctrl_c().await.context("wait for ctrl-c")?;
    }

    proc.shutdown().await?;

    // Only sync if s3_prefix was specified
    if let Some(s3_prefix) = maybe_s3_prefix {
        info!("upload pgdata");
        aws_s3_sync::sync(Utf8Path::new(&pgdata_dir), &s3_prefix.append("/pgdata/"))
            .await
            .context("sync dump directory to destination")?;

        info!("write status");
        {
            let status_dir = workdir.join("status");
            std::fs::create_dir(&status_dir).context("create status directory")?;
            let status_file = status_dir.join("pgdata");
            std::fs::write(&status_file, serde_json::json!({"done": true}).to_string())
                .context("write status file")?;
            aws_s3_sync::sync(&status_dir, &s3_prefix.append("/status/"))
                .await
                .context("sync status directory to destination")?;
        }
    }

    Ok(())
}

async fn cmd_dumprestore(
    kms_client: Option<aws_sdk_kms::Client>,
    maybe_spec: Option<Spec>,
    source_connection_string: Option<String>,
    destination_connection_string: Option<String>,
    workdir: Utf8PathBuf,
    pg_bin_dir: Utf8PathBuf,
    pg_lib_dir: Utf8PathBuf,
) -> Result<(), anyhow::Error> {
    let (source_connstring, destination_connstring) = if let Some(spec) = maybe_spec {
        match spec.encryption_secret {
            EncryptionSecret::KMS { key_id } => {
                let source = decode_connstring(
                    kms_client.as_ref().unwrap(),
                    &key_id,
                    spec.source_connstring_ciphertext_base64,
                )
                .await?;

                let dest = if let Some(dest_ciphertext) =
                    spec.destination_connstring_ciphertext_base64
                {
                    decode_connstring(kms_client.as_ref().unwrap(), &key_id, dest_ciphertext)
                        .await?
                } else {
                    bail!("destination connection string must be provided in spec for dump_restore command");
                };

                (source, dest)
            }
        }
    } else {
        (
            source_connection_string.unwrap(),
            if let Some(val) = destination_connection_string {
                val
            } else {
                bail!("destination connection string must be provided for dump_restore command");
            },
        )
    };

    run_dump_restore(
        workdir,
        pg_bin_dir,
        pg_lib_dir,
        source_connstring,
        destination_connstring,
    )
    .await
}

#[tokio::main]
pub(crate) async fn main() -> anyhow::Result<()> {
    utils::logging::init(
        utils::logging::LogFormat::Json,
        utils::logging::TracingErrorLayerEnablement::EnableWithRustLogFilter,
        utils::logging::Output::Stdout,
    )?;

    info!("starting");

    let args = Args::parse();

    // Initialize AWS clients only if s3_prefix is specified
    let (aws_config, kms_client) = if args.s3_prefix.is_some() {
        let config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
        let kms = aws_sdk_kms::Client::new(&config);
        (Some(config), Some(kms))
    } else {
        (None, None)
    };

    let spec: Option<Spec> = if let Some(s3_prefix) = &args.s3_prefix {
        let spec_key = s3_prefix.append("/spec.json");
        let s3_client = aws_sdk_s3::Client::new(aws_config.as_ref().unwrap());
        let object = s3_client
            .get_object()
            .bucket(&spec_key.bucket)
            .key(spec_key.key)
            .send()
            .await
            .context("get spec from s3")?
            .body
            .collect()
            .await
            .context("download spec body")?;
        serde_json::from_slice(&object.into_bytes()).context("parse spec as json")?
    } else {
        None
    };

    match tokio::fs::create_dir(&args.working_directory).await {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            if !is_directory_empty(&args.working_directory)
                .await
                .context("check if working directory is empty")?
            {
                bail!("working directory is not empty");
            } else {
                // ok
            }
        }
        Err(e) => return Err(anyhow::Error::new(e).context("create working directory")),
    }

    match args.command {
        Command::Pgdata {
            source_connection_string,
            interactive,
            pg_port,
            num_cpus,
            memory_mb,
        } => {
            cmd_pgdata(
                kms_client,
                args.s3_prefix,
                spec,
                source_connection_string,
                interactive,
                pg_port,
                args.working_directory,
                args.pg_bin_dir,
                args.pg_lib_dir,
                num_cpus,
                memory_mb,
            )
            .await?;
        }
        Command::DumpRestore {
            source_connection_string,
            destination_connection_string,
        } => {
            cmd_dumprestore(
                kms_client,
                spec,
                source_connection_string,
                destination_connection_string,
                args.working_directory,
                args.pg_bin_dir,
                args.pg_lib_dir,
            )
            .await?;
        }
    }

    Ok(())
}
