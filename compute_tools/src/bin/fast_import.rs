use anyhow::Context;
use aws_config::BehaviorVersion;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use nix::unistd::Pid;
use tracing::{info, info_span, warn, Instrument};

#[path = "fast_import/child_stdio_to_log.rs"]
mod child_stdio_to_log;
#[path = "fast_import/s3_uri.rs"]
mod s3_uri;
#[path = "fast_import/s5cmd.rs"]
mod s5cmd;

#[derive(clap::Parser)]
struct Args {
    working_directory: Utf8PathBuf,
    s3_prefix: s3_uri::S3Uri,
    pg_bin_dir: Utf8PathBuf,
    pg_lib_dir: Utf8PathBuf,
}

#[serde_with::serde_as]
#[derive(serde::Deserialize)]
struct Spec {
    #[serde_as(as = "serde_with::base64::Base64")]
    source_connstring_kms_encrypted_base64: Vec<u8>,
    source_connstring_kms_key_id: String,
}

#[tokio::main]
pub(crate) async fn main() -> anyhow::Result<()> {
    let Args {
        working_directory,
        s3_prefix,
        pg_bin_dir,
        pg_lib_dir,
    } = Args::parse();

    let aws_config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;

    let spec: Spec = {
        let spec_key = s3_prefix.append("/spec.json");
        let s3_client = aws_sdk_s3::Client::new(&aws_config);
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
    };

    tokio::fs::create_dir(&working_directory)
        .await
        .context("create working directory")?;
    let pgdata_dir = working_directory.join("pgdata");
    tokio::fs::create_dir(&pgdata_dir)
        .await
        .context("create pgdata directory")?;

    //
    // Setup clients
    //
    let aws_config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
    let kms_client = aws_sdk_kms::Client::new(&aws_config);

    //
    //  Initialize pgdata
    //
    let superuser = "cloud_admin"; // XXX: this shouldn't be hard-coded
    postgres_initdb::do_run_initdb(postgres_initdb::RunInitdbArgs {
        superuser,
        initdb_bin: pg_bin_dir.join("initdb").as_ref(),
        library_search_path: &pg_lib_dir, // TODO: is this right? Prob works in compute image, not sure about neon_local.
        pgdata: &pgdata_dir,
    })
    .await
    .context("initdb")?;

    let nproc = num_cpus::get();

    //
    // Launch postgres process
    //
    let mut postgres_proc = tokio::process::Command::new(pg_bin_dir.join("postgres"))
        .arg("-D")
        .arg(&pgdata_dir)
        .args(["-c", "wal_level=minimal"])
        .args(["-c", "shared_buffers=10GB"])
        .args(["-c", "max_wal_senders=0"])
        .args(["-c", "fsync=off"])
        .args(["-c", "full_page_writes=off"])
        .args(["-c", "synchronous_commit=off"])
        .args(["-c", "maintenance_work_mem=8388608"])
        .args(["-c", &format!("max_parallel_maintenance_workers={nproc}")])
        .args(["-c", &format!("max_parallel_workers={nproc}")])
        .args(["-c", &format!("max_parallel_workers_per_gather={nproc}")])
        .args(["-c", &format!("max_worker_processes={nproc}")])
        .args(["-c", "effective_io_concurrency=100"])
        .env_clear()
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("spawn postgres")?;

    info!("spawned postgres, waiting for it to become ready");
    tokio::spawn(
        child_stdio_to_log::relay_process_output(
            postgres_proc.stdout.take(),
            postgres_proc.stderr.take(),
        )
        .instrument(info_span!("postgres")),
    );
    let restore_pg_connstring =
        format!("host=localhost port=5432 user={superuser} dbname=postgres");
    loop {
        let res = tokio_postgres::connect(&restore_pg_connstring, tokio_postgres::NoTls).await;
        if res.is_ok() {
            info!("postgres is ready, could connect to it");
            break;
        }
    }

    //
    // Decrypt connection string
    //
    let source_connection_string = {
        let mut output = kms_client
            .decrypt()
            .key_id(spec.source_connstring_kms_key_id)
            .ciphertext_blob(aws_sdk_s3::primitives::Blob::new(
                spec.source_connstring_kms_encrypted_base64,
            ))
            .send()
            .await
            .context("decrypt source connection string")?;
        let plaintext = output
            .plaintext
            .take()
            .context("get plaintext source connection string")?;
        String::from_utf8(plaintext.into_inner())
            .context("parse source connection string as utf8")?
    };

    //
    // Start the work
    //

    let dumpdir = working_directory.join("dumpdir");

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
        // location
        "--file".to_string(),
        dumpdir.to_string(),
        // concurrency
        "--jobs".to_string(),
        num_cpus::get().to_string(),
        // progress updates
        "--verbose".to_string(),
    ];

    info!("dump into the working directory");
    {
        let mut pg_dump = tokio::process::Command::new("pg_dump")
            .args(&common_args)
            // source db (db name included in connection string)
            .arg("-d")
            .arg(&source_connection_string)
            .arg("--no-sync")
            // how we run it
            .env_clear()
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
            warn!(status=%st, "pg_dump failed, restore will likely fail as well");
        }
    }

    // TODO: do it in a streaming way, plenty of internal research done on this already
    // TODO: do the unlogged table trick

    info!("restore from working directory into vanilla postgres");
    {
        let mut pg_restore = tokio::process::Command::new("pg_restore")
            .args(&common_args)
            .arg("-d")
            .arg(&restore_pg_connstring)
            // how we restore
            .arg("--single-transaction")
            // how we run it
            .env_clear()
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
            warn!(status=%st, "pg_restore failed, restore will likely fail as well");
        }
    }

    info!("shutdown postgres");
    {
        nix::sys::signal::kill(
            Pid::from_raw(
                i32::try_from(postgres_proc.id().unwrap()).expect("convert child pid to i32"),
            ),
            nix::sys::signal::SIGTERM,
        )
        .context("signal postgres to shut down")?;
        postgres_proc
            .wait()
            .await
            .context("wait for postgres to shut down")?;
    }

    info!("upload pgdata");
    s5cmd::sync(Utf8Path::new(&pgdata_dir), &s3_prefix.append("/pgdata/"))
        .await
        .context("sync dump directory to destination")?;

    info!("write status");
    {
        let status_dir = working_directory.join("status");
        std::fs::create_dir(&status_dir).context("create status directory")?;
        std::fs::write(
            status_dir.join("status"),
            serde_json::json!({"done": true}).to_string(),
        )
        .context("write status file")?;
        s5cmd::sync(&status_dir, &s3_prefix.append("/status/"))
            .await
            .context("sync status directory to destination")?;
    }

    Ok(())
}
