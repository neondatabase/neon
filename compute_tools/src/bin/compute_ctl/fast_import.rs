use std::{fmt::Display, process::ExitStatus, str::FromStr, sync::Arc};

use anyhow::Context;
use aws_config::BehaviorVersion;
use camino::Utf8PathBuf;
use compute_tools::compute::{ComputeNode, ParsedSpec};
use nix::{libc::SIGTERM, unistd::Pid};
use tracing::{info, info_span, warn, Instrument};

#[path = "fast_import/child_stdio_to_log.rs"]
mod child_stdio_to_log;
#[path = "fast_import/s3_uri.rs"]
mod s3_uri;
#[path = "fast_import/s5cmd.rs"]
mod s5cmd;

fn must_get_config_var<T: FromStr<Err: Display>>(varname: &str) -> T {
    utils::env::var(&varname)
        .with_context(|| format!("missing nev var {varname:?}"))
        .unwrap()
}

pub(crate) async fn entrypoint(
    pspec: ParsedSpec,
    compute: Arc<ComputeNode>,
    child: &mut std::process::Child,
) -> anyhow::Result<()> {
    //
    // Retrieve configuraiton
    //
    let source_connstring_kms_encryption_key_id: String =
        must_get_config_var("NEON_IMPORTER_SOURCE_CONNNECTION_STRING_KMS_ENCRYPTION_KEY_ID");
    let source_connstring_encrypted: Vec<u8> = base64::decode(must_get_config_var::<String>(
        "NEON_IMPORTER_SOURCE_CONNNECTION_STRING_UTF8_ENCRYPT_BASE64",
    ))
    .context("decode base64-encoded encrypted connection string")?;
    let destination_s3_uri: s3_uri::S3Uri = must_get_config_var("NEON_IMPORTER_PGDATA_DESTINATION");

    let working_directory: Utf8PathBuf = todo!();

    //
    // Setup clients
    //
    let aws_config = aws_config::load_defaults(BehaviorVersion::v2024_03_28()).await;
    let s3_client = aws_sdk_s3::Client::new(&aws_config);
    let kms_client = aws_sdk_kms::Client::new(&aws_config);

    //
    // Validate access
    //
    {
        let testdir = working_directory.join("testdir");
        let testfile = testdir.join("testfile");
        let dest_testdir = destination_s3_uri.append("/testdir/");
        std::fs::create_dir(testdir).context("create test directory")?;
        std::fs::write(testfile, "testcontent").context("write test file")?;
        s5cmd::sync(&testdir, &dest_testdir)
            .context("sync test directory to destination (more details might be in logs)")?;
        std::fs::remove_file(testfile).context("remove test file")?;
        s5cmd::sync(&testdir, &dest_testdir).context(
            "secod sync of test directory to destination (more details might be in logs)",
        )?;
        std::fs::remove_dir(testdir).context("remove test directory")?;
    }

    //
    // Start the work
    //

    let source_connection_string = {
        let output = kms_client
            .decrypt()
            .key_id(source_connstring_kms_encryption_key_id)
            .ciphertext_blob(aws_sdk_s3::primitives::Blob::new(
                source_connstring_encrypted,
            ))
            .send()
            .await
            .context("decrypt source connection string")?;
        let plaintext = output
            .plaintext()
            .context("get plaintext source connection string")?;
        String::from_utf8(plaintext.into_inner())
            .context("parse source connection string as utf8")?
    };

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

    // dump into the working directory
    {
        let pg_dump = tokio::process::Command::new("pg_dump")
            .args(common_args)
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

    // restore from working directory into vanilla postgres
    {
        let pg_restore = tokio::process::Command::new("pg_restore")
            .args(common_args)
            .arg("-d")
            .arg(&compute.connstr.as_str())
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

    // shutdown postgres
    {
        nix::sys::signal::kill(
            Pid::from_raw(i32::try_from(child.id()).expect("convert child pid to i32")),
            nix::sys::signal::SIGTERM,
        )
        .context("signal postgres to shut down")?;
        let st: ExitStatus = std::thread::scope(|scope| scope.spawn(|| child.wait()).join())
            .expect("temp thread panicked")
            .context("wait for postgres to shut down")?;
        if st.success() {
            info!("postgres shut down successfully");
        } else {
            warn!(status=%st, "postgres shut down with non-zero status");
        }
    }

    // write status
    let status_dir = working_directory.join("status");
    std::fs::create_dir(&status_dir).context("create status directory")?;
    std::fs::write(
        status_dir.join("status"),
        serde_json::json!({"done": true}).to_string(),
    )
    .context("write status file")?;

    // sync pgdata into s3
    todo!()
    // s5cmd::sync(&working_directory, &destination_s3_uri)
}
