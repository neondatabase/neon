//! The canonical way we run `initdb` in Neon.
//!
//! initdb has implicit defaults that are dependent on the environment, e.g., locales & collations.
//!
//! This module's job is to eliminate the environment-dependence as much as possible.

use std::fmt;

use camino::Utf8Path;

pub struct RunInitdbArgs<'a> {
    pub superuser: &'a str,
    pub initdb_bin: &'a Utf8Path,
    pub library_search_path: &'a Utf8Path,
    pub pgdata: &'a Utf8Path,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    Spawn(std::io::Error),
    Failed {
        status: std::process::ExitStatus,
        stderr: Vec<u8>,
    },
    WaitOutput(std::io::Error),
    Other(anyhow::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Spawn(e) => write!(f, "Error spawning command: {:?}", e),
            Error::Failed { status, stderr } => write!(
                f,
                "Command failed with status {:?}: {}",
                status,
                String::from_utf8_lossy(stderr)
            ),
            Error::WaitOutput(e) => write!(f, "Error waiting for command output: {:?}", e),
            Error::Other(e) => write!(f, "Error: {:?}", e),
        }
    }
}

pub async fn do_run_initdb(args: RunInitdbArgs<'_>) -> Result<(), Error> {
    let RunInitdbArgs {
        superuser,
        initdb_bin: initdb_bin_path,
        library_search_path,
        pgdata,
    } = args;
    let mut initdb_command_builder = tokio::process::Command::new(initdb_bin_path);
    initdb_command_builder
        .args(["--pgdata", pgdata.as_ref()])
        .args(["--username", superuser])
        .args(["--encoding", "utf8"])
        .arg("--no-instructions")
        .arg("--no-sync")
        .env_clear()
        .env("LD_LIBRARY_PATH", library_search_path)
        .env("DYLD_LIBRARY_PATH", library_search_path)
        .stdin(std::process::Stdio::null())
        // stdout invocation produces the same output every time, we don't need it
        .stdout(std::process::Stdio::null())
        // we would be interested in the stderr output, if there was any
        .stderr(std::process::Stdio::piped());

    let initdb_command = initdb_command_builder.spawn().map_err(Error::Spawn)?;

    // Ideally we'd select here with the cancellation token, but the problem is that
    // we can't safely terminate initdb: it launches processes of its own, and killing
    // initdb doesn't kill them. After we return from this function, we want the target
    // directory to be able to be cleaned up.
    // See https://github.com/neondatabase/neon/issues/6385
    let initdb_output = initdb_command
        .wait_with_output()
        .await
        .map_err(Error::WaitOutput)?;
    if !initdb_output.status.success() {
        return Err(Error::Failed {
            status: initdb_output.status,
            stderr: initdb_output.stderr,
        });
    }

    Ok(())
}
