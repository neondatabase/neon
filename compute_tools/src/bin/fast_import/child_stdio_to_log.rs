use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{ChildStderr, ChildStdout};
use tracing::info;

/// Asynchronously relays the output from a child process's `stdout` and `stderr` to the tracing log.
/// Each line is read and logged individually, with lossy UTF-8 conversion.
///
/// # Arguments
///
/// * `stdout`: An `Option<ChildStdout>` from the child process.
/// * `stderr`: An `Option<ChildStderr>` from the child process.
///
pub(crate) async fn relay_process_output(stdout: Option<ChildStdout>, stderr: Option<ChildStderr>) {
    let stdout_fut = async {
        if let Some(stdout) = stdout {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                info!(fd = "stdout", "{}", line);
            }
        }
    };

    let stderr_fut = async {
        if let Some(stderr) = stderr {
            let reader = BufReader::new(stderr);
            let mut lines = reader.lines();
            while let Ok(Some(line)) = lines.next_line().await {
                info!(fd = "stderr", "{}", line);
            }
        }
    };

    tokio::join!(stdout_fut, stderr_fut);
}
