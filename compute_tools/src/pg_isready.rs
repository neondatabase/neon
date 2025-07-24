use anyhow::{Context, anyhow};

// Run `/usr/local/bin/pg_isready -p {port}`
// Check the connectivity of PG
// Success means PG is listening on the port and accepting connections
// Note that PG does not need to authenticate the connection, nor reserve a connection quota for it.
// See https://www.postgresql.org/docs/current/app-pg-isready.html
pub fn pg_isready(bin: &str, port: u16) -> anyhow::Result<()> {
    let child_result = std::process::Command::new(bin)
        .arg("-p")
        .arg(port.to_string())
        .spawn();

    child_result
        .context("spawn() failed")
        .and_then(|mut child| child.wait().context("wait() failed"))
        .and_then(|status| match status.success() {
            true => Ok(()),
            false => Err(anyhow!("process exited with {status}")),
        })
        // wrap any prior error with the overall context that we couldn't run the command
        .with_context(|| format!("could not run `{bin} --port {port}`"))
}

// It's safe to assume pg_isready is under the same directory with postgres,
// because it is a PG util bin installed along with postgres
pub fn get_pg_isready_bin(pgbin: &str) -> String {
    let split = pgbin.split("/").collect::<Vec<&str>>();
    split[0..split.len() - 1].join("/") + "/pg_isready"
}
