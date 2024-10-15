use anyhow::Context;
use camino::Utf8Path;

use super::s3_uri::S3Uri;

pub(crate) async fn sync(local: &Utf8Path, remote: &S3Uri) -> anyhow::Result<()> {
    let st = tokio::process::Command::new("s5cmd")
        .arg("sync")
        .arg(local.as_str())
        .arg(remote.to_string())
        .spawn()
        .context("spawn s5cmd")?
        .wait()
        .await
        .context("wait for s5cmd")?;
    if st.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("s5cmd failed"))
    }
}
