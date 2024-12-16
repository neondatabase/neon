use anyhow::Context;
use camino::Utf8Path;

use super::s3_uri::S3Uri;

pub(crate) async fn sync(local: &Utf8Path, remote: &S3Uri) -> anyhow::Result<()> {
    let mut builder = tokio::process::Command::new("aws");
    // s5cmd uses aws-sdk-go v1, hence doesn't support AWS_ENDPOINT_URL
    if let Some(val) = std::env::var_os("AWS_ENDPOINT_URL") {
        builder.arg("--endpoint-url").arg(val);
    }
    builder
        .arg("s3")
        .arg("sync")
        .arg(local.as_str())
        .arg(remote.to_string());
    let st = builder
        .spawn()
        .context("spawn aws s3 sync")?
        .wait()
        .await
        .context("wait for aws s3 sync")?;
    if st.success() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("aws s3 sync failed"))
    }
}
