use anyhow::Context;
use camino::Utf8Path;

use super::s3_uri::S3Uri;

pub(crate) async fn sync(local: &Utf8Path, remote: &S3Uri) -> anyhow::Result<()> {
    let mut builder = tokio::process::Command::new("s5cmd");
    // s5cmd uses aws-sdk-go v1, hence doesn't support AWS_ENDPOINT_URL
    if let Some(val) = std::env::var_os("AWS_ENDPOINT_URL") {
        builder.arg("--endpoint-url").arg(val);
    }
    builder
        .arg("sync")
        .arg(local.as_str())
        .arg(remote.to_string());
    let st = builder
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
