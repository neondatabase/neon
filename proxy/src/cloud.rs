mod local;

mod legacy;
pub use legacy::{AuthError, AuthErrorImpl, Legacy};

pub mod api;
pub use api::{Api, BoxedApi};

use crate::mgmt;
use crate::waiters::{self, Waiter, Waiters};
use lazy_static::lazy_static;

lazy_static! {
    static ref CPLANE_WAITERS: Waiters<mgmt::ComputeReady> = Default::default();
}

/// Give caller an opportunity to wait for the cloud's reply.
pub async fn with_waiter<R, T, E>(
    psql_session_id: impl Into<String>,
    action: impl FnOnce(Waiter<'static, mgmt::ComputeReady>) -> R,
) -> Result<T, E>
where
    R: std::future::Future<Output = Result<T, E>>,
    E: From<waiters::RegisterError>,
{
    let waiter = CPLANE_WAITERS.register(psql_session_id.into())?;
    action(waiter).await
}

pub fn notify(psql_session_id: &str, msg: mgmt::ComputeReady) -> Result<(), waiters::NotifyError> {
    CPLANE_WAITERS.notify(psql_session_id, msg)
}

/// Construct a new opaque cloud API provider.
pub fn new(url: reqwest::Url) -> anyhow::Result<BoxedApi> {
    Ok(match url.scheme() {
        "https" | "http" => {
            todo!("build a real cloud wrapper")
        }
        "postgresql" | "postgres" | "pg" => {
            // Just point to a local running postgres instance.
            Box::new(local::Local { url })
        }
        other => anyhow::bail!("unsupported url scheme: {other}"),
    })
}
