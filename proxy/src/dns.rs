//! Async dns resolvers

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use hickory_resolver::error::ResolveError;
use tokio::time::Instant;
use tracing::trace;

#[derive(Clone)]
pub struct Dns {
    resolver: Arc<hickory_resolver::TokioAsyncResolver>,
}

impl Default for Dns {
    fn default() -> Self {
        Self::new()
    }
}

impl Dns {
    pub fn new() -> Self {
        let (config, options) =
            hickory_resolver::system_conf::read_system_conf().expect("could not read resolv.conf");

        let resolver = Arc::new(hickory_resolver::TokioAsyncResolver::tokio(config, options));

        Self { resolver }
    }

    pub async fn resolve(&self, name: &str) -> Result<impl Iterator<Item = IpAddr>, ResolveError> {
        let start = Instant::now();

        let res = self.resolver.lookup_ip(name).await;

        let resolve_duration = start.elapsed();
        trace!(duration = ?resolve_duration, addr = %name, "resolve host complete");

        Ok(res?.into_iter())
    }
}

impl reqwest::dns::Resolve for Dns {
    fn resolve(&self, name: hyper::client::connect::dns::Name) -> reqwest::dns::Resolving {
        let this = self.clone();
        Box::pin(async move {
            match this.resolve(name.as_str()).await {
                Ok(iter) => Ok(Box::new(iter.map(|ip| SocketAddr::new(ip, 0))) as Box<_>),
                Err(e) => Err(e.into()),
            }
        })
    }
}
