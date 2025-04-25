//! A stand-alone program that routes connections, e.g. from
//! `aaa--bbb--1234.external.domain` to `aaa.bbb.internal.domain:1234`.
//!
//! This allows connecting to pods/services running in the same Kubernetes cluster from
//! the outside. Similar to an ingress controller for HTTPS.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    proxy::binary::pg_sni_router::run().await
}
