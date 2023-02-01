//! The broker client instance of the pageserver, created during pageserver startup.
//! Used by each timelines' [`walreceiver`].

use crate::config::PageServerConf;

use anyhow::Context;
use once_cell::sync::OnceCell;
use storage_broker::BrokerClientChannel;
use tracing::*;

static BROKER_CLIENT: OnceCell<BrokerClientChannel> = OnceCell::new();

///
/// Initialize the broker client. This must be called once at page server startup.
///
pub async fn init_broker_client(conf: &'static PageServerConf) -> anyhow::Result<()> {
    let broker_endpoint = conf.broker_endpoint.clone();

    // Note: we do not attempt connecting here (but validate endpoints sanity).
    let broker_client =
        storage_broker::connect(broker_endpoint.clone(), conf.broker_keepalive_interval).context(
            format!(
                "Failed to create broker client to {}",
                &conf.broker_endpoint
            ),
        )?;

    if BROKER_CLIENT.set(broker_client).is_err() {
        panic!("broker already initialized");
    }

    info!(
        "Initialized broker client with endpoints: {}",
        broker_endpoint
    );
    Ok(())
}

///
/// Get a handle to the broker client
///
pub fn get_broker_client() -> &'static BrokerClientChannel {
    BROKER_CLIENT.get().expect("broker client not initialized")
}

pub fn is_broker_client_initialized() -> bool {
    BROKER_CLIENT.get().is_some()
}
