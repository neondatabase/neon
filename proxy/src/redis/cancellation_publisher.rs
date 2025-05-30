use core::net::IpAddr;
use std::sync::Arc;

use tokio::sync::Mutex;
use uuid::Uuid;

use crate::pqproto::CancelKeyData;

pub trait CancellationPublisherMut: Send + Sync + 'static {
    #[allow(async_fn_in_trait)]
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()>;
}

pub trait CancellationPublisher: Send + Sync + 'static {
    #[allow(async_fn_in_trait)]
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()>;
}

impl CancellationPublisher for () {
    async fn try_publish(
        &self,
        _cancel_key_data: CancelKeyData,
        _session_id: Uuid,
        _peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<P: CancellationPublisher> CancellationPublisherMut for P {
    async fn try_publish(
        &mut self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        <P as CancellationPublisher>::try_publish(self, cancel_key_data, session_id, peer_addr)
            .await
    }
}

impl<P: CancellationPublisher> CancellationPublisher for Option<P> {
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        if let Some(p) = self {
            p.try_publish(cancel_key_data, session_id, peer_addr).await
        } else {
            Ok(())
        }
    }
}

impl<P: CancellationPublisherMut> CancellationPublisher for Arc<Mutex<P>> {
    async fn try_publish(
        &self,
        cancel_key_data: CancelKeyData,
        session_id: Uuid,
        peer_addr: IpAddr,
    ) -> anyhow::Result<()> {
        self.lock()
            .await
            .try_publish(cancel_key_data, session_id, peer_addr)
            .await
    }
}
