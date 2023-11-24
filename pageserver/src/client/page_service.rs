use std::pin::Pin;

use futures::SinkExt;
use pageserver_api::{models::{
    PagestreamBeMessage, PagestreamFeMessage, PagestreamGetPageRequest, PagestreamGetPageResponse,
}, reltag::RelTag};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub struct Client {
    copy_both: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>,
    cancel_on_client_drop: Option<tokio_util::sync::DropGuard>,
    conn_task: JoinHandle<()>,
}

pub struct RelTagBlockNo {
    pub rel_tag: RelTag,
    pub block_no: u32,
}

impl Client {
    pub async fn new(
        connstring: String,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(&connstring, postgres::NoTls).await?;

        let conn_task_cancel = CancellationToken::new();
        let conn_task = tokio::spawn({
            let conn_task_cancel = conn_task_cancel.clone();
            async move {
                tokio::select! {
                    _ = conn_task_cancel.cancelled() => { }
                    res = connection => {
                        res.unwrap();
                    }
                }
            }
        });

        let copy_both: tokio_postgres::CopyBothDuplex<bytes::Bytes> = client
            .copy_both_simple(&format!("pagestream {tenant_id} {timeline_id}"))
            .await?;

        Ok(Self {
            copy_both: Box::pin(copy_both),
            conn_task,
            cancel_on_client_drop: Some(conn_task_cancel.drop_guard()),
        })
    }

    pub async fn shutdown(mut self) {
        let _ = self.cancel_on_client_drop.take();
        self.conn_task.await.unwrap();
    }

    pub async fn getpage(
        &mut self,
        key: RelTagBlockNo,
        lsn: Lsn,
    ) -> anyhow::Result<PagestreamGetPageResponse> {
        let req = PagestreamGetPageRequest {
            latest: false,
            rel: key.rel_tag,
            blkno: key.block_no,
            lsn,
        };
        let req = PagestreamFeMessage::GetPage(req);
        let req: bytes::Bytes = req.serialize();
        // let mut req = tokio_util::io::ReaderStream::new(&req);
        let mut req = tokio_stream::once(Ok(req));

        self.copy_both.send_all(&mut req).await?;

        let next: Option<Result<bytes::Bytes, _>> = self.copy_both.next().await;
        let next = next.unwrap().unwrap();

        match PagestreamBeMessage::deserialize(next)? {
            PagestreamBeMessage::Exists(_) => todo!(),
            PagestreamBeMessage::Nblocks(_) => todo!(),
            PagestreamBeMessage::GetPage(p) => Ok(p),
            PagestreamBeMessage::Error(e) => anyhow::bail!("Error: {:?}", e),
            PagestreamBeMessage::DbSize(_) => todo!(),
        }
    }
}
