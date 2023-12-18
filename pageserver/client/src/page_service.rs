use std::pin::Pin;

use futures::SinkExt;
use pageserver_api::{
    models::{
        PagestreamBeMessage, PagestreamFeMessage, PagestreamGetPageRequest,
        PagestreamGetPageResponse,
    },
    reltag::RelTag,
};
use tokio::task::JoinHandle;
use tokio_postgres::CopyOutStream;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use utils::{
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

pub struct Client {
    client: tokio_postgres::Client,
    cancel_on_client_drop: Option<tokio_util::sync::DropGuard>,
    conn_task: JoinHandle<()>,
}

pub struct BasebackupRequest {
    pub tenant_id: TenantId,
    pub timeline_id: TimelineId,
    pub lsn: Option<Lsn>,
    pub gzip: bool,
}

impl Client {
    pub async fn new(connstring: String) -> anyhow::Result<Self> {
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
        Ok(Self {
            cancel_on_client_drop: Some(conn_task_cancel.drop_guard()),
            conn_task,
            client,
        })
    }

    pub async fn pagestream(
        self,
        tenant_id: TenantId,
        timeline_id: TimelineId,
    ) -> anyhow::Result<PagestreamClient> {
        let copy_both: tokio_postgres::CopyBothDuplex<bytes::Bytes> = self
            .client
            .copy_both_simple(&format!("pagestream {tenant_id} {timeline_id}"))
            .await?;
        let Client {
            cancel_on_client_drop,
            conn_task,
            client: _,
        } = self;
        Ok(PagestreamClient {
            copy_both: Box::pin(copy_both),
            conn_task,
            cancel_on_client_drop,
        })
    }

    pub async fn basebackup(&self, req: &BasebackupRequest) -> anyhow::Result<CopyOutStream> {
        let BasebackupRequest {
            tenant_id,
            timeline_id,
            lsn,
            gzip,
        } = req;
        let mut args = Vec::with_capacity(5);
        args.push("basebackup".to_string());
        args.push(format!("{tenant_id}"));
        args.push(format!("{timeline_id}"));
        if let Some(lsn) = lsn {
            args.push(format!("{lsn}"));
        }
        if *gzip {
            args.push("--gzip".to_string())
        }
        Ok(self.client.copy_out(&args.join(" ")).await?)
    }
}

/// Create using [`Client::pagestream`].
pub struct PagestreamClient {
    copy_both: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>,
    cancel_on_client_drop: Option<tokio_util::sync::DropGuard>,
    conn_task: JoinHandle<()>,
}

pub struct RelTagBlockNo {
    pub rel_tag: RelTag,
    pub block_no: u32,
}

impl PagestreamClient {
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

        let msg = PagestreamBeMessage::deserialize(next)?;
        match msg {
            PagestreamBeMessage::GetPage(p) => Ok(p),
            PagestreamBeMessage::Error(e) => anyhow::bail!("Error: {:?}", e),
            PagestreamBeMessage::Exists(_)
            | PagestreamBeMessage::Nblocks(_)
            | PagestreamBeMessage::DbSize(_) => {
                anyhow::bail!(
                    "unexpected be message kind in response to getpage request: {}",
                    msg.kind()
                )
            }
        }
    }
}
