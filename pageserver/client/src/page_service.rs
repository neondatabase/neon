use std::sync::{Arc, Mutex};

use futures::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use pageserver_api::{
    models::{
        PagestreamBeMessage, PagestreamFeMessage, PagestreamGetPageRequest,
        PagestreamGetPageResponse,
    },
    reltag::RelTag,
};
use tokio::task::JoinHandle;
use tokio_postgres::CopyOutStream;
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
            .copy_both_simple(&format!("pagestream_v3 {tenant_id} {timeline_id}"))
            .await?;
        let (sink, stream) = copy_both.split(); // TODO: actually support splitting of the CopyBothDuplex so the lock inside this split adaptor goes away.
        let Client {
            cancel_on_client_drop,
            conn_task,
            client: _,
        } = self;
        let shared = Arc::new(Mutex::new(PagestreamShared::ConnTaskRunning(
            ConnTaskRunning {
                cancel_on_client_drop,
                conn_task,
            },
        )));
        Ok(PagestreamClient {
            sink: PagestreamSender {
                shared: shared.clone(),
                sink,
            },
            stream: PagestreamReceiver {
                shared: shared.clone(),
                stream,
            },
            shared,
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
    shared: Arc<Mutex<PagestreamShared>>,
    sink: PagestreamSender,
    stream: PagestreamReceiver,
}

pub struct PagestreamSender {
    #[allow(dead_code)]
    shared: Arc<Mutex<PagestreamShared>>,
    sink: SplitSink<tokio_postgres::CopyBothDuplex<bytes::Bytes>, bytes::Bytes>,
}

pub struct PagestreamReceiver {
    #[allow(dead_code)]
    shared: Arc<Mutex<PagestreamShared>>,
    stream: SplitStream<tokio_postgres::CopyBothDuplex<bytes::Bytes>>,
}

enum PagestreamShared {
    ConnTaskRunning(ConnTaskRunning),
    ConnTaskCancelledJoinHandleReturnedOrDropped,
}
struct ConnTaskRunning {
    cancel_on_client_drop: Option<tokio_util::sync::DropGuard>,
    conn_task: JoinHandle<()>,
}

pub struct RelTagBlockNo {
    pub rel_tag: RelTag,
    pub block_no: u32,
}

impl PagestreamClient {
    pub async fn shutdown(self) {
        let Self {
            shared,
            sink,
            stream,
        } = { self };
        // The `copy_both` split into `sink` and `stream` contains internal channel sender, the receiver of which is polled by `conn_task`.
        // When `conn_task` observes the sender has been dropped, it sends a `FeMessage::CopyFail` into the connection.
        // (see https://github.com/neondatabase/rust-postgres/blob/2005bf79573b8add5cf205b52a2b208e356cc8b0/tokio-postgres/src/copy_both.rs#L56).
        //
        // If we drop(copy_both) first, but then immediately drop the `cancel_on_client_drop`,
        // the CopyFail mesage only makes it to the socket sometimes (i.e., it's a race).
        //
        // Further, the pageserver makes a lot of noise when it receives CopyFail.
        // Computes don't send it in practice, they just hard-close the connection.
        //
        // So, let's behave like the computes and suppress the CopyFail as follows:
        // kill the socket first, then drop copy_both.
        //
        // See also: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY
        //
        // NB: page_service doesn't have a use case to exit the `pagestream` mode currently.
        // => https://github.com/neondatabase/neon/issues/6390
        let ConnTaskRunning {
            cancel_on_client_drop,
            conn_task,
        } = {
            let mut guard = shared.lock().unwrap();
            match std::mem::replace(
                &mut *guard,
                PagestreamShared::ConnTaskCancelledJoinHandleReturnedOrDropped,
            ) {
                PagestreamShared::ConnTaskRunning(conn_task_running) => conn_task_running,
                PagestreamShared::ConnTaskCancelledJoinHandleReturnedOrDropped => unreachable!(),
            }
        };
        let _ = cancel_on_client_drop.unwrap();
        conn_task.await.unwrap();

        // Now drop the split copy_both.
        drop(sink);
        drop(stream);
    }

    pub fn split(self) -> (PagestreamSender, PagestreamReceiver) {
        let Self {
            shared: _,
            sink,
            stream,
        } = self;
        (sink, stream)
    }

    pub async fn getpage(
        &mut self,
        req: PagestreamGetPageRequest,
    ) -> anyhow::Result<PagestreamGetPageResponse> {
        self.getpage_send(req).await?;
        self.getpage_recv().await
    }

    pub async fn getpage_send(&mut self, req: PagestreamGetPageRequest) -> anyhow::Result<()> {
        self.sink.getpage_send(req).await
    }

    pub async fn getpage_recv(&mut self) -> anyhow::Result<PagestreamGetPageResponse> {
        self.stream.getpage_recv().await
    }
}

impl PagestreamSender {
    // TODO: maybe make this impl Sink instead for better composability?
    pub async fn send(&mut self, msg: PagestreamFeMessage) -> anyhow::Result<()> {
        let msg = msg.serialize();
        self.sink.send_all(&mut tokio_stream::once(Ok(msg))).await?;
        Ok(())
    }

    pub async fn getpage_send(&mut self, req: PagestreamGetPageRequest) -> anyhow::Result<()> {
        self.send(PagestreamFeMessage::GetPage(req)).await
    }
}

impl PagestreamReceiver {
    // TODO: maybe make this impl Stream instead for better composability?
    pub async fn recv(&mut self) -> anyhow::Result<PagestreamBeMessage> {
        let next: Option<Result<bytes::Bytes, _>> = self.stream.next().await;
        let next: bytes::Bytes = next.unwrap()?;
        PagestreamBeMessage::deserialize(next)
    }

    pub async fn getpage_recv(&mut self) -> anyhow::Result<PagestreamGetPageResponse> {
        let next: PagestreamBeMessage = self.recv().await?;
        match next {
            PagestreamBeMessage::GetPage(p) => Ok(p),
            PagestreamBeMessage::Error(e) => anyhow::bail!("Error: {:?}", e),
            PagestreamBeMessage::Exists(_)
            | PagestreamBeMessage::Nblocks(_)
            | PagestreamBeMessage::DbSize(_)
            | PagestreamBeMessage::GetSlruSegment(_) => {
                anyhow::bail!(
                    "unexpected be message kind in response to getpage request: {}",
                    next.kind()
                )
            }
            #[cfg(feature = "testing")]
            PagestreamBeMessage::Test(_) => {
                anyhow::bail!(
                    "unexpected be message kind in response to getpage request: {}",
                    next.kind()
                )
            }
        }
    }
}
