use anyhow::Context;
use clap::Parser;

use pageserver::pgdatadir_mapping::{is_rel_block_key, key_to_rel_block};
use pageserver::repository;

use pageserver_api::reltag::RelTag;
use rand::prelude::*;
use tokio::sync::Barrier;
use tracing::info;
use utils::id::{TenantId, TimelineId};
use utils::logging;

use std::future::Future;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::task::JoinHandle;

struct KeyRange {
    start: i128,
    end: i128,
}

impl KeyRange {
    fn len(&self) -> i128 {
        self.end - self.start
    }
}

struct RelTagBlockNo {
    rel_tag: RelTag,
    block_no: u32,
}

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    mgmt_api_endpoint: String,
    #[clap(long, default_value = "postgres://postgres@localhost:64000")]
    page_service_connstring: String,
    // tenant_id: String,
    // timeline_id: String,
    #[clap(long)]
    num_tasks: usize,
    #[clap(long)]
    num_requests: usize,
    #[clap(long)]
    pick_n_tenants: Option<usize>,
    tenants: Option<Vec<TenantId>>,
}

#[derive(Debug, Default)]
struct LiveStats {
    completed_requests: AtomicU64,
}

impl LiveStats {
    fn inc(&self) {
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();

    let args: &'static Args = Box::leak(Box::new(Args::parse()));

    let client = Arc::new(mgmt_api_client::Client::new(args.mgmt_api_endpoint.clone()));

    let mut tenants: Vec<TenantId> = if let Some(tenants) = &args.tenants {
        tenants.clone()
    } else {
        client
            .list_tenants()
            .await?
            .into_iter()
            .map(|ti| ti.id)
            .collect()
    };
    let tenants = if let Some(n) = args.pick_n_tenants {
        tenants.truncate(n);
        if tenants.len() != n {
            anyhow::bail!("too few tenants: {} < {}", tenants.len(), n);
        }
        tenants
    } else {
        tenants
    };

    let mut tenant_timelines = Vec::new();
    for tenant_id in tenants {
        tenant_timelines.extend(
            client
                .list_timelines(tenant_id)
                .await?
                .into_iter()
                .map(|ti| (tenant_id, ti.timeline_id)),
        );
    }
    info!("tenant_timelines:\n{:?}", tenant_timelines);

    let stats = Arc::new(LiveStats::default());

    let num_work_tasks = tenant_timelines.len() * args.num_tasks;

    let start_work_barrier = Arc::new(tokio::sync::Barrier::new(num_work_tasks + 1));

    tokio::spawn({
        let stats = Arc::clone(&stats);
        let start_work_barrier = Arc::clone(&start_work_barrier);
        async move {
            start_work_barrier.wait().await;
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                let elapsed = start.elapsed();
                info!(
                    "RPS: {:.0}",
                    completed_requests as f64 / elapsed.as_secs_f64()
                );
            }
        }
    });

    let mut tasks = Vec::new();
    for (tenant_id, timeline_id) in tenant_timelines {
        let stats = Arc::clone(&stats);
        let t = tokio::spawn(timeline(
            args,
            client.clone(),
            tenant_id,
            timeline_id,
            Arc::clone(&start_work_barrier),
            stats,
        ));
        tasks.push(t);
    }

    for t in tasks {
        t.await.unwrap().unwrap();
    }

    anyhow::Ok(())
}

fn timeline(
    args: &'static Args,
    mgmt_api_client: Arc<mgmt_api_client::Client>,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    start_work_barrier: Arc<Barrier>,
    stats: Arc<LiveStats>,
) -> impl Future<Output = anyhow::Result<()>> + Send + Sync {
    async move {
        let partitioning = mgmt_api_client.keyspace(tenant_id, timeline_id).await?;
        let lsn = partitioning.at_lsn;
        let ranges = partitioning
            .keys
            .ranges
            .iter()
            .filter_map(|r| {
                let start = r.start;
                let end = r.end;
                // filter out non-relblock keys
                match (is_rel_block_key(start), is_rel_block_key(end)) {
                    (true, true) => Some(KeyRange {
                        start: start.to_i128(),
                        end: end.to_i128(),
                    }),
                    (true, false) | (false, true) => {
                        unimplemented!("split up range")
                    }
                    (false, false) => None,
                }
            })
            .collect::<Vec<_>>();

        // weighted ranges
        let weights = ranges.iter().map(|r| r.len()).collect::<Vec<_>>();

        let ranges = Arc::new(ranges);
        let weights = Arc::new(weights);

        let mut tasks = Vec::<JoinHandle<()>>::new();

        for _i in 0..args.num_tasks {
            let ranges = ranges.clone();
            let _weights = weights.clone();
            let start_work_barrier = Arc::clone(&start_work_barrier);
            let task = tokio::spawn({
                let stats = Arc::clone(&stats);
                async move {
                    let mut getpage_client = getpage_client::Client::new(
                        args.page_service_connstring.clone(),
                        tenant_id,
                        timeline_id,
                    )
                    .await
                    .unwrap();
                    start_work_barrier.wait().await;
                    for _i in 0..args.num_requests {
                        let key = {
                            let mut rng = rand::thread_rng();
                            let r = ranges.choose_weighted(&mut rng, |r| r.len()).unwrap();
                            let key: i128 = rng.gen_range(r.start..r.end);
                            let key = repository::Key::from_i128(key);
                            let (rel_tag, block_no) = key_to_rel_block(key)
                                .expect("we filter non-rel-block keys out above");
                            RelTagBlockNo { rel_tag, block_no }
                        };
                        getpage_client
                            .getpage(key, lsn)
                            .await
                            .with_context(|| {
                                format!("getpage for tenant {} timeline {}", tenant_id, timeline_id)
                            })
                            .unwrap();
                        stats.inc();
                    }
                    getpage_client.shutdown().await;
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
        }

        Ok(())
    }
}

mod mgmt_api_client {
    use anyhow::Context;

    use hyper::{client::HttpConnector, Uri};
    use utils::id::{TenantId, TimelineId};

    pub(crate) struct Client {
        mgmt_api_endpoint: String,
        pub(crate) client: hyper::Client<HttpConnector, hyper::Body>,
    }

    impl Client {
        pub fn new(mgmt_api_endpoint: String) -> Self {
            Self {
                mgmt_api_endpoint,
                client: hyper::client::Client::new(),
            }
        }

        pub async fn list_tenants(
            &self,
        ) -> anyhow::Result<Vec<pageserver_api::models::TenantInfo>> {
            let uri = Uri::try_from(format!("{}/v1/tenant", self.mgmt_api_endpoint))?;
            let resp = self.client.get(uri).await?;
            if !resp.status().is_success() {
                anyhow::bail!("status error");
            }
            let body = hyper::body::to_bytes(resp).await?;
            Ok(serde_json::from_slice(&body)?)
        }

        pub async fn list_timelines(
            &self,
            tenant_id: TenantId,
        ) -> anyhow::Result<Vec<pageserver_api::models::TimelineInfo>> {
            let uri = Uri::try_from(format!(
                "{}/v1/tenant/{tenant_id}/timeline",
                self.mgmt_api_endpoint
            ))?;
            let resp = self.client.get(uri).await?;
            if !resp.status().is_success() {
                anyhow::bail!("status error");
            }
            let body = hyper::body::to_bytes(resp).await?;
            Ok(serde_json::from_slice(&body)?)
        }

        pub async fn keyspace(
            &self,
            tenant_id: TenantId,
            timeline_id: TimelineId,
        ) -> anyhow::Result<pageserver::http::models::partitioning::Partitioning> {
            let uri = Uri::try_from(format!(
                "{}/v1/tenant/{tenant_id}/timeline/{timeline_id}/keyspace?check_serialization_roundtrip=true",
                self.mgmt_api_endpoint
            ))?;
            let resp = self.client.get(uri).await?;
            if !resp.status().is_success() {
                anyhow::bail!("status error");
            }
            let body = hyper::body::to_bytes(resp).await?;
            Ok(serde_json::from_slice(&body).context("deserialize")?)
        }
    }
}

mod getpage_client {
    use std::pin::Pin;

    use futures::SinkExt;
    use pageserver_api::models::{
        PagestreamBeMessage, PagestreamFeMessage, PagestreamGetPageRequest,
        PagestreamGetPageResponse,
    };
    use tokio::task::JoinHandle;
    use tokio_stream::StreamExt;
    use tokio_util::sync::CancellationToken;
    use utils::{
        id::{TenantId, TimelineId},
        lsn::Lsn,
    };

    use crate::RelTagBlockNo;

    pub(crate) struct Client {
        copy_both: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>,
        cancel_on_client_drop: Option<tokio_util::sync::DropGuard>,
        conn_task: JoinHandle<()>,
    }

    impl Client {
        pub async fn new(
            connstring: String,
            tenant_id: TenantId,
            timeline_id: TimelineId,
        ) -> anyhow::Result<Self> {
            let (client, connection) =
                tokio_postgres::connect(&connstring, postgres::NoTls).await?;

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
}
