use anyhow::Context;
use clap::Parser;

use hyper::client::HttpConnector;
use hyper::{Client, Uri};

use pageserver::pgdatadir_mapping::{is_rel_block_key, key_to_rel_block};
use pageserver::repository;

use pageserver_api::reltag::RelTag;
use rand::prelude::*;
use tracing::info;
use utils::logging;

use std::future::Future;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::task::JoinHandle;

use utils::lsn::Lsn;

struct Key(repository::Key);

impl std::str::FromStr for Key {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        repository::Key::from_hex(s).map(Key)
    }
}

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
    tenants: Option<Vec<String>>,
}

#[derive(Debug, Default)]
struct Stats {
    completed_requests: AtomicU64,
}

impl Stats {
    fn inc(&self) {
        self.completed_requests.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() {
    logging::init(
        logging::LogFormat::Plain,
        logging::TracingErrorLayerEnablement::Disabled,
        logging::Output::Stderr,
    )
    .unwrap();

    let args: &'static Args = Box::leak(Box::new(Args::parse()));

    let client = Client::new();

    let tenants = if let Some(tenants) = &args.tenants {
        tenants.clone()
    } else {
        let resp = client
            .get(Uri::try_from(&format!("{}/v1/tenant", args.mgmt_api_endpoint)).unwrap())
            .await
            .unwrap();

        let body = hyper::body::to_bytes(resp).await.unwrap();
        let tenants: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let mut out = Vec::new();
        for t in tenants.as_array().unwrap() {
            if let Some(limit) = args.pick_n_tenants {
                if out.len() >= limit {
                    break;
                }
            }
            out.push(t.get("id").unwrap().as_str().unwrap().to_owned());
        }
        if let Some(limit) = args.pick_n_tenants {
            assert_eq!(out.len(), limit);
        }
        out
    };

    let mut tenant_timelines = Vec::new();
    for tenant_id in tenants {
        let resp = client
            .get(
                Uri::try_from(&format!(
                    "{}/v1/tenant/{}/timeline",
                    args.mgmt_api_endpoint, tenant_id
                ))
                .unwrap(),
            )
            .await
            .unwrap();

        let body = hyper::body::to_bytes(resp).await.unwrap();
        let timelines: serde_json::Value = serde_json::from_slice(&body).unwrap();
        for t in timelines.as_array().unwrap() {
            let timeline_id = t.get("timeline_id").unwrap().as_str().unwrap().to_owned();
            tenant_timelines.push((tenant_id.clone(), timeline_id));
        }
    }
    info!("tenant_timelines:\n{:?}", tenant_timelines);

    let stats = Arc::new(Stats::default());

    tokio::spawn({
        let stats = Arc::clone(&stats);
        async move {
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
            stats,
        ));
        tasks.push(t);
    }

    for t in tasks {
        t.await.unwrap();
    }
}

fn timeline(
    args: &'static Args,
    http_client: Client<HttpConnector, hyper::Body>,
    tenant_id: String,
    timeline_id: String,
    stats: Arc<Stats>,
) -> impl Future<Output = ()> + Send + Sync {
    async move {
        let resp = http_client
            .get(
                Uri::try_from(&format!(
                    "{}/v1/tenant/{}/timeline/{}/keyspace",
                    args.mgmt_api_endpoint, tenant_id, timeline_id
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        if !resp.status().is_success() {
            panic!("Failed to get keyspace: {resp:?}");
        }
        let body = hyper::body::to_bytes(resp).await.unwrap();
        let keyspace: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let lsn: Lsn = keyspace["at_lsn"].as_str().unwrap().parse().unwrap();

        let ranges = keyspace["keys"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|r| {
                let r = r.as_array().unwrap();
                assert_eq!(r.len(), 2);
                let start = Key::from_str(r[0].as_str().unwrap()).unwrap();
                let end = Key::from_str(r[1].as_str().unwrap()).unwrap();
                // filter out non-relblock keys
                match (is_rel_block_key(start.0), is_rel_block_key(end.0)) {
                    (true, true) => Some(KeyRange {
                        start: start.0.to_i128(),
                        end: end.0.to_i128(),
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

        let _start = std::time::Instant::now();

        for _i in 0..args.num_tasks {
            let ranges = ranges.clone();
            let _weights = weights.clone();
            let _client = http_client.clone();
            let tenant_id = tenant_id.clone();
            let timeline_id = timeline_id.clone();
            let task = tokio::spawn({
                let stats = Arc::clone(&stats);
                async move {
                    let mut client = getpage_client::Client::new(
                        args.page_service_connstring.clone(),
                        tenant_id.clone(),
                        timeline_id.clone(),
                    )
                    .await
                    .unwrap();
                    for _i in 0..args.num_requests {
                        let key = {
                            let mut rng = rand::thread_rng();
                            let r = ranges.choose_weighted(&mut rng, |r| r.len()).unwrap();
                            let key: i128 = rng.gen_range(r.start..r.end);
                            let key = repository::Key::from_i128(key);
                            // XXX filter these out when we iterate the keyspace
                            assert!(
                                is_rel_block_key(key),
                                "we filter non-relblock keys out above"
                            );
                            let (rel_tag, block_no) =
                                key_to_rel_block(key).expect("we just checked");
                            RelTagBlockNo { rel_tag, block_no }
                        };
                        client
                            .getpage(key, lsn)
                            .await
                            .with_context(|| {
                                format!("getpage for tenant {} timeline {}", tenant_id, timeline_id)
                            })
                            .unwrap();
                        stats.inc();
                    }
                    client.shutdown().await;
                }
            });
            tasks.push(task);
        }

        for task in tasks {
            task.await.unwrap();
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
    use utils::lsn::Lsn;

    use crate::RelTagBlockNo;

    pub(crate) struct Client {
        copy_both: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>,
        cancel_on_client_drop: Option<tokio_util::sync::DropGuard>,
        conn_task: JoinHandle<()>,
    }

    impl Client {
        pub async fn new(
            connstring: String,
            tenant_id: String,
            timeline_id: String,
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
