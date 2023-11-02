use clap::Parser;
use hyper::client::conn::Parts;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Uri};
use pageserver::{repository, tenant};
use rand::prelude::*;
use std::env::args;
use std::future::Future;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

struct Key(repository::Key);

impl std::str::FromStr for Key {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        repository::Key::from_hex(s).map(Key)
    }
}

struct KeyRange {
    start: Key,
    end: Key,
}

impl KeyRange {
    fn len(&self) -> i128 {
        self.end.0.to_i128() - self.start.0.to_i128()
    }
}

#[derive(clap::Parser)]
struct Args {
    #[clap(long, default_value = "http://localhost:9898")]
    ps_endpoint: String,
    // tenant_id: String,
    // timeline_id: String,
    num_tasks: usize,
    num_requests: usize,
    tenants: Option<Vec<String>>,
    #[clap(long)]
    pick_n_tenants: Option<usize>,
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
    let args: &'static Args = Box::leak(Box::new(Args::parse()));

    let client = Client::new();

    let tenants = if let Some(tenants) = &args.tenants {
        tenants.clone()
    } else {
        // let tenant_id = "b97965931096047b2d54958756baee7b";
        // let timeline_id = "2868f84a8d166779e4c651b116c45059";

        let resp = client
            .get(Uri::try_from(&format!("{}/v1/tenant", args.ps_endpoint)).unwrap())
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
                    args.ps_endpoint, tenant_id
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
    println!("tenant_timelines:\n{:?}", tenant_timelines);

    let mut stats = Arc::new(Stats::default());

    tokio::spawn({
        let stats = Arc::clone(&stats);
        async move {
            loop {
                let start = std::time::Instant::now();
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let completed_requests = stats.completed_requests.swap(0, Ordering::Relaxed);
                let elapsed = start.elapsed();
                println!(
                    "RPS: {:.0}",
                    completed_requests as f64 / elapsed.as_secs_f64()
                );
            }
        }
    });

    let mut tasks = Vec::new();
    for (tenant_id, timeline_id) in tenant_timelines {
        let t = tokio::spawn(timeline(
            args,
            client.clone(),
            tenant_id,
            timeline_id,
            Arc::clone(&stats),
        ));
        tasks.push(t);
    }

    for t in tasks {
        t.await.unwrap();
    }
}

fn timeline(
    args: &'static Args,
    client: Client<HttpConnector, Body>,
    tenant_id: String,
    timeline_id: String,
    stats: Arc<Stats>,
) -> impl Future<Output = ()> {
    async move {
        let mut resp = client
            .get(
                Uri::try_from(&format!(
                    "{}/v1/tenant/{}/timeline/{}/keyspace",
                    args.ps_endpoint, tenant_id, timeline_id
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

        let lsn = Arc::new(keyspace["at_lsn"].as_str().unwrap().to_owned());

        let ranges = keyspace["keys"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                let r = r.as_array().unwrap();
                assert_eq!(r.len(), 2);
                let start = Key::from_str(r[0].as_str().unwrap()).unwrap();
                let end = Key::from_str(r[1].as_str().unwrap()).unwrap();
                KeyRange { start, end }
            })
            .collect::<Vec<_>>();

        // weighted ranges
        let weights = ranges.iter().map(|r| r.len()).collect::<Vec<_>>();

        let ranges = Arc::new(ranges);
        let weights = Arc::new(weights);

        let (tx, mut rx) = channel::<i32>(1000);
        let tx = Arc::new(AsyncMutex::new(tx));

        let mut tasks = Vec::<JoinHandle<()>>::new();

        let start = std::time::Instant::now();

        for i in 0..args.num_tasks {
            let ranges = ranges.clone();
            let weights = weights.clone();
            let lsn = lsn.clone();
            let client = client.clone();
            let tenant_id = tenant_id.clone();
            let timeline_id = timeline_id.clone();
            let stats = Arc::clone(&stats);
            let task = tokio::spawn(async move {
                for i in 0..args.num_requests {
                    let key = {
                        let mut rng = rand::thread_rng();
                        let r = ranges.choose_weighted(&mut rng, |r| r.len()).unwrap();
                        let key = rng.gen_range((r.start.0.to_i128()..r.end.0.to_i128()));
                        key
                    };
                    let url = format!(
                        "{}/v1/tenant/{}/timeline/{}/getpage?key={:036x}&lsn={}",
                        args.ps_endpoint, tenant_id, timeline_id, key, lsn
                    );
                    let uri = url.parse::<Uri>().unwrap();
                    let resp = client.get(uri).await.unwrap();
                    stats.inc();
                }
            });
            tasks.push(task);
        }

        drop(tx);

        for task in tasks {
            task.await.unwrap();
        }

        let elapsed = start.elapsed();
        println!(
            "RPS: {:.0}",
            (args.num_requests * args.num_tasks) as f64 / elapsed.as_secs_f64()
        );
    }
}
