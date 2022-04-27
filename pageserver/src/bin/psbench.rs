//! Pageserver benchmark tool
//!
//! This tool connects directly to a pageserver, issues queries and measures performance.
//!
//! Ideally the tool would be ablle to stream WAL into the pageserver, and (possibly
//! simultaneously) make read requests. Currently wal streaming is not implemented,
//! so this tool assumes the pageserver is prepopulated with some data, and only
//! issues read queries. It also currently assumes that the pageserver writes out some
//! metadata describing the write access pattern on the workload that was performed on it.
//!
//! This tool runs a variety of workloads. See the Args enum below, or run the tool
//! with --help to see the available workloads.
//!
use bytes::{BufMut, BytesMut};
use clap::{Parser, Subcommand};
use futures::future;
use pageserver::wal_metadata::{Page, WalEntryMetadata};
use postgres_ffi::pg_constants::BLCKSZ;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Cursor},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use utils::zid::{ZTenantId, ZTimelineId};
use utils::{
    lsn::Lsn,
    pq_proto::{BeMessage, FeMessage},
};

use anyhow::Result;

/// Client for the pageserver's pagestream API
struct PagestreamApi {
    stream: TcpStream,
}

/// Good enough implementation for these tests
impl PagestreamApi {
    async fn connect(tenant: &ZTenantId, timeline: &ZTimelineId) -> Result<PagestreamApi> {
        let mut stream = TcpStream::connect("localhost:15000").await?;

        // Connect to pageserver
        // TODO read host, port, dbname, user from command line
        let (client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(15000)
            .dbname("postgres")
            .user("zenith_admin")
            .connect_raw(&mut stream, tokio_postgres::NoTls)
            .await?;

        // Enter pagestream protocol
        let init_query = format!("pagestream {} {}", tenant, timeline);
        tokio::select! {
            _ = conn => panic!("connection closed during pagestream initialization"),
            _ = client.query(init_query.as_str(), &[]) => (),
        };

        Ok(PagestreamApi { stream })
    }

    async fn get_page(&mut self, lsn: &Lsn, page: &Page, latest: bool) -> anyhow::Result<Vec<u8>> {
        let latest: u8 = if latest { 1 } else { 0 };
        let msg = {
            let query = {
                let mut query = BytesMut::new();
                query.put_u8(2); // Specifies get_page query
                query.put_u8(latest);
                query.put_u64(lsn.0);
                page.write(&mut query).await?;
                query.freeze()
            };

            let mut buf = BytesMut::new();
            let copy_msg = BeMessage::CopyData(&query);
            BeMessage::write(&mut buf, &copy_msg)?;
            buf.freeze()
        };

        self.stream.write(&msg).await?;

        let response = match FeMessage::read_fut(&mut self.stream).await? {
            Some(FeMessage::CopyData(page)) => page,
            r => panic!("Expected CopyData message, got: {:?}", r),
        };

        let page = {
            let mut cursor = Cursor::new(response);
            let tag = AsyncReadExt::read_u8(&mut cursor).await?;

            match tag {
                102 => {
                    let mut page = Vec::<u8>::new();
                    cursor.read_to_end(&mut page).await?;
                    if page.len() != (BLCKSZ as usize) {
                        panic!("Expected 8kb page, got: {:?}", page.len());
                    }
                    page
                }
                103 => {
                    let mut bytes = Vec::<u8>::new();
                    cursor.read_to_end(&mut bytes).await?;
                    let message = String::from_utf8(bytes)?;
                    panic!("Got error message: {}", message);
                }
                _ => panic!("Unhandled tag {:?}", tag),
            }
        };

        Ok(page)
    }
}

/// Parsed wal_metadata file with additional derived
/// statistics for convenience.
#[derive(Clone)]
struct Metadata {
    // Parsed from metadata file
    wal_metadata: Vec<WalEntryMetadata>,

    // Derived from wal_metadata
    total_wal_size: usize,
    affected_pages: HashSet<Page>,
    latest_lsn: Lsn,
}

impl Metadata {
    /// Construct metadata object from wal_metadata file emitted by pageserver
    fn build(wal_metadata_path: &Path) -> Result<Metadata> {
        let wal_metadata_file = File::open(wal_metadata_path).expect("error opening wal_metadata");
        let wal_metadata: Vec<WalEntryMetadata> = BufReader::new(wal_metadata_file)
            .lines()
            .map(|result| result.expect("error reading from file"))
            .map(|line| serde_json::from_str(&line).expect("corrupt metadata file"))
            .collect();

        let total_wal_size: usize = wal_metadata.iter().map(|m| m.size).sum();
        let affected_pages: HashSet<_> = wal_metadata
            .iter()
            .map(|m| m.affected_pages.clone())
            .flatten()
            .collect();
        let latest_lsn = wal_metadata.iter().map(|m| m.lsn).max().unwrap();

        Ok(Metadata {
            wal_metadata,
            total_wal_size,
            affected_pages,
            latest_lsn,
        })
    }

    /// Print results in a format readable by benchmark_fixture.py
    fn report_latency(&self, latencies: &[Duration]) -> Result<()> {
        let mut latencies: Vec<&Duration> = latencies.iter().collect();
        latencies.sort();

        println!("test_param num_pages {}", self.affected_pages.len());
        println!("test_param num_wal_entries {}", self.wal_metadata.len());
        println!("test_param total_wal_size {} bytes", self.total_wal_size);
        println!(
            "lower_is_better fastest {:?} microseconds",
            latencies.first().unwrap().as_micros()
        );
        println!(
            "lower_is_better median {:?} microseconds",
            latencies[latencies.len() / 2].as_micros()
        );
        println!(
            "lower_is_better average {:.2} microseconds",
            (latencies.iter().map(|l| l.as_micros()).sum::<u128>() as f64)
                / (latencies.len() as f64)
        );
        println!(
            "lower_is_better p99 {:?} microseconds",
            latencies[latencies.len() - 1 - latencies.len() / 100].as_micros()
        );
        println!(
            "lower_is_better slowest {:?} microseconds",
            latencies.last().unwrap().as_micros()
        );
        Ok(())
    }
}

/// Sequentially get the latest version of each page and report latencies
async fn test_latest_pages(
    tenant: &ZTenantId,
    timeline: &ZTimelineId,
    metadata: &Metadata,
) -> Result<Vec<Duration>> {
    let mut api = PagestreamApi::connect(tenant, timeline).await.unwrap();
    let mut latencies: Vec<Duration> = vec![];
    let mut page_order: Vec<&Page> = metadata.affected_pages.iter().collect();
    page_order.shuffle(&mut thread_rng());
    for page in page_order {
        let start = Instant::now();
        let _page_bytes = api.get_page(&metadata.latest_lsn, page, true).await?;
        let duration = start.elapsed();

        latencies.push(duration);
    }
    Ok(latencies)
}

#[derive(Parser, Debug, Clone)]
struct Args {
    // TODO maybe one metadata file per timeline?
    wal_metadata_path: PathBuf,

    // TODO get these from wal metadata
    tenant: ZTenantId,
    timeline: ZTimelineId,

    // TODO change to `clients_per_timeline`
    #[clap(long, default_value = "1")]
    num_clients: usize,

    #[clap(subcommand)]
    test: PsbenchTest,
}

#[derive(Subcommand, Debug, Clone)]
enum PsbenchTest {
    /// Query the latest version of each page, in a random sequential order.
    /// If multiple clients are used, all clients will independently query
    /// every page in a different random order.
    GetLatestPages,
    // TODO add more tests:
    // - Query with realistic read pattern
    // - Query every page after every change of that page
    // - Query all pages at given point in time
    // - etc.
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let metadata = Metadata::build(&args.wal_metadata_path).unwrap();

    let latencies: Vec<Duration> = match args.test {
        PsbenchTest::GetLatestPages => {
            // TODO explicitly spawn a thread for each?
            future::join_all(
                (0..args.num_clients)
                    .map(|_| test_latest_pages(&args.tenant, &args.timeline, &metadata)),
            )
            .await
            .into_iter()
            .map(|v| v.unwrap())
            .flatten()
            .collect()
        }
    };

    println!("test_param num_clients {}", args.num_clients);
    metadata.report_latency(&latencies).unwrap();
    Ok(())
}
