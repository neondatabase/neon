//! Pageserver benchmark tool
//!
//! Usually it's easier to write python perf tests, but here the performance
//! of the tester matters, and the pagestream API is easier to call from rust.
use bytes::{BufMut, BytesMut};
use clap::{Parser, Subcommand};
use pageserver::wal_metadata::{Page, WalEntryMetadata};
use tokio::net::TcpStream;
use std::fs::File;
use std::path::PathBuf;
use std::time::Instant;
use std::{
    collections::HashSet,
    io::{BufRead, BufReader, Cursor},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zenith_utils::{
    lsn::Lsn,
    pq_proto::{BeMessage, FeMessage},
};

use anyhow::Result;

const BYTES_IN_PAGE: usize = 8 * 1024;

/// Client for the pageserver's pagestream API
struct PagestreamApi {
    stream: TcpStream
}

/// Good enough implementation for these tests
impl PagestreamApi {
    async fn connect(tenant_hex: &str, timeline: &str) -> Result<PagestreamApi> {
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
        let init_query = format!("pagestream {} {}", tenant_hex, timeline);
        tokio::select! {
            _ = conn => panic!("AAAA"),
            _ = client.query(init_query.as_str(), &[]) => (),
        };

        Ok(PagestreamApi{stream})
    }

    async fn get_page(
        &mut self,
        lsn: &Lsn,
        page: &Page,
        latest: bool,
    ) -> anyhow::Result<Vec<u8>> {
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
                    if page.len() != BYTES_IN_PAGE {
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
    fn build(wal_metadata_path: &PathBuf) -> Result<Metadata> {
        let wal_metadata_file = File::open(wal_metadata_path)
            .expect("error opening wal_metadata");
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
    fn report_latency(&self, latencies: &Vec<Duration>) -> Result<()> {
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
async fn test_latest_pages(api: &mut PagestreamApi, metadata: &Metadata) -> Result<Vec<Duration>>{
    let mut latencies: Vec<Duration> = vec![];
    for page in &metadata.affected_pages {
        let start = Instant::now();
        let _page_bytes = api.get_page(&metadata.latest_lsn, &page, true).await?;
        let duration = start.elapsed();

        latencies.push(duration);
    }
    latencies.sort();
    Ok(latencies)
}

#[derive(Parser, Debug)]
struct Args {
    wal_metadata_path: PathBuf,
    tenant_hex: String,
    timeline: String,

    #[clap(subcommand)]
    test: PsbenchTest,
}

#[derive(Subcommand, Debug)]
enum PsbenchTest {
    GetLatestPages,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize setup
    let metadata = Metadata::build(&args.wal_metadata_path)?;
    let mut pagestream = PagestreamApi::connect(&args.tenant_hex, &args.timeline).await?;

    // Run test
    let latencies = match args.test {
        PsbenchTest::GetLatestPages => test_latest_pages(&mut pagestream, &metadata)
    }.await?;

    // Report results
    metadata.report_latency(&latencies)?;
    Ok(())
}
