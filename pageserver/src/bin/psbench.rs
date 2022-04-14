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

pub async fn get_page(
    pagestream: &mut tokio::net::TcpStream,
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

    pagestream.write(&msg).await?;

    let response = match FeMessage::read_fut(pagestream).await? {
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

struct Metadata {
    // Parsed from metadata file
    wal_metadata: Vec<WalEntryMetadata>,

    // Derived from wal_metadata
    total_wal_size: usize,
    affected_pages: HashSet<Page>,
    latest_lsn: Lsn,
}

impl Metadata {
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

    fn report_results(&self, durations: &Vec<Duration>) -> Result<()> {
        // Format is optimized for easy parsing from benchmark_fixture.py
        println!("test_param num_pages {}", self.affected_pages.len());
        println!("test_param num_wal_entries {}", self.wal_metadata.len());
        println!("test_param total_wal_size {} bytes", self.total_wal_size);
        println!(
            "lower_is_better fastest {:?} microseconds",
            durations.first().unwrap().as_micros()
        );
        println!(
            "lower_is_better median {:?} microseconds",
            durations[durations.len() / 2].as_micros()
        );
        println!(
            "lower_is_better p99 {:?} microseconds",
            durations[durations.len() - 1 - durations.len() / 100].as_micros()
        );
        println!(
            "lower_is_better slowest {:?} microseconds",
            durations.last().unwrap().as_micros()
        );
        Ok(())
    }
}

async fn test_latest_pages(pagestream: &mut TcpStream, metadata: &Metadata) -> Result<Vec<Duration>>{
    let mut durations: Vec<Duration> = vec![];
    for page in &metadata.affected_pages {
        let start = Instant::now();
        let _page_bytes = get_page(pagestream, &metadata.latest_lsn, &page, true).await?;
        let duration = start.elapsed();

        durations.push(duration);
    }
    durations.sort();
    Ok(durations)
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

    // Parse wal metadata from file
    let metadata = Metadata::build(&args.wal_metadata_path)?;

    // Get raw TCP connection to the pageserver postgres protocol port
    let mut pagestream = TcpStream::connect("localhost:15000").await?;
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(15000)
        .dbname("postgres")
        .user("zenith_admin")
        .connect_raw(&mut pagestream, tokio_postgres::NoTls)
        .await?;

    // Enter pagestream protocol
    let init_query = format!("pagestream {} {}", args.tenant_hex, args.timeline);
    tokio::select! {
        _ = conn => panic!("AAAA"),
        _ = client.query(init_query.as_str(), &[]) => (),
    };

    // Run test
    let durations = match args.test {
        PsbenchTest::GetLatestPages => test_latest_pages(&mut pagestream, &metadata)
    }.await?;
    metadata.report_results(&durations)?;

    Ok(())
}
