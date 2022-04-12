//! Pageserver benchmark tool
//!
//! Usually it's easier to write python perf tests, but here the performance
//! of the tester matters, and the pagestream API is easier to call from rust.
use bytes::{BufMut, BytesMut};
use clap::{App, Arg};
use pageserver::wal_metadata::{Page, WalEntryMetadata};
use std::fs::File;
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
    GIT_VERSION,
};

use anyhow::Result;

const BYTES_IN_PAGE: usize = 8 * 1024;

pub fn read_lines_buffered(file_name: &str) -> impl Iterator<Item = String> {
    BufReader::new(File::open(file_name).unwrap())
        .lines()
        .map(|result| result.unwrap())
}

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

#[tokio::main]
async fn main() -> Result<()> {
    let arg_matches = App::new("LALALA")
        .about("lalala")
        .version(GIT_VERSION)
        .arg(
            Arg::new("wal_metadata_file")
                .help("Path to wal metadata file")
                .required(true)
                .index(1),
        )
        .arg(Arg::new("tenant_hex").help("TODO").required(true).index(2))
        .arg(Arg::new("timeline").help("TODO").required(true).index(3))
        .get_matches();

    let metadata_file = arg_matches.value_of("wal_metadata_file").unwrap();
    let tenant_hex = arg_matches.value_of("tenant_hex").unwrap();
    let timeline = arg_matches.value_of("timeline").unwrap();

    // Parse log lines
    let wal_metadata: Vec<WalEntryMetadata> = read_lines_buffered(metadata_file)
        .map(|line| serde_json::from_str(&line).expect("corrupt metadata file"))
        .collect();

    // Get raw TCP connection to the pageserver postgres protocol port
    let mut socket = tokio::net::TcpStream::connect("localhost:15000").await?;
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(15000)
        .dbname("postgres")
        .user("zenith_admin")
        .connect_raw(&mut socket, tokio_postgres::NoTls)
        .await?;

    // Enter pagestream protocol
    let init_query = format!("pagestream {} {}", tenant_hex, timeline);
    tokio::select! {
        _ = conn => panic!("AAAA"),
        _ = client.query(init_query.as_str(), &[]) => (),
    };

    // Derive some variables
    let total_wal_size: usize = wal_metadata.iter().map(|m| m.size).sum();
    let affected_pages: HashSet<_> = wal_metadata
        .iter()
        .map(|m| m.affected_pages.clone())
        .flatten()
        .collect();
    let latest_lsn = wal_metadata.iter().map(|m| m.lsn).max().unwrap();

    // Get all latest pages
    let mut durations: Vec<Duration> = vec![];
    for page in &affected_pages {
        let start = Instant::now();
        let _page_bytes = get_page(&mut socket, &latest_lsn, &page, true).await?;
        let duration = start.elapsed();

        durations.push(duration);
    }

    durations.sort();
    // Format is optimized for easy parsing from benchmark_fixture.py
    println!("test_param num_pages {}", affected_pages.len());
    println!("test_param num_wal_entries {}", wal_metadata.len());
    println!("test_param total_wal_size {} bytes", total_wal_size);
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
