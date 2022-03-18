//! Pageserver benchmark tool
//!
//! Usually it's easier to write python perf tests, but here the performance
//! of the tester matters, and the API is easier to work with from rust.
use std::{collections::HashMap, io::{BufRead, BufReader, Cursor}, net::SocketAddr, ops::AddAssign, time::Duration};
use byteorder::ReadBytesExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BufMut, Bytes, BytesMut};
use clap::{App, Arg};
use std::fs::File;
use zenith_utils::{GIT_VERSION, pq_proto::{BeMessage, BeParameterStatusMessage, FeMessage}};
use std::time::Instant;

use anyhow::Result;

pub fn read_lines_buffered(file_name: &str) -> impl Iterator<Item = String> {
    BufReader::new(File::open(file_name).unwrap())
        .lines()
        .map(|result| result.unwrap())
}

pub async fn get_page(
    pagestream: &mut tokio::net::TcpStream,
    lsn: &Lsn,
    page: &Page,
) -> anyhow::Result<Vec<u8>> {
    let msg = {
        let query = {
            let mut query = BytesMut::new();
            query.put_u8(2);  // Specifies get_page query
            query.put_u8(0);  // Specifies this is not a "latest page" query
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
                if page.len() != 8 * 1024 {
                    panic!("Expected 8kb page, got: {:?}", page.len());
                }
                page
            },
            103 => {
                let mut bytes = Vec::<u8>::new();
                cursor.read_to_end(&mut bytes).await?;
                let message = String::from_utf8(bytes)?;
                panic!("Got error message: {}", message);
            },
            _ => panic!("Unhandled tag {:?}", tag)
        }
    };

    Ok(page)
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Lsn(pub u64);

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Page {
    spcnode: u32,
    dbnode: u32,
    relnode: u32,
    forknum: u8,
    blkno: u32,
}

impl Page {
    async fn read<Reader>(buf: &mut Reader) -> Result<Page>
    where
        Reader: tokio::io::AsyncRead + Unpin,
    {
        let spcnode = buf.read_u32().await?;
        let dbnode = buf.read_u32().await?;
        let relnode = buf.read_u32().await?;
        let forknum = buf.read_u8().await?;
        let blkno = buf.read_u32().await?;
        Ok(Page { spcnode, dbnode, relnode, forknum, blkno })
    }

    async fn write(&self, buf: &mut BytesMut) -> Result<()> {
        buf.put_u32(self.spcnode);
        buf.put_u32(self.dbnode);
        buf.put_u32(self.relnode);
        buf.put_u8(self.forknum);
        buf.put_u32(self.blkno);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg_matches = App::new("LALALA")
        .about("lalala")
        .version(GIT_VERSION)
        .arg(
            Arg::new("path")
                .help("Path to file to dump")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("ps_connstr")
                .help("Connection string to pageserver")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("tenant_hex")
                .help("TODO")
                .required(true)
                .index(3),
        )
        .arg(
            Arg::new("timeline")
                .help("TODO")
                .required(true)
                .index(4),
        )
        .get_matches();

    let log_file = arg_matches.value_of("path").unwrap();
    let ps_connstr = arg_matches.value_of("ps_connstr").unwrap();
    let tenant_hex = arg_matches.value_of("tenant_hex").unwrap();
    let timeline = arg_matches.value_of("timeline").unwrap();

    // Parse log lines
    let relevant = read_lines_buffered(log_file) .filter_map(|line| line.strip_prefix("wal-at-lsn-modified-page ").map(|x| x.to_string()));
    let mut lsn_page_pairs = Vec::<(Lsn, Page)>::new();
    for line in relevant {
        let (lsn, page) = line.split_once(" ").unwrap();

        let lsn = hex::decode(lsn)?;
        let lsn = Lsn(AsyncReadExt::read_u64(&mut Cursor::new(lsn)).await?);

        let page = hex::decode(page)?;
        let page = Page::read(&mut Cursor::new(page)).await?;

        lsn_page_pairs.push((lsn, page))
    }

    // Organize write info
    let mut writes_per_entry = HashMap::<Lsn, Vec<Page>>::new();
    for (lsn, page) in lsn_page_pairs.clone() {
        writes_per_entry.entry(lsn).or_insert(vec![]).push(page);
    }

    // Print some stats
    let mut updates_per_page = HashMap::<Page, usize>::new();
    for (_, page) in lsn_page_pairs.clone() {
        updates_per_page.entry(page).or_insert(0).add_assign(1);
    }
    let mut updates_per_page: Vec<(&usize, &Page)> = updates_per_page
        .iter().map(|(k, v)| (v, k)).collect();
    updates_per_page.sort();
    updates_per_page.reverse();
    dbg!(&updates_per_page);

    let hottest_page = updates_per_page[0].1;
    let first_update = lsn_page_pairs
        .iter()
        .filter(|(_lsn, page)| page == hottest_page)
        .map(|(lsn, _page)| lsn)
        .min()
        .unwrap();

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

    // TODO merge with LSM branch. Nothing to test otherwise, too many images.
    // - I get error: tried to request a page version that was garbage collected
    // TODO be mindful of caching, take multiple measurements, use monotonic time.
    // TODO make harder test case. More writes, fewer images.
    // TODO concurrent requests: multiple reads, also writes.

    // Do some warmup
    let _page = get_page(&mut socket, &first_update, &hottest_page).await?;

    let mut results: Vec<(Lsn, Duration)> = vec![];
    for (i, (lsn, _pages)) in writes_per_entry.iter().enumerate() {
        if lsn >= first_update {

            // Just to speed up things
            if i % 1000 != 0 {
                continue
            }

            // println!("Running get_page {:?} at {:?}", hottest_page, lsn);
            let start = Instant::now();
            let _page = get_page(&mut socket, &lsn, &hottest_page).await?;
            let duration = start.elapsed();
            results.push((lsn.clone(), duration));
            // println!("Time: {:?}", duration);
        }
    }
    results.sort();
    let x: Vec<_> = results.iter().map(|(lsn, _)| lsn.0).collect();
    let y: Vec<_> = results.iter().map(|(_, duration)| duration.as_micros()).collect();

    use plotly::{Plot, Scatter};
    use plotly::common::Mode;
    let get_page_trace = Scatter::new(x, y).name("get_page").mode(Mode::Lines);

    let mut plot = Plot::new();
    plot.add_trace(get_page_trace);
    plot.show();

    Ok(())
}
