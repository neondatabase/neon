//! Pageserver benchmark tool
//!
//! Usually it's easier to write python perf tests, but here the performance
//! of the tester matters, and the API is easier to work with from rust.
use std::{collections::{HashMap, HashSet}, io::{BufRead, BufReader, Cursor}, net::SocketAddr, ops::AddAssign, time::Duration};
use byteorder::ReadBytesExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BufMut, Bytes, BytesMut};
use clap::{App, Arg};
use std::fs::File;
use zenith_utils::{GIT_VERSION, pq_proto::{BeMessage, BeParameterStatusMessage, FeMessage}};
use std::time::Instant;
use plotly::{Plot, Scatter, Histogram};
use plotly::common::Mode;

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
                if page.len() != BYTES_IN_PAGE {
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
    let relevant = read_lines_buffered(log_file).filter_map(|line| line.strip_prefix("wal-at-lsn-modified-page ").map(|x| x.to_string()));
    let mut wal_entry_sizes = HashMap::<Lsn, usize>::new();
    let mut lsn_page_pairs = Vec::<(Lsn, Page)>::new();
    for line in relevant {
        let (lsn, rest) = line.split_once(" ").unwrap();
        let (page, size) = rest.split_once(" ").unwrap();

        let lsn = hex::decode(lsn)?;
        let lsn = Lsn(AsyncReadExt::read_u64(&mut Cursor::new(lsn)).await?);

        let page = hex::decode(page)?;
        let page = Page::read(&mut Cursor::new(page)).await?;

        let size: usize = size.parse().unwrap();

        lsn_page_pairs.push((lsn, page));
        wal_entry_sizes.insert(lsn, size);
    }
    lsn_page_pairs.sort();

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
    // dbg!(&updates_per_page);
    dbg!(updates_per_page[0]);

    let updated_pages: HashSet<&&Page> = updates_per_page.iter().filter_map(|(count, page)| {
            if **count > 1 {
                Some(page)
            } else {
                None
            }
        }).collect();

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

    let entries = {
        let mut entries: Vec<(Lsn, usize, Vec<Page>)> = vec![];
        for (lsn, page) in lsn_page_pairs.clone() {
            if let Some(last) = entries.last_mut() {
                if last.0 == lsn {
                    last.2.push(page)
                } else {
                    entries.push((lsn, wal_entry_sizes[&lsn], vec![page]));
                }
            } else {
                entries.push((lsn, wal_entry_sizes[&lsn], vec![page]));
            }
        }
        entries
    };

    let mut results: Vec<(Lsn, usize)> = vec![];
    let mut page_data = HashMap::<Page, Vec<u8>>::new();
    let mut total_dirty_units = 0;
    let mut total_size = 0;
    for (lsn, size, pages) in entries {
        // if !updated_pages.contains(&&page) {
            // continue;
        // }
        for page in pages {
            let page_bytes = get_page(&mut socket, &lsn, &page).await?;

            let dirty_units = if let Some(prev_bytes) = page_data.get(&page) {
                let mut dirty_units = 0;
                for unit_idx in 0..BYTES_IN_PAGE/64 {
                    let mut is_dirty = 0;
                    for offset in 0..64 {
                        let byte_idx = 64 * unit_idx + offset;
                        let xor = page_bytes[byte_idx] ^ prev_bytes[byte_idx];
                        if xor > 0 {
                            is_dirty = 1;
                        }
                    }
                    dirty_units += is_dirty;
                }
                if dirty_units > BYTES_IN_PAGE/64 {
                    panic!("wtf")
                }
                dirty_units
            } else {
                BYTES_IN_PAGE/64  // TODO we can do better
            };
            page_data.insert(page, page_bytes);

            total_dirty_units += dirty_units;
        }
        total_size += size;
    }

    dbg!(total_dirty_units, total_size);
    dbg!(((64 * total_dirty_units) as f64) / (total_size as f64));
    // Returned:
    // - 0.22 using 0 for new page dirty estimate
    return Ok(());

    results.sort();
    let x: Vec<_> = results.iter().map(|(lsn, _)| lsn.0).collect();
    let z: Vec<_> = results.iter().map(|(_, modified)| modified.clone()).collect();

    // let modified_bits_trace = Scatter::new(x, z).name("modified_bits").mode(Mode::Lines);
    let modified_bits_histogram = Histogram::new(z).name("modbits");

    let mut plot = Plot::new();
    // plot.add_trace(get_page_trace);
    // plot.add_trace(modified_bits_trace);
    plot.add_trace(modified_bits_histogram);
    plot.show();

    return Ok(());

    // Do some warmup
    let mut prev_page = get_page(&mut socket, &first_update, &hottest_page).await?;

    let mut results: Vec<(Lsn, (Duration, usize))> = vec![];
    for (i, (lsn, _pages)) in writes_per_entry.iter().enumerate() {
        if lsn >= first_update {

            // Just to speed up things
            // if i % 1000 != 0 {
            //     continue
            // }

            // println!("Running get_page {:?} at {:?}", hottest_page, lsn);
            let start = Instant::now();
            let page = get_page(&mut socket, &lsn, &hottest_page).await?;
            let duration = start.elapsed();

            // TODO why is most modified page constant? Is this test correct?
            let modified_bits = {
                let mut modified_bits = 0;
                for byte_idx in 0..BYTES_IN_PAGE {
                    let xor = page[byte_idx] ^ prev_page[byte_idx];
                    modified_bits = xor.count_ones() as usize;
                }
                modified_bits
            };
            prev_page = page;

            results.push((lsn.clone(), (duration, modified_bits)));
            // println!("Time: {:?}", duration);
        }
    }
    results.sort();
    let x: Vec<_> = results.iter().map(|(lsn, _)| lsn.0).collect();
    let y: Vec<_> = results.iter().map(|(_, (duration, _))| duration.as_micros()).collect();
    let z: Vec<_> = results.iter().map(|(_, (_, modified))| modified.clone()).collect();

    let get_page_trace = Scatter::new(x.clone(), y).name("get_page").mode(Mode::Lines);
    let modified_bits_trace = Scatter::new(x, z).name("modified_bits").mode(Mode::Lines);

    let mut plot = Plot::new();
    // plot.add_trace(get_page_trace);
    plot.add_trace(modified_bits_trace);
    plot.show();

    Ok(())
}
