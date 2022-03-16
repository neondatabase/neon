//! Pageserver benchmark tool
//!
//! Usually it's easier to write python perf tests, but here the performance
//! of the tester matters, and the API is easier to work with from rust.
use std::{io::{BufRead, BufReader, Cursor}, net::SocketAddr};
use bytes::{Bytes, BytesMut};
use clap::{App, Arg};
use std::fs::File;
use zenith_utils::{GIT_VERSION, pq_proto::{BeMessage, BeParameterStatusMessage, FeMessage}};

use anyhow::Result;

pub fn read_lines_buffered(file_name: &str) -> impl Iterator<Item = String> {
    BufReader::new(File::open(file_name).unwrap())
        .lines()
        .map(|result| result.unwrap())
}

#[tokio::main]
async fn main() -> Result<()> {

    // TODO do I need connection string to pageserver?

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

    let lsn_page_pairs: Vec<_> = read_lines_buffered(log_file)
        .filter_map(|line| line.strip_prefix("wal-at-lsn-modified-page ").map(|x| x.to_string()))
        .map(|rest| {
            let (lsn, page) = rest.split_once(" ").unwrap();
            let lsn = hex::decode(lsn).unwrap();
            if lsn.len() != 8 {
                panic!("AAA")
            }
            let page = hex::decode(page).unwrap();
            if page.len() != 17 {
                panic!("AAA")
            }
            (lsn, page)
        })
        .collect();

    let (some_lsn, some_page) = lsn_page_pairs[0].clone();

    let mut socket = tokio::net::TcpStream::connect("localhost:15000").await?;
    println!("AYY got socket");
    let (client, conn) = tokio_postgres::Config::new()
        .host("127.0.0.1")
        .port(15000)
        .dbname("postgres")
        .user("zenith_admin")
        .connect_raw(&mut socket, tokio_postgres::NoTls)
        .await?;

    let query = format!("pagestream {} {}", tenant_hex, timeline);
    tokio::select! {
        _ = conn => panic!("AAAA"),
        _ = client.query(query.as_str(), &[]) => (),
    };

    println!("AYYYYYYYYYYYY");

    let msg = {
        let query = {
            use bytes::buf::BufMut;
            let mut query = BytesMut::new();
            query.put_u8(2);  // Specifies get_page query
            query.put_u8(0);  // Specifies this is not a "latest page" query
            for byte in some_lsn {
                query.put_u8(byte);
            }
            for byte in some_page {
                query.put_u8(byte);
            }
            query.freeze()
        };

        let mut buf = BytesMut::new();
        let copy_msg = BeMessage::CopyData(&query);
        BeMessage::write(&mut buf, &copy_msg)?;
        buf.freeze()
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    socket.write(&msg).await?;

    let response = match FeMessage::read_fut(&mut socket).await? {
        Some(FeMessage::CopyData(page)) => page,
        _ => panic!("AAAAA"),
    };

    let page = {
        let mut cursor = Cursor::new(response);
        let tag = cursor.read_u8().await?;
        if tag != 102 {
            panic!("AA");
        }

        let mut page = Vec::<u8>::new();
        cursor.read_to_end(&mut page).await?;
        dbg!(page.len());
        if page.len() != 8 * 1024 {
            panic!("AA");
        }

        page
    };

    print!("yay done");

    Ok(())
}
