use std::str::FromStr;

use anyhow::Result;
use postgres_ffi::{pg_constants::WAL_SEGMENT_SIZE, waldecoder::WalStreamDecoder};
use utils::zid::{ZTenantId, ZTimelineId};
use tokio::net::TcpStream;
use utils::lsn::Lsn;


struct PageServiceApi {
    stream: TcpStream,
}

impl PageServiceApi {
    async fn connect(tenant: &ZTenantId, timeline: &ZTimelineId, connstr: &str) -> Result<Self> {
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

        let init_query = format!("callmemaybe {} {} {}", tenant, timeline, connstr);
        tokio::select! {
            _ = conn => panic!("connection closed during callmemaybe"),
            _ = client.query(init_query.as_str(), &[]) => (),
        };

        Ok(Self { stream })
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    use clap::{App, Arg};
    let arg_matches = App::new("Replay")
        .arg(
            Arg::new("tenant")
                .long("tenant")
                .takes_value(true)
        )
        .arg(
            Arg::new("timeline")
                .long("timeline")
                .takes_value(true)
        )
        .get_matches();

    let partial_path = "/home/bojan/tmp/sk_wal";
    let startpos = Lsn(23761464);  // I got this by grepping sk log for "restart decoder"
    let xlogoff: usize = startpos.segment_offset(WAL_SEGMENT_SIZE);

    let mut decoder = WalStreamDecoder::new(startpos);
    let bytes = std::fs::read(partial_path)?;
    decoder.feed_bytes(&bytes[xlogoff..(xlogoff+10000)]);

    while let Some((lsn, rec)) = decoder.poll_decode()? {
        println!("lsn: {}", lsn);
    }

    // TODO start replication server, get connstr

    let tenant = ZTenantId::from_str(arg_matches.value_of("tenant").unwrap())?;
    let timeline = ZTimelineId::from_str(arg_matches.value_of("timeline").unwrap())?;
    let connstr = "lol";
    let mut api = PageServiceApi::connect(&tenant, &timeline, connstr).await?;

    Ok(())
}
