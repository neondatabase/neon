use bytes::BytesMut;
use pageserver::page_service::PagestreamFeMessage;
use std::{
    fs::{read_dir, File},
    io::BufReader,
    path::PathBuf,
    str::FromStr,
};
use tokio::{io::AsyncWriteExt, net::TcpStream};

use clap::{App, Arg};
use utils::{
    pq_proto::{BeMessage, FeMessage},
    zid::{ZTenantId, ZTimelineId},
};

// TODO put this in library, dedup with stuff in control_plane
/// Client for the pageserver's pagestream API
struct PagestreamApi {
    stream: TcpStream,
}

impl PagestreamApi {
    async fn connect(
        connstr: &str,
        tenant: &ZTenantId,
        timeline: &ZTimelineId,
    ) -> anyhow::Result<PagestreamApi> {
        // Parse connstr
        let config = tokio_postgres::Config::from_str(connstr).expect("bad connstr");
        let tcp_addr = format!("localhost:{}", config.get_ports()[0]);

        // Connect
        let mut stream = TcpStream::connect(tcp_addr).await?;
        let (client, conn) = config
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

    async fn make_request(&mut self, msg: PagestreamFeMessage) -> anyhow::Result<()> {
        let request = {
            let msg_bytes = msg.serialize();
            let mut buf = BytesMut::new();
            let copy_msg = BeMessage::CopyData(&msg_bytes);

            // TODO it's actually a fe message but it doesn't have a serializer yet
            BeMessage::write(&mut buf, &copy_msg)?;
            buf.freeze()
        };
        self.stream.write_all(&request).await?;

        // TODO It's actually a be message, but it doesn't have a parser.
        // So error response (code b'E' parses incorrectly as FeExecuteMessage)
        let _response = match FeMessage::read_fut(&mut self.stream).await? {
            Some(FeMessage::CopyData(page)) => page,
            r => panic!("Expected CopyData message, got: {:?}", r),
        };

        Ok(())
    }
}

async fn replay_trace<R: std::io::Read>(
    reader: &mut R,
    mut pagestream: PagestreamApi,
) -> anyhow::Result<()> {
    while let Ok(msg) = PagestreamFeMessage::parse(reader) {
        println!("Parsed message {:?}", msg);
        pagestream.make_request(msg).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO upgrade to struct macro arg parsing
    let arg_matches = App::new("Pageserver trace replay tool")
        .about("Replays wal or read traces to test pageserver performance")
        .arg(
            Arg::new("traces_dir")
                .takes_value(true)
                .help("Directory where the read traces are stored"),
        )
        .arg(
            Arg::new("pageserver_connstr")
                .takes_value(true)
                .help("Pageserver pg endpoint to connect to"),
        )
        .get_matches();

    let connstr = arg_matches.value_of("pageserver_connstr").unwrap();

    let traces_dir = PathBuf::from(arg_matches.value_of("traces_dir").unwrap());
    for tenant_dir in read_dir(traces_dir)? {
        let entry = tenant_dir?;
        let path = entry.path();
        let tenant_id = ZTenantId::from_str(path.file_name().unwrap().to_str().unwrap())?;

        for timeline_dir in read_dir(path)? {
            let entry = timeline_dir?;
            let path = entry.path();
            let timeline_id = ZTimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

            for trace_dir in read_dir(path)? {
                let entry = trace_dir?;
                let path = entry.path();
                let _conn_id = ZTimelineId::from_str(path.file_name().unwrap().to_str().unwrap())?;

                // TODO The pageserver deletes existing traces?
                // LOL yes because I use tenant ID as trace id
                let pagestream = PagestreamApi::connect(connstr, &tenant_id, &timeline_id).await?;

                let file = File::open(path.clone())?;
                let mut reader = BufReader::new(file);
                // let len = file.metadata().unwrap().len();
                // println!("replaying {:?} trace {} bytes", path, len);
                replay_trace(&mut reader, pagestream).await?;
            }
        }
    }

    Ok(())
}
