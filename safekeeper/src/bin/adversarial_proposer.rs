use safekeeper::safekeeper::{AcceptorGreeting, AcceptorProposerMessage, ProposerAcceptorMessage, ProposerGreeting, SafeKeeper};
use tokio::net::TcpStream;
use bytes::{BufMut, BytesMut};
use clap::{Parser, Subcommand};
use zenith_utils::zid::{ZTenantId, ZTimelineId};
use std::fs::File;
use std::path::{Path, PathBuf};
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



struct SafekeeperApi {
    stream: TcpStream,
}

impl SafekeeperApi {
    async fn connect() -> Result<SafekeeperApi> {
        // XXX that's not the right port
        let mut stream = TcpStream::connect("localhost:15000").await?;

        // Connect to safekeeper
        // TODO connect to all nodes
        // TODO read host, port, dbname, user from command line
        let (client, conn) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(15000)
            .dbname("postgres")
            .user("zenith_admin")
            .connect_raw(&mut stream, tokio_postgres::NoTls)
            .await?;

        Ok(SafekeeperApi { stream })
    }

    async fn propose(&mut self, msg: &ProposerAcceptorMessage) -> Result<()> {
        let mut buf = BytesMut::new();
        BeMessage::write(&mut buf, &BeMessage::CopyData(&msg.serialize()?));
        self.stream.write(&buf).await?;

        Ok(())
    }

    async fn get_response(&mut self) -> Result<AcceptorProposerMessage> {
        // See process_msg in safekeeper.rs to find out which msgs have replies
        let response = match FeMessage::read_fut(&mut self.stream).await? {
            Some(FeMessage::CopyData(page)) => page,
            r => panic!("Expected CopyData message, got: {:?}", r),
        };

        AcceptorProposerMessage::parse(response)
    }
}

async fn say_hi(api: &mut SafekeeperApi, tenant: &ZTenantId, timeline: &ZTimelineId) -> Result<()> {
    // TODO this is just copied from somewhere
    api.propose(&ProposerAcceptorMessage::Greeting(ProposerGreeting {
        protocol_version: 1, // current protocol
        pg_version: 0,       // unknown
        proposer_id: [0u8; 16],
        system_id: 0,
        ztli: timeline.clone(),
        tenant_id: tenant.clone(),
        tli: 0,
        wal_seg_size: 16 * 1024, // 16MB, default for tests
    })).await?;

    match api.get_response().await? {
        AcceptorProposerMessage::Greeting(g) => println!("response: {:?}", g),
        _ => panic!("Expected greeting response")
    }

    Ok(())
}

#[derive(Parser, Debug)]
struct Args {
    tenant: ZTenantId,
    timeline: ZTimelineId,

    #[clap(subcommand)]
    strategy: AdversarialStrategy,
}

#[derive(Subcommand, Debug)]
enum AdversarialStrategy {
    JustSayHi,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut api = SafekeeperApi::connect().await?;
    say_hi(&mut api, &args.tenant, &args.timeline).await?;
    println!("YAY IT WORKED");
    Ok(())
}
