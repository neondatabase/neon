//use crate::page_cache::*;

//use postgres_protocol::message::backend::XLogDataBody;

use tokio_stream::StreamExt;
use tokio::runtime;

use crate::walreader::WalStreamDecoder;

use tokio_postgres::{connect_replication, NoTls, Error, ReplicationMode};
use postgres_protocol::message::backend::ReplicationMessage;

//use ReplicationMessage::XLogData;

pub fn thread_main() {

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap(); // FIXME don't unwrap

    println!("Starting WAL receiver");

    runtime.block_on( async {
        walreceiver_main().await;
    });
}

pub async fn walreceiver_main() -> Result<(), Error> {
    // Connect to the database.

    println!("connecting...");
    
    let (mut rclient, connection) =
        connect_replication("host=localhost user=heikki", NoTls, ReplicationMode::Physical).await?;

    println!("connected!");
    
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let identify_system = rclient.identify_system().await?;

    println!("identify_system");

    let mut physical_stream = rclient
        .start_physical_replication(None, identify_system.xlogpos(), None)
        .await.unwrap();

    let mut walreader = WalStreamDecoder::new(u64::from(identify_system.xlogpos()));
    
    //let record = walreader.read_record(&mut physical_stream);

    while let Some(replication_message) = physical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {
                println!("received XLogData:");
                walreader.feed_bytes(xlog_data.data());
                let rec = walreader.poll_decode();

                if rec.is_some() {
                    crate::walreader::decode_wal_record(&rec.unwrap());
                    println!("decoded record");
                }
            }
            ReplicationMessage::PrimaryKeepAlive(_keepalive) => {
                println!("received PrimaryKeepAlive");
            }
            _ => (),
        }
    }

    return Ok(());
}
