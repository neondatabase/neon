use tokio_stream::StreamExt;
use tokio::runtime;

use crate::waldecoder::WalStreamDecoder;

use tokio_postgres::{connect_replication, NoTls, Error, ReplicationMode};
use postgres_protocol::message::backend::ReplicationMessage;

//
// This is the entry point for the WAL receiver thread.
//
// TODO: if the connection is lost, reconnect.
//
pub fn thread_main() {

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap(); // FIXME don't unwrap

    println!("Starting WAL receiver");

    runtime.block_on( async {
        let _unused = walreceiver_main().await;
    });
}

pub async fn walreceiver_main() -> Result<(), Error> {

    // Connect to the database in replication mode.
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

    //
    // Start streaming the WAL.
    //
    // TODO: currently, we start streaming at the primary's last insert location.
    // We should start at the last LSN that we had streamed previously, instead.
    //
    let mut physical_stream = rclient
        .start_physical_replication(None, identify_system.xlogpos(), None)
        .await.unwrap();

    let mut waldecoder = WalStreamDecoder::new(u64::from(identify_system.xlogpos()));
    
    while let Some(replication_message) = physical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {

                println!("received XLogData");

                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                waldecoder.feed_bytes(xlog_data.data());

                loop {
                    let rec = waldecoder.poll_decode();

                    if rec.is_none() {
                        break;
                    }

                    crate::waldecoder::decode_wal_record(&rec.unwrap());
                    println!("decoded record");

                    // TODO: Put the WAL record to the page cache
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
