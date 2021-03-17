use tokio_stream::StreamExt;
use tokio::runtime;

use crate::waldecoder::WalStreamDecoder;
use crate::page_cache;
use crate::page_cache::BufferTag;

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
                    if let Some((lsn, recdata)) = waldecoder.poll_decode() {

                        let decoded = crate::waldecoder::decode_wal_record(lsn, recdata.clone());
                        println!("decoded record");

                        // Put the WAL record to the page cache. We make a separate copy of
                        // it for every block it modifes. (The actual WAL record is kept in
                        // a Bytes, which uses a reference counter for the underlying buffer,
                        // so having multiple copies of it doesn't cost that much)
                        for blk in decoded.blocks.iter() {
                            let tag = BufferTag {
                                spcnode: blk.rnode_spcnode,
                                dbnode: blk.rnode_dbnode,
                                relnode: blk.rnode_relnode,
                                forknum: blk.forknum as u32,
                                blknum: blk.blkno
                            };

                            let rec = page_cache::WALRecord {
                                lsn: lsn,
                                will_init: blk.will_init || blk.apply_image,
                                rec: recdata.clone()
                            };

                            page_cache::put_wal_record(tag, rec);
                        }

                    } else {
                        break;
                    }
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
