//
// WAL receiver
//
// The WAL receiver connects to the WAL safekeeper service, and streams WAL.
// For each WAL record, it decodes the record to figure out which data blocks
// the record affects, and adds the records to the page cache.
//

use tokio_stream::StreamExt;
use tokio::runtime;
use tokio::time::{sleep, Duration};

use crate::waldecoder::WalStreamDecoder;
use crate::page_cache;
use crate::page_cache::BufferTag;

use tokio_postgres::{connect_replication, NoTls, Error, ReplicationMode};
use postgres_protocol::message::backend::ReplicationMessage;

//
// This is the entry point for the WAL receiver thread.
//
pub fn thread_main() {

    println!("Starting WAL receiver");

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on( async {
        loop {
            let _res = walreceiver_main().await;

            // TODO: print/log the error
            println!("WAL streaming connection failed, retrying in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
        }
    });
}

async fn walreceiver_main() -> Result<(), Error> {

    // Connect to the database in replication mode.
    println!("connecting...");
    let (mut rclient, connection) =
        connect_replication("host=localhost user=stas dbname=postgres port=65432", NoTls, ReplicationMode::Physical).await?;

    println!("connected!");
    
    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let identify_system = rclient.identify_system().await?;

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If this is the first time we start up, start streaming from the primary's
    // current end of WAL.
    //
    // TODO: We should persist the last valid LSN over page server restarts (and
    // all the data, too, of course). And have some mechanism of bootstrapping.
    //
    let last_valid_lsn = page_cache::get_last_valid_lsn();
    let startpoint = {
        if last_valid_lsn != 0 {
            tokio_postgres::types::Lsn::from(last_valid_lsn)
        } else {
            let primary_lsn = identify_system.xlogpos();
            page_cache::advance_first_valid_lsn(u64::from(primary_lsn));

            primary_lsn
        }
    };

    let mut physical_stream = rclient
        .start_physical_replication(None, startpoint, None)
        .await?;

    let mut waldecoder = WalStreamDecoder::new(u64::from(startpoint));
    
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
                                forknum: blk.forknum as u8,
                                blknum: blk.blkno
                            };

                            let rec = page_cache::WALRecord {
                                lsn: lsn,
                                will_init: blk.will_init || blk.apply_image,
                                rec: recdata.clone()
                            };

                            page_cache::put_wal_record(tag, rec);
                        }

                        // Now that this record has been handled, let the page cache know that
                        // it is up-to-date to this LSN
                        page_cache::advance_last_valid_lsn(lsn);

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
