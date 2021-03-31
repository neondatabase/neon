//
// WAL receiver
//
// The WAL receiver connects to the WAL safekeeper service, and streams WAL.
// For each WAL record, it decodes the record to figure out which data blocks
// the record affects, and adds the records to the page cache.
//
use log::*;

use tokio_stream::StreamExt;
use tokio::runtime;
use tokio::time::{sleep, Duration};

use crate::waldecoder::WalStreamDecoder;
use crate::page_cache;
use crate::page_cache::BufferTag;
use crate::PageServerConf;

use tokio_postgres::{connect_replication, NoTls, Error, ReplicationMode};
use postgres_protocol::message::backend::ReplicationMessage;

//
// This is the entry point for the WAL receiver thread.
//
pub fn thread_main(conf: PageServerConf) {

    info!("WAL receiver thread started");

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on( async {
        loop {
            let _res = walreceiver_main(&conf).await;

            // TODO: print/log the error
            info!("WAL streaming connection failed, retrying in 1 second...: {:?}", _res);
            sleep(Duration::from_secs(1)).await;
        }
    });
}

async fn walreceiver_main(conf: &PageServerConf) -> Result<(), Error> {

    // Connect to the database in replication mode.
    debug!("connecting to {}...", conf.wal_producer_connstr);
    let (mut rclient, connection) = connect_replication(
        conf.wal_producer_connstr.as_str(),
        NoTls,
        ReplicationMode::Physical
    ).await?;
    debug!("connected!");

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    let _identify_system = rclient.identify_system().await?;

    //
    // Start streaming the WAL, from where we left off previously.
    //
    let mut startpoint = page_cache::get_last_valid_lsn();
    if startpoint == 0 {
        page_cache::init_valid_lsn(u64::from(_identify_system.xlogpos()));
        startpoint = u64::from(_identify_system.xlogpos());
    }
    let startpoint = tokio_postgres::types::Lsn::from(startpoint);

    debug!("starting replication from {:?}...", startpoint);
    let mut physical_stream = rclient
        .start_physical_replication(None, startpoint, None)
        .await?;

    let mut waldecoder = WalStreamDecoder::new(u64::from(startpoint));
    
    while let Some(replication_message) = physical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {

                trace!("received XLogData , lsn: {}", xlog_data.wal_start());

                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                waldecoder.feed_bytes(xlog_data.data());

                loop {
                    if let Some((startlsn, endlsn, recdata)) = waldecoder.poll_decode() {

                        let decoded = crate::waldecoder::decode_wal_record(startlsn, recdata.clone());

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
                                lsn: startlsn,
                                will_init: blk.will_init || blk.apply_image,
                                rec: recdata.clone()
                            };

                            page_cache::put_wal_record(tag, rec);
                        }

                        // Now that this record has been handled, let the page cache know that
                        // it is up-to-date to this LSN
                        page_cache::advance_last_valid_lsn(endlsn);

                    } else {
                        break;
                    }
                }
            }
            ReplicationMessage::PrimaryKeepAlive(_keepalive) => {
                trace!("received PrimaryKeepAlive");
                // FIXME: Reply, or the connection will time out
            }
            _ => (),
        }
    }
    return Ok(());
}
