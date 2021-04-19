//!
//! WAL receiver
//!
//! The WAL receiver connects to the WAL safekeeper service, and streams WAL.
//! For each WAL record, it decodes the record to figure out which data blocks
//! the record affects, and adds the records to the page cache.
//!

use crate::page_cache;
use crate::page_cache::{BufferTag, RelTag};
use crate::waldecoder::{decode_wal_record, WalStreamDecoder};
use crate::PageServerConf;
use anyhow::Error;
use log::*;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::str::FromStr;
use tokio::runtime;
use tokio::time::{sleep, Duration};
use tokio_postgres::replication::{PgTimestamp, ReplicationStream};
use tokio_postgres::{NoTls, SimpleQueryMessage, SimpleQueryRow};
use tokio_stream::StreamExt;

//
// This is the entry point for the WAL receiver thread.
//
pub fn thread_main(conf: &PageServerConf, wal_producer_connstr: &str) {
    info!("WAL receiver thread started: '{}'", wal_producer_connstr);

    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        loop {
            let res = walreceiver_main(conf, wal_producer_connstr).await;

            if let Err(e) = res {
                info!(
                    "WAL streaming connection failed ({}), retrying in 1 second",
                    e
                );
                sleep(Duration::from_secs(1)).await;
            }
        }
    });
}

async fn walreceiver_main(conf: &PageServerConf, wal_producer_connstr: &str) -> Result<(), Error> {
    // Connect to the database in replication mode.
    info!("connecting to {:?}", wal_producer_connstr);
    let connect_cfg = format!("{} replication=true", wal_producer_connstr);
    let (rclient, connection) = tokio_postgres::connect(&connect_cfg, NoTls).await?;
    info!("connected!");

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });

    let identify = identify_system(&rclient).await?;
    info!("{:?}", identify);
    let end_of_wal = u64::from(identify.xlogpos);
    let mut caught_up = false;

    let pcache = page_cache::get_pagecache(conf, identify.systemid);

    //
    // Start streaming the WAL, from where we left off previously.
    //
    let mut startpoint = pcache.get_last_valid_lsn();
    if startpoint == 0 {
        // If we start here with identify.xlogpos we will have race condition with
        // postgres start: insert into postgres may request page that was modified with lsn
        // smaller than identify.xlogpos.
        //
        // Current procedure for starting postgres will anyway be changed to something
        // different like having 'initdb' method on a pageserver (or importing some shared
        // empty database snapshot), so for now I just put start of first segment which
        // seems to be a valid record.
        pcache.init_valid_lsn(0x_1_000_000_u64);
        startpoint = 0x_1_000_000_u64;
    } else {
        // There might be some padding after the last full record, skip it.
        //
        // FIXME: It probably would be better to always start streaming from the beginning
        // of the page, or the segment, so that we could check the page/segment headers
        // too. Just for the sake of paranoia.
        if startpoint % 8 != 0 {
            startpoint += 8 - (startpoint % 8);
        }
    }
    debug!(
        "starting replication from {:X}/{:X}, server is at {:X}/{:X}...",
        (startpoint >> 32),
        (startpoint & 0xffffffff),
        (end_of_wal >> 32),
        (end_of_wal & 0xffffffff)
    );

    let startpoint = PgLsn::from(startpoint);
    let query = format!("START_REPLICATION PHYSICAL {}", startpoint);
    let copy_stream = rclient.copy_both_simple::<bytes::Bytes>(&query).await?;

    let physical_stream = ReplicationStream::new(copy_stream);
    tokio::pin!(physical_stream);

    let mut waldecoder = WalStreamDecoder::new(u64::from(startpoint));

    while let Some(replication_message) = physical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = xlog_data.wal_start();
                let endlsn = startlsn + data.len() as u64;

                trace!(
                    "received XLogData between {:X}/{:X} and {:X}/{:X}",
                    (startlsn >> 32),
                    (startlsn & 0xffffffff),
                    (endlsn >> 32),
                    (endlsn & 0xffffffff)
                );

                waldecoder.feed_bytes(data);

                loop {
                    if let Some((lsn, recdata)) = waldecoder.poll_decode() {
                        let decoded = decode_wal_record(startlsn, recdata.clone());

                        // Put the WAL record to the page cache. We make a separate copy of
                        // it for every block it modifies. (The actual WAL record is kept in
                        // a Bytes, which uses a reference counter for the underlying buffer,
                        // so having multiple copies of it doesn't cost that much)
                        for blk in decoded.blocks.iter() {
                            let tag = BufferTag {
                                rel: RelTag {
                                    spcnode: blk.rnode_spcnode,
                                    dbnode: blk.rnode_dbnode,
                                    relnode: blk.rnode_relnode,
                                    forknum: blk.forknum as u8,
                                },
                                blknum: blk.blkno,
                            };

                            let rec = page_cache::WALRecord {
                                lsn,
                                will_init: blk.will_init || blk.apply_image,
                                truncate: false,
                                rec: recdata.clone(),
                            };

                            pcache.put_wal_record(tag, rec);
                        }
                        // include truncate wal record in all pages
                        if decoded.xl_rmid == RM_SMGR_ID
                            && (decoded.xl_info & XLR_RMGR_INFO_MASK) == XLOG_SMGR_TRUNCATE
                        {
                            let truncate = decode_truncate_record(&decoded);
                            if (truncate.flags & SMGR_TRUNCATE_HEAP) != 0 {
                                let tag = BufferTag {
                                    rel: RelTag {
                                        spcnode: truncate.rnode.spcnode,
                                        dbnode: truncate.rnode.dbnode,
                                        relnode: truncate.rnode.relnode,
                                        forknum: MAIN_FORKNUM,
                                    },
                                    blknum: truncate.blkno,
                                };
                                let rec = page_cache::WALRecord {
                                    lsn: lsn,
                                    will_init: false,
                                    truncate: true,
                                    rec: recdata.clone(),
                                };
                                pcache.put_rel_wal_record(tag, rec);
                            }
                        }
                        // Now that this record has been handled, let the page cache know that
                        // it is up-to-date to this LSN
                        pcache.advance_last_valid_lsn(lsn);
                    } else {
                        break;
                    }
                }

                // Update the last_valid LSN value in the page cache one more time. We updated
                // it in the loop above, between each WAL record, but we might have received
                // a partial record after the last completed record. Our page cache's value
                // better reflect that, because GetPage@LSN requests might also point in the
                // middle of a record, if the request LSN was taken from the server's current
                // flush ptr.
                pcache.advance_last_valid_lsn(endlsn);

                if !caught_up && endlsn >= end_of_wal {
                    info!(
                        "caught up at LSN {:X}/{:X}",
                        (endlsn >> 32),
                        (endlsn & 0xffffffff)
                    );
                    caught_up = true;
                }
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested: bool = keepalive.reply() != 0;

                trace!(
                    "received PrimaryKeepAlive(wal_end: {}, timestamp: {} reply: {})",
                    wal_end,
                    timestamp,
                    reply_requested,
                );
                if reply_requested {
                    // TODO: More thought should go into what values are sent here.
                    let last_lsn = PgLsn::from(pcache.get_last_valid_lsn());
                    let write_lsn = last_lsn;
                    let flush_lsn = last_lsn;
                    let apply_lsn = PgLsn::INVALID;
                    let ts = PgTimestamp::now()?;
                    const NO_REPLY: u8 = 0u8;

                    physical_stream
                        .as_mut()
                        .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, NO_REPLY)
                        .await?;
                }
            }
            _ => (),
        }
    }
    return Ok(());
}

/// Data returned from the postgres `IDENTIFY_SYSTEM` command
///
/// See the [postgres docs] for more details.
///
/// [postgres docs]: https://www.postgresql.org/docs/current/protocol-replication.html
#[derive(Debug)]
pub struct IdentifySystem {
    systemid: u64,
    timeline: u32,
    xlogpos: PgLsn,
    dbname: Option<String>,
}

/// There was a problem parsing the response to
/// a postgres IDENTIFY_SYSTEM command.
#[derive(Debug, thiserror::Error)]
#[error("IDENTIFY_SYSTEM parse error")]
pub struct IdentifyError;

/// Run the postgres `IDENTIFY_SYSTEM` command
pub async fn identify_system(client: &tokio_postgres::Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str).await?;

    // get(N) from row, then parse it as some destination type.
    fn get_parse<T>(row: &SimpleQueryRow, idx: usize) -> Result<T, IdentifyError>
    where
        T: FromStr,
    {
        let val = row.get(idx).ok_or(IdentifyError)?;
        val.parse::<T>().or(Err(IdentifyError))
    }

    // extract the row contents into an IdentifySystem struct.
    // written as a closure so I can use ? for Option here.
    if let Some(SimpleQueryMessage::Row(first_row)) = response.get(0) {
        Ok(IdentifySystem {
            systemid: get_parse(first_row, 0)?,
            timeline: get_parse(first_row, 1)?,
            xlogpos: get_parse(first_row, 2)?,
            dbname: get_parse(first_row, 3).ok(),
        })
    } else {
        Err(IdentifyError)?
    }
}
