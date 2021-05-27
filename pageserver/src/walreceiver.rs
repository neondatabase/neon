//!
//! WAL receiver
//!
//! The WAL receiver connects to the WAL safekeeper service, and streams WAL.
//! For each WAL record, it decodes the record to figure out which data blocks
//! the record affects, and adds the records to the page cache.
//!

use crate::page_cache;
use crate::restore_local_repo;
use crate::waldecoder::*;
use crate::PageServerConf;
use crate::ZTimelineId;
use anyhow::{Error, Result};
use lazy_static::lazy_static;
use log::*;
use postgres::fallible_iterator::FallibleIterator;
use postgres::replication::ReplicationIter;
use postgres::{Client, NoTls, SimpleQueryMessage, SimpleQueryRow};
use postgres_ffi::pg_constants;
use postgres_ffi::xlog_utils::*;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_types::PgLsn;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Mutex;
use std::thread;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use zenith_utils::lsn::Lsn;

//
// We keep one WAL Receiver active per timeline.
//
struct WalReceiverEntry {
    wal_producer_connstr: String,
}

lazy_static! {
    static ref WAL_RECEIVERS: Mutex<HashMap<ZTimelineId, WalReceiverEntry>> =
        Mutex::new(HashMap::new());
}

// Launch a new WAL receiver, or tell one that's running about change in connection string
pub fn launch_wal_receiver(
    conf: &'static PageServerConf,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
) {
    let mut receivers = WAL_RECEIVERS.lock().unwrap();

    match receivers.get_mut(&timelineid) {
        Some(receiver) => {
            receiver.wal_producer_connstr = wal_producer_connstr.into();
        }
        None => {
            let receiver = WalReceiverEntry {
                wal_producer_connstr: wal_producer_connstr.into(),
            };
            receivers.insert(timelineid, receiver);

            // Also launch a new thread to handle this connection
            let _walreceiver_thread = thread::Builder::new()
                .name("WAL receiver thread".into())
                .spawn(move || {
                    thread_main(conf, timelineid);
                })
                .unwrap();
        }
    };
}

// Look up current WAL producer connection string in the hash table
fn get_wal_producer_connstr(timelineid: ZTimelineId) -> String {
    let receivers = WAL_RECEIVERS.lock().unwrap();

    receivers
        .get(&timelineid)
        .unwrap()
        .wal_producer_connstr
        .clone()
}

//
// This is the entry point for the WAL receiver thread.
//
fn thread_main(conf: &'static PageServerConf, timelineid: ZTimelineId) {
    info!(
        "WAL receiver thread started for timeline : '{}'",
        timelineid
    );

    //
    // Make a connection to the WAL safekeeper, or directly to the primary PostgreSQL server,
    // and start streaming WAL from it. If the connection is lost, keep retrying.
    //
    loop {
        // Look up the current WAL producer address
        let wal_producer_connstr = get_wal_producer_connstr(timelineid);

        let res = walreceiver_main(conf, timelineid, &wal_producer_connstr);

        if let Err(e) = res {
            info!(
                "WAL streaming connection failed ({}), retrying in 1 second",
                e
            );
            sleep(Duration::from_secs(1));
        }
    }
}

fn walreceiver_main(
    _conf: &PageServerConf,
    timelineid: ZTimelineId,
    wal_producer_connstr: &str,
) -> Result<(), Error> {
    // Connect to the database in replication mode.
    info!("connecting to {:?}", wal_producer_connstr);
    let connect_cfg = format!("{} replication=true", wal_producer_connstr);

    let mut rclient = Client::connect(&connect_cfg, NoTls)?;
    info!("connected!");

    let identify = identify_system(&mut rclient)?;
    info!("{:?}", identify);
    let end_of_wal = Lsn::from(u64::from(identify.xlogpos));
    let mut caught_up = false;

    let repository = page_cache::get_repository();
    let timeline = repository.get_timeline(timelineid).unwrap();

    //
    // Start streaming the WAL, from where we left off previously.
    //
    // If we had previously received WAL up to some point in the middle of a WAL record, we
    // better start from the end of last full WAL record, not in the middle of one. Hence,
    // use 'last_record_lsn' rather than 'last_valid_lsn' here.
    let mut last_rec_lsn = timeline.get_last_record_lsn();
    let mut startpoint = last_rec_lsn;

    if startpoint == Lsn(0) {
        error!("No previous WAL position");
    }

    // There might be some padding after the last full record, skip it.
    //
    // FIXME: It probably would be better to always start streaming from the beginning
    // of the page, or the segment, so that we could check the page/segment headers
    // too. Just for the sake of paranoia.
    startpoint += startpoint.calc_padding(8u32);

    debug!(
        "last_record_lsn {} starting replication from {} for timeline {}, server is at {}...",
        last_rec_lsn, startpoint, timelineid, end_of_wal
    );

    let query = format!("START_REPLICATION PHYSICAL {}", startpoint);

    let copy_stream = rclient.copy_both_simple(&query)?;
    let mut physical_stream = ReplicationIter::new(copy_stream);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    while let Some(replication_message) = physical_stream.next()? {
        match replication_message {
            ReplicationMessage::XLogData(xlog_data) => {
                // Pass the WAL data to the decoder, and see if we can decode
                // more records as a result.
                let data = xlog_data.data();
                let startlsn = Lsn::from(xlog_data.wal_start());
                let endlsn = startlsn + data.len() as u64;
                let prev_last_rec_lsn = last_rec_lsn;

                write_wal_file(startlsn, timelineid, pg_constants::WAL_SEGMENT_SIZE, data)?;

                trace!("received XLogData between {} and {}", startlsn, endlsn);

                waldecoder.feed_bytes(data);

                while let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                    let decoded = decode_wal_record(recdata.clone());
                    restore_local_repo::save_decoded_record(&*timeline, decoded, recdata, lsn)?;

                    // Now that this record has been handled, let the page cache know that
                    // it is up-to-date to this LSN
                    timeline.advance_last_record_lsn(lsn);
                    last_rec_lsn = lsn;
                }

                // Update the last_valid LSN value in the page cache one more time. We updated
                // it in the loop above, between each WAL record, but we might have received
                // a partial record after the last completed record. Our page cache's value
                // better reflect that, because GetPage@LSN requests might also point in the
                // middle of a record, if the request LSN was taken from the server's current
                // flush ptr.
                timeline.advance_last_valid_lsn(endlsn);

                // Somewhat arbitrarily, if we have at least 10 complete wal segments (16 MB each),
                // "checkpoint" the repository to flush all the changes from WAL we've processed
                // so far to disk. After this, we don't need the original WAL anymore, and it
                // can be removed.
                //
                // TODO: We don't actually dare to remove the WAL. It's useful for debugging,
                // and we might it for logical decoiding other things in the future. Although
                // we should also be able to fetch it back from the WAL safekeepers or S3 if
                // needed.
                if prev_last_rec_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE)
                    != last_rec_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE)
                {
                    info!("switched segment {} to {}", prev_last_rec_lsn, last_rec_lsn);
                    let (oldest_segno, newest_segno) = find_wal_file_range(
                        timelineid,
                        pg_constants::WAL_SEGMENT_SIZE,
                        last_rec_lsn,
                    )?;

                    if newest_segno - oldest_segno >= 10 {
                        timeline.checkpoint()?;

                        // TODO: This is where we could remove WAL older than last_rec_lsn.
                        //remove_wal_files(timelineid, pg_constants::WAL_SEGMENT_SIZE, last_rec_lsn)?;
                    }
                }

                if !caught_up && endlsn >= end_of_wal {
                    info!("caught up at LSN {}", endlsn);
                    caught_up = true;
                }
            }

            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                let wal_end = keepalive.wal_end();
                let timestamp = keepalive.timestamp();
                let reply_requested: bool = keepalive.reply() != 0;

                trace!(
                    "received PrimaryKeepAlive(wal_end: {}, timestamp: {:?} reply: {})",
                    wal_end,
                    timestamp,
                    reply_requested,
                );
                if reply_requested {
                    // TODO: More thought should go into what values are sent here.
                    let last_lsn = PgLsn::from(u64::from(timeline.get_last_valid_lsn()));
                    let write_lsn = last_lsn;
                    let flush_lsn = last_lsn;
                    let apply_lsn = PgLsn::from(0);
                    let ts = SystemTime::now();
                    const NO_REPLY: u8 = 0u8;

                    physical_stream
                        .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, NO_REPLY)?;
                }
            }
            _ => (),
        }
    }
    Ok(())
}

fn find_wal_file_range(
    timeline: ZTimelineId,
    wal_seg_size: usize,
    written_upto: Lsn,
) -> Result<(u64, u64)> {
    let written_upto_segno = written_upto.segment_number(wal_seg_size);

    let mut oldest_segno = written_upto_segno;
    let mut newest_segno = written_upto_segno;
    // Scan the wal directory, and count how many WAL filed we could remove
    let wal_dir = PathBuf::from(format!("timelines/{}/wal", timeline));
    for entry in fs::read_dir(wal_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            continue;
        }

        let filename = path.file_name().unwrap().to_str().unwrap();

        if IsXLogFileName(filename) {
            let (segno, _tli) = XLogFromFileName(filename, wal_seg_size);

            if segno > written_upto_segno {
                // that's strange.
                warn!("there is a WAL file from future at {}", path.display());
                continue;
            }

            oldest_segno = min(oldest_segno, segno);
            newest_segno = max(newest_segno, segno);
        }
    }
    // FIXME: would be good to assert that there are no gaps in the WAL files

    Ok((oldest_segno, newest_segno))
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
pub fn identify_system(client: &mut Client) -> Result<IdentifySystem, Error> {
    let query_str = "IDENTIFY_SYSTEM";
    let response = client.simple_query(query_str)?;

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
        Err(IdentifyError.into())
    }
}

fn write_wal_file(
    startpos: Lsn,
    timeline: ZTimelineId,
    wal_seg_size: usize,
    buf: &[u8],
) -> anyhow::Result<()> {
    let mut bytes_left: usize = buf.len();
    let mut bytes_written: usize = 0;
    let mut partial;
    let mut start_pos = startpos;
    const ZERO_BLOCK: &[u8] = &[0u8; XLOG_BLCKSZ];

    let wal_dir = PathBuf::from(format!("timelines/{}/wal", timeline));

    /* Extract WAL location for this block */
    let mut xlogoff = start_pos.segment_offset(wal_seg_size);

    while bytes_left != 0 {
        let bytes_to_write;

        /*
         * If crossing a WAL boundary, only write up until we reach wal
         * segment size.
         */
        if xlogoff + bytes_left > wal_seg_size {
            bytes_to_write = wal_seg_size - xlogoff;
        } else {
            bytes_to_write = bytes_left;
        }

        /* Open file */
        let segno = start_pos.segment_number(wal_seg_size);
        let wal_file_name = XLogFileName(
            1, // FIXME: always use Postgres timeline 1
            segno,
            wal_seg_size,
        );
        let wal_file_path = wal_dir.join(wal_file_name.clone());
        let wal_file_partial_path = wal_dir.join(wal_file_name.clone() + ".partial");

        {
            let mut wal_file: File;
            /* Try to open already completed segment */
            if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_path) {
                wal_file = file;
                partial = false;
            } else if let Ok(file) = OpenOptions::new().write(true).open(&wal_file_partial_path) {
                /* Try to open existed partial file */
                wal_file = file;
                partial = true;
            } else {
                /* Create and fill new partial file */
                partial = true;
                match OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&wal_file_partial_path)
                {
                    Ok(mut file) => {
                        for _ in 0..(wal_seg_size / XLOG_BLCKSZ) {
                            file.write_all(&ZERO_BLOCK)?;
                        }
                        wal_file = file;
                    }
                    Err(e) => {
                        error!("Failed to open log file {:?}: {}", &wal_file_path, e);
                        return Err(e.into());
                    }
                }
            }
            wal_file.seek(SeekFrom::Start(xlogoff as u64))?;
            wal_file.write_all(&buf[bytes_written..(bytes_written + bytes_to_write)])?;

            // FIXME: Flush the file
            //wal_file.sync_all()?;
        }
        /* Write was successful, advance our position */
        bytes_written += bytes_to_write;
        bytes_left -= bytes_to_write;
        start_pos += bytes_to_write as u64;
        xlogoff += bytes_to_write;

        /* Did we reach the end of a WAL segment? */
        if start_pos.segment_offset(wal_seg_size) == 0 {
            xlogoff = 0;
            if partial {
                fs::rename(&wal_file_partial_path, &wal_file_path)?;
            }
        }
    }
    Ok(())
}
