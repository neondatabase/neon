//
// Restore chunks from local Zenith repository
//
// This runs once at Page Server startup. It loads all the "snapshots" and all
// WAL from all timelines from the local zenith repository into the in-memory page
// cache.
//
// This also initializes the "last valid LSN" in the page cache to the last LSN
// seen in the WAL, so that when the WAL receiver is started, it starts
// streaming from that LSN.
//

use log::*;

use std::cmp::max;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::Result;
use bytes::Bytes;

use crate::repository::{BufferTag, RelTag, Timeline};
use crate::waldecoder::{decode_wal_record, WalStreamDecoder};
use crate::PageServerConf;
use crate::ZTimelineId;
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::xlog_utils::*;
use zenith_utils::lsn::Lsn;

///
/// Load all WAL and optionally all relation data pages from local disk into the repository.
///
/// If 'restore_snapshot' is true, loads all snapshots from the <timeline>/snapshots/
/// directory, too. If it's false, only loads the WAL and initializes the valid LSN.
///
pub fn restore_timeline(
    conf: &PageServerConf,
    timeline: &dyn Timeline,
    timelineid: ZTimelineId,
    restore_snapshot_arg: bool,
) -> Result<()> {
    debug!("restoring timeline {}", timelineid);

    let timelinepath = PathBuf::from("timelines").join(timelineid.to_string());

    if !timelinepath.exists() {
        anyhow::bail!("timeline {} does not exist in the page server's repository");
    }

    // Scan .zenith/timelines/<timeline>/snapshots
    let snapshotspath = PathBuf::from("timelines")
        .join(timelineid.to_string())
        .join("snapshots");

    let mut last_snapshot_lsn: Lsn = Lsn(0);

    for direntry in fs::read_dir(&snapshotspath).unwrap() {
        let direntry = direntry?;
        let filename = direntry.file_name();
        let lsn = Lsn::from_filename(&filename)?;
        last_snapshot_lsn = max(lsn, last_snapshot_lsn);

        let filename_str = filename.into_string().unwrap();
        if restore_snapshot_arg {
            // FIXME: pass filename as Path instead of str?
            restore_snapshot(conf, timeline, timelineid, &filename_str)?;
        }
        info!("found snapshot at {:?}", filename_str);
    }

    if last_snapshot_lsn == Lsn(0) {
        error!(
            "could not find valid snapshot in {}",
            snapshotspath.display()
        );
        // TODO return error?
    }
    timeline.init_valid_lsn(last_snapshot_lsn);

    restore_wal(timeline, timelineid, last_snapshot_lsn)?;

    Ok(())
}

///
/// Find latest snapshot in a timeline's 'snapshots' directory
///
pub fn find_latest_snapshot(timeline: ZTimelineId) -> Result<Lsn> {
    let snapshotspath = format!("timelines/{}/snapshots", timeline);

    let mut last_snapshot_lsn = Lsn(0);
    for direntry in fs::read_dir(&snapshotspath).unwrap() {
        let filename = direntry.unwrap().file_name();

        if let Ok(lsn) = Lsn::from_filename(&filename) {
            last_snapshot_lsn = max(lsn, last_snapshot_lsn);
        } else {
            error!("unrecognized file in snapshots directory: {:?}", filename);
        }
    }

    if last_snapshot_lsn == Lsn(0) {
        error!("could not find valid snapshot in {}", &snapshotspath);
        // TODO return error?
    }
    Ok(last_snapshot_lsn)
}

fn restore_snapshot(
    conf: &PageServerConf,
    timeline: &dyn Timeline,
    timelineid: ZTimelineId,
    snapshot: &str,
) -> Result<()> {
    let snapshotpath = PathBuf::from("timelines")
        .join(timelineid.to_string())
        .join("snapshots")
        .join(snapshot);

    // Scan 'global'
    for direntry in fs::read_dir(snapshotpath.join("global"))? {
        let direntry = direntry?;
        match direntry.file_name().to_str() {
            None => continue,

            // These special files appear in the snapshot, but are not needed by the page server
            Some("pg_control") => continue,
            Some("pg_filenode.map") => continue,

            // Load any relation files into the page server
            _ => restore_relfile(
                timeline,
                snapshot,
                pg_constants::GLOBALTABLESPACE_OID,
                0,
                &direntry.path(),
            )?,
        }
    }

    // Scan 'base'. It contains database dirs, the database OID is the filename.
    // E.g. 'base/12345', where 12345 is the database OID.
    for direntry in fs::read_dir(snapshotpath.join("base"))? {
        let direntry = direntry?;

        let dboid = u32::from_str_radix(direntry.file_name().to_str().unwrap(), 10)?;

        for direntry in fs::read_dir(direntry.path())? {
            let direntry = direntry?;
            match direntry.file_name().to_str() {
                None => continue,

                // These special files appear in the snapshot, but are not needed by the page server
                Some("PG_VERSION") => continue,
                Some("pg_filenode.map") => continue,

                // Load any relation files into the page server
                _ => restore_relfile(
                    timeline,
                    snapshot,
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    &direntry.path(),
                )?,
            }
        }
    }
    for entry in fs::read_dir(snapshotpath.join("pg_xact"))? {
        let entry = entry?;
        restore_nonrelfile(
            conf,
            timeline,
            timelineid,
            snapshot,
            pg_constants::PG_XACT_FORKNUM,
            &entry.path(),
        )?;
    }
    // TODO: Scan pg_tblspc

    Ok(())
}

///
/// Scan one relfile from a snapshot, loading all pages into the given Timeline
///
/// The relfile is just a flat file in the same format used by PostgreSQL. There
/// is no version information in it.
///
pub fn restore_relfile(
    timeline: &dyn Timeline,
    snapshot: &str,
    spcoid: u32,
    dboid: u32,
    path: &Path,
) -> Result<()> {
    let lsn = Lsn::from_hex(snapshot)?;

    // Does it look like a relation file?

    let p = parse_relfilename(path.file_name().unwrap().to_str().unwrap());
    if let Err(e) = p {
        warn!("unrecognized file in snapshot: {:?} ({})", path, e);
        return Err(e.into());
    }
    let (relnode, forknum, segno) = p.unwrap();

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];

    // FIXME: use constants (BLCKSZ)
    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / 8192);
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                let tag = BufferTag {
                    rel: RelTag {
                        spcnode: spcoid,
                        dbnode: dboid,
                        relnode,
                        forknum: forknum as u8,
                    },
                    blknum,
                };
                timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buf));
                /*
                if oldest_lsn == 0 || p.lsn < oldest_lsn {
                    oldest_lsn = p.lsn;
                }
                 */
            }

            // TODO: UnexpectedEof is expected
            Err(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    // FIXME: maybe check that we read the full length of the file?
                    break;
                }
                _ => {
                    error!("error reading file: {:?} ({})", path, e);
                    break;
                }
            },
        };
        blknum += 1;
    }

    Ok(())
}

fn restore_nonrelfile(
    _conf: &PageServerConf,
    timeline: &dyn Timeline,
    _timelineid: ZTimelineId,
    snapshot: &str,
    forknum: u32,
    path: &Path,
) -> Result<()> {
    let lsn = Lsn::from_hex(snapshot)?;

    // Does it look like a relation file?

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];
    let segno = u32::from_str_radix(path.file_name().unwrap().to_str().unwrap(), 16)?;

    // FIXME: use constants (BLCKSZ)
    let mut blknum: u32 = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                let tag = BufferTag {
                    rel: RelTag {
                        spcnode: 0,
                        dbnode: 0,
                        relnode: 0,
                        forknum: forknum as u8,
                    },
                    blknum,
                };
                timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buf));
                /*
                if oldest_lsn == 0 || p.lsn < oldest_lsn {
                    oldest_lsn = p.lsn;
                }
                 */
            }

            // TODO: UnexpectedEof is expected
            Err(e) => match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    // FIXME: maybe check that we read the full length of the file?
                    break;
                }
                _ => {
                    error!("error reading file: {:?} ({})", path, e);
                    break;
                }
            },
        };
        blknum += 1;
    }

    Ok(())
}

// Scan WAL on a timeline, starting from gien LSN, and load all the records
// into the page cache.
fn restore_wal(timeline: &dyn Timeline, timelineid: ZTimelineId, startpoint: Lsn) -> Result<()> {
    let walpath = format!("timelines/{}/wal", timelineid);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    const SEG_SIZE: u64 = 16 * 1024 * 1024;
    let mut segno = startpoint.segment_number(SEG_SIZE);
    let mut offset = startpoint.segment_offset(SEG_SIZE);
    let mut last_lsn = Lsn(0);
    loop {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, 16 * 1024 * 1024);
        let mut path = walpath.clone() + "/" + &filename;

        // It could be as .partial
        if !PathBuf::from(&path).exists() {
            path += ".partial";
        }

        // Slurp the WAL file
        let open_result = File::open(&path);
        if let Err(e) = &open_result {
            if e.kind() == std::io::ErrorKind::NotFound {
                break;
            }
        }
        let mut file = open_result?;

        if offset > 0 {
            file.seek(SeekFrom::Start(offset as u64))?;
        }

        let mut buf = Vec::new();
        let nread = file.read_to_end(&mut buf)?;
        if nread != 16 * 1024 * 1024 - offset as usize {
            // Maybe allow this for .partial files?
            error!("read only {} bytes from WAL file", nread);
        }
        waldecoder.feed_bytes(&buf);

        let mut nrecords = 0;
        loop {
            let rec = waldecoder.poll_decode();
            if rec.is_err() {
                // Assume that an error means we've reached the end of
                // a partial WAL record. So that's ok.
                break;
            }
            if let Some((lsn, recdata)) = rec.unwrap() {
                let decoded = decode_wal_record(recdata.clone());
                timeline.save_decoded_record(decoded, recdata, lsn)?;
                last_lsn = lsn;
            } else {
                break;
            }
            nrecords += 1;
        }

        info!("restored {} records from WAL file {}", nrecords, filename);

        segno += 1;
        offset = 0;
    }
    info!("reached end of WAL at {}", last_lsn);

    Ok(())
}
