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
use crate::waldecoder::{decode_wal_record, Oid, WalStreamDecoder};
use crate::PageServerConf;
use crate::ZTimelineId;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::*;
use zenith_utils::lsn::Lsn;

///
/// Load all WAL and all relation data pages from local disk into the repository.
///
pub fn restore_timeline(
    conf: &PageServerConf,
    timeline: &dyn Timeline,
    timelineid: ZTimelineId,
) -> Result<()> {
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

        // FIXME: pass filename as Path instead of str?
        let filename_str = filename.into_string().unwrap();
        restore_snapshot(conf, timeline, timelineid, &filename_str)?;
        info!("restored snapshot at {:?}", filename_str);
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
pub fn find_latest_snapshot(_conf: &PageServerConf, timeline: ZTimelineId) -> Result<Lsn> {
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
            Some("pg_control") => restore_nonrel_file(
                conf,
                timeline,
                timelineid,
                "0",
                0,
                0,
                pg_constants::PG_CONTROLFILE_FORKNUM,
                0,
                &direntry.path(),
            )?,
            Some("pg_filenode.map") => restore_nonrel_file(
                conf,
                timeline,
                timelineid,
                snapshot,
                pg_constants::GLOBALTABLESPACE_OID,
                0,
                pg_constants::PG_FILENODEMAP_FORKNUM,
                0,
                &direntry.path(),
            )?,

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

        let dboid = direntry.file_name().to_str().unwrap().parse::<u32>()?;

        for direntry in fs::read_dir(direntry.path())? {
            let direntry = direntry?;
            match direntry.file_name().to_str() {
                None => continue,

                // These special files appear in the snapshot, but are not needed by the page server
                Some("PG_VERSION") => continue,
                Some("pg_filenode.map") => restore_nonrel_file(
                    conf,
                    timeline,
                    timelineid,
                    snapshot,
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    pg_constants::PG_FILENODEMAP_FORKNUM,
                    0,
                    &direntry.path(),
                )?,

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
        restore_slru_file(
            conf,
            timeline,
            timelineid,
            snapshot,
            pg_constants::PG_XACT_FORKNUM,
            &entry.path(),
        )?;
    }
    for entry in fs::read_dir(snapshotpath.join("pg_multixact").join("members"))? {
        let entry = entry?;
        restore_slru_file(
            conf,
            timeline,
            timelineid,
            snapshot,
            pg_constants::PG_MXACT_MEMBERS_FORKNUM,
            &entry.path(),
        )?;
    }
    for entry in fs::read_dir(snapshotpath.join("pg_multixact").join("offsets"))? {
        let entry = entry?;
        restore_slru_file(
            conf,
            timeline,
            timelineid,
            snapshot,
            pg_constants::PG_MXACT_OFFSETS_FORKNUM,
            &entry.path(),
        )?;
    }
    for entry in fs::read_dir(snapshotpath.join("pg_twophase"))? {
        let entry = entry?;
        let xid = u32::from_str_radix(&entry.path().to_str().unwrap(), 16)?;
        restore_nonrel_file(
            conf,
            timeline,
            timelineid,
            snapshot,
            0,
            0,
            pg_constants::PG_TWOPHASE_FORKNUM,
            xid,
            &entry.path(),
        )?;
    }
    // TODO: Scan pg_tblspc

    Ok(())
}

fn restore_relfile(
    timeline: &dyn Timeline,
    snapshot: &str,
    spcoid: Oid,
    dboid: Oid,
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

    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / pg_constants::BLCKSZ as u32);
    loop {
        let r = file.read_exact(&mut buf);
        match r {
            Ok(_) => {
                let tag = BufferTag {
                    rel: RelTag {
                        spcnode: spcoid,
                        dbnode: dboid,
                        relnode,
                        forknum,
                    },
                    blknum,
                };
                timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buf))?;
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

fn restore_nonrel_file(
    _conf: &PageServerConf,
    timeline: &dyn Timeline,
    _timelineid: ZTimelineId,
    snapshot: &str,
    spcoid: Oid,
    dboid: Oid,
    forknum: u8,
    blknum: u32,
    path: &Path,
) -> Result<()> {
    let lsn = Lsn::from_hex(snapshot)?;

    // Does it look like a relation file?

    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    // read the whole file
    file.read_to_end(&mut buffer)?;

    let tag = BufferTag {
        rel: RelTag {
            spcnode: spcoid,
            dbnode: dboid,
            relnode: 0,
            forknum,
        },
        blknum,
    };
    timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buffer[..]))?;
    Ok(())
}

fn restore_slru_file(
    _conf: &PageServerConf,
    timeline: &dyn Timeline,
    _timelineid: ZTimelineId,
    snapshot: &str,
    forknum: u8,
    path: &Path,
) -> Result<()> {
    let lsn = Lsn::from_hex(snapshot)?;

    // Does it look like a relation file?

    let mut file = File::open(path)?;
    let mut buf: [u8; 8192] = [0u8; 8192];
    let segno = u32::from_str_radix(path.file_name().unwrap().to_str().unwrap(), 16)?;

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
                        forknum,
                    },
                    blknum,
                };
                timeline.put_page_image(tag, lsn, Bytes::copy_from_slice(&buf))?;
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

// Scan WAL on a timeline, starting from given LSN, and load all the records
// into the page cache.
fn restore_wal(timeline: &dyn Timeline, timelineid: ZTimelineId, startpoint: Lsn) -> Result<()> {
    let walpath = format!("timelines/{}/wal", timelineid);

    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut segno = startpoint.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let mut offset = startpoint.segment_offset(pg_constants::WAL_SEGMENT_SIZE);
    let mut last_lsn = Lsn(0);

    let mut checkpoint = CheckPoint::new(startpoint.0, 1);
    let checkpoint_tag = BufferTag::fork(pg_constants::PG_CHECKPOINT_FORKNUM);
    let pg_control_tag = BufferTag::fork(pg_constants::PG_CONTROLFILE_FORKNUM);
    if let Some(pg_control_bytes) = timeline.get_page_image(pg_control_tag, Lsn(0))? {
        let pg_control = decode_pg_control(pg_control_bytes)?;
        checkpoint = pg_control.checkPointCopy.clone();
    } else {
        error!("No control file is found in reposistory");
    }

    loop {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
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
        if nread != pg_constants::WAL_SEGMENT_SIZE - offset as usize {
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
                trace!("WAL decoder error {:?}", rec);
                waldecoder.set_position(Lsn((segno + 1) * pg_constants::WAL_SEGMENT_SIZE as u64));
                break;
            }
            if let Some((lsn, recdata)) = rec.unwrap() {
                let decoded = decode_wal_record(&mut checkpoint, recdata.clone());
                timeline.save_decoded_record(decoded, recdata, lsn)?;
                last_lsn = lsn;
            } else {
                break;
            }
            nrecords += 1;
        }

        info!(
            "restored {} records from WAL file {} at {}",
            nrecords, filename, last_lsn
        );

        segno += 1;
        offset = 0;
    }
    info!("reached end of WAL at {}", last_lsn);
    let checkpoint_bytes = encode_checkpoint(checkpoint);
    timeline.put_page_image(checkpoint_tag, Lsn(0), checkpoint_bytes)?;
    Ok(())
}
