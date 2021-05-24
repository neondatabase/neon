//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! zenith repository
//!
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
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::xlog_utils::*;
use zenith_utils::lsn::Lsn;

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

///
/// Import all relation data pages from local disk into the repository.
///
pub fn import_timeline_from_postgres_datadir(
    path: &Path,
    timeline: &dyn Timeline,
    lsn: Lsn,
) -> Result<()> {
    // Scan 'global'
    for direntry in fs::read_dir(path.join("global"))? {
        let direntry = direntry?;
        match direntry.file_name().to_str() {
            None => continue,

            // These special files appear in the snapshot, but are not needed by the page server
            Some("pg_control") => continue,
            Some("pg_filenode.map") => continue,

            // Load any relation files into the page server
            _ => import_relfile(
                &direntry.path(),
                timeline,
                lsn,
                pg_constants::GLOBALTABLESPACE_OID,
                0,
            )?,
        }
    }

    // Scan 'base'. It contains database dirs, the database OID is the filename.
    // E.g. 'base/12345', where 12345 is the database OID.
    for direntry in fs::read_dir(path.join("base"))? {
        let direntry = direntry?;

        let dboid = direntry.file_name().to_str().unwrap().parse::<u32>()?;

        for direntry in fs::read_dir(direntry.path())? {
            let direntry = direntry?;
            match direntry.file_name().to_str() {
                None => continue,

                // These special files appear in the snapshot, but are not needed by the page server
                Some("PG_VERSION") => continue,
                Some("pg_filenode.map") => continue,

                // Load any relation files into the page server
                _ => import_relfile(
                    &direntry.path(),
                    timeline,
                    lsn,
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                )?,
            }
        }
    }
    // TODO: Scan pg_tblspc

    timeline.checkpoint()?;

    Ok(())
}

// subroutine of import_timeline_from_postgres_datadir(), to load one relation file.
fn import_relfile(
    path: &Path,
    timeline: &dyn Timeline,
    lsn: Lsn,
    spcoid: Oid,
    dboid: Oid,
) -> Result<()> {
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

/// Scan PostgreSQL WAL files in given directory, and load all records >= 'startpoint' into
/// the repository.
pub fn import_timeline_wal(walpath: &Path, timeline: &dyn Timeline, startpoint: Lsn) -> Result<()> {
    let mut waldecoder = WalStreamDecoder::new(startpoint);

    let mut segno = startpoint.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let mut offset = startpoint.segment_offset(pg_constants::WAL_SEGMENT_SIZE);
    let mut last_lsn = startpoint;
    loop {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, pg_constants::WAL_SEGMENT_SIZE);
        let mut path = walpath.join(&filename);

        // It could be as .partial
        if !PathBuf::from(&path).exists() {
            path = walpath.join(filename + ".partial");
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

        info!(
            "imported {} records from WAL file {} up to {}",
            nrecords,
            path.display(),
            last_lsn
        );

        segno += 1;
        offset = 0;
    }
    info!("reached end of WAL at {}", last_lsn);
    Ok(())
}
