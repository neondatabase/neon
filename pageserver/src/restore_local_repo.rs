//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! zenith repository
//!
use log::*;
use std::cmp::{max, min};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::Result;
use bytes::Bytes;

use crate::repository::{BufferTag, RelTag, Timeline, WALRecord};
use crate::waldecoder::{decode_wal_record, DecodedWALRecord, Oid, WalStreamDecoder};
use crate::waldecoder::{XlCreateDatabase, XlSmgrTruncate};
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
                save_decoded_record(timeline, &decoded, recdata, lsn)?;
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

///
/// Helper function to parse a WAL record and call the Timeline's PUT functions for all the
/// relations/pages that the record affects.
///
pub fn save_decoded_record(
    timeline: &dyn Timeline,
    decoded: &DecodedWALRecord,
    recdata: Bytes,
    lsn: Lsn,
) -> Result<()> {
    // Iterate through all the blocks that the record modifies, and
    // "put" a separate copy of the record for each block.
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

        let rec = WALRecord {
            lsn,
            will_init: blk.will_init || blk.apply_image,
            rec: recdata.clone(),
            main_data_offset: decoded.main_data_offset as u32,
        };

        timeline.put_wal_record(tag, rec)?;
    }

    // Handle a few special record types
    if decoded.xl_rmid == pg_constants::RM_SMGR_ID
        && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK) == pg_constants::XLOG_SMGR_TRUNCATE
    {
        let truncate = XlSmgrTruncate::decode(&decoded);
        save_xlog_smgr_truncate(timeline, lsn, &truncate)?;
    } else if decoded.xl_rmid == pg_constants::RM_DBASE_ID
        && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK) == pg_constants::XLOG_DBASE_CREATE
    {
        let createdb = XlCreateDatabase::decode(&decoded);
        save_xlog_dbase_create(timeline, lsn, &createdb)?;
    }

    // Now that this record has been handled, let the repository know that
    // it is up-to-date to this LSN
    timeline.advance_last_record_lsn(lsn);
    Ok(())
}

/// Subroutine of save_decoded_record(), to handle an XLOG_DBASE_CREATE record.
fn save_xlog_dbase_create(timeline: &dyn Timeline, lsn: Lsn, rec: &XlCreateDatabase) -> Result<()> {
    let db_id = rec.db_id;
    let tablespace_id = rec.tablespace_id;
    let src_db_id = rec.src_db_id;
    let src_tablespace_id = rec.src_tablespace_id;

    // Creating a database is implemented by copying the template (aka. source) database.
    // To copy all the relations, we need to ask for the state as of the same LSN, but we
    // cannot pass 'lsn' to the Timeline.get_* functions, or they will block waiting for
    // the last valid LSN to advance up to it. So we use the previous record's LSN in the
    // get calls instead.
    let req_lsn = min(timeline.get_last_record_lsn(), lsn);

    let rels = timeline.list_rels(src_tablespace_id, src_db_id, req_lsn)?;

    trace!("save_create_database: {} rels", rels.len());

    let mut num_rels_copied = 0;
    let mut num_blocks_copied = 0;
    for src_rel in rels {
        assert_eq!(src_rel.spcnode, src_tablespace_id);
        assert_eq!(src_rel.dbnode, src_db_id);

        let nblocks = timeline.get_rel_size(src_rel, req_lsn)?;
        let dst_rel = RelTag {
            spcnode: tablespace_id,
            dbnode: db_id,
            relnode: src_rel.relnode,
            forknum: src_rel.forknum,
        };

        // Copy content
        for blknum in 0..nblocks {
            let src_key = BufferTag {
                rel: src_rel,
                blknum,
            };
            let dst_key = BufferTag {
                rel: dst_rel,
                blknum,
            };

            let content = timeline.get_page_at_lsn(src_key, req_lsn)?;

            info!("copying block {:?} to {:?}", src_key, dst_key);

            timeline.put_page_image(dst_key, lsn, content)?;
            num_blocks_copied += 1;
        }

        if nblocks == 0 {
            // make sure we have some trace of the relation, even if it's empty
            timeline.put_truncation(dst_rel, lsn, 0)?;
        }

        num_rels_copied += 1;
    }
    info!(
        "Created database {}/{}, copied {} blocks in {} rels at {}",
        tablespace_id, db_id, num_blocks_copied, num_rels_copied, lsn
    );
    Ok(())
}

/// Subroutine of save_decoded_record(), to handle an XLOG_SMGR_TRUNCATE record.
///
/// This is the same logic as in PostgreSQL's smgr_redo() function.
fn save_xlog_smgr_truncate(timeline: &dyn Timeline, lsn: Lsn, rec: &XlSmgrTruncate) -> Result<()> {
    let spcnode = rec.rnode.spcnode;
    let dbnode = rec.rnode.dbnode;
    let relnode = rec.rnode.relnode;

    if (rec.flags & pg_constants::SMGR_TRUNCATE_HEAP) != 0 {
        let rel = RelTag {
            spcnode,
            dbnode,
            relnode,
            forknum: pg_constants::MAIN_FORKNUM,
        };
        timeline.put_truncation(rel, lsn, rec.blkno)?;
    }
    if (rec.flags & pg_constants::SMGR_TRUNCATE_FSM) != 0 {
        let rel = RelTag {
            spcnode,
            dbnode,
            relnode,
            forknum: pg_constants::FSM_FORKNUM,
        };

        // FIXME: 'blkno' stored in the WAL record is the new size of the
        // heap. The formula for calculating the new size of the FSM is
        // pretty complicated (see FreeSpaceMapPrepareTruncateRel() in
        // PostgreSQL), and we should also clear bits in the tail FSM block,
        // and update the upper level FSM pages. None of that has been
        // implemented. What we do instead, is always just truncate the FSM
        // to zero blocks. That's bad for performance, but safe. (The FSM
        // isn't needed for correctness, so we could also leave garbage in
        // it. Seems more tidy to zap it away.)
        if rec.blkno != 0 {
            info!("Partial truncation of FSM is not supported");
        }
        let num_fsm_blocks = 0;
        timeline.put_truncation(rel, lsn, num_fsm_blocks)?;
    }
    if (rec.flags & pg_constants::SMGR_TRUNCATE_VM) != 0 {
        let rel = RelTag {
            spcnode,
            dbnode,
            relnode,
            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
        };

        // FIXME: Like with the FSM above, the logic to truncate the VM
        // correctly has not been implemented. Just zap it away completely,
        // always. Unlike the FSM, the VM must never have bits incorrectly
        // set. From a correctness point of view, it's always OK to clear
        // bits or remove it altogether, though.
        if rec.blkno != 0 {
            info!("Partial truncation of VM is not supported");
        }
        let num_vm_blocks = 0;
        timeline.put_truncation(rel, lsn, num_vm_blocks)?;
    }
    Ok(())
}
