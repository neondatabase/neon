//!
//! Generate a tarball with files needed to bootstrap ComputeNode.
//!
//! TODO: this module has nothing to do with PostgreSQL pg_basebackup.
//! It could use a better name.
//!
//! Stateless Postgres compute node is lauched by sending taball which contains on-relational data (multixacts, clog, filenodemaps, twophase files)
//! and generate pg_control and dummy segment of WAL. This module is responsible for creation of such tarball from snapshot directry and
//! data stored in object storage.
//!
use crate::ZTimelineId;
use bytes::{BufMut, BytesMut};
use log::*;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;
use tar::{Builder, Header};
use std::fs;

use crate::object_key::*;
use postgres_ffi::relfile_utils::*;
use crate::repository::{DatabaseTag, ObjectTag, Timeline, BufferTag};
use postgres_ffi::xlog_utils::*;
use postgres_ffi::*;
use zenith_utils::lsn::Lsn;

use postgres_ffi::pg_constants;
use zenith_utils::s3_utils::S3Storage;
use crate::page_cache;
use std::fs::{File};
use std::io::Read;

use crate::branches;

/// This is shorliving object only for the time of tarball creation,
/// created mostly to avoid passing a lot of parameters between varyouds functions
/// used for constructing tarball.
pub struct Basebackup<'a> {
    ar: Builder<&'a mut dyn Write>,
    timeline: &'a Arc<dyn Timeline>,
    lsn: Lsn,
    snappath: String,
    slru_buf: [u8; pg_constants::SLRU_SEG_SIZE],
    slru_segno: u32,
    slru_path: &'static str,
}

impl<'a> Basebackup<'a> {
    pub fn new(
        write: &'a mut dyn Write,
        timelineid: ZTimelineId,
        timeline: &'a Arc<dyn Timeline>,
        lsn: Lsn,
        snapshot_lsn: Lsn,
    ) -> Basebackup<'a> {
        Basebackup {
            ar: Builder::new(write),
            timeline,
            lsn,
            snappath: format!("timelines/{}/snapshots/{:016X}", timelineid, snapshot_lsn.0),
            slru_path: "",
            slru_segno: u32::MAX,
            slru_buf: [0u8; pg_constants::SLRU_SEG_SIZE],
        }
    }

    pub fn send_tarball(&mut self) -> anyhow::Result<()> {
        debug!("sending tarball of snapshot in {}", self.snappath);

        // We need a few config files to start compute node and now we don't store/generate them in pageserver.
        // So we preserve them in snappath directory at zenith-init.
        // FIXME this is a temporary hack. Config files should be handled by some other service.
        for i in 0..pg_constants::PGDATA_SPECIAL_FILES.len()
        {
            let path = pg_constants::PGDATA_SPECIAL_FILES[i];
            self.ar.append_path_with_name(std::path::Path::new(&self.snappath).join(path), path)?;
        }

        // Generate non-relational files.
        // Iteration is sorted order: all objects of the same time are grouped and traversed
        // in key ascending order. For example all pg_xact records precede pg_multixact records and are sorted by block number.
        // It allows to easily construct SLRU segments (32 blocks).
        for obj in self.timeline.list_nonrels(self.lsn)? {
            match obj {
                ObjectTag::Clog(slru) => self.add_slru_segment("pg_xact", &obj, slru.blknum)?,
                ObjectTag::MultiXactMembers(slru) => {
                    self.add_slru_segment("pg_multixact/members", &obj, slru.blknum)?
                }
                ObjectTag::MultiXactOffsets(slru) => {
                    self.add_slru_segment("pg_multixact/offsets", &obj, slru.blknum)?
                }
                ObjectTag::FileNodeMap(db) => self.add_relmap_file(&obj, &db)?,
                ObjectTag::TwoPhase(prepare) => self.add_twophase_file(&obj, prepare.xid)?,
                _ => {}
            }
        }
        self.finish_slru_segment()?; // write last non-completed SLRU segment (if any)
        self.add_pgcontrol_file()?;
        self.ar.finish()?;
        debug!("all tarred up!");
        Ok(())
    }

    //
    // Generate SRLU segment files from repository. Path identifiers SLRU kind (pg_xact, pg_multixact/members, ...).
    // Intiallly pass is empty string.
    //
    fn add_slru_segment(
        &mut self,
        path: &'static str,
        tag: &ObjectTag,
        page: u32,
    ) -> anyhow::Result<()> {
        let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
        // Zero length image indicates truncated segment: just skip it
        if !img.is_empty() {
            assert!(img.len() == pg_constants::BLCKSZ as usize);
            let segno = page / pg_constants::SLRU_PAGES_PER_SEGMENT;
            if self.slru_path != "" && (self.slru_segno != segno || self.slru_path != path) {
                // Switch to new segment: save old one
                let segname = format!("{}/{:>04X}", self.slru_path, self.slru_segno);
                let header = new_tar_header(&segname, pg_constants::SLRU_SEG_SIZE as u64)?;
                self.ar.append(&header, &self.slru_buf[..])?;
                self.slru_buf = [0u8; pg_constants::SLRU_SEG_SIZE]; // reinitialize segment buffer
            }
            self.slru_segno = segno;
            self.slru_path = path;
            let offs_start = (page % pg_constants::SLRU_PAGES_PER_SEGMENT) as usize
                * pg_constants::BLCKSZ as usize;
            let offs_end = offs_start + pg_constants::BLCKSZ as usize;
            self.slru_buf[offs_start..offs_end].copy_from_slice(&img);
        }
        Ok(())
    }

    //
    // We flush SLRU segments to the tarball once them are completed.
    // This method is used to flush last (may be incompleted) segment.
    //
    fn finish_slru_segment(&mut self) -> anyhow::Result<()> {
        if self.slru_path != "" {
            // is there is some incompleted segment
            let segname = format!("{}/{:>04X}", self.slru_path, self.slru_segno);
            let header = new_tar_header(&segname, pg_constants::SLRU_SEG_SIZE as u64)?;
            self.ar.append(&header, &self.slru_buf[..])?;
        }
        Ok(())
    }

    //
    // Extract pg_filenode.map files from repository
    // Along with them also send PG_VERSION for each database.
    //
    fn add_relmap_file(&mut self, tag: &ObjectTag, db: &DatabaseTag) -> anyhow::Result<()> {
        let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
        info!("add_relmap_file {:?}", db);
        let path = if db.spcnode == pg_constants::GLOBALTABLESPACE_OID {

            let dst_path = format!("PG_VERSION");
            //TODO fix hardcoded value. Get this version num somewhere
            let data = "14".as_bytes();
            let header = new_tar_header(&dst_path, data.len() as u64)?;
            self.ar.append(&header, &data[..])?;

            let dst_path = format!("global/PG_VERSION");
            let data = "14".as_bytes();
            let header = new_tar_header(&dst_path, data.len() as u64)?;
            self.ar.append(&header, &data[..])?;

            String::from("global/pg_filenode.map") // filenode map for global tablespace
        } else {
            // User defined tablespaces are not supported
            assert!(db.spcnode == pg_constants::DEFAULTTABLESPACE_OID);

            // Append dir path for each database
            let path = format!("base/{}", db.dbnode);
            let fullpath = std::path::Path::new(&self.snappath).join(path.clone());

            //FIXME It's a hack to send dir with append_dir()
            info!("create dir before {:?}", fullpath.clone());
            fs::create_dir_all(fullpath.clone())?;
            info!("create dir {:?}", fullpath.clone());
            self.ar.append_dir(path, fullpath.clone())?;
            info!("append dir done {:?}", fullpath.clone());

            let dst_path = format!("base/{}/PG_VERSION", db.dbnode);
            let data = "14".as_bytes();
            let header = new_tar_header(&dst_path, data.len() as u64)?;
            self.ar.append(&header, &data[..])?;

            format!("base/{}/pg_filenode.map", db.dbnode)
        };
        assert!(img.len() == 512);
        let header = new_tar_header(&path, img.len() as u64)?;
        self.ar.append(&header, &img[..])?;
        Ok(())
    }

    //
    // Extract twophase state files
    //
    fn add_twophase_file(&mut self, tag: &ObjectTag, xid: TransactionId) -> anyhow::Result<()> {
        // Include in tarball two-phase files only of in-progress transactions
        if self.timeline.get_tx_status(xid, self.lsn)?
            == pg_constants::TRANSACTION_STATUS_IN_PROGRESS
        {
            let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
            let mut buf = BytesMut::new();
            buf.extend_from_slice(&img[..]);
            let crc = crc32c::crc32c(&img[..]);
            buf.put_u32_le(crc);
            let path = format!("pg_twophase/{:>08X}", xid);
            let header = new_tar_header(&path, buf.len() as u64)?;
            self.ar.append(&header, &buf[..])?;
        }
        Ok(())
    }

    //
    // Add generated pg_control file
    //
    fn add_pgcontrol_file(&mut self) -> anyhow::Result<()> {
        let checkpoint_bytes = self
            .timeline
            .get_page_at_lsn_nowait(ObjectTag::Checkpoint, self.lsn)?;
        let pg_control_bytes = self
            .timeline
            .get_page_at_lsn_nowait(ObjectTag::ControlFile, self.lsn)?;
        let mut pg_control = ControlFileData::decode(&pg_control_bytes)?;
        let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;

        // Generate new pg_control and WAL needed for bootstrap
        let checkpoint_segno = self.lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE);
        let checkpoint_lsn = XLogSegNoOffsetToRecPtr(
            checkpoint_segno,
            XLOG_SIZE_OF_XLOG_LONG_PHD as u32,
            pg_constants::WAL_SEGMENT_SIZE,
        );
        checkpoint.redo = self.lsn.0 + self.lsn.calc_padding(8u32);

        //reset some fields we don't want to preserve
        checkpoint.oldestActiveXid = 0;

        //save new values in pg_control
        pg_control.checkPoint = checkpoint_lsn;
        pg_control.checkPointCopy = checkpoint;
        info!("pg_control.state = {}", pg_control.state);
        pg_control.state = pg_constants::DB_SHUTDOWNED;

        // add zenith.signal file
<<<<<<< HEAD
        self.ar
            .append(&new_tar_header("zenith.signal", 0)?, &b""[..])?;
=======
        self.ar.append(&new_tar_header("zenith.signal", 0)?, &b""[..])?;
>>>>>>> f86e9c0... Implement export to s3 in pgdata compatible format.

        //send pg_control
        let pg_control_bytes = pg_control.encode();
        let header = new_tar_header("global/pg_control", pg_control_bytes.len() as u64)?;
        self.ar.append(&header, &pg_control_bytes[..])?;

        //send wal segment
        let wal_file_name = XLogFileName(
            1, // FIXME: always use Postgres timeline 1
            checkpoint_segno,
            pg_constants::WAL_SEGMENT_SIZE,
        );
        let wal_file_path = format!("pg_wal/{}", wal_file_name);
        let header = new_tar_header(&wal_file_path, pg_constants::WAL_SEGMENT_SIZE as u64)?;
        let wal_seg = generate_wal_segment(&pg_control);
        self.ar.append(&header, &wal_seg[..])?;
        Ok(())
    }
}

// -------------- TODO move this code to relfile_utils.rs or remove

// ///
// /// Parse a path, relative to the root of PostgreSQL data directory, as
// /// a PostgreSQL relation data file.
// ///
// fn parse_rel_file_path(path: &str) -> Result<(), FilePathError> {
//     /*
//      * Relation data files can be in one of the following directories:
//      *
//      * global/
//      *		shared relations
//      *
//      * base/<db oid>/
//      *		regular relations, default tablespace
//      *
//      * pg_tblspc/<tblspc oid>/<tblspc version>/
//      *		within a non-default tablespace (the name of the directory
//      *		depends on version)
//      *
//      * And the relation data files themselves have a filename like:
//      *
//      * <oid>.<segment number>
//      */
//     if let Some(fname) = path.strip_prefix("global/") {
//         let (_relnode, _forknum, _segno) = parse_relfilename(fname)?;

//         Ok(())
//     } else if let Some(dbpath) = path.strip_prefix("base/") {
//         let mut s = dbpath.split('/');
//         let dbnode_str = s.next().ok_or(FilePathError::InvalidFileName)?;
//         let _dbnode = dbnode_str.parse::<u32>()?;
//         let fname = s.next().ok_or(FilePathError::InvalidFileName)?;
//         if s.next().is_some() {
//             return Err(FilePathError::InvalidFileName);
//         };

//         let (_relnode, _forknum, _segno) = parse_relfilename(fname)?;

//         Ok(())
//     } else if path.strip_prefix("pg_tblspc/").is_some() {
//         // TODO
//         error!("tablespaces not implemented yet");
//         Err(FilePathError::InvalidFileName)
//     } else {
//         Err(FilePathError::InvalidFileName)
//     }
// }

// //
// // Check if it is relational file
// //
// fn is_rel_file_path(path: &str) -> bool {
//     parse_rel_file_path(path).is_ok()
// }

// ---------------

//
// Create new tarball entry header
//
fn new_tar_header(path: &str, size: u64) -> anyhow::Result<Header> {
    let mut header = Header::new_gnu();
    header.set_size(size);
    header.set_path(path)?;
    header.set_mode(0b110000000); // -rw-------
    header.set_mtime(
        // use currenttime as last modified time
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    header.set_cksum();
    Ok(header)
}

// ----------- S3 -----------

pub struct SegmentState {
    buf: Vec<u8>,
    segno: u32,
    path: String,
}


pub fn send_files_to_s3(s3_storage: S3Storage, timeline_id: ZTimelineId) -> anyhow::Result<()>
{
    let repo = page_cache::get_repository();
    let timeline = repo.get_timeline(timeline_id)?;

    // TODO should we ask for specific lsn?
    let end_of_wal_lsn = repo
        .get_timeline(timeline_id)?
        .get_last_record_lsn();

    println!("pushing at end of WAL: {}", end_of_wal_lsn);

     // Create pgdata subdirs structure
     for i in 0..pg_constants::PGDATA_SUBDIRS.len() {
        let relative_path = format!("{}/",pg_constants::PGDATA_SUBDIRS[i]);

        s3_storage.put_object(relative_path.to_string(), &[]);

    }

    //send special files
    for i in 0..pg_constants::PGDATA_SPECIAL_FILES.len()
    {
        let (_, snapshotdir) = branches::find_latest_snapshot(repo.get_conf(), timeline_id)?;

        println!("get config from timelinedir {:?}", snapshotdir);
        let relative_path = pg_constants::PGDATA_SPECIAL_FILES[i];
        let mut file = File::open(&snapshotdir.join(relative_path))?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;

        s3_storage.put_object(relative_path.to_string(), &content[..]);

    }

    // Send relation files
    let rels = timeline.list_rels(0, 0, end_of_wal_lsn)?;
    let mut state = SegmentState
    {
        path: "".to_string(),
        segno: 0,
        buf: Vec::new()
    };

    for rel in rels {
        let nblocks = timeline.get_rel_size(rel, end_of_wal_lsn)?;
        println!("relation {} has nblocks {}. segsize {}", rel, nblocks,
         (pg_constants::RELSEG_SIZE * pg_constants::BLCKSZ as u32));

        for blknum in 0..nblocks {
            let src_key = ObjectTag::RelationBuffer(BufferTag {
                rel: rel,
                blknum,
            });

            let img = timeline.get_page_at_lsn_nowait(src_key, end_of_wal_lsn)?;
            let segno = blknum/pg_constants::RELSEG_SIZE;
            debug!("copying block {:?} rel {}", src_key, rel);

            state.path = rel.to_pgdata_path().clone();
            state.buf.put(img);

            if state.segno != segno || blknum == nblocks - 1
            {
                let segname = if segno == 0
                    {format!("{}", state.path)}
                    else { format!("{}.{}", state.path, segno) };

                debug!("writing segno {} segname {} ", segno, segname);
                
                s3_storage.put_object(segname, &state.buf.as_slice());

                state.buf = Vec::new();
            }
            state.segno = segno;
        }
    }

    let mut state = SegmentState
    {
        path: "".to_string(),
        segno: u32::MAX,
        buf: Vec::new()
    };

    // Send non-relation files
    for obj in timeline.list_nonrels(end_of_wal_lsn)? {
        match obj {
            ObjectTag::Clog(slru) | ObjectTag::MultiXactMembers(slru) |  ObjectTag::MultiXactOffsets(slru) =>
            {
                let path =  match obj {
                    ObjectTag::Clog(_) => "pg_xact".to_string(),
                    ObjectTag::MultiXactMembers(_) => "pg_multixact/members".to_string(),
                    ObjectTag::MultiXactOffsets(_) => "pg_multixact/offsets".to_string(),
                    _ => "".to_string(),
                };

                let img = timeline.get_page_at_lsn_nowait(obj, end_of_wal_lsn)?;

                if !img.is_empty() {
                    assert!(img.len() == pg_constants::BLCKSZ as usize);
                    let segno = slru.blknum / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    if state.path != "" && (state.segno != segno || state.path != path)
                    {
                        let segname = format!("{}/{:>04X}", state.path, state.segno);
                        // send block to s3
                        trace!("send segname {}", segname);

                        s3_storage.put_object(segname, &state.buf.as_slice());
                        state.buf = Vec::new();
                    }

                    state.segno = segno;
                    state.path = path;
                    state.buf.put(img);
                }
            },
            ObjectTag::FileNodeMap(db) =>
            {
                let img = timeline.get_page_at_lsn_nowait(obj, end_of_wal_lsn)?;
                info!("add_relmap_file {:?}", db);
                let path = if db.spcnode == pg_constants::GLOBALTABLESPACE_OID {
                    String::from("global/pg_filenode.map") // filenode map for global tablespace
                } else {
                    // User defined tablespaces are not supported
                    assert!(db.spcnode == pg_constants::DEFAULTTABLESPACE_OID);
                    format!("base/{}/pg_filenode.map", db.dbnode)
                };
                assert!(img.len() == 512);
                s3_storage.put_object(path, &img[..]);

                const MESSAGE: &str = "14";

                if db.spcnode == pg_constants::GLOBALTABLESPACE_OID {
                    let pg_version_path = "PG_VERSION";
                    s3_storage.put_object(pg_version_path.to_string(), MESSAGE.as_bytes());

                    let pg_version_path = "global/PG_VERSION";
                    s3_storage.put_object(pg_version_path.to_string(), MESSAGE.as_bytes());

                } else {
                    // User defined tablespaces are not supported
                    assert!(db.spcnode == pg_constants::DEFAULTTABLESPACE_OID);
                    let pg_version_path = format!("base/{}/PG_VERSION", db.dbnode);
                    s3_storage.put_object(pg_version_path, MESSAGE.as_bytes());
                };

            },
            ObjectTag::TwoPhase(prepare) =>
            {
                if timeline.get_tx_status(prepare.xid, end_of_wal_lsn)?
                    == pg_constants::TRANSACTION_STATUS_IN_PROGRESS
                {
                    let img = timeline.get_page_at_lsn_nowait(obj, end_of_wal_lsn)?;
                    let mut buf = BytesMut::new();
                    buf.extend_from_slice(&img[..]);
                    let crc = crc32c::crc32c(&img[..]);
                    buf.put_u32_le(crc);
                    let path = format!("pg_twophase/{:>08X}", prepare.xid);
                    s3_storage.put_object(path, &buf[..]);
                }
            }
            _ => {}
        }
    }

    if state.path != "" {
        // is there is some incompleted segment
        let segname = format!("{}/{:>04X}", state.path, state.segno);
        // send block to s3
        s3_storage.put_object(segname, &state.buf[..]);
    }

    let checkpoint_bytes = timeline
        .get_page_at_lsn_nowait(ObjectTag::Checkpoint, end_of_wal_lsn)?;
    let pg_control_bytes = timeline
        .get_page_at_lsn_nowait(ObjectTag::ControlFile, end_of_wal_lsn)?;
    let mut pg_control = ControlFileData::decode(&pg_control_bytes)?;
    let mut checkpoint = CheckPoint::decode(&checkpoint_bytes)?;


    // Generate new pg_control and WAL needed for bootstrap
    let checkpoint_segno = end_of_wal_lsn.segment_number(pg_constants::WAL_SEGMENT_SIZE);
    let checkpoint_lsn = XLogSegNoOffsetToRecPtr(
        checkpoint_segno,
        XLOG_SIZE_OF_XLOG_LONG_PHD as u32,
        pg_constants::WAL_SEGMENT_SIZE,
    );

    // XXX This is a hack just to let the postgres start.
    // Similar to what pg_rewind does.

    // FIXME We need to get real WAL segment somewhere
    // or learn to gather it from page_cache and set "real" checkpoint redo lsn
    // which will refer to the LSN of the exported snapshot.
    checkpoint.redo = checkpoint_lsn;

    //reset some fields we don't want to preserve
    checkpoint.oldestActiveXid = 0;
    
    //save new values in pg_control
    pg_control.checkPoint = checkpoint_lsn;
    pg_control.checkPointCopy = checkpoint;
    info!("pg_control.state = {}", pg_control.state);
    pg_control.state = pg_constants::DB_SHUTDOWNED;

    //send pg_control
    let pg_control_bytes = pg_control.encode();
    let pg_control_path = "global/pg_control";

    s3_storage.put_object(pg_control_path.to_string(), &pg_control_bytes[..]);

    //send wal segment
    let wal_file_name = XLogFileName(
        1, // FIXME: always use Postgres timeline 1
        checkpoint_segno,
        pg_constants::WAL_SEGMENT_SIZE,
    );
    let wal_file_path = format!("pg_wal/{}", wal_file_name);
    let wal_seg = generate_wal_segment(&pg_control);
    s3_storage.put_object(wal_file_path, &wal_seg[..]);
    Ok(())
}

// ----------- S3 -----------
