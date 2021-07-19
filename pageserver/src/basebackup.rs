//!
//! Generate a tarball with files needed to bootstrap ComputeNode.
//!
//! TODO: this module has nothing to do with PostgreSQL pg_basebackup.
//! It could use a better name.
//!
//! Stateless Postgres compute node is launched by sending a tarball
//! which contains non-relational data (multixacts, clog, filenodemaps, twophase files),
//! generated pg_control and dummy segment of WAL.
//! This module is responsible for creation of such tarball
//! from data stored in object storage.
//!
use bytes::{BufMut, BytesMut};
use log::*;
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;
use tar::{Builder, EntryType, Header};

use crate::object_key::{DatabaseTag, ObjectTag};
use crate::repository::Timeline;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::*;
use zenith_utils::lsn::Lsn;

/// This is short-living object only for the time of tarball creation,
/// created mostly to avoid passing a lot of parameters between various functions
/// used for constructing tarball.
pub struct Basebackup<'a> {
    ar: Builder<&'a mut dyn Write>,
    timeline: &'a Arc<dyn Timeline>,
    lsn: Lsn,
    prev_record_lsn: Lsn,
    slru_buf: [u8; pg_constants::SLRU_SEG_SIZE],
    slru_segno: u32,
    slru_path: &'static str,
}

impl<'a> Basebackup<'a> {
    pub fn new(
        write: &'a mut dyn Write,
        timeline: &'a Arc<dyn Timeline>,
        lsn: Lsn,
        prev_record_lsn: Lsn,
    ) -> Basebackup<'a> {
        Basebackup {
            ar: Builder::new(write),
            timeline,
            lsn,
            prev_record_lsn,
            slru_path: "",
            slru_segno: u32::MAX,
            slru_buf: [0u8; pg_constants::SLRU_SEG_SIZE],
        }
    }

    pub fn send_tarball(&mut self) -> anyhow::Result<()> {
        // Send empty config files.
        for filepath in pg_constants::PGDATA_SPECIAL_FILES.iter() {
            if *filepath == "pg_hba.conf" {
                let data = pg_constants::PG_HBA.as_bytes();
                let header = new_tar_header(&filepath, data.len() as u64)?;
                self.ar.append(&header, &data[..])?;
            } else {
                let header = new_tar_header(&filepath, 0)?;
                self.ar.append(&header, &mut io::empty())?;
            }
        }

        // Gather non-relational files from object storage pages.
        // Iteration is sorted order: all objects of the same type are grouped and traversed
        // in key ascending order. For example all pg_xact records precede pg_multixact records and are sorted by block number.
        // It allows to easily construct SLRU segments.
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

        // write last non-completed SLRU segment (if any)
        self.finish_slru_segment()?;
        // Generate pg_control and bootstrap WAL segment.
        self.add_pgcontrol_file()?;
        self.ar.finish()?;
        debug!("all tarred up!");
        Ok(())
    }

    //
    // Generate SLRU segment files from repository. Path identifies SLRU kind (pg_xact, pg_multixact/members, ...).
    //
    fn add_slru_segment(
        &mut self,
        path: &'static str,
        tag: &ObjectTag,
        blknum: u32,
    ) -> anyhow::Result<()> {
        let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
        // Zero length image indicates truncated segment: just skip it
        if !img.is_empty() {
            assert!(img.len() == pg_constants::BLCKSZ as usize);
            let segno = blknum / pg_constants::SLRU_PAGES_PER_SEGMENT;
            if self.slru_path != "" && (self.slru_segno != segno || self.slru_path != path) {
                // Switch to new segment: save old one
                let segname = format!("{}/{:>04X}", self.slru_path, self.slru_segno);
                let header = new_tar_header(&segname, pg_constants::SLRU_SEG_SIZE as u64)?;
                self.ar.append(&header, &self.slru_buf[..])?;
                self.slru_buf = [0u8; pg_constants::SLRU_SEG_SIZE]; // reinitialize segment buffer
            }
            self.slru_segno = segno;
            self.slru_path = path;
            let offs_start = (blknum % pg_constants::SLRU_PAGES_PER_SEGMENT) as usize
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
        trace!("add_relmap_file {:?}", db);
        let img = self.timeline.get_page_at_lsn_nowait(*tag, self.lsn)?;
        let path = if db.spcnode == pg_constants::GLOBALTABLESPACE_OID {
            let dst_path = "PG_VERSION";
            let version_bytes = pg_constants::PG_MAJORVERSION.as_bytes();
            let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
            self.ar.append(&header, &version_bytes[..])?;

            let dst_path = format!("global/PG_VERSION");
            let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
            self.ar.append(&header, &version_bytes[..])?;

            String::from("global/pg_filenode.map") // filenode map for global tablespace
        } else {
            // User defined tablespaces are not supported
            assert!(db.spcnode == pg_constants::DEFAULTTABLESPACE_OID);

            // Append dir path for each database
            let path = format!("base/{}", db.dbnode);
            let header = new_tar_header_dir(&path)?;
            self.ar.append(&header, &mut io::empty())?;

            let dst_path = format!("base/{}/PG_VERSION", db.dbnode);
            let version_bytes = pg_constants::PG_MAJORVERSION.as_bytes();
            let header = new_tar_header(&dst_path, version_bytes.len() as u64)?;
            self.ar.append(&header, &version_bytes[..])?;

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
    // Add generated pg_control file and bootstrap WAL segment.
    // Also send zenith.signal file with extra bootstrap data.
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
        //TODO Check this.
        //We may need to determine the value from twophase data.
        checkpoint.oldestActiveXid = 0;

        //save new values in pg_control
        pg_control.checkPoint = checkpoint_lsn;
        pg_control.checkPointCopy = checkpoint;
        pg_control.state = pg_constants::DB_SHUTDOWNED;

        // add zenith.signal file
        self.ar.append(
            &new_tar_header("zenith.signal", 8)?,
            &self.prev_record_lsn.0.to_le_bytes()[..],
        )?;

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

fn new_tar_header_dir(path: &str) -> anyhow::Result<Header> {
    let mut header = Header::new_gnu();
    header.set_size(0);
    header.set_path(path)?;
    header.set_mode(0o755); // -rw-------
    header.set_entry_type(EntryType::dir());
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
