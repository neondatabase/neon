//!
//! Parse PostgreSQL WAL records and store them in a zenith Timeline.
//!
//! The pipeline for ingesting WAL looks like this:
//!
//! WAL receiver  ->   WalIngest  ->   Repository
//!
//! The WAL receiver receives a stream of WAL from the WAL safekeepers,
//! and decodes it to individual WAL records. It feeds the WAL records
//! to WalIngest, which parses them and stores them in the Repository.
//!
//! The zenith Repository can store page versions in two formats: as
//! page images, or a WAL records. WalIngest::ingest_record() extracts
//! page images out of some WAL records, but most it stores as WAL
//! records. If a WAL record modifies multple pages, WalIngest
//! will call Repository::put_wal_record or put_page_image functions
//! separately for each modified page.
//!
//! To reconstruct a page using a WAL record, the Repository calls the
//! code in walredo.rs. walredo.rs passes most WAL records to the WAL
//! redo Postgres process, but some records it can handle directly with
//! bespoken Rust code.

use chrono::format::format;
use postgres_ffi::nonrelfile_utils::clogpage_precedes;
use postgres_ffi::nonrelfile_utils::slru_may_delete_clogsegment;

use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use tracing::*;

use std::collections::HashMap;

use crate::pgdatadir_mapping::*;
use crate::relish::*;
use crate::repository::Repository;
use crate::wal_metadata::WalEntryMetadata;
use crate::walrecord::*;
use postgres_ffi::nonrelfile_utils::mx_offset_to_member_segment;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::TransactionId;
use postgres_ffi::{pg_constants, CheckPoint};
use zenith_utils::lsn::Lsn;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

pub struct WalIngest<'a, R: Repository> {
    timeline: &'a DatadirTimeline<R>,

    checkpoint: CheckPoint,
    checkpoint_modified: bool,

    relsize_cache: HashMap<RelTag, BlockNumber>,
}

impl<'a, R: Repository> WalIngest<'a, R> {
    pub fn new(timeline: &DatadirTimeline<R>, startpoint: Lsn) -> Result<WalIngest<R>> {
        // Fetch the latest checkpoint into memory, so that we can compare with it
        // quickly in `ingest_record` and update it when it changes.
        let checkpoint_bytes = timeline.get_checkpoint(startpoint)?;
        let checkpoint = CheckPoint::decode(&checkpoint_bytes)?;
        trace!("CheckPoint.nextXid = {}", checkpoint.nextXid.value);

        Ok(WalIngest {
            timeline,
            checkpoint,
            checkpoint_modified: false,
            relsize_cache: HashMap::new(),
        })
    }

    ///
    /// Decode a PostgreSQL WAL record and store it in the repository, in the given timeline.
    ///
    ///
    /// Helper function to parse a WAL record and call the Timeline's PUT functions for all the
    /// relations/pages that the record affects.
    ///
    pub fn ingest_record(
        &mut self,
        timeline: &DatadirTimeline<R>,
        recdata: Bytes,
        lsn: Lsn,
    ) -> Result<()> {
        let mut writer = timeline.begin_record(lsn);

        let recdata_len = recdata.len();
        let mut decoded = decode_wal_record(recdata);
        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);

        assert!(!self.checkpoint_modified);
        if self.checkpoint.update_next_xid(decoded.xl_xid) {
            self.checkpoint_modified = true;
        }

        // Heap AM records need some special handling, because they modify VM pages
        // without registering them with the standard mechanism.
        if decoded.xl_rmid == pg_constants::RM_HEAP_ID
            || decoded.xl_rmid == pg_constants::RM_HEAP2_ID
        {
            self.ingest_heapam_record(&mut buf, &mut writer, &mut decoded)?;
        }
        // Handle other special record types
        if decoded.xl_rmid == pg_constants::RM_SMGR_ID
            && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_SMGR_CREATE
        {
            let create = XlSmgrCreate::decode(&mut buf);
            self.ingest_xlog_smgr_create(&mut writer, &create)?;
        } else if decoded.xl_rmid == pg_constants::RM_SMGR_ID
            && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_SMGR_TRUNCATE
        {
            let truncate = XlSmgrTruncate::decode(&mut buf);
            self.ingest_xlog_smgr_truncate(&mut writer, &truncate)?;
        } else if decoded.xl_rmid == pg_constants::RM_DBASE_ID {
            if (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_DBASE_CREATE
            {
                let createdb = XlCreateDatabase::decode(&mut buf);
                self.ingest_xlog_dbase_create(&mut writer, &createdb)?;
            } else if (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_DBASE_DROP
            {
                let dropdb = XlDropDatabase::decode(&mut buf);
                for tablespace_id in dropdb.tablespace_ids {
                    trace!("Drop db {}, {}", tablespace_id, dropdb.db_id);
                    writer.drop_dbdir(tablespace_id, dropdb.db_id)?;
                }
            }
        } else if decoded.xl_rmid == pg_constants::RM_TBLSPC_ID {
            trace!("XLOG_TBLSPC_CREATE/DROP is not handled yet");
        } else if decoded.xl_rmid == pg_constants::RM_CLOG_ID {
            let info = decoded.xl_info & !pg_constants::XLR_INFO_MASK;
            if info == pg_constants::CLOG_ZEROPAGE {
                let pageno = buf.get_u32_le();
                let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                self.put_slru_page_image(
                    &mut writer,
                    SlruKind::Clog,
                    segno,
                    rpageno,
                    ZERO_PAGE.clone(),
                )?;
            } else {
                assert!(info == pg_constants::CLOG_TRUNCATE);
                let xlrec = XlClogTruncate::decode(&mut buf);
                self.ingest_clog_truncate_record(&mut writer, &xlrec)?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_XACT_ID {
            let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;
            if info == pg_constants::XLOG_XACT_COMMIT || info == pg_constants::XLOG_XACT_ABORT {
                let parsed_xact =
                    XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
                self.ingest_xact_record(
                    &mut writer,
                    &parsed_xact,
                    info == pg_constants::XLOG_XACT_COMMIT,
                )?;
            } else if info == pg_constants::XLOG_XACT_COMMIT_PREPARED
                || info == pg_constants::XLOG_XACT_ABORT_PREPARED
            {
                let parsed_xact =
                    XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
                self.ingest_xact_record(
                    &mut writer,
                    &parsed_xact,
                    info == pg_constants::XLOG_XACT_COMMIT_PREPARED,
                )?;
                // Remove twophase file. see RemoveTwoPhaseFile() in postgres code
                trace!(
                    "Drop twophaseFile for xid {} parsed_xact.xid {} here at {}",
                    decoded.xl_xid,
                    parsed_xact.xid,
                    lsn,
                );
                writer.drop_twophase_file(parsed_xact.xid)?;
            } else if info == pg_constants::XLOG_XACT_PREPARE {
                writer.put_twophase_file(decoded.xl_xid, Bytes::copy_from_slice(&buf[..]))?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_MULTIXACT_ID {
            let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

            if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
                let pageno = buf.get_u32_le();
                let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                self.put_slru_page_image(
                    &mut writer,
                    SlruKind::MultiXactOffsets,
                    segno,
                    rpageno,
                    ZERO_PAGE.clone(),
                )?;
            } else if info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE {
                let pageno = buf.get_u32_le();
                let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                self.put_slru_page_image(
                    &mut writer,
                    SlruKind::MultiXactMembers,
                    segno,
                    rpageno,
                    ZERO_PAGE.clone(),
                )?;
            } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
                let xlrec = XlMultiXactCreate::decode(&mut buf);
                self.ingest_multixact_create_record(&mut writer, &xlrec)?;
            } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
                let xlrec = XlMultiXactTruncate::decode(&mut buf);
                self.ingest_multixact_truncate_record(&mut writer, &xlrec)?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_RELMAP_ID {
            let xlrec = XlRelmapUpdate::decode(&mut buf);
            self.ingest_relmap_page(&mut writer, &xlrec, &decoded)?;
        } else if decoded.xl_rmid == pg_constants::RM_XLOG_ID {
            let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            if info == pg_constants::XLOG_NEXTOID {
                let next_oid = buf.get_u32_le();
                if self.checkpoint.nextOid != next_oid {
                    self.checkpoint.nextOid = next_oid;
                    self.checkpoint_modified = true;
                }
            } else if info == pg_constants::XLOG_CHECKPOINT_ONLINE
                || info == pg_constants::XLOG_CHECKPOINT_SHUTDOWN
            {
                let mut checkpoint_bytes = [0u8; SIZEOF_CHECKPOINT];
                buf.copy_to_slice(&mut checkpoint_bytes);
                let xlog_checkpoint = CheckPoint::decode(&checkpoint_bytes).unwrap();
                trace!(
                    "xlog_checkpoint.oldestXid={}, checkpoint.oldestXid={}",
                    xlog_checkpoint.oldestXid,
                    self.checkpoint.oldestXid
                );
                if (self
                    .checkpoint
                    .oldestXid
                    .wrapping_sub(xlog_checkpoint.oldestXid) as i32)
                    < 0
                {
                    self.checkpoint.oldestXid = xlog_checkpoint.oldestXid;
                    self.checkpoint_modified = true;
                }
            }
        }

        // Iterate through all the blocks that the record modifies, and
        // "put" a separate copy of the record for each block.
        for blk in decoded.blocks.iter() {

            let lsn_hex = {
                use bytes::BufMut;
                let mut bytes = BytesMut::new();
                bytes.put_u64(lsn.0);
                hex::encode(bytes.freeze())
            };
            let page_hex = {
                let foo: DecodedBkpBlock;
                use bytes::BufMut;
                let mut page = BytesMut::new();
                page.put_u32(blk.rnode_spcnode);
                page.put_u32(blk.rnode_dbnode);
                page.put_u32(blk.rnode_relnode);
                page.put_u8(blk.forknum);
                page.put_u32(blk.blkno);
                hex::encode(page.freeze())
            };
            println!("wal-at-lsn-modified-page {} {} {}", lsn_hex, page_hex, recdata_len);

            self.ingest_decoded_block(&mut writer, lsn, &decoded, blk)?;
        }

        // Emit wal entry metadata, if configured to do so
        crate::wal_metadata::write(WalEntryMetadata {
            lsn,
            size: recdata_len,
            affected_pages: decoded.blocks.iter().map(|blk| blk.into()).collect()
        });

        // If checkpoint data was updated, store the new version in the repository
        if self.checkpoint_modified {
            let new_checkpoint_bytes = self.checkpoint.encode();

            writer.put_checkpoint(new_checkpoint_bytes)?;
            self.checkpoint_modified = false;
        }

        // Now that this record has been fully handled, including updating the
        // checkpoint data, let the repository know that it is up-to-date to this LSN
        writer.finish()?;

        Ok(())
    }

    fn ingest_decoded_block(
        &mut self,
        timeline: &mut DatadirTimelineWriter<R>,
        lsn: Lsn,
        decoded: &DecodedWALRecord,
        blk: &DecodedBkpBlock,
    ) -> Result<()> {
        let rel = RelTag {
            spcnode: blk.rnode_spcnode,
            dbnode: blk.rnode_dbnode,
            relnode: blk.rnode_relnode,
            forknum: blk.forknum as u8,
        };

        //
        // Instead of storing full-page-image WAL record,
        // it is better to store extracted image: we can skip wal-redo
        // in this case. Also some FPI records may contain multiple (up to 32) pages,
        // so them have to be copied multiple times.
        //
        if blk.apply_image
            && blk.has_image
            && decoded.xl_rmid == pg_constants::RM_XLOG_ID
            && (decoded.xl_info == pg_constants::XLOG_FPI
                || decoded.xl_info == pg_constants::XLOG_FPI_FOR_HINT)
        // compression of WAL is not yet supported: fall back to storing the original WAL record
            && (blk.bimg_info & pg_constants::BKPIMAGE_IS_COMPRESSED) == 0
        {
            // Extract page image from FPI record
            let img_len = blk.bimg_len as usize;
            let img_offs = blk.bimg_offset as usize;
            let mut image = BytesMut::with_capacity(pg_constants::BLCKSZ as usize);
            image.extend_from_slice(&decoded.record[img_offs..img_offs + img_len]);

            if blk.hole_length != 0 {
                let tail = image.split_off(blk.hole_offset as usize);
                image.resize(image.len() + blk.hole_length as usize, 0u8);
                image.unsplit(tail);
            }
            image[0..4].copy_from_slice(&((lsn.0 >> 32) as u32).to_le_bytes());
            image[4..8].copy_from_slice(&(lsn.0 as u32).to_le_bytes());
            assert_eq!(image.len(), pg_constants::BLCKSZ as usize);
            self.put_rel_page_image(timeline, rel, blk.blkno, image.freeze())?;
        } else {
            let rec = ZenithWalRecord::Postgres {
                will_init: blk.will_init || blk.apply_image,
                rec: decoded.record.clone(),
            };
            self.put_rel_wal_record(timeline, rel, blk.blkno, rec)?;
        }
        Ok(())
    }

    fn ingest_heapam_record(
        &mut self,
        buf: &mut Bytes,
        timeline: &mut DatadirTimelineWriter<R>,
        decoded: &mut DecodedWALRecord,
    ) -> Result<()> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
            let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            if info == pg_constants::XLOG_HEAP_INSERT {
                let xlrec = XlHeapInsert::decode(buf);
                assert_eq!(0, buf.remaining());
                if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                    new_heap_blkno = Some(decoded.blocks[0].blkno);
                }
            } else if info == pg_constants::XLOG_HEAP_DELETE {
                let xlrec = XlHeapDelete::decode(buf);
                assert_eq!(0, buf.remaining());
                if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                    new_heap_blkno = Some(decoded.blocks[0].blkno);
                }
            } else if info == pg_constants::XLOG_HEAP_UPDATE
                || info == pg_constants::XLOG_HEAP_HOT_UPDATE
            {
                let xlrec = XlHeapUpdate::decode(buf);
                // the size of tuple data is inferred from the size of the record.
                // we can't validate the remaining number of bytes without parsing
                // the tuple data.
                if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                    old_heap_blkno = Some(decoded.blocks[0].blkno);
                }
                if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                    // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                    // non-HOT update where the new tuple goes to different page than
                    // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                    // set.
                    new_heap_blkno = Some(decoded.blocks[1].blkno);
                }
            }
        } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
            let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                let xlrec = XlHeapMultiInsert::decode(buf);

                let offset_array_len = if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                    // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                    0
                } else {
                    std::mem::size_of::<u16>() * xlrec.ntuples as usize
                };
                assert_eq!(offset_array_len, buf.remaining());

                if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                    new_heap_blkno = Some(decoded.blocks[0].blkno);
                }
            }
        }
        // FIXME: What about XLOG_HEAP_LOCK and XLOG_HEAP2_LOCK_UPDATED?

        // Clear the VM bits if required.
        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            let mut new_vm_blk = new_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);
            let mut old_vm_blk = old_heap_blkno.map(pg_constants::HEAPBLK_TO_MAPBLOCK);

            // Sometimes, Postgres seems to create heap WAL records with the
            // ALL_VISIBLE_CLEARED flag set, even though the bit in the VM page is
            // not set. In fact, it's possible that the VM page does not exist at all.
            // In that case, we don't want to store a record to clear the VM bit;
            // replaying it would fail to find the previous image of the page, because
            // it doesn't exist. So check if the VM page(s) exist, and skip the WAL
            // record if it doesn't.
            let vm_size = self.get_relsize(vm_rel)?;
            if let Some(blknum) = new_vm_blk {
                if blknum >= vm_size {
                    new_vm_blk = None;
                }
            }
            if let Some(blknum) = old_vm_blk {
                if blknum >= vm_size {
                    old_vm_blk = None;
                }
            }

            if new_vm_blk.is_some() || old_vm_blk.is_some() {
                if new_vm_blk == old_vm_blk {
                    // An UPDATE record that needs to clear the bits for both old and the
                    // new page, both of which reside on the same VM page.
                    self.put_rel_wal_record(
                        timeline,
                        vm_rel,
                        new_vm_blk.unwrap(),
                        ZenithWalRecord::ClearVisibilityMapFlags {
                            new_heap_blkno,
                            old_heap_blkno,
                            flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                        },
                    )?;
                } else {
                    // Clear VM bits for one heap page, or for two pages that reside on
                    // different VM pages.
                    if let Some(new_vm_blk) = new_vm_blk {
                        self.put_rel_wal_record(
                            timeline,
                            vm_rel,
                            new_vm_blk,
                            ZenithWalRecord::ClearVisibilityMapFlags {
                                new_heap_blkno,
                                old_heap_blkno: None,
                                flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                            },
                        )?;
                    }
                    if let Some(old_vm_blk) = old_vm_blk {
                        self.put_rel_wal_record(
                            timeline,
                            vm_rel,
                            old_vm_blk,
                            ZenithWalRecord::ClearVisibilityMapFlags {
                                new_heap_blkno: None,
                                old_heap_blkno,
                                flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                            },
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_DBASE_CREATE record.
    fn ingest_xlog_dbase_create(
        &mut self,
        timeline: &mut DatadirTimelineWriter<R>,
        rec: &XlCreateDatabase,
    ) -> Result<()> {
        let db_id = rec.db_id;
        let tablespace_id = rec.tablespace_id;
        let src_db_id = rec.src_db_id;
        let src_tablespace_id = rec.src_tablespace_id;

        // Creating a database is implemented by copying the template (aka. source) database.
        // To copy all the relations, we need to ask for the state as of the same LSN, but we
        // cannot pass 'lsn' to the Timeline.get_* functions, or they will block waiting for
        // the last valid LSN to advance up to it. So we use the previous record's LSN in the
        // get calls instead.
        let req_lsn = timeline.get_last_record_lsn();

        let rels = timeline.list_rels(src_tablespace_id, src_db_id, req_lsn)?;

        debug!("ingest_xlog_dbase_create: {} rels", rels.len());

        // Copy relfilemap
        let filemap = timeline.get_relmap_file(src_tablespace_id, src_db_id, req_lsn)?;
        timeline.put_relmap_file(tablespace_id, db_id, filemap)?;

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

            timeline.put_rel_creation(dst_rel, nblocks)?;

            // Copy content
            debug!("copying rel {} to {}, {} blocks", src_rel, dst_rel, nblocks);
            for blknum in 0..nblocks {
                debug!("copying block {} from {} to {}", blknum, src_rel, dst_rel);

                let content = timeline.get_rel_page_at_lsn(src_rel, blknum, req_lsn)?;
                timeline.put_rel_page_image(dst_rel, blknum, content)?;
                num_blocks_copied += 1;
            }

            num_rels_copied += 1;
        }

        info!(
            "Created database {}/{}, copied {} blocks in {} rels",
            tablespace_id, db_id, num_blocks_copied, num_rels_copied
        );
        Ok(())
    }

    fn ingest_xlog_smgr_create(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rec: &XlSmgrCreate,
    ) -> Result<()> {
        let rel = RelTag {
            spcnode: rec.rnode.spcnode,
            dbnode: rec.rnode.dbnode,
            relnode: rec.rnode.relnode,
            forknum: rec.forknum,
        };
        self.put_rel_creation(writer, rel)?;
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_SMGR_TRUNCATE record.
    ///
    /// This is the same logic as in PostgreSQL's smgr_redo() function.
    fn ingest_xlog_smgr_truncate(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rec: &XlSmgrTruncate,
    ) -> Result<()> {
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
            self.put_rel_truncation(writer, rel, rec.blkno)?;
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
            self.put_rel_truncation(writer, rel, num_fsm_blocks)?;
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
            self.put_rel_truncation(writer, rel, num_vm_blocks)?;
        }
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_XACT_* records.
    ///
    fn ingest_xact_record(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        parsed: &XlXactParsedRecord,
        is_commit: bool,
    ) -> Result<()> {
        // Record update of CLOG pages
        let mut pageno = parsed.xid / pg_constants::CLOG_XACTS_PER_PAGE;
        let mut segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let mut rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
        let mut page_xids: Vec<TransactionId> = vec![parsed.xid];

        for subxact in &parsed.subxacts {
            let subxact_pageno = subxact / pg_constants::CLOG_XACTS_PER_PAGE;
            if subxact_pageno != pageno {
                // This subxact goes to different page. Write the record
                // for all the XIDs on the previous page, and continue
                // accumulating XIDs on this new page.
                writer.put_slru_wal_record(
                    SlruKind::Clog,
                    segno,
                    rpageno,
                    if is_commit {
                        ZenithWalRecord::ClogSetCommitted { xids: page_xids }
                    } else {
                        ZenithWalRecord::ClogSetAborted { xids: page_xids }
                    },
                )?;
                page_xids = Vec::new();
            }
            pageno = subxact_pageno;
            segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
            page_xids.push(*subxact);
        }
        writer.put_slru_wal_record(
            SlruKind::Clog,
            segno,
            rpageno,
            if is_commit {
                ZenithWalRecord::ClogSetCommitted { xids: page_xids }
            } else {
                ZenithWalRecord::ClogSetAborted { xids: page_xids }
            },
        )?;

        for xnode in &parsed.xnodes {
            for forknum in pg_constants::MAIN_FORKNUM..=pg_constants::VISIBILITYMAP_FORKNUM {
                let rel = RelTag {
                    forknum,
                    spcnode: xnode.spcnode,
                    dbnode: xnode.dbnode,
                    relnode: xnode.relnode,
                };
                let last_lsn = self.timeline.get_last_record_lsn();
                if writer.get_rel_exists(rel, last_lsn)? {
                    self.put_rel_drop(writer, rel)?;
                }
            }
        }
        Ok(())
    }

    fn ingest_clog_truncate_record(
        &mut self,
        timeline: &mut DatadirTimelineWriter<R>,
        xlrec: &XlClogTruncate,
    ) -> Result<()> {
        info!(
            "RM_CLOG_ID truncate pageno {} oldestXid {} oldestXidDB {}",
            xlrec.pageno, xlrec.oldest_xid, xlrec.oldest_xid_db
        );

        // Here we treat oldestXid and oldestXidDB
        // differently from postgres redo routines.
        // In postgres checkpoint.oldestXid lags behind xlrec.oldest_xid
        // until checkpoint happens and updates the value.
        // Here we can use the most recent value.
        // It's just an optimization, though and can be deleted.
        // TODO Figure out if there will be any issues with replica.
        self.checkpoint.oldestXid = xlrec.oldest_xid;
        self.checkpoint.oldestXidDB = xlrec.oldest_xid_db;
        self.checkpoint_modified = true;

        // TODO Treat AdvanceOldestClogXid() or write a comment why we don't need it

        let latest_page_number =
            self.checkpoint.nextXid.value as u32 / pg_constants::CLOG_XACTS_PER_PAGE;

        // Now delete all segments containing pages between xlrec.pageno
        // and latest_page_number.

        // First, make an important safety check:
        // the current endpoint page must not be eligible for removal.
        // See SimpleLruTruncate() in slru.c
        if clogpage_precedes(latest_page_number, xlrec.pageno) {
            info!("could not truncate directory pg_xact apparent wraparound");
            return Ok(());
        }

        // Iterate via SLRU CLOG segments and drop segments that we're ready to truncate
        //
        // We cannot pass 'lsn' to the Timeline.list_nonrels(), or it
        // will block waiting for the last valid LSN to advance up to
        // it. So we use the previous record's LSN in the get calls
        // instead.
        let req_lsn = timeline.get_last_record_lsn();
        for segno in timeline.list_slru_segments(SlruKind::Clog, req_lsn)? {
            let segpage = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;
            if slru_may_delete_clogsegment(segpage, xlrec.pageno) {
                timeline.drop_slru_segment(SlruKind::Clog, segno)?;
                trace!("Drop CLOG segment {:>04X}", segno);
            }
        }

        Ok(())
    }

    fn ingest_multixact_create_record(
        &mut self,
        timeline: &mut DatadirTimelineWriter<R>,
        xlrec: &XlMultiXactCreate,
    ) -> Result<()> {
        // Create WAL record for updating the multixact-offsets page
        let pageno = xlrec.mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

        timeline.put_slru_wal_record(
            SlruKind::MultiXactOffsets,
            segno,
            rpageno,
            ZenithWalRecord::MultixactOffsetCreate {
                mid: xlrec.mid,
                moff: xlrec.moff,
            },
        )?;

        // Create WAL records for the update of each affected multixact-members page
        let mut members = xlrec.members.iter();
        let mut offset = xlrec.moff;
        loop {
            let pageno = offset / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

            // How many members fit on this page?
            let page_remain = pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32
                - offset % pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;

            let mut this_page_members: Vec<MultiXactMember> = Vec::new();
            for _ in 0..page_remain {
                if let Some(m) = members.next() {
                    this_page_members.push(m.clone());
                } else {
                    break;
                }
            }
            if this_page_members.is_empty() {
                // all done
                break;
            }
            let n_this_page = this_page_members.len();

            timeline.put_slru_wal_record(
                SlruKind::MultiXactMembers,
                pageno / pg_constants::SLRU_PAGES_PER_SEGMENT,
                pageno % pg_constants::SLRU_PAGES_PER_SEGMENT,
                ZenithWalRecord::MultixactMembersCreate {
                    moff: offset,
                    members: this_page_members,
                },
            )?;

            // Note: The multixact members can wrap around, even within one WAL record.
            offset = offset.wrapping_add(n_this_page as u32);
        }
        if xlrec.mid >= self.checkpoint.nextMulti {
            self.checkpoint.nextMulti = xlrec.mid + 1;
            self.checkpoint_modified = true;
        }
        if xlrec.moff + xlrec.nmembers > self.checkpoint.nextMultiOffset {
            self.checkpoint.nextMultiOffset = xlrec.moff + xlrec.nmembers;
            self.checkpoint_modified = true;
        }
        let max_mbr_xid = xlrec.members.iter().fold(0u32, |acc, mbr| {
            if mbr.xid.wrapping_sub(acc) as i32 > 0 {
                mbr.xid
            } else {
                acc
            }
        });

        if self.checkpoint.update_next_xid(max_mbr_xid) {
            self.checkpoint_modified = true;
        }
        Ok(())
    }

    fn ingest_multixact_truncate_record(
        &mut self,
        timeline: &mut DatadirTimelineWriter<R>,
        xlrec: &XlMultiXactTruncate,
    ) -> Result<()> {
        self.checkpoint.oldestMulti = xlrec.end_trunc_off;
        self.checkpoint.oldestMultiDB = xlrec.oldest_multi_db;
        self.checkpoint_modified = true;

        // PerformMembersTruncation
        let maxsegment: i32 = mx_offset_to_member_segment(pg_constants::MAX_MULTIXACT_OFFSET);
        let startsegment: i32 = mx_offset_to_member_segment(xlrec.start_trunc_memb);
        let endsegment: i32 = mx_offset_to_member_segment(xlrec.end_trunc_memb);
        let mut segment: i32 = startsegment;

        // Delete all the segments except the last one. The last segment can still
        // contain, possibly partially, valid data.
        while segment != endsegment {
            timeline.drop_slru_segment(SlruKind::MultiXactMembers, segment as u32)?;

            /* move to next segment, handling wraparound correctly */
            if segment == maxsegment {
                segment = 0;
            } else {
                segment += 1;
            }
        }

        // Truncate offsets
        // FIXME: this did not handle wraparound correctly

        Ok(())
    }

    fn ingest_relmap_page(
        &mut self,
        timeline: &mut DatadirTimelineWriter<R>,
        xlrec: &XlRelmapUpdate,
        decoded: &DecodedWALRecord,
    ) -> Result<()> {
        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);
        // skip xl_relmap_update
        buf.advance(12);

        timeline.put_relmap_file(xlrec.tsid, xlrec.dbid, Bytes::copy_from_slice(&buf[..]))?;

        Ok(())
    }

    fn put_rel_creation(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rel: RelTag,
    ) -> Result<()> {
        self.relsize_cache.insert(rel, 0);
        writer.put_rel_creation(rel, 0)?;
        Ok(())
    }

    fn put_rel_page_image(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rel: RelTag,
        blknum: BlockNumber,
        img: Bytes,
    ) -> Result<()> {
        self.handle_rel_extend(writer, rel, blknum)?;
        writer.put_rel_page_image(rel, blknum, img)?;
        Ok(())
    }

    fn put_rel_wal_record(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rel: RelTag,
        blknum: BlockNumber,
        rec: ZenithWalRecord,
    ) -> Result<()> {
        self.handle_rel_extend(writer, rel, blknum)?;
        writer.put_rel_wal_record(rel, blknum, rec)?;
        Ok(())
    }

    fn put_rel_truncation(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rel: RelTag,
        nblocks: BlockNumber,
    ) -> Result<()> {
        writer.put_rel_truncation(rel, nblocks)?;
        self.relsize_cache.insert(rel, nblocks);
        Ok(())
    }

    fn put_rel_drop(&mut self, writer: &mut DatadirTimelineWriter<R>, rel: RelTag) -> Result<()> {
        writer.put_rel_drop(rel)?;
        self.relsize_cache.remove(&rel);
        Ok(())
    }

    fn get_relsize(&mut self, rel: RelTag) -> Result<BlockNumber> {
        if let Some(nblocks) = self.relsize_cache.get(&rel) {
            Ok(*nblocks)
        } else {
            let last_lsn = self.timeline.get_last_record_lsn();
            let nblocks = if !self.timeline.get_rel_exists(rel, last_lsn)? {
                0
            } else {
                self.timeline.get_rel_size(rel, last_lsn)?
            };
            self.relsize_cache.insert(rel, nblocks);
            Ok(nblocks)
        }
    }

    fn handle_rel_extend(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        rel: RelTag,
        blknum: BlockNumber,
    ) -> Result<()> {
        let new_nblocks = blknum + 1;
        let old_nblocks = if let Some(nblocks) = self.relsize_cache.get(&rel) {
            *nblocks
        } else {
            // Check if the relation exists. We implicitly create relations on first
            // record.
            // TODO: would be nice if to be more explicit about it
            let last_lsn = self.timeline.get_last_record_lsn();
            let nblocks = if !self.timeline.get_rel_exists(rel, last_lsn)? {
                // create it with 0 size initially, the logic below will extend it
                writer.put_rel_creation(rel, 0)?;
                0
            } else {
                self.timeline.get_rel_size(rel, last_lsn)?
            };
            self.relsize_cache.insert(rel, nblocks);
            nblocks
        };

        if new_nblocks > old_nblocks {
            //info!("extending {} {} to {}", rel, old_nblocks, new_nblocks);
            writer.put_rel_extend(rel, new_nblocks)?;

            // fill the gap with zeros
            for gap_blknum in old_nblocks..blknum {
                writer.put_rel_page_image(rel, gap_blknum, ZERO_PAGE.clone())?;
            }
            self.relsize_cache.insert(rel, new_nblocks);
        }
        Ok(())
    }

    fn put_slru_page_image(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
        img: Bytes,
    ) -> Result<()> {
        self.handle_slru_extend(writer, kind, segno, blknum)?;
        writer.put_slru_page_image(kind, segno, blknum, img)?;
        Ok(())
    }

    fn handle_slru_extend(
        &mut self,
        writer: &mut DatadirTimelineWriter<R>,
        kind: SlruKind,
        segno: u32,
        blknum: BlockNumber,
    ) -> Result<()> {
        // we don't use a cache for this like we do for relations. SLRUS are explcitly
        // extended with ZEROPAGE records, not with commit records, so it happens
        // a lot less frequently.

        let new_nblocks = blknum + 1;
        // Check if the relation exists. We implicitly create relations on first
        // record.
        // TODO: would be nice if to be more explicit about it
        let last_lsn = self.timeline.get_last_record_lsn();
        let old_nblocks = if !self
            .timeline
            .get_slru_segment_exists(kind, segno, last_lsn)?
        {
            // create it with 0 size initially, the logic below will extend it
            writer.put_slru_segment_creation(kind, segno, 0)?;
            0
        } else {
            self.timeline.get_slru_segment_size(kind, segno, last_lsn)?
        };

        if new_nblocks > old_nblocks {
            trace!(
                "extending SLRU {:?} seg {} from {} to {} blocks",
                kind,
                segno,
                old_nblocks,
                new_nblocks
            );
            writer.put_slru_extend(kind, segno, new_nblocks)?;

            // fill the gap with zeros
            for gap_blknum in old_nblocks..blknum {
                writer.put_slru_page_image(kind, segno, gap_blknum, ZERO_PAGE.clone())?;
            }
        }
        Ok(())
    }
}

///
/// Tests that should work the same with any Repository/Timeline implementation.
///
#[allow(clippy::bool_assert_comparison)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pgdatadir_mapping::create_test_timeline;
    use crate::repository::repo_harness::*;
    use postgres_ffi::pg_constants;

    /// Arbitrary relation tag, for testing.
    const TESTREL_A: RelTag = RelTag {
        spcnode: 0,
        dbnode: 111,
        relnode: 1000,
        forknum: 0,
    };

    fn assert_current_logical_size<R: Repository>(_timeline: &DatadirTimeline<R>, _lsn: Lsn) {
        // TODO
    }

    static ZERO_CHECKPOINT: Bytes = Bytes::from_static(&[0u8; SIZEOF_CHECKPOINT]);

    fn init_walingest_test<R: Repository>(tline: &DatadirTimeline<R>) -> Result<WalIngest<R>> {
        let mut writer = tline.begin_record(Lsn(0x10));
        writer.put_checkpoint(ZERO_CHECKPOINT.clone())?;
        writer.put_relmap_file(0, 111, Bytes::from(""))?; // dummy relmapper file
        writer.finish()?;
        let walingest = WalIngest::new(tline, Lsn(0x10))?;

        Ok(walingest)
    }

    #[test]
    fn test_relsize() -> Result<()> {
        let repo = RepoHarness::create("test_relsize")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;
        let mut walingest = init_walingest_test(&tline)?;

        let mut writer = tline.begin_record(Lsn(0x20));
        walingest.put_rel_creation(&mut writer, TESTREL_A)?;
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 0, TEST_IMG("foo blk 0 at 2"))?;
        writer.finish()?;
        let mut writer = tline.begin_record(Lsn(0x30));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 0, TEST_IMG("foo blk 0 at 3"))?;
        writer.finish()?;
        let mut writer = tline.begin_record(Lsn(0x40));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 1, TEST_IMG("foo blk 1 at 4"))?;
        writer.finish()?;
        let mut writer = tline.begin_record(Lsn(0x50));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 2, TEST_IMG("foo blk 2 at 5"))?;
        writer.finish()?;

        assert_current_logical_size(&tline, Lsn(0x50));

        // The relation was created at LSN 2, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x10))?, false);

        // FIXME: should error out?
        //assert!(tline.get_rel_size(TESTREL_A, Lsn(0x10))?.is_none());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x20))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x20))?, 1);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x50))?, 3);

        // Check page contents at each LSN
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 0, Lsn(0x20))?,
            TEST_IMG("foo blk 0 at 2")
        );

        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 0, Lsn(0x30))?,
            TEST_IMG("foo blk 0 at 3")
        );

        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 0, Lsn(0x40))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x40))?,
            TEST_IMG("foo blk 1 at 4")
        );

        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 0, Lsn(0x50))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x50))?,
            TEST_IMG("foo blk 1 at 4")
        );
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 2, Lsn(0x50))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate last block
        let mut writer = tline.begin_record(Lsn(0x60));
        walingest.put_rel_truncation(&mut writer, TESTREL_A, 2)?;
        writer.finish()?;
        assert_current_logical_size(&tline, Lsn(0x60));

        // Check reported size and contents after truncation
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x60))?, 2);
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 0, Lsn(0x60))?,
            TEST_IMG("foo blk 0 at 3")
        );
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x60))?,
            TEST_IMG("foo blk 1 at 4")
        );

        // should still see the truncated block with older LSN
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x50))?, 3);
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 2, Lsn(0x50))?,
            TEST_IMG("foo blk 2 at 5")
        );

        // Truncate to zero length
        let mut writer = tline.begin_record(Lsn(0x68));
        walingest.put_rel_truncation(&mut writer, TESTREL_A, 0)?;
        writer.finish()?;
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x68))?, 0);

        // Extend from 0 to 2 blocks, leaving a gap
        let mut writer = tline.begin_record(Lsn(0x70));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 1, TEST_IMG("foo blk 1"))?;
        writer.finish()?;
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x70))?, 2);
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 0, Lsn(0x70))?,
            ZERO_PAGE
        );
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 1, Lsn(0x70))?,
            TEST_IMG("foo blk 1")
        );

        // Extend a lot more, leaving a big gap that spans across segments
        let mut writer = tline.begin_record(Lsn(0x80));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 1500, TEST_IMG("foo blk 1500"))?;
        writer.finish()?;
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x80))?, 1501);
        for blk in 2..1500 {
            assert_eq!(
                tline.get_rel_page_at_lsn(TESTREL_A, blk, Lsn(0x80))?,
                ZERO_PAGE
            );
        }
        assert_eq!(
            tline.get_rel_page_at_lsn(TESTREL_A, 1500, Lsn(0x80))?,
            TEST_IMG("foo blk 1500")
        );

        Ok(())
    }

    // Test what happens if we dropped a relation
    // and then created it again within the same layer.
    #[test]
    fn test_drop_extend() -> Result<()> {
        let repo = RepoHarness::create("test_drop_extend")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;
        let mut walingest = init_walingest_test(&tline)?;

        let mut writer = tline.begin_record(Lsn(0x20));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 0, TEST_IMG("foo blk 0 at 2"))?;
        writer.finish()?;

        // Check that rel exists and size is correct
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x20))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x20))?, 1);

        // Drop rel
        let mut writer = tline.begin_record(Lsn(0x30));
        walingest.put_rel_drop(&mut writer, TESTREL_A)?;
        writer.finish()?;

        // Check that rel is not visible anymore
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x30))?, false);

        // FIXME: should fail
        //assert!(tline.get_rel_size(TESTREL_A, Lsn(0x30))?.is_none());

        // Re-create it
        let mut writer = tline.begin_record(Lsn(0x40));
        walingest.put_rel_page_image(&mut writer, TESTREL_A, 0, TEST_IMG("foo blk 0 at 4"))?;
        writer.finish()?;

        // Check that rel exists and size is correct
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x40))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x40))?, 1);

        Ok(())
    }

    // Test what happens if we truncated a relation
    // so that one of its segments was dropped
    // and then extended it again within the same layer.
    #[test]
    fn test_truncate_extend() -> Result<()> {
        let repo = RepoHarness::create("test_truncate_extend")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;
        let mut walingest = init_walingest_test(&tline)?;

        //from storage_layer.rs
        const RELISH_SEG_SIZE: u32 = 10 * 1024 * 1024 / 8192;
        let relsize = RELISH_SEG_SIZE * 2;

        // Create relation with relsize blocks
        let mut writer = tline.begin_record(Lsn(0x20));
        for blkno in 0..relsize {
            let data = format!("foo blk {} at {}", blkno, Lsn(0x20));
            walingest.put_rel_page_image(&mut writer, TESTREL_A, blkno, TEST_IMG(&data))?;
        }
        writer.finish()?;

        // The relation was created at LSN 20, not visible at LSN 1 yet.
        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x10))?, false);

        // FIXME: should fail
        // assert!(tline.get_rel_size(TESTREL_A, Lsn(0x10))?.is_none());

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x20))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x20))?, relsize);

        // Check relation content
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_rel_page_at_lsn(TESTREL_A, blkno, lsn)?,
                TEST_IMG(&data)
            );
        }

        // Truncate relation so that second segment was dropped
        // - only leave one page
        let mut writer = tline.begin_record(Lsn(0x60));
        walingest.put_rel_truncation(&mut writer, TESTREL_A, 1)?;
        writer.finish()?;

        // Check reported size and contents after truncation
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x60))?, 1);

        for blkno in 0..1 {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_rel_page_at_lsn(TESTREL_A, blkno, Lsn(0x60))?,
                TEST_IMG(&data)
            );
        }

        // should still see all blocks with older LSN
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x50))?, relsize);
        for blkno in 0..relsize {
            let lsn = Lsn(0x20);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_rel_page_at_lsn(TESTREL_A, blkno, Lsn(0x50))?,
                TEST_IMG(&data)
            );
        }

        // Extend relation again.
        // Add enough blocks to create second segment
        let lsn = Lsn(0x80);
        let mut writer = tline.begin_record(lsn);
        for blkno in 0..relsize {
            let data = format!("foo blk {} at {}", blkno, lsn);
            walingest.put_rel_page_image(&mut writer, TESTREL_A, blkno, TEST_IMG(&data))?;
        }
        writer.finish()?;

        assert_eq!(tline.get_rel_exists(TESTREL_A, Lsn(0x80))?, true);
        assert_eq!(tline.get_rel_size(TESTREL_A, Lsn(0x80))?, relsize);
        // Check relation content
        for blkno in 0..relsize {
            let lsn = Lsn(0x80);
            let data = format!("foo blk {} at {}", blkno, lsn);
            assert_eq!(
                tline.get_rel_page_at_lsn(TESTREL_A, blkno, Lsn(0x80))?,
                TEST_IMG(&data)
            );
        }

        Ok(())
    }

    /// Test get_relsize() and truncation with a file larger than 1 GB, so that it's
    /// split into multiple 1 GB segments in Postgres.
    #[test]
    fn test_large_rel() -> Result<()> {
        let repo = RepoHarness::create("test_large_rel")?.load();
        let tline = create_test_timeline(repo, TIMELINE_ID)?;
        let mut walingest = init_walingest_test(&tline)?;

        let mut lsn = 0x10;
        for blknum in 0..pg_constants::RELSEG_SIZE + 1 {
            lsn += 0x10;
            let mut writer = tline.begin_record(Lsn(lsn));
            let img = TEST_IMG(&format!("foo blk {} at {}", blknum, Lsn(lsn)));
            walingest.put_rel_page_image(&mut writer, TESTREL_A, blknum as BlockNumber, img)?;
            writer.finish()?;
        }

        assert_current_logical_size(&tline, Lsn(lsn));

        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE + 1
        );

        // Truncate one block
        lsn += 0x10;
        let mut writer = tline.begin_record(Lsn(lsn));
        walingest.put_rel_truncation(&mut writer, TESTREL_A, pg_constants::RELSEG_SIZE)?;
        writer.finish()?;
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE
        );
        assert_current_logical_size(&tline, Lsn(lsn));

        // Truncate another block
        lsn += 0x10;
        let mut writer = tline.begin_record(Lsn(lsn));
        walingest.put_rel_truncation(&mut writer, TESTREL_A, pg_constants::RELSEG_SIZE - 1)?;
        writer.finish()?;
        assert_eq!(
            tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
            pg_constants::RELSEG_SIZE - 1
        );
        assert_current_logical_size(&tline, Lsn(lsn));

        // Truncate to 1500, and then truncate all the way down to 0, one block at a time
        // This tests the behavior at segment boundaries
        let mut size: i32 = 3000;
        while size >= 0 {
            lsn += 0x10;
            let mut writer = tline.begin_record(Lsn(lsn));
            walingest.put_rel_truncation(&mut writer, TESTREL_A, size as BlockNumber)?;
            writer.finish()?;
            assert_eq!(
                tline.get_rel_size(TESTREL_A, Lsn(lsn))?,
                size as BlockNumber
            );

            size -= 1;
        }
        assert_current_logical_size(&tline, Lsn(lsn));

        Ok(())
    }
}
