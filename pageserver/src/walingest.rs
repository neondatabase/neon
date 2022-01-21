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

use postgres_ffi::nonrelfile_utils::clogpage_precedes;
use postgres_ffi::nonrelfile_utils::slru_may_delete_clogsegment;
use std::cmp::min;

use anyhow::Result;
use bytes::{Buf, Bytes, BytesMut};
use tracing::*;

use crate::relish::*;
use crate::repository::*;
use crate::walrecord::*;
use postgres_ffi::nonrelfile_utils::mx_offset_to_member_segment;
use postgres_ffi::xlog_utils::*;
use postgres_ffi::TransactionId;
use postgres_ffi::{pg_constants, CheckPoint};
use zenith_utils::lsn::Lsn;

static ZERO_PAGE: Bytes = Bytes::from_static(&[0u8; 8192]);

pub struct WalIngest {
    checkpoint: CheckPoint,
    checkpoint_modified: bool,
    last_xid_page: u32,
}

impl WalIngest {
    pub fn new(timeline: &dyn Timeline, startpoint: Lsn) -> Result<WalIngest> {
        // Fetch the latest checkpoint into memory, so that we can compare with it
        // quickly in `ingest_record` and update it when it changes.
        let checkpoint_bytes = timeline.get_page_at_lsn(RelishTag::Checkpoint, 0, startpoint)?;
        let checkpoint = CheckPoint::decode(&checkpoint_bytes)?;
        trace!("CheckPoint.nextXid = {}", checkpoint.nextXid.value);

        Ok(WalIngest {
            checkpoint,
            checkpoint_modified: false,
            last_xid_page: 0,
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
        timeline: &dyn TimelineWriter,
        recdata: Bytes,
        lsn: Lsn,
    ) -> Result<()> {
        let mut decoded = decode_wal_record(recdata);
        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);

        assert!(!self.checkpoint_modified);
        if self.checkpoint.update_next_xid(decoded.xl_xid) {
            self.checkpoint_modified = true;

            let xid = self.checkpoint.nextXid.value as u32;
            let xid_page = xid / pg_constants::CLOG_XACTS_PER_PAGE;
            if xid_page > self.last_xid_page {
                let segno = xid_page / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let rpageno = xid_page % pg_constants::SLRU_PAGES_PER_SEGMENT;
                self.last_xid_page = xid_page;
                timeline.put_wal_record(
                    lsn,
                    RelishTag::Slru {
                        slru: SlruKind::Clog,
                        segno,
                    },
                    rpageno,
                    ZenithWalRecord::ClogSetInProgress { xid },
                )?;
                self.last_xid_page = xid_page;
                timeline.put_page_image(
                    RelishTag::Slru {
                        slru: SlruKind::Clog,
                        segno,
                    },
                    rpageno,
                    lsn,
                    ZERO_PAGE.clone(),
                )?;
            }
        }
        // Heap AM records need some special handling, because they modify VM pages
        // without registering them with the standard mechanism.
        if decoded.xl_rmid == pg_constants::RM_HEAP_ID
            || decoded.xl_rmid == pg_constants::RM_HEAP2_ID
        {
            self.ingest_heapam_record(&mut buf, timeline, lsn, &mut decoded)?;
        }
        // Handle other special record types
        if decoded.xl_rmid == pg_constants::RM_SMGR_ID
            && (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_SMGR_TRUNCATE
        {
            let truncate = XlSmgrTruncate::decode(&mut buf);
            self.ingest_xlog_smgr_truncate(timeline, lsn, &truncate)?;
        } else if decoded.xl_rmid == pg_constants::RM_DBASE_ID {
            if (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_DBASE_CREATE
            {
                let createdb = XlCreateDatabase::decode(&mut buf);
                self.ingest_xlog_dbase_create(timeline, lsn, &createdb)?;
            } else if (decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK)
                == pg_constants::XLOG_DBASE_DROP
            {
                let dropdb = XlDropDatabase::decode(&mut buf);

                // To drop the database, we need to drop all the relations in it. Like in
                // ingest_xlog_dbase_create(), use the previous record's LSN in the list_rels() call
                let req_lsn = min(timeline.get_last_record_lsn(), lsn);

                for tablespace_id in dropdb.tablespace_ids {
                    let rels = timeline.list_rels(tablespace_id, dropdb.db_id, req_lsn)?;
                    for rel in rels {
                        timeline.drop_relish(rel, lsn)?;
                    }
                    trace!(
                        "Drop FileNodeMap {}, {} at lsn {}",
                        tablespace_id,
                        dropdb.db_id,
                        lsn
                    );
                    timeline.drop_relish(
                        RelishTag::FileNodeMap {
                            spcnode: tablespace_id,
                            dbnode: dropdb.db_id,
                        },
                        lsn,
                    )?;
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
                timeline.put_page_image(
                    RelishTag::Slru {
                        slru: SlruKind::Clog,
                        segno,
                    },
                    rpageno,
                    lsn,
                    ZERO_PAGE.clone(),
                )?;
            } else {
                assert!(info == pg_constants::CLOG_TRUNCATE);
                let xlrec = XlClogTruncate::decode(&mut buf);
                self.ingest_clog_truncate_record(timeline, lsn, &xlrec)?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_XACT_ID {
            let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;
            if info == pg_constants::XLOG_XACT_COMMIT || info == pg_constants::XLOG_XACT_ABORT {
                let parsed_xact =
                    XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
                self.ingest_xact_record(
                    timeline,
                    lsn,
                    &parsed_xact,
                    info == pg_constants::XLOG_XACT_COMMIT,
                )?;
            } else if info == pg_constants::XLOG_XACT_COMMIT_PREPARED
                || info == pg_constants::XLOG_XACT_ABORT_PREPARED
            {
                let parsed_xact =
                    XlXactParsedRecord::decode(&mut buf, decoded.xl_xid, decoded.xl_info);
                self.ingest_xact_record(
                    timeline,
                    lsn,
                    &parsed_xact,
                    info == pg_constants::XLOG_XACT_COMMIT_PREPARED,
                )?;
                // Remove twophase file. see RemoveTwoPhaseFile() in postgres code
                trace!(
                    "Drop twophaseFile for xid {} parsed_xact.xid {} here at {}",
                    decoded.xl_xid,
                    parsed_xact.xid,
                    lsn
                );
                timeline.drop_relish(
                    RelishTag::TwoPhase {
                        xid: parsed_xact.xid,
                    },
                    lsn,
                )?;
            } else if info == pg_constants::XLOG_XACT_PREPARE {
                timeline.put_page_image(
                    RelishTag::TwoPhase {
                        xid: decoded.xl_xid,
                    },
                    0,
                    lsn,
                    Bytes::copy_from_slice(&buf[..]),
                )?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_MULTIXACT_ID {
            let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

            if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE {
                let pageno = buf.get_u32_le();
                let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                timeline.put_page_image(
                    RelishTag::Slru {
                        slru: SlruKind::MultiXactOffsets,
                        segno,
                    },
                    rpageno,
                    lsn,
                    ZERO_PAGE.clone(),
                )?;
            } else if info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE {
                let pageno = buf.get_u32_le();
                let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                timeline.put_page_image(
                    RelishTag::Slru {
                        slru: SlruKind::MultiXactMembers,
                        segno,
                    },
                    rpageno,
                    lsn,
                    ZERO_PAGE.clone(),
                )?;
            } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
                let xlrec = XlMultiXactCreate::decode(&mut buf);
                self.ingest_multixact_create_record(timeline, lsn, &xlrec)?;
            } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
                let xlrec = XlMultiXactTruncate::decode(&mut buf);
                self.ingest_multixact_truncate_record(timeline, lsn, &xlrec)?;
            }
        } else if decoded.xl_rmid == pg_constants::RM_RELMAP_ID {
            let xlrec = XlRelmapUpdate::decode(&mut buf);
            self.ingest_relmap_page(timeline, lsn, &xlrec, &decoded)?;
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
            self.ingest_decoded_block(timeline, lsn, &decoded, blk)?;
        }

        // If checkpoint data was updated, store the new version in the repository
        if self.checkpoint_modified {
            let new_checkpoint_bytes = self.checkpoint.encode();

            timeline.put_page_image(RelishTag::Checkpoint, 0, lsn, new_checkpoint_bytes)?;
            self.checkpoint_modified = false;
        }

        // Now that this record has been fully handled, including updating the
        // checkpoint data, let the repository know that it is up-to-date to this LSN
        timeline.advance_last_record_lsn(lsn);

        Ok(())
    }

    fn ingest_decoded_block(
        &mut self,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
        decoded: &DecodedWALRecord,
        blk: &DecodedBkpBlock,
    ) -> Result<()> {
        let tag = RelishTag::Relation(RelTag {
            spcnode: blk.rnode_spcnode,
            dbnode: blk.rnode_dbnode,
            relnode: blk.rnode_relnode,
            forknum: blk.forknum as u8,
        });

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
            timeline.put_page_image(tag, blk.blkno, lsn, image.freeze())?;
        } else {
            let rec = ZenithWalRecord::Postgres {
                will_init: blk.will_init || blk.apply_image,
                rec: decoded.record.clone(),
            };
            timeline.put_wal_record(lsn, tag, blk.blkno, rec)?;
        }
        Ok(())
    }

    fn ingest_heapam_record(
        &mut self,
        buf: &mut Bytes,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
        decoded: &mut DecodedWALRecord,
    ) -> Result<()> {
        // Handle VM bit updates that are implicitly part of heap records.
        if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
            let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
            if info == pg_constants::XLOG_HEAP_INSERT {
                let xlrec = XlHeapInsert::decode(buf);
                assert_eq!(0, buf.remaining());
                if (xlrec.flags
                    & (pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED
                        | pg_constants::XLH_INSERT_ALL_FROZEN_SET))
                    != 0
                {
                    timeline.put_wal_record(
                        lsn,
                        RelishTag::Relation(RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: decoded.blocks[0].rnode_spcnode,
                            dbnode: decoded.blocks[0].rnode_dbnode,
                            relnode: decoded.blocks[0].rnode_relnode,
                        }),
                        decoded.blocks[0].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                        ZenithWalRecord::ClearVisibilityMapFlags {
                            heap_blkno: decoded.blocks[0].blkno,
                            flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                        },
                    )?;
                }
            } else if info == pg_constants::XLOG_HEAP_DELETE {
                let xlrec = XlHeapDelete::decode(buf);
                assert_eq!(0, buf.remaining());
                if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                    timeline.put_wal_record(
                        lsn,
                        RelishTag::Relation(RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: decoded.blocks[0].rnode_spcnode,
                            dbnode: decoded.blocks[0].rnode_dbnode,
                            relnode: decoded.blocks[0].rnode_relnode,
                        }),
                        decoded.blocks[0].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                        ZenithWalRecord::ClearVisibilityMapFlags {
                            heap_blkno: decoded.blocks[0].blkno,
                            flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                        },
                    )?;
                }
            } else if info == pg_constants::XLOG_HEAP_UPDATE
                || info == pg_constants::XLOG_HEAP_HOT_UPDATE
            {
                let xlrec = XlHeapUpdate::decode(buf);
                // the size of tuple data is inferred from the size of the record.
                // we can't validate the remaining number of bytes without parsing
                // the tuple data.
                if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                    timeline.put_wal_record(
                        lsn,
                        RelishTag::Relation(RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: decoded.blocks[0].rnode_spcnode,
                            dbnode: decoded.blocks[0].rnode_dbnode,
                            relnode: decoded.blocks[0].rnode_relnode,
                        }),
                        decoded.blocks[0].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                        ZenithWalRecord::ClearVisibilityMapFlags {
                            heap_blkno: decoded.blocks[0].blkno,
                            flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                        },
                    )?;
                }
                if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0
                    && decoded.blocks.len() > 1
                {
                    timeline.put_wal_record(
                        lsn,
                        RelishTag::Relation(RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: decoded.blocks[1].rnode_spcnode,
                            dbnode: decoded.blocks[1].rnode_dbnode,
                            relnode: decoded.blocks[1].rnode_relnode,
                        }),
                        decoded.blocks[1].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                        ZenithWalRecord::ClearVisibilityMapFlags {
                            heap_blkno: decoded.blocks[1].blkno,
                            flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                        },
                    )?;
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

                // FIXME: why also ALL_FROZEN_SET?
                if (xlrec.flags
                    & (pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED
                        | pg_constants::XLH_INSERT_ALL_FROZEN_SET))
                    != 0
                {
                    timeline.put_wal_record(
                        lsn,
                        RelishTag::Relation(RelTag {
                            forknum: pg_constants::VISIBILITYMAP_FORKNUM,
                            spcnode: decoded.blocks[0].rnode_spcnode,
                            dbnode: decoded.blocks[0].rnode_dbnode,
                            relnode: decoded.blocks[0].rnode_relnode,
                        }),
                        decoded.blocks[0].blkno / pg_constants::HEAPBLOCKS_PER_PAGE as u32,
                        ZenithWalRecord::ClearVisibilityMapFlags {
                            heap_blkno: decoded.blocks[0].blkno,
                            flags: pg_constants::VISIBILITYMAP_VALID_BITS,
                        },
                    )?;
                }
            }
        }

        // FIXME: What about XLOG_HEAP_LOCK and XLOG_HEAP2_LOCK_UPDATED?

        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_DBASE_CREATE record.
    fn ingest_xlog_dbase_create(
        &mut self,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
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
        let req_lsn = min(timeline.get_last_record_lsn(), lsn);

        let rels = timeline.list_rels(src_tablespace_id, src_db_id, req_lsn)?;

        trace!("ingest_xlog_dbase_create: {} rels", rels.len());

        let mut num_rels_copied = 0;
        let mut num_blocks_copied = 0;
        for rel in rels {
            if let RelishTag::Relation(src_rel) = rel {
                assert_eq!(src_rel.spcnode, src_tablespace_id);
                assert_eq!(src_rel.dbnode, src_db_id);

                let nblocks = timeline.get_relish_size(rel, req_lsn)?.unwrap_or(0);
                let dst_rel = RelTag {
                    spcnode: tablespace_id,
                    dbnode: db_id,
                    relnode: src_rel.relnode,
                    forknum: src_rel.forknum,
                };

                // Copy content
                for blknum in 0..nblocks {
                    let content = timeline.get_page_at_lsn(rel, blknum, req_lsn)?;

                    debug!("copying block {} from {} to {}", blknum, src_rel, dst_rel);

                    timeline.put_page_image(RelishTag::Relation(dst_rel), blknum, lsn, content)?;
                    num_blocks_copied += 1;
                }

                if nblocks == 0 {
                    // make sure we have some trace of the relation, even if it's empty
                    timeline.put_truncation(RelishTag::Relation(dst_rel), lsn, 0)?;
                }

                num_rels_copied += 1;
            }
        }

        // Copy relfilemap
        // TODO This implementation is very inefficient -
        // it scans all non-rels only to find FileNodeMaps
        for tag in timeline.list_nonrels(req_lsn)? {
            if let RelishTag::FileNodeMap { spcnode, dbnode } = tag {
                if spcnode == src_tablespace_id && dbnode == src_db_id {
                    let img = timeline.get_page_at_lsn(tag, 0, req_lsn)?;
                    let new_tag = RelishTag::FileNodeMap {
                        spcnode: tablespace_id,
                        dbnode: db_id,
                    };
                    timeline.put_page_image(new_tag, 0, lsn, img)?;
                    break;
                }
            }
        }
        info!(
            "Created database {}/{}, copied {} blocks in {} rels at {}",
            tablespace_id, db_id, num_blocks_copied, num_rels_copied, lsn
        );
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_SMGR_TRUNCATE record.
    ///
    /// This is the same logic as in PostgreSQL's smgr_redo() function.
    fn ingest_xlog_smgr_truncate(
        &mut self,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
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
            timeline.put_truncation(RelishTag::Relation(rel), lsn, rec.blkno)?;
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
            timeline.put_truncation(RelishTag::Relation(rel), lsn, num_fsm_blocks)?;
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
            timeline.put_truncation(RelishTag::Relation(rel), lsn, num_vm_blocks)?;
        }
        Ok(())
    }

    /// Subroutine of ingest_record(), to handle an XLOG_XACT_* records.
    ///
    fn ingest_xact_record(
        &mut self,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
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
                timeline.put_wal_record(
                    lsn,
                    RelishTag::Slru {
                        slru: SlruKind::Clog,
                        segno,
                    },
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
        timeline.put_wal_record(
            lsn,
            RelishTag::Slru {
                slru: SlruKind::Clog,
                segno,
            },
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
                timeline.drop_relish(RelishTag::Relation(rel), lsn)?;
            }
        }
        Ok(())
    }

    fn ingest_clog_truncate_record(
        &mut self,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
        xlrec: &XlClogTruncate,
    ) -> Result<()> {
        info!(
            "RM_CLOG_ID truncate pageno {} oldestXid {} oldestXidDB {} lsn {}",
            xlrec.pageno, xlrec.oldest_xid, xlrec.oldest_xid_db, lsn
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
        // TODO This implementation is very inefficient -
        // it scans all non-rels only to find Clog
        //
        // We cannot pass 'lsn' to the Timeline.list_nonrels(), or it
        // will block waiting for the last valid LSN to advance up to
        // it. So we use the previous record's LSN in the get calls
        // instead.
        let req_lsn = min(timeline.get_last_record_lsn(), lsn);
        for obj in timeline.list_nonrels(req_lsn)? {
            if let RelishTag::Slru { slru, segno } = obj {
                if slru == SlruKind::Clog {
                    let segpage = segno * pg_constants::SLRU_PAGES_PER_SEGMENT;
                    if slru_may_delete_clogsegment(segpage, xlrec.pageno) {
                        timeline.drop_relish(RelishTag::Slru { slru, segno }, lsn)?;
                        trace!("Drop CLOG segment {:>04X} at lsn {}", segno, lsn);
                    }
                }
            }
        }

        Ok(())
    }

    fn ingest_multixact_create_record(
        &mut self,
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
        xlrec: &XlMultiXactCreate,
    ) -> Result<()> {
        // Create WAL record for updating the multixact-offsets page
        let pageno = xlrec.mid / pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32;
        let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
        let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

        timeline.put_wal_record(
            lsn,
            RelishTag::Slru {
                slru: SlruKind::MultiXactOffsets,
                segno,
            },
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

            timeline.put_wal_record(
                lsn,
                RelishTag::Slru {
                    slru: SlruKind::MultiXactMembers,
                    segno: pageno / pg_constants::SLRU_PAGES_PER_SEGMENT,
                },
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
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
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
            timeline.drop_relish(
                RelishTag::Slru {
                    slru: SlruKind::MultiXactMembers,
                    segno: segment as u32,
                },
                lsn,
            )?;

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
        timeline: &dyn TimelineWriter,
        lsn: Lsn,
        xlrec: &XlRelmapUpdate,
        decoded: &DecodedWALRecord,
    ) -> Result<()> {
        let tag = RelishTag::FileNodeMap {
            spcnode: xlrec.tsid,
            dbnode: xlrec.dbid,
        };

        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);
        // skip xl_relmap_update
        buf.advance(12);

        timeline.put_page_image(tag, 0, lsn, Bytes::copy_from_slice(&buf[..]))?;

        Ok(())
    }
}
