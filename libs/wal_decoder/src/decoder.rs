//! This module contains logic for decoding and interpreting
//! raw bytes which represent a raw Postgres WAL record.

use crate::models::*;
use bytes::{Buf, Bytes, BytesMut};
use pageserver_api::key::rel_block_to_key;
use pageserver_api::record::NeonWalRecord;
use pageserver_api::reltag::{RelTag, SlruKind};
use pageserver_api::shard::ShardIdentity;
use pageserver_api::value::Value;
use postgres_ffi::relfile_utils::VISIBILITYMAP_FORKNUM;
use postgres_ffi::walrecord::*;
use postgres_ffi::{page_is_new, page_set_lsn, pg_constants, BLCKSZ};
use utils::lsn::Lsn;

impl InterpretedWalRecord {
    /// Decode and interpreted raw bytes which represent one Postgres WAL record.
    /// Data blocks which do not match the provided shard identity are filtered out.
    /// Shard 0 is a special case since it tracks all relation sizes. We only give it
    /// the keys that are being written as that is enough for updating relation sizes.
    pub fn from_bytes_filtered(
        buf: Bytes,
        shard: &ShardIdentity,
        lsn: Lsn,
        pg_version: u32,
    ) -> anyhow::Result<InterpretedWalRecord> {
        let mut decoded = DecodedWALRecord::default();
        decode_wal_record(buf, &mut decoded, pg_version)?;

        let flush_uncommitted = if decoded.is_dbase_create_copy(pg_version) {
            FlushUncommittedRecords::Yes
        } else {
            FlushUncommittedRecords::No
        };

        let metadata_record = MetadataRecord::from_decoded(&decoded, lsn, pg_version)?;

        let mut blocks = Vec::default();
        for blk in decoded.blocks.iter() {
            let rel = RelTag {
                spcnode: blk.rnode_spcnode,
                dbnode: blk.rnode_dbnode,
                relnode: blk.rnode_relnode,
                forknum: blk.forknum,
            };

            let key = rel_block_to_key(rel, blk.blkno);

            if !key.is_valid_key_on_write_path() {
                anyhow::bail!("Unsupported key decoded at LSN {}: {}", lsn, key);
            }

            let key_is_local = shard.is_key_local(&key);

            tracing::debug!(
                lsn=%lsn,
                key=%key,
                "ingest: shard decision {}",
                if !key_is_local { "drop" } else { "keep" },
            );

            if !key_is_local {
                if shard.is_shard_zero() {
                    // Shard 0 tracks relation sizes.  Although we will not store this block, we will observe
                    // its blkno in case it implicitly extends a relation.
                    blocks.push((key.to_compact(), None));
                }

                continue;
            }

            // Instead of storing full-page-image WAL record,
            // it is better to store extracted image: we can skip wal-redo
            // in this case. Also some FPI records may contain multiple (up to 32) pages,
            // so them have to be copied multiple times.
            //
            let value = if blk.apply_image
                && blk.has_image
                && decoded.xl_rmid == pg_constants::RM_XLOG_ID
                && (decoded.xl_info == pg_constants::XLOG_FPI
                || decoded.xl_info == pg_constants::XLOG_FPI_FOR_HINT)
                // compression of WAL is not yet supported: fall back to storing the original WAL record
                && !postgres_ffi::bkpimage_is_compressed(blk.bimg_info, pg_version)
                // do not materialize null pages because them most likely be soon replaced with real data
                && blk.bimg_len != 0
            {
                // Extract page image from FPI record
                let img_len = blk.bimg_len as usize;
                let img_offs = blk.bimg_offset as usize;
                let mut image = BytesMut::with_capacity(BLCKSZ as usize);
                // TODO(vlad): skip the copy
                image.extend_from_slice(&decoded.record[img_offs..img_offs + img_len]);

                if blk.hole_length != 0 {
                    let tail = image.split_off(blk.hole_offset as usize);
                    image.resize(image.len() + blk.hole_length as usize, 0u8);
                    image.unsplit(tail);
                }
                //
                // Match the logic of XLogReadBufferForRedoExtended:
                // The page may be uninitialized. If so, we can't set the LSN because
                // that would corrupt the page.
                //
                if !page_is_new(&image) {
                    page_set_lsn(&mut image, lsn)
                }
                assert_eq!(image.len(), BLCKSZ as usize);

                Value::Image(image.freeze())
            } else {
                Value::WalRecord(NeonWalRecord::Postgres {
                    will_init: blk.will_init || blk.apply_image,
                    rec: decoded.record.clone(),
                })
            };

            blocks.push((key.to_compact(), Some(value)));
        }

        Ok(InterpretedWalRecord {
            metadata_record,
            blocks,
            lsn,
            flush_uncommitted,
            xid: decoded.xl_xid,
        })
    }
}

impl MetadataRecord {
    fn from_decoded(
        decoded: &DecodedWALRecord,
        lsn: Lsn,
        pg_version: u32,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Note: this doesn't actually copy the bytes since
        // the [`Bytes`] type implements it via a level of indirection.
        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);

        match decoded.xl_rmid {
            pg_constants::RM_HEAP_ID | pg_constants::RM_HEAP2_ID => {
                Self::decode_heapam_record(&mut buf, decoded, pg_version)
            }
            pg_constants::RM_NEON_ID => Self::decode_neonmgr_record(&mut buf, decoded, pg_version),
            // Handle other special record types
            pg_constants::RM_SMGR_ID => Self::decode_smgr_record(&mut buf, decoded),
            pg_constants::RM_DBASE_ID => Self::decode_dbase_record(&mut buf, decoded, pg_version),
            pg_constants::RM_TBLSPC_ID => {
                tracing::trace!("XLOG_TBLSPC_CREATE/DROP is not handled yet");
                Ok(None)
            }
            pg_constants::RM_CLOG_ID => Self::decode_clog_record(&mut buf, decoded, pg_version),
            pg_constants::RM_XACT_ID => Self::decode_xact_record(&mut buf, decoded, lsn),
            pg_constants::RM_MULTIXACT_ID => {
                Self::decode_multixact_record(&mut buf, decoded, pg_version)
            }
            pg_constants::RM_RELMAP_ID => Self::decode_relmap_record(&mut buf, decoded),
            // This is an odd duck. It needs to go to all shards.
            // Since it uses the checkpoint image (that's initialized from CHECKPOINT_KEY
            // in WalIngest::new), we have to send the whole DecodedWalRecord::record to
            // the pageserver and decode it there.
            //
            // Alternatively, one can make the checkpoint part of the subscription protocol
            // to the pageserver. This should work fine, but can be done at a later point.
            pg_constants::RM_XLOG_ID => Self::decode_xlog_record(&mut buf, decoded, lsn),
            pg_constants::RM_LOGICALMSG_ID => {
                Self::decode_logical_message_record(&mut buf, decoded)
            }
            pg_constants::RM_STANDBY_ID => Self::decode_standby_record(&mut buf, decoded),
            pg_constants::RM_REPLORIGIN_ID => Self::decode_replorigin_record(&mut buf, decoded),
            _unexpected => {
                // TODO: consider failing here instead of blindly doing something without
                // understanding the protocol
                Ok(None)
            }
        }
    }

    fn decode_heapam_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: u32,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        let mut flags = pg_constants::VISIBILITYMAP_VALID_BITS;

        match pg_version {
            14 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v14::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v14::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v14::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v14::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v14::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v14::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            15 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v15::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v15::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v15::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v15::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v15::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v15::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            16 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v16::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v16::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v16::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v16::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v16::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v16::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            17 => {
                if decoded.xl_rmid == pg_constants::RM_HEAP_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                    if info == pg_constants::XLOG_HEAP_INSERT {
                        let xlrec = v17::XlHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_DELETE {
                        let xlrec = v17::XlHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_UPDATE
                        || info == pg_constants::XLOG_HEAP_HOT_UPDATE
                    {
                        let xlrec = v17::XlHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP_LOCK {
                        let xlrec = v17::XlHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else if decoded.xl_rmid == pg_constants::RM_HEAP2_ID {
                    let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;
                    if info == pg_constants::XLOG_HEAP2_MULTI_INSERT {
                        let xlrec = v17::XlHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    } else if info == pg_constants::XLOG_HEAP2_LOCK_UPDATED {
                        let xlrec = v17::XlHeapLockUpdated::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                } else {
                    anyhow::bail!("Unknown RMGR {} for Heap decoding", decoded.xl_rmid);
                }
            }
            _ => {}
        }

        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            Ok(Some(MetadataRecord::Heapam(HeapamRecord::ClearVmBits(
                ClearVmBits {
                    new_heap_blkno,
                    old_heap_blkno,
                    vm_rel,
                    flags,
                },
            ))))
        } else {
            Ok(None)
        }
    }

    fn decode_neonmgr_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: u32,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // Handle VM bit updates that are implicitly part of heap records.

        // First, look at the record to determine which VM bits need
        // to be cleared. If either of these variables is set, we
        // need to clear the corresponding bits in the visibility map.
        let mut new_heap_blkno: Option<u32> = None;
        let mut old_heap_blkno: Option<u32> = None;
        let mut flags = pg_constants::VISIBILITYMAP_VALID_BITS;

        assert_eq!(decoded.xl_rmid, pg_constants::RM_NEON_ID);

        match pg_version {
            16 | 17 => {
                let info = decoded.xl_info & pg_constants::XLOG_HEAP_OPMASK;

                match info {
                    pg_constants::XLOG_NEON_HEAP_INSERT => {
                        let xlrec = v17::rm_neon::XlNeonHeapInsert::decode(buf);
                        assert_eq!(0, buf.remaining());
                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_DELETE => {
                        let xlrec = v17::rm_neon::XlNeonHeapDelete::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_DELETE_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_UPDATE
                    | pg_constants::XLOG_NEON_HEAP_HOT_UPDATE => {
                        let xlrec = v17::rm_neon::XlNeonHeapUpdate::decode(buf);
                        // the size of tuple data is inferred from the size of the record.
                        // we can't validate the remaining number of bytes without parsing
                        // the tuple data.
                        if (xlrec.flags & pg_constants::XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks.last().unwrap().blkno);
                        }
                        if (xlrec.flags & pg_constants::XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED) != 0 {
                            // PostgreSQL only uses XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED on a
                            // non-HOT update where the new tuple goes to different page than
                            // the old one. Otherwise, only XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED is
                            // set.
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_MULTI_INSERT => {
                        let xlrec = v17::rm_neon::XlNeonHeapMultiInsert::decode(buf);

                        let offset_array_len =
                            if decoded.xl_info & pg_constants::XLOG_HEAP_INIT_PAGE > 0 {
                                // the offsets array is omitted if XLOG_HEAP_INIT_PAGE is set
                                0
                            } else {
                                size_of::<u16>() * xlrec.ntuples as usize
                            };
                        assert_eq!(offset_array_len, buf.remaining());

                        if (xlrec.flags & pg_constants::XLH_INSERT_ALL_VISIBLE_CLEARED) != 0 {
                            new_heap_blkno = Some(decoded.blocks[0].blkno);
                        }
                    }
                    pg_constants::XLOG_NEON_HEAP_LOCK => {
                        let xlrec = v17::rm_neon::XlNeonHeapLock::decode(buf);
                        if (xlrec.flags & pg_constants::XLH_LOCK_ALL_FROZEN_CLEARED) != 0 {
                            old_heap_blkno = Some(decoded.blocks[0].blkno);
                            flags = pg_constants::VISIBILITYMAP_ALL_FROZEN;
                        }
                    }
                    info => anyhow::bail!("Unknown WAL record type for Neon RMGR: {}", info),
                }
            }
            _ => anyhow::bail!(
                "Neon RMGR has no known compatibility with PostgreSQL version {}",
                pg_version
            ),
        }

        if new_heap_blkno.is_some() || old_heap_blkno.is_some() {
            let vm_rel = RelTag {
                forknum: VISIBILITYMAP_FORKNUM,
                spcnode: decoded.blocks[0].rnode_spcnode,
                dbnode: decoded.blocks[0].rnode_dbnode,
                relnode: decoded.blocks[0].rnode_relnode,
            };

            Ok(Some(MetadataRecord::Neonrmgr(NeonrmgrRecord::ClearVmBits(
                ClearVmBits {
                    new_heap_blkno,
                    old_heap_blkno,
                    vm_rel,
                    flags,
                },
            ))))
        } else {
            Ok(None)
        }
    }

    fn decode_smgr_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_SMGR_CREATE {
            let create = XlSmgrCreate::decode(buf);
            let rel = RelTag {
                spcnode: create.rnode.spcnode,
                dbnode: create.rnode.dbnode,
                relnode: create.rnode.relnode,
                forknum: create.forknum,
            };

            return Ok(Some(MetadataRecord::Smgr(SmgrRecord::Create(SmgrCreate {
                rel,
            }))));
        } else if info == pg_constants::XLOG_SMGR_TRUNCATE {
            let truncate = XlSmgrTruncate::decode(buf);
            return Ok(Some(MetadataRecord::Smgr(SmgrRecord::Truncate(truncate))));
        }

        Ok(None)
    }

    fn decode_dbase_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: u32,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        // TODO: Refactor this to avoid the duplication between postgres versions.

        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        tracing::debug!(%info, %pg_version, "handle RM_DBASE_ID");

        if pg_version == 14 {
            if info == postgres_ffi::v14::bindings::XLOG_DBASE_CREATE {
                let createdb = XlCreateDatabase::decode(buf);
                tracing::debug!("XLOG_DBASE_CREATE v14");

                let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                    db_id: createdb.db_id,
                    tablespace_id: createdb.tablespace_id,
                    src_db_id: createdb.src_db_id,
                    src_tablespace_id: createdb.src_tablespace_id,
                }));

                return Ok(Some(record));
            } else if info == postgres_ffi::v14::bindings::XLOG_DBASE_DROP {
                let dropdb = XlDropDatabase::decode(buf);

                let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                    db_id: dropdb.db_id,
                    tablespace_ids: dropdb.tablespace_ids,
                }));

                return Ok(Some(record));
            }
        } else if pg_version == 15 {
            if info == postgres_ffi::v15::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                tracing::debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
            } else if info == postgres_ffi::v15::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                // The XLOG record was renamed between v14 and v15,
                // but the record format is the same.
                // So we can reuse XlCreateDatabase here.
                tracing::debug!("XLOG_DBASE_CREATE_FILE_COPY");

                let createdb = XlCreateDatabase::decode(buf);
                let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                    db_id: createdb.db_id,
                    tablespace_id: createdb.tablespace_id,
                    src_db_id: createdb.src_db_id,
                    src_tablespace_id: createdb.src_tablespace_id,
                }));

                return Ok(Some(record));
            } else if info == postgres_ffi::v15::bindings::XLOG_DBASE_DROP {
                let dropdb = XlDropDatabase::decode(buf);
                let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                    db_id: dropdb.db_id,
                    tablespace_ids: dropdb.tablespace_ids,
                }));

                return Ok(Some(record));
            }
        } else if pg_version == 16 {
            if info == postgres_ffi::v16::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                tracing::debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
            } else if info == postgres_ffi::v16::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                // The XLOG record was renamed between v14 and v15,
                // but the record format is the same.
                // So we can reuse XlCreateDatabase here.
                tracing::debug!("XLOG_DBASE_CREATE_FILE_COPY");

                let createdb = XlCreateDatabase::decode(buf);
                let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                    db_id: createdb.db_id,
                    tablespace_id: createdb.tablespace_id,
                    src_db_id: createdb.src_db_id,
                    src_tablespace_id: createdb.src_tablespace_id,
                }));

                return Ok(Some(record));
            } else if info == postgres_ffi::v16::bindings::XLOG_DBASE_DROP {
                let dropdb = XlDropDatabase::decode(buf);
                let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                    db_id: dropdb.db_id,
                    tablespace_ids: dropdb.tablespace_ids,
                }));

                return Ok(Some(record));
            }
        } else if pg_version == 17 {
            if info == postgres_ffi::v17::bindings::XLOG_DBASE_CREATE_WAL_LOG {
                tracing::debug!("XLOG_DBASE_CREATE_WAL_LOG: noop");
            } else if info == postgres_ffi::v17::bindings::XLOG_DBASE_CREATE_FILE_COPY {
                // The XLOG record was renamed between v14 and v15,
                // but the record format is the same.
                // So we can reuse XlCreateDatabase here.
                tracing::debug!("XLOG_DBASE_CREATE_FILE_COPY");

                let createdb = XlCreateDatabase::decode(buf);
                let record = MetadataRecord::Dbase(DbaseRecord::Create(DbaseCreate {
                    db_id: createdb.db_id,
                    tablespace_id: createdb.tablespace_id,
                    src_db_id: createdb.src_db_id,
                    src_tablespace_id: createdb.src_tablespace_id,
                }));

                return Ok(Some(record));
            } else if info == postgres_ffi::v17::bindings::XLOG_DBASE_DROP {
                let dropdb = XlDropDatabase::decode(buf);
                let record = MetadataRecord::Dbase(DbaseRecord::Drop(DbaseDrop {
                    db_id: dropdb.db_id,
                    tablespace_ids: dropdb.tablespace_ids,
                }));

                return Ok(Some(record));
            }
        }

        Ok(None)
    }

    fn decode_clog_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: u32,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & !pg_constants::XLR_INFO_MASK;

        if info == pg_constants::CLOG_ZEROPAGE {
            let pageno = if pg_version < 17 {
                buf.get_u32_le()
            } else {
                buf.get_u64_le() as u32
            };
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

            Ok(Some(MetadataRecord::Clog(ClogRecord::ZeroPage(
                ClogZeroPage { segno, rpageno },
            ))))
        } else {
            assert!(info == pg_constants::CLOG_TRUNCATE);
            let xlrec = XlClogTruncate::decode(buf, pg_version);

            Ok(Some(MetadataRecord::Clog(ClogRecord::Truncate(
                ClogTruncate {
                    pageno: xlrec.pageno,
                    oldest_xid: xlrec.oldest_xid,
                    oldest_xid_db: xlrec.oldest_xid_db,
                },
            ))))
        }
    }

    fn decode_xact_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        lsn: Lsn,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLOG_XACT_OPMASK;
        let origin_id = decoded.origin_id;
        let xl_xid = decoded.xl_xid;

        if info == pg_constants::XLOG_XACT_COMMIT {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::Commit(XactCommon {
                parsed,
                origin_id,
                xl_xid,
                lsn,
            }))));
        } else if info == pg_constants::XLOG_XACT_ABORT {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::Abort(XactCommon {
                parsed,
                origin_id,
                xl_xid,
                lsn,
            }))));
        } else if info == pg_constants::XLOG_XACT_COMMIT_PREPARED {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::CommitPrepared(
                XactCommon {
                    parsed,
                    origin_id,
                    xl_xid,
                    lsn,
                },
            ))));
        } else if info == pg_constants::XLOG_XACT_ABORT_PREPARED {
            let parsed = XlXactParsedRecord::decode(buf, decoded.xl_xid, decoded.xl_info);
            return Ok(Some(MetadataRecord::Xact(XactRecord::AbortPrepared(
                XactCommon {
                    parsed,
                    origin_id,
                    xl_xid,
                    lsn,
                },
            ))));
        } else if info == pg_constants::XLOG_XACT_PREPARE {
            return Ok(Some(MetadataRecord::Xact(XactRecord::Prepare(
                XactPrepare {
                    xl_xid: decoded.xl_xid,
                    data: Bytes::copy_from_slice(&buf[..]),
                },
            ))));
        }

        Ok(None)
    }

    fn decode_multixact_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        pg_version: u32,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;

        if info == pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE
            || info == pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE
        {
            let pageno = if pg_version < 17 {
                buf.get_u32_le()
            } else {
                buf.get_u64_le() as u32
            };
            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;

            let slru_kind = match info {
                pg_constants::XLOG_MULTIXACT_ZERO_OFF_PAGE => SlruKind::MultiXactOffsets,
                pg_constants::XLOG_MULTIXACT_ZERO_MEM_PAGE => SlruKind::MultiXactMembers,
                _ => unreachable!(),
            };

            return Ok(Some(MetadataRecord::MultiXact(MultiXactRecord::ZeroPage(
                MultiXactZeroPage {
                    slru_kind,
                    segno,
                    rpageno,
                },
            ))));
        } else if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
            let xlrec = XlMultiXactCreate::decode(buf);
            return Ok(Some(MetadataRecord::MultiXact(MultiXactRecord::Create(
                xlrec,
            ))));
        } else if info == pg_constants::XLOG_MULTIXACT_TRUNCATE_ID {
            let xlrec = XlMultiXactTruncate::decode(buf);
            return Ok(Some(MetadataRecord::MultiXact(MultiXactRecord::Truncate(
                xlrec,
            ))));
        }

        Ok(None)
    }

    fn decode_relmap_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let update = XlRelmapUpdate::decode(buf);

        let mut buf = decoded.record.clone();
        buf.advance(decoded.main_data_offset);
        // skip xl_relmap_update
        buf.advance(12);

        Ok(Some(MetadataRecord::Relmap(RelmapRecord::Update(
            RelmapUpdate {
                update,
                buf: Bytes::copy_from_slice(&buf[..]),
            },
        ))))
    }

    fn decode_xlog_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
        lsn: Lsn,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        Ok(Some(MetadataRecord::Xlog(XlogRecord::Raw(RawXlogRecord {
            info,
            lsn,
            buf: buf.clone(),
        }))))
    }

    fn decode_logical_message_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_LOGICAL_MESSAGE {
            let xlrec = XlLogicalMessage::decode(buf);
            let prefix = std::str::from_utf8(&buf[0..xlrec.prefix_size - 1])?;

            #[cfg(feature = "testing")]
            if prefix == "neon-test" {
                return Ok(Some(MetadataRecord::LogicalMessage(
                    LogicalMessageRecord::Failpoint,
                )));
            }

            if let Some(path) = prefix.strip_prefix("neon-file:") {
                let buf_size = xlrec.prefix_size + xlrec.message_size;
                let buf = Bytes::copy_from_slice(&buf[xlrec.prefix_size..buf_size]);
                return Ok(Some(MetadataRecord::LogicalMessage(
                    LogicalMessageRecord::Put(PutLogicalMessage {
                        path: path.to_string(),
                        buf,
                    }),
                )));
            }
        }

        Ok(None)
    }

    fn decode_standby_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_RUNNING_XACTS {
            let xlrec = XlRunningXacts::decode(buf);
            return Ok(Some(MetadataRecord::Standby(StandbyRecord::RunningXacts(
                StandbyRunningXacts {
                    oldest_running_xid: xlrec.oldest_running_xid,
                },
            ))));
        }

        Ok(None)
    }

    fn decode_replorigin_record(
        buf: &mut Bytes,
        decoded: &DecodedWALRecord,
    ) -> anyhow::Result<Option<MetadataRecord>> {
        let info = decoded.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
        if info == pg_constants::XLOG_REPLORIGIN_SET {
            let xlrec = XlReploriginSet::decode(buf);
            return Ok(Some(MetadataRecord::Replorigin(ReploriginRecord::Set(
                xlrec,
            ))));
        } else if info == pg_constants::XLOG_REPLORIGIN_DROP {
            let xlrec = XlReploriginDrop::decode(buf);
            return Ok(Some(MetadataRecord::Replorigin(ReploriginRecord::Drop(
                xlrec,
            ))));
        }

        Ok(None)
    }
}
