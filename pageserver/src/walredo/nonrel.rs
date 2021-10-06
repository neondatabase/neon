use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use postgres_ffi::{
    nonrelfile_utils::{
        mx_offset_to_flags_bitshift, mx_offset_to_flags_offset, mx_offset_to_member_offset,
        transaction_id_set_status,
    },
    pg_constants, XLogRecord,
};

use crate::{
    relish::{RelishTag, SlruKind},
    waldecoder::{XlMultiXactCreate, XlXactParsedRecord},
};

use super::WalRedoRequest;

pub(super) fn apply_nonrel(request: &WalRedoRequest) -> Bytes {
    let rel = request.rel;
    let blknum = request.blknum;

    // Non-relational WAL records are handled here, with custom code that has the
    // same effects as the corresponding Postgres WAL redo function.
    const ZERO_PAGE: [u8; 8192] = [0u8; 8192];
    let mut page = BytesMut::new();
    if let Some(fpi) = &request.base_img {
        // If full-page image is provided, then use it...
        page.extend_from_slice(&fpi[..]);
    } else {
        // otherwise initialize page with zeros
        page.extend_from_slice(&ZERO_PAGE);
    }
    // Apply all collected WAL records
    for record in &request.records {
        let mut buf = record.rec.clone();

        super::WAL_REDO_RECORD_COUNTER.inc();

        // 1. Parse XLogRecord struct
        // FIXME: refactor to avoid code duplication.
        let xlogrec = XLogRecord::from_bytes(&mut buf);

        //move to main data
        // TODO probably, we should store some records in our special format
        // to avoid this weird parsing on replay
        let skip = (record.main_data_offset - pg_constants::SIZEOF_XLOGRECORD) as usize;
        if buf.remaining() > skip {
            buf.advance(skip);
        }

        if xlogrec.xl_rmid == pg_constants::RM_XACT_ID {
            // Transaction manager stuff
            let rec_segno = match rel {
                RelishTag::Slru { slru, segno } => {
                    assert!(
                        slru == SlruKind::Clog,
                        "Not valid XACT relish tag {:?}",
                        rel
                    );
                    segno
                }
                _ => panic!("Not valid XACT relish tag {:?}", rel),
            };
            let parsed_xact = XlXactParsedRecord::decode(&mut buf, xlogrec.xl_xid, xlogrec.xl_info);
            if parsed_xact.info == pg_constants::XLOG_XACT_COMMIT
                || parsed_xact.info == pg_constants::XLOG_XACT_COMMIT_PREPARED
            {
                transaction_id_set_status(
                    parsed_xact.xid,
                    pg_constants::TRANSACTION_STATUS_COMMITTED,
                    &mut page,
                );
                for subxact in &parsed_xact.subxacts {
                    let pageno = *subxact as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    // only update xids on the requested page
                    if rec_segno == segno && blknum == rpageno {
                        transaction_id_set_status(
                            *subxact,
                            pg_constants::TRANSACTION_STATUS_COMMITTED,
                            &mut page,
                        );
                    }
                }
            } else if parsed_xact.info == pg_constants::XLOG_XACT_ABORT
                || parsed_xact.info == pg_constants::XLOG_XACT_ABORT_PREPARED
            {
                transaction_id_set_status(
                    parsed_xact.xid,
                    pg_constants::TRANSACTION_STATUS_ABORTED,
                    &mut page,
                );
                for subxact in &parsed_xact.subxacts {
                    let pageno = *subxact as u32 / pg_constants::CLOG_XACTS_PER_PAGE;
                    let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                    let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                    // only update xids on the requested page
                    if rec_segno == segno && blknum == rpageno {
                        transaction_id_set_status(
                            *subxact,
                            pg_constants::TRANSACTION_STATUS_ABORTED,
                            &mut page,
                        );
                    }
                }
            }
        } else if xlogrec.xl_rmid == pg_constants::RM_MULTIXACT_ID {
            // Multixact operations
            let info = xlogrec.xl_info & pg_constants::XLR_RMGR_INFO_MASK;
            if info == pg_constants::XLOG_MULTIXACT_CREATE_ID {
                let xlrec = XlMultiXactCreate::decode(&mut buf);
                if let RelishTag::Slru {
                    slru,
                    segno: rec_segno,
                } = rel
                {
                    if slru == SlruKind::MultiXactMembers {
                        for i in 0..xlrec.nmembers {
                            let pageno = i / pg_constants::MULTIXACT_MEMBERS_PER_PAGE as u32;
                            let segno = pageno / pg_constants::SLRU_PAGES_PER_SEGMENT;
                            let rpageno = pageno % pg_constants::SLRU_PAGES_PER_SEGMENT;
                            if segno == rec_segno && rpageno == blknum {
                                // update only target block
                                let offset = xlrec.moff + i;
                                let memberoff = mx_offset_to_member_offset(offset);
                                let flagsoff = mx_offset_to_flags_offset(offset);
                                let bshift = mx_offset_to_flags_bitshift(offset);
                                let mut flagsval =
                                    LittleEndian::read_u32(&page[flagsoff..flagsoff + 4]);
                                flagsval &= !(((1 << pg_constants::MXACT_MEMBER_BITS_PER_XACT)
                                    - 1)
                                    << bshift);
                                flagsval |= xlrec.members[i as usize].status << bshift;
                                LittleEndian::write_u32(
                                    &mut page[flagsoff..flagsoff + 4],
                                    flagsval,
                                );
                                LittleEndian::write_u32(
                                    &mut page[memberoff..memberoff + 4],
                                    xlrec.members[i as usize].xid,
                                );
                            }
                        }
                    } else {
                        // Multixact offsets SLRU
                        let offs = (xlrec.mid % pg_constants::MULTIXACT_OFFSETS_PER_PAGE as u32 * 4)
                            as usize;
                        LittleEndian::write_u32(&mut page[offs..offs + 4], xlrec.moff);
                    }
                } else {
                    panic!();
                }
            } else {
                panic!();
            }
        }
    }

    page.freeze()
}
