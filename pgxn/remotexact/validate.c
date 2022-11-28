/*-------------------------------------------------------------------------
 * contrib/remotexact/validate.c
 * 
 * This file contains function to validate the read set in the rwset
 * of a remote transaction.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/csn_log.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/remotexact.h"
#include "access/table.h"
#include "access/tableam.h"
#include "storage/bufmgr.h"
#include "rwset.h"
#include "validate.h"

void validate_index_scan(RWSetRelation *rw_rel)
{
    Oid relid = rw_rel->relid;
	XidCSN read_csn = rw_rel->csn;
    Relation rel;
    HeapScanDesc scan;
    int nkeys = 0;
    ScanKey keys = NULL;
    int scan_flags = 0;
    int i; 
    RWSetPage *page = NULL;
    Page index_page;
    XLogRecPtr page_lsn = InvalidXLogRecPtr;

    // This function must only be called for index scans in current_region.
    Assert(rw_rel->region == current_region);
    Assert(rw_rel->is_index && !rw_rel->is_table_scan);

    // Lock in the same mode as SELECT (AccessShareLock).
    // TODO(pooja): To avoid starvation, we might want to use RowShareLock.
    rel = index_open(relid, AccessShareLock);
    scan = (HeapScanDesc)heap_beginscan(rel, SnapshotAny, nkeys, keys, NULL,
                                        scan_flags);

    // For each index page, check if the lsn has been updated.
    for (i = 0; i < rw_rel->n_pages; i++)
    {
        page = &(rw_rel->pages[i]);

        // Advance the hscan to specified block and lock the page for sharing.
        heap_getpageonly(scan, page->blkno);
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_SHARE);

        // Get the page_lsn and unlock the page.
        index_page = BufferGetPage(scan->rs_cbuf);
        page_lsn = PageGetLSN(index_page);
        LockBuffer(scan->rs_cbuf, BUFFER_LOCK_UNLOCK);

        if (page_lsn > read_csn) {
            /* 
             * This page has been updated since the last snapshot, so
             * we need to fail validation. 
             * TODO(pooja): We need to check for each tuple to avoid
             * frequent aborts. 
             */
            break;
        }
    }

    heap_endscan((TableScanDesc)scan);
    index_close(rel, AccessShareLock);

    if (page_lsn > read_csn) {
        ereport(ERROR,
        (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
            errmsg("read out-of-date index data from a remote partition")));
    }
}

void
validate_table_scan(RWSetRelation *rw_rel)
{
    Oid relid = rw_rel->relid;
    XidCSN read_csn = rw_rel->csn;
    Relation rel;
    TableScanDesc scan;
    HeapScanDesc hscan;
	HeapTuple	htup;
    Snapshot    snapshot = GetActiveSnapshot();

    // This function must only be called for table scans in current_region.
    Assert(rw_rel->region == current_region);
    Assert(rw_rel->is_table_scan && !rw_rel->is_index);

    rel = table_open(relid, AccessShareLock);

    /*
     * Use SnapshotAny to scan over all tuples
     */
    scan = table_beginscan(rel, SnapshotAny, 0, NULL);
	hscan = (HeapScanDesc) scan;

	while ((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
    {
        HeapTupleHeader tuple = htup->t_data;
        TransactionId checked_xid = InvalidTransactionId;

        /*
         * Must lock the buffer before checking for visibility
         */
		LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_SHARE);

        /*
         * If a tuple is visible now, it must also be visible to read_csn
         */
        if (HeapTupleSatisfiesVisibility(current_region, htup, snapshot, hscan->rs_cbuf))
        {
            TransactionId xmin = HeapTupleHeaderGetRawXmin(tuple);

            /*
             * Current transaction must not make any modification prior to validation
             */
            Assert(!TransactionIdIsCurrentTransactionId(xmin));

            checked_xid = xmin;
        }
        /*
         * If a tuple is not visible now, it must also not be visible to read_csn.
         * We only need to consider tuples that are committed and then removed
         * as seen by the current snapshot here. In-progress and aborted tuples
         * are never visible to read_csn.
         * 
         * TODO (ctring): There is an edge case where a tuple is removed and
         * then immediately vacuumed after the remote transaction starts but
         * before validation. The physical tuple is no longer available for
         * us to do these checks, resulting in wrong validation. One way to counter
         * this is counting the number of visible tuples while scanning them and
         * compare it with the number of visible tuples during validation.
         */
        else if (HeapTupleHeaderXminCommitted(tuple) &&
                 (HeapTupleHeaderXminFrozen(tuple) ||
                  !XidInMVCCSnapshot(HeapTupleHeaderGetRawXmin(tuple), snapshot)))
        {
            TransactionId xmax;

            /*
             * Xmax must be valid because the tuple is invisible because it 
             * was deleted.
             */
            Assert(!(tuple->t_infomask & HEAP_XMAX_INVALID) &&
                   !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask));

            /*
             * Extract xmax based on whether it is a multixact or not
             */
            if (tuple->t_infomask & HEAP_XMAX_IS_MULTI)
                xmax = HeapTupleGetUpdateXid(tuple);
            else
                xmax = HeapTupleHeaderGetRawXmax(tuple);

            /*
             * Cannot be the current transaction because it does not make any
             * modification before validation.
             */
            Assert(!TransactionIdIsCurrentTransactionId(xmax));
            
            checked_xid = xmax;
        }

        LockBuffer(hscan->rs_cbuf, BUFFER_LOCK_UNLOCK);

        /*
         * Translate checked_xid into csn and compare it with csn used for the
         * initial read.
         */
        if (TransactionIdIsValid(checked_xid) &&
            CSNLogGetCSNByXid(current_region, checked_xid) > read_csn)
            ereport(ERROR,
                    (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
                        errmsg("read out-of-date data from a remote partition")));

    }

    table_endscan(scan);
    table_close(rel, AccessShareLock);
}