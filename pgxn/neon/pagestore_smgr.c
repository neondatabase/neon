/*-------------------------------------------------------------------------
 *
 * pagestore_smgr.c
 *
 *
 *
 * Temporary and unlogged rels
 * ---------------------------
 *
 * Temporary and unlogged tables are stored locally, by md.c. The functions
 * here just pass the calls through to corresponding md.c functions.
 *
 * Index build operations that use the buffer cache are also handled locally,
 * just like unlogged tables. Such operations must be marked by calling
 * smgr_start_unlogged_build() and friends.
 *
 * In order to know what relations are permanent and which ones are not, we
 * have added a 'smgr_relpersistence' field to SmgrRelationData, and it is set
 * by smgropen() callers, when they have the relcache entry at hand.  However,
 * sometimes we need to open an SmgrRelation for a relation without the
 * relcache. That is needed when we evict a buffer; we might not have the
 * SmgrRelation for that relation open yet. To deal with that, the
 * 'relpersistence' can be left to zero, meaning we don't know if it's
 * permanent or not. Most operations are not allowed with relpersistence==0,
 * but smgrwrite() does work, which is what we need for buffer eviction.  and
 * smgrunlink() so that a backend doesn't need to have the relcache entry at
 * transaction commit, where relations that were dropped in the transaction
 * are unlinked.
 *
 * If smgrwrite() is called and smgr_relpersistence == 0, we check if the
 * relation file exists locally or not. If it does exist, we assume it's an
 * unlogged relation and write the page there. Otherwise it must be a
 * permanent relation, WAL-logged and stored on the page server, and we ignore
 * the write like we do for permanent relations.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/parallel.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xloginsert.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "catalog/pg_class.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/interrupt.h"
#include "port/pg_iovec.h"
#include "replication/walsender.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/fsm_internals.h"
#include "storage/md.h"
#include "storage/smgr.h"

#include "bitmap.h"
#include "communicator.h"
#include "file_cache.h"
#include "neon.h"
#include "neon_lwlsncache.h"
#include "neon_perf_counters.h"
#include "pagestore_client.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

#if PG_VERSION_NUM < 160000
typedef PGAlignedBlock PGIOAlignedBlock;
#endif

/*
 * If DEBUG_COMPARE_LOCAL is defined, we pass through all the SMGR API
 * calls to md.c, and *also* do the calls to the Page Server. On every
 * read, compare the versions we read from local disk and Page Server,
 * and Assert that they are identical.
 */
/* #define DEBUG_COMPARE_LOCAL */

#ifdef DEBUG_COMPARE_LOCAL
#include "access/nbtree.h"
#include "storage/bufpage.h"
#include "access/xlog_internal.h"

static char *hexdump_page(char *page);
#endif

#define IS_LOCAL_REL(reln) (\
	NInfoGetDbOid(InfoFromSMgrRel(reln)) != 0 && \
		NInfoGetRelNumber(InfoFromSMgrRel(reln)) >= FirstNormalObjectId \
)

const int	SmgrTrace = DEBUG5;

/* unlogged relation build states */
typedef enum
{
	UNLOGGED_BUILD_NOT_IN_PROGRESS = 0,
	UNLOGGED_BUILD_PHASE_1,
	UNLOGGED_BUILD_PHASE_2,
	UNLOGGED_BUILD_NOT_PERMANENT
} UnloggedBuildPhase;

static SMgrRelation unlogged_build_rel = NULL;
static UnloggedBuildPhase unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;

static bool neon_redo_read_buffer_filter(XLogReaderState *record, uint8 block_id);
static bool (*old_redo_read_buffer_filter) (XLogReaderState *record, uint8 block_id) = NULL;

static BlockNumber neon_nblocks(SMgrRelation reln, ForkNumber forknum);

/*
 * Wrapper around log_newpage() that makes a temporary copy of the block and
 * WAL-logs that. This makes it safe to use while holding only a shared lock
 * on the page, see XLogSaveBufferForHint. We don't use XLogSaveBufferForHint
 * directly because it skips the logging if the LSN is new enough.
 */
static XLogRecPtr
log_newpage_copy(NRelFileInfo * rinfo, ForkNumber forkNum, BlockNumber blkno,
				 Page page, bool page_std)
{
	PGIOAlignedBlock copied_buffer;

	memcpy(copied_buffer.data, page, BLCKSZ);
	return log_newpage(rinfo, forkNum, blkno, copied_buffer.data, page_std);
}

#if PG_MAJORVERSION_NUM >= 17
/*
 * Wrapper around log_newpages() that makes a temporary copy of the block and
 * WAL-logs that. This makes it safe to use while holding only a shared lock
 * on the page, see XLogSaveBufferForHint. We don't use XLogSaveBufferForHint
 * directly because it skips the logging if the LSN is new enough.
 */
static XLogRecPtr
log_newpages_copy(NRelFileInfo * rinfo, ForkNumber forkNum, BlockNumber blkno,
				  BlockNumber nblocks, Page *pages, bool page_std)
{
	PGIOAlignedBlock copied_buffer[XLR_MAX_BLOCK_ID];
	BlockNumber	blknos[XLR_MAX_BLOCK_ID];
	Page		pageptrs[XLR_MAX_BLOCK_ID];
	int			nregistered = 0;

	for (int i = 0; i < nblocks; i++)
	{
		Page	page = copied_buffer[nregistered].data;
		memcpy(page, pages[i], BLCKSZ);
		pageptrs[nregistered] = page;
		blknos[nregistered] = blkno + i;

		++nregistered;

		if (nregistered >= XLR_MAX_BLOCK_ID)
		{
			log_newpages(rinfo, forkNum, nregistered, blknos, pageptrs,
						 page_std);
			nregistered = 0;
		}
	}

	if (nregistered != 0)
	{
		log_newpages(rinfo, forkNum, nregistered, blknos, pageptrs,
					 page_std);
	}

	return ProcLastRecPtr;
}
#endif /* PG_MAJORVERSION_NUM >= 17 */

/*
 * Is 'buffer' identical to a freshly initialized empty heap page?
 */
static bool
PageIsEmptyHeapPage(char *buffer)
{
	PGIOAlignedBlock empty_page;

	PageInit((Page) empty_page.data, BLCKSZ, 0);

	return memcmp(buffer, empty_page.data, BLCKSZ) == 0;
}

#if PG_MAJORVERSION_NUM >= 17
static void
neon_wallog_pagev(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
				  BlockNumber nblocks, const char **buffers, bool force)
{
#define BLOCK_BATCH_SIZE	16
	bool		log_pages;
	BlockNumber	batch_blockno = blocknum;
	XLogRecPtr	lsns[BLOCK_BATCH_SIZE];
	int			batch_size = 0;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	log_pages = false;
	if (force)
	{
		Assert(XLogInsertAllowed());
		log_pages = true;
	}
	else if (XLogInsertAllowed() &&
			 (forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM))
	{
		log_pages = true;
	}

	if (log_pages)
	{
		XLogRecPtr	recptr;
		recptr = log_newpages_copy(&InfoFromSMgrRel(reln), forknum, blocknum,
								   nblocks, (Page *) buffers, false);

		for (int i = 0; i < nblocks; i++)
			PageSetLSN(unconstify(char *, buffers[i]), recptr);

		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Page %u through %u of relation %u/%u/%u.%u "
								 "were force logged, lsn=%X/%X",
						blocknum, blocknum + nblocks,
						RelFileInfoFmt(InfoFromSMgrRel(reln)),
						forknum, LSN_FORMAT_ARGS(recptr))));
	}

	for (int i = 0; i < nblocks; i++)
	{
		Page		page = (Page) buffers[i];
		BlockNumber blkno = blocknum + i;
		XLogRecPtr	lsn = PageGetLSN(page);

		if (lsn == InvalidXLogRecPtr)
		{
			/*
			 * When PostgreSQL extends a relation, it calls smgrextend() with an
			 * all-zeros pages, and we can just ignore that in Neon. We do need to
			 * remember the new size, though, so that smgrnblocks() returns the
			 * right answer after the rel has been extended. We rely on the
			 * relsize cache for that.
			 *
			 * A completely empty heap page doesn't need to be WAL-logged, either.
			 * The heapam can leave such a page behind, if e.g. an insert errors
			 * out after initializing the page, but before it has inserted the
			 * tuple and WAL-logged the change. When we read the page from the
			 * page server, it will come back as all-zeros. That's OK, the heapam
			 * will initialize an all-zeros page on first use.
			 *
			 * In other scenarios, evicting a dirty page with no LSN is a bad
			 * sign: it implies that the page was not WAL-logged, and its contents
			 * will be lost when it's evicted.
			 */
			if (PageIsNew(page))
			{
				ereport(SmgrTrace,
						(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is all-zeros",
								blkno,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum)));
			}
			else if (PageIsEmptyHeapPage(page))
			{
				ereport(SmgrTrace,
						(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is an empty heap page with no LSN",
								blkno,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum)));
			}
			else if (forknum != FSM_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
			{
				/*
				 * Its a bad sign if there is a page with zero LSN in the buffer
				 * cache in a standby, too. However, PANICing seems like a cure
				 * worse than the disease, as the damage has likely already been
				 * done in the primary. So in a standby, make this an assertion,
				 * and in a release build just LOG the error and soldier on. We
				 * update the last-written LSN of the page with a conservative
				 * value in that case, which is the last replayed LSN.
				 */
				ereport(RecoveryInProgress() ? LOG : PANIC,
						(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
								blkno,
								RelFileInfoFmt(InfoFromSMgrRel(reln)),
								forknum)));
				Assert(false);

				lsn = GetXLogReplayRecPtr(NULL); /* in standby mode, soldier on */
			}
		}
		else
		{
			ereport(SmgrTrace,
					(errmsg(NEON_TAG "Evicting page %u of relation %u/%u/%u.%u with lsn=%X/%X",
							blkno,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum, LSN_FORMAT_ARGS(lsn))));
		}

		/*
		 * Remember the LSN on this page. When we read the page again, we must
		 * read the same or newer version of it.
		 */
		lsns[batch_size++] = lsn;

		if (batch_size >= BLOCK_BATCH_SIZE)
		{
			neon_set_lwlsn_block_v(lsns, InfoFromSMgrRel(reln), forknum,
									   batch_blockno,
									   batch_size);
			batch_blockno += batch_size;
			batch_size = 0;
		}
	}

	if (batch_size != 0)
	{
		neon_set_lwlsn_block_v(lsns, InfoFromSMgrRel(reln), forknum,
								   batch_blockno,
								   batch_size);
	}
}
#endif

/*
 * A page is being evicted from the shared buffer cache. Update the
 * last-written LSN of the page, and WAL-log it if needed.
 */
#if PG_MAJORVERSION_NUM < 16
static void
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool force)
#else
static void
neon_wallog_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool force)
#endif
{
	XLogRecPtr	lsn = PageGetLSN((Page) buffer);
	bool		log_page;

	/*
	 * Whenever a VM or FSM page is evicted, WAL-log it. FSM and (some) VM
	 * changes are not WAL-logged when the changes are made, so this is our
	 * last chance to log them, otherwise they're lost. That's OK for
	 * correctness, the non-logged updates are not critical. But we want to
	 * have a reasonably up-to-date VM and FSM in the page server.
	 */
	log_page = false;
	if (force)
	{
		Assert(XLogInsertAllowed());
		log_page = true;
	}
	else if (XLogInsertAllowed() &&
			 !ShutdownRequestPending &&
			 (forknum == FSM_FORKNUM || forknum == VISIBILITYMAP_FORKNUM))
	{
		log_page = true;
	}

	if (log_page)
	{
		XLogRecPtr	recptr;

		recptr = log_newpage_copy(&InfoFromSMgrRel(reln), forknum, blocknum,
								  (Page) buffer, false);
		XLogFlush(recptr);
		lsn = recptr;
		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u was force logged. Evicted at lsn=%X/%X",
						blocknum,
						RelFileInfoFmt(InfoFromSMgrRel(reln)),
						forknum, LSN_FORMAT_ARGS(lsn))));
	}

	if (lsn == InvalidXLogRecPtr)
	{
		/*
		 * When PostgreSQL extends a relation, it calls smgrextend() with an
		 * all-zeros pages, and we can just ignore that in Neon. We do need to
		 * remember the new size, though, so that smgrnblocks() returns the
		 * right answer after the rel has been extended. We rely on the
		 * relsize cache for that.
		 *
		 * A completely empty heap page doesn't need to be WAL-logged, either.
		 * The heapam can leave such a page behind, if e.g. an insert errors
		 * out after initializing the page, but before it has inserted the
		 * tuple and WAL-logged the change. When we read the page from the
		 * page server, it will come back as all-zeros. That's OK, the heapam
		 * will initialize an all-zeros page on first use.
		 *
		 * In other scenarios, evicting a dirty page with no LSN is a bad
		 * sign: it implies that the page was not WAL-logged, and its contents
		 * will be lost when it's evicted.
		 */
		if (PageIsNew((Page) buffer))
		{
			ereport(SmgrTrace,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is all-zeros",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
		}
		else if (PageIsEmptyHeapPage((Page) buffer))
		{
			ereport(SmgrTrace,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is an empty heap page with no LSN",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
		}
		else if (forknum != FSM_FORKNUM && forknum != VISIBILITYMAP_FORKNUM)
		{
			/*
			 * Its a bad sign if there is a page with zero LSN in the buffer
			 * cache in a standby, too. However, PANICing seems like a cure
			 * worse than the disease, as the damage has likely already been
			 * done in the primary. So in a standby, make this an assertion,
			 * and in a release build just LOG the error and soldier on. We
			 * update the last-written LSN of the page with a conservative
			 * value in that case, which is the last replayed LSN.
			 */
			ereport(RecoveryInProgress() ? LOG : PANIC,
					(errmsg(NEON_TAG "Page %u of relation %u/%u/%u.%u is evicted with zero LSN",
							blocknum,
							RelFileInfoFmt(InfoFromSMgrRel(reln)),
							forknum)));
			Assert(false);

			lsn = GetXLogReplayRecPtr(NULL); /* in standby mode, soldier on */
		}
	}
	else
	{
		ereport(SmgrTrace,
				(errmsg(NEON_TAG "Evicting page %u of relation %u/%u/%u.%u with lsn=%X/%X",
						blocknum,
						RelFileInfoFmt(InfoFromSMgrRel(reln)),
						forknum, LSN_FORMAT_ARGS(lsn))));
	}

	/*
	 * Remember the LSN on this page. When we read the page again, we must
	 * read the same or newer version of it.
	 */
	neon_set_lwlsn_block(lsn, InfoFromSMgrRel(reln), forknum, blocknum);
}

/*
 *	neon_init() -- Initialize private state
 */
static void
neon_init(void)
{
	/*
	 * Sanity check that theperf counters array is sized correctly. We got
	 * this wrong once, and the formula for max number of backends and aux
	 * processes might well change in the future, so better safe than sorry.
	 * This is a very cheap check so we do it even without assertions.  On
	 * v14, this gets called before initializing MyProc, so we cannot perform
	 * the check here. That's OK, we don't expect the logic to change in old
	 * releases.
	 */
#if PG_VERSION_NUM>=150000
	if (MyNeonCounters >= &neon_per_backend_counters_shared[NUM_NEON_PERF_COUNTER_SLOTS])
		elog(ERROR, "MyNeonCounters points past end of array");
#endif

	old_redo_read_buffer_filter = redo_read_buffer_filter;
	redo_read_buffer_filter = neon_redo_read_buffer_filter;

#ifdef DEBUG_COMPARE_LOCAL
	mdinit();
#endif
}

/*
 * GetXLogInsertRecPtr uses XLogBytePosToRecPtr to convert logical insert (reserved) position
 * to physical position in WAL. It always adds SizeOfXLogShortPHD:
 *		seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
 * so even if there are no records on the page, offset will be SizeOfXLogShortPHD.
 * It may cause problems with XLogFlush. So return pointer backward to the origin of the page.
 */
static XLogRecPtr
nm_adjust_lsn(XLogRecPtr lsn)
{
	/*
	 * If lsn points to the beging of first record on page or segment, then
	 * "return" it back to the page origin
	 */
	if ((lsn & (XLOG_BLCKSZ - 1)) == SizeOfXLogShortPHD)
	{
		lsn -= SizeOfXLogShortPHD;
	}
	else if ((lsn & (wal_segment_size - 1)) == SizeOfXLogLongPHD)
	{
		lsn -= SizeOfXLogLongPHD;
	}
	return lsn;
}


/*
 * Return LSN for requesting pages and number of blocks from page server
 *
 * XXX: exposed so that prefetch_do_request() can call back here.
 */
void
neon_get_request_lsns(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno,
					  neon_request_lsns *output, BlockNumber nblocks)
{
	XLogRecPtr	last_written_lsns[PG_IOV_MAX];

	Assert(nblocks <= PG_IOV_MAX);

	neon_get_lwlsn_v(rinfo, forknum, blkno, (int) nblocks, last_written_lsns);

	for (int i = 0; i < nblocks; i++)
	{
		last_written_lsns[i] = nm_adjust_lsn(last_written_lsns[i]);
		Assert(last_written_lsns[i] != InvalidXLogRecPtr);
	}

	if (RecoveryInProgress())
	{
		/*---
		 * In broad strokes, a replica always requests the page at the current
		 * replay LSN. But looking closer, what exactly is the replay LSN? Is
		 * it the last replayed record, or the record being replayed? And does
		 * the startup process performing the replay need to do something
		 * differently than backends running queries? Let's take a closer look
		 * at the different scenarios:
		 *
		 * 1. Startup process reads a page, last_written_lsn is old.
		 *
		 * Read the old version of the page. We will apply the WAL record on
		 * it to bring it up-to-date.
		 *
		 * We could read the new version, with the changes from this WAL
		 * record already applied, to offload the work of replaying the record
		 * to the pageserver. The pageserver might not have received the WAL
		 * record yet, though, so a read of the old page version and applying
		 * the record ourselves is likely faster. Also, the redo function
		 * might be surprised if the changes have already applied. That's
		 * normal during crash recovery, but not in hot standby.
		 *
		 * 2. Startup process reads a page, last_written_lsn == record we're
		 *    replaying.
		 *
		 * Can this happen? There are a few theoretical cases when it might:
		 *
		 * A) The redo function reads the same page twice. We had already read
		 *    and applied the changes once, and now we're reading it for the
		 *    second time.  That would be a rather silly thing for a redo
		 *    function to do, and I'm not aware of any that would do it.
		 *
		 * B) The redo function modifies multiple pages, and it already
		 *    applied the changes to one of the pages, released the lock on
		 *    it, and is now reading a second page.  Furthermore, the first
		 *    page was already evicted from the buffer cache, and also from
		 *    the last-written LSN cache, so that the per-relation or global
		 *    last-written LSN was already updated. All the WAL redo functions
		 *    hold the locks on pages that they modify, until all the changes
		 *    have been modified (?), which would make that impossible.
		 *    However, we skip the locking, if the page isn't currently in the
		 *    page cache (see neon_redo_read_buffer_filter below).
		 *
		 * Even if the one of the above cases were possible in theory, they
		 * would also require the pages being modified by the redo function to
		 * be immediately evicted from the page cache.
		 *
		 * So this probably does not happen in practice. But if it does, we
		 * request the new version, including the changes from the record
		 * being replayed. That seems like the correct behavior in any case.
		 *
		 * 3. Backend process reads a page with old last-written LSN
		 *
		 * Nothing special here. Read the old version.
		 *
		 * 4. Backend process reads a page with last_written_lsn == record being replayed
		 *
		 * This can happen, if the redo function has started to run, and saw
		 * that the page isn't present in the page cache (see
		 * neon_redo_read_buffer_filter below).  Normally, in a normal
		 * Postgres server, the redo function would hold a lock on the page,
		 * so we would get blocked waiting the redo function to release the
		 * lock. To emulate that, wait for the WAL replay of the record to
		 * finish.
		 */
		/* Request the page at the end of the last fully replayed LSN. */
		XLogRecPtr replay_lsn = GetXLogReplayRecPtr(NULL);

		for (int i = 0; i < nblocks; i++)
		{
			neon_request_lsns *result = &output[i];
			XLogRecPtr	last_written_lsn = last_written_lsns[i];

			if (last_written_lsn > replay_lsn)
			{
				/* GetCurrentReplayRecPtr was introduced in v15 */
#if PG_VERSION_NUM >= 150000
				Assert(last_written_lsn == GetCurrentReplayRecPtr(NULL));
#endif

				/*
				 * Cases 2 and 4. If this is a backend (case 4), the
				 * neon_read_at_lsn() call later will wait for the WAL record to be
				 * fully replayed.
				 */
				result->request_lsn = last_written_lsn;
			}
			else
			{
				/* cases 1 and 3 */
				result->request_lsn = replay_lsn;
			}

			result->not_modified_since = last_written_lsn;
			result->effective_request_lsn = result->request_lsn;
			Assert(last_written_lsn <= result->request_lsn);

			neon_log(DEBUG1, "neon_get_request_lsns request lsn %X/%X, not_modified_since %X/%X",
					 LSN_FORMAT_ARGS(result->request_lsn), LSN_FORMAT_ARGS(result->not_modified_since));
		}
	}
	else
	{
		XLogRecPtr	flushlsn;
#if PG_VERSION_NUM >= 150000
		flushlsn = GetFlushRecPtr(NULL);
#else
		flushlsn = GetFlushRecPtr();
#endif

		for (int i = 0; i < nblocks; i++)
		{
			neon_request_lsns *result = &output[i];
			XLogRecPtr	last_written_lsn = last_written_lsns[i];

			/*
			 * Use the latest LSN that was evicted from the buffer cache as the
			 * 'not_modified_since' hint. Any pages modified by later WAL records
			 * must still in the buffer cache, so our request cannot concern
			 * those.
			 */
			neon_log(DEBUG1, "neon_get_request_lsns GetLastWrittenLSN lsn %X/%X",
					 LSN_FORMAT_ARGS(last_written_lsn));

			/*
			 * Is it possible that the last-written LSN is ahead of last flush
			 * LSN? Generally not, we shouldn't evict a page from the buffer cache
			 * before all its modifications have been safely flushed. That's the
			 * "WAL before data" rule. However, such case does exist at index
			 * building, _bt_blwritepage logs the full page without flushing WAL
			 * before smgrextend (files are fsynced before build ends).
			 */
			if (last_written_lsn > flushlsn)
			{
				neon_log(DEBUG5, "last-written LSN %X/%X is ahead of last flushed LSN %X/%X",
						 LSN_FORMAT_ARGS(last_written_lsn),
						 LSN_FORMAT_ARGS(flushlsn));
				XLogFlush(last_written_lsn);
			}

			/*
			 * Request the very latest version of the page. In principle we
			 * want to read the page at the current insert LSN, and we could
			 * use that value in the request. However, there's a corner case
			 * with pageserver's garbage collection. If the GC horizon is
			 * set to a very small value, it's possible that by the time
			 * that the pageserver processes our request, the GC horizon has
			 * already moved past the LSN we calculate here. Standby servers
			 * always have that problem as the can always lag behind the
			 * primary, but for the primary we can avoid it by always
			 * requesting the latest page, by setting request LSN to
			 * UINT64_MAX.
			 *
			 * effective_request_lsn is used to check that received response is still valid.
			 * In case of primary node it is last written LSN. Originally we used flush_lsn here,
			 * but it is not correct. Consider the following scenario:
			 * 1. Backend A wants to prefetch block X
			 * 2. Backend A checks that block X is not present in the shared buffer cache
			 * 3. Backend A calls prefetch_do_request, which calls neon_get_request_lsns
			 * 4. neon_get_request_lsns obtains LwLSN=11 for the block
			 * 5. Backend B downloads block X, updates and wallogs it with LSN=13
			 * 6. Block X is once again evicted from shared buffers, its LwLSN is set to LSN=13
			 * 7. Backend A is still executing in neon_get_request_lsns(). It calls 'flushlsn = GetFlushRecPtr();'.
			 *    Let's say that it is LSN=14
			 * 8. Backend A uses LSN=14 as effective_lsn in the prefetch slot. The request stored in the slot is
			 *    [not_modified_since=11, effective_request_lsn=14]
			 * 9. Backend A sends the prefetch request, pageserver processes it, and sends response.
			 *    The last LSN that the pageserver had processed was LSN=12, so the page image in the response is valid at LSN=12.
			 * 10. Backend A calls smgrread() for page X with LwLSN=13
			 * 11. Backend A finds in prefetch ring the response for the prefetch request with [not_modified_since=11, effective_lsn=Lsn14],
			 * so it satisfies neon_prefetch_response_usable condition.
			 *
			 * Things go wrong in step 7-8, when [not_modified_since=11, effective_request_lsn=14] is determined for the request.
			 * That is incorrect, because the page has in fact been modified at LSN=13. The invariant is that for any request,
			 * there should not be any modifications to a page between its not_modified_since and (effective_)request_lsn values.
			 *
			 * The problem can be fixed by callingGetFlushRecPtr() before checking if the page is in the buffer cache.
			 * But you can't do that within smgrprefetch(), would need to modify the caller.
			 */
			result->request_lsn = UINT64_MAX;
			result->not_modified_since = last_written_lsn;
			result->effective_request_lsn = last_written_lsn;
		}
	}
}

/*
 *	neon_exists() -- Does the physical file exist?
 */
static bool
neon_exists(SMgrRelation reln, ForkNumber forkNum)
{
	BlockNumber n_blocks;
	neon_request_lsns request_lsns;

	switch (reln->smgr_relpersistence)
	{
		case 0:

			/*
			 * We don't know if it's an unlogged rel stored locally, or
			 * permanent rel stored in the page server. First check if it
			 * exists locally. If it does, great. Otherwise check if it exists
			 * in the page server.
			 */
			if (mdexists(reln, forkNum))
				return true;
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdexists(reln, forkNum);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(InfoFromSMgrRel(reln), forkNum, &n_blocks))
	{
		return true;
	}

	/*
	 * \d+ on a view calls smgrexists with 0/0/0 relfilenode. The page server
	 * will error out if you check that, because the whole dbdir for
	 * tablespace 0, db 0 doesn't exists. We possibly should change the page
	 * server to accept that and return 'false', to be consistent with
	 * mdexists(). But we probably also should fix pg_table_size() to not call
	 * smgrexists() with bogus relfilenode.
	 *
	 * For now, handle that special case here.
	 */
#if PG_MAJORVERSION_NUM >= 16
	if (reln->smgr_rlocator.locator.spcOid == 0 &&
		reln->smgr_rlocator.locator.dbOid == 0 &&
		reln->smgr_rlocator.locator.relNumber == 0)
#else
	if (reln->smgr_rnode.node.spcNode == 0 &&
		reln->smgr_rnode.node.dbNode == 0 &&
		reln->smgr_rnode.node.relNode == 0)
#endif
	{
		return false;
	}

	neon_get_request_lsns(InfoFromSMgrRel(reln), forkNum,
						  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1);

	return communicator_exists(InfoFromSMgrRel(reln), forkNum, &request_lsns);
}

/*
 *	neon_create() -- Create a new relation on neond storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
static void
neon_create(SMgrRelation reln, ForkNumber forkNum, bool isRedo)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrcreate() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
#ifdef DEBUG_COMPARE_LOCAL
			mdcreate(reln, forkNum, forkNum == INIT_FORKNUM || isRedo);
			if (forkNum == MAIN_FORKNUM)
				mdcreate(reln, INIT_FORKNUM, true);
#else
			mdcreate(reln, forkNum, isRedo);
#endif
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_log(SmgrTrace, "Create relation %u/%u/%u.%u",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forkNum);

	/*
	 * Newly created relation is empty, remember that in the relsize cache.
	 *
	 * Note that in REDO, this is called to make sure the relation fork
	 * exists, but it does not truncate the relation. So, we can only update
	 * the relsize if it didn't exist before.
	 *
	 * Also, in redo, we must make sure to update the cached size of the
	 * relation, as that is the primary source of truth for REDO's file length
	 * considerations, and as file extension isn't (perfectly) logged, we need
	 * to take care of that before we hit file size checks.
	 *
	 * FIXME: This is currently not just an optimization, but required for
	 * correctness. Postgres can call smgrnblocks() on the newly-created
	 * relation. Currently, we don't call SetLastWrittenLSN() when a new
	 * relation created, so if we didn't remember the size in the relsize
	 * cache, we might call smgrnblocks() on the newly-created relation before
	 * the creation WAL record hass been received by the page server.
	 */
	if (isRedo)
	{
		update_cached_relsize(InfoFromSMgrRel(reln), forkNum, 0);
		get_cached_relsize(InfoFromSMgrRel(reln), forkNum,
						   &reln->smgr_cached_nblocks[forkNum]);
	}
	else
		set_cached_relsize(InfoFromSMgrRel(reln), forkNum, 0);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdcreate(reln, forkNum, isRedo);
#endif
}

/*
 *	neon_unlink() -- Unlink a relation.
 *
 * Note that we're passed a RelFileNodeBackend --- by the time this is called,
 * there won't be an SMgrRelation hashtable entry anymore.
 *
 * forkNum can be a fork number to delete a specific fork, or InvalidForkNumber
 * to delete all forks.
 *
 *
 * If isRedo is true, it's unsurprising for the relation to be already gone.
 * Also, we should remove the file immediately instead of queuing a request
 * for later, since during redo there's no possibility of creating a
 * conflicting relation.
 *
 * Note: any failure should be reported as WARNING not ERROR, because
 * we are usually not in a transaction anymore when this is called.
 */
static void
neon_unlink(NRelFileInfoBackend rinfo, ForkNumber forkNum, bool isRedo)
{
	/*
	 * Might or might not exist locally, depending on whether it's an unlogged
	 * or permanent relation (or if DEBUG_COMPARE_LOCAL is set). Try to
	 * unlink, it won't do any harm if the file doesn't exist.
	 */
	mdunlink(rinfo, forkNum, isRedo);
	if (!NRelFileInfoBackendIsTemp(rinfo))
	{
		forget_cached_relsize(InfoFromNInfoB(rinfo), forkNum);
	}
}

/*
 *	neon_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
static void
#if PG_MAJORVERSION_NUM < 16
neon_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			char *buffer, bool skipFsync)
#else
neon_extend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno,
			const void *buffer, bool skipFsync)
#endif
{
	XLogRecPtr	lsn;
	BlockNumber n_blocks = 0;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrextend() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdextend(reln, forkNum, blkno, buffer, skipFsync);
			/* Update LFC in case of unlogged index build */
			if (reln == unlogged_build_rel && unlogged_build_phase == UNLOGGED_BUILD_PHASE_2)
				lfc_write(InfoFromSMgrRel(reln), forkNum, blkno, buffer);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/*
	 * Check that the cluster size limit has not been exceeded.
	 *
	 * Temporary and unlogged relations are not included in the cluster size
	 * measured by the page server, so ignore those. Autovacuum processes are
	 * also exempt.
	 */
	if (max_cluster_size > 0 &&
		reln->smgr_relpersistence == RELPERSISTENCE_PERMANENT &&
		!AmAutoVacuumWorkerProcess())
	{
		uint64		current_size = GetNeonCurrentClusterSize();

		if (current_size >= ((uint64) max_cluster_size) * 1024 * 1024)
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("could not extend file because project size limit (%d MB) has been exceeded",
							max_cluster_size),
					 errhint("This limit is defined externally by the project size limit, and internally by neon.max_cluster_size GUC")));
	}

	/*
	 * Usually Postgres doesn't extend relation on more than one page (leaving
	 * holes). But this rule is violated in PG-15 where
	 * CreateAndCopyRelationData call smgrextend for destination relation n
	 * using size of source relation
	 */
	n_blocks = neon_nblocks(reln, forkNum);
	while (n_blocks < blkno)
		neon_wallog_page(reln, forkNum, n_blocks++, buffer, true);

	neon_wallog_page(reln, forkNum, blkno, buffer, false);
	set_cached_relsize(InfoFromSMgrRel(reln), forkNum, blkno + 1);

	lsn = PageGetLSN((Page) buffer);
	neon_log(SmgrTrace, "smgrextend called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forkNum, blkno,
		 (uint32) (lsn >> 32), (uint32) lsn);

	lfc_write(InfoFromSMgrRel(reln), forkNum, blkno, buffer);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdextend(reln, forkNum, blkno, buffer, skipFsync);
#endif

	/*
	 * smgr_extend is often called with an all-zeroes page, so
	 * lsn==InvalidXLogRecPtr. An smgr_write() call will come for the buffer
	 * later, after it has been initialized with the real page contents, and
	 * it is eventually evicted from the buffer cache. But we need a valid LSN
	 * to the relation metadata update now.
	 */
	if (lsn == InvalidXLogRecPtr)
	{
		lsn = GetXLogInsertRecPtr();
		neon_set_lwlsn_block(lsn, InfoFromSMgrRel(reln), forkNum, blkno);
	}
	neon_set_lwlsn_relation(lsn, InfoFromSMgrRel(reln), forkNum);
}

#if PG_MAJORVERSION_NUM >= 16
static void
neon_zeroextend(SMgrRelation reln, ForkNumber forkNum, BlockNumber blocknum,
				int nblocks, bool skipFsync)
{
	const PGIOAlignedBlock buffer = {0};
	int			remblocks = nblocks;
	XLogRecPtr	lsn = 0;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrextend() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdzeroextend(reln, forkNum, blocknum, nblocks, skipFsync);
			/* Update LFC in case of unlogged index build */
			if (reln == unlogged_build_rel && unlogged_build_phase == UNLOGGED_BUILD_PHASE_2)
			{
				for (int i = 0; i < nblocks; i++)
				{
					lfc_write(InfoFromSMgrRel(reln), forkNum, blocknum + i, buffer.data);
				}
			}
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (max_cluster_size > 0 &&
		reln->smgr_relpersistence == RELPERSISTENCE_PERMANENT &&
		!AmAutoVacuumWorkerProcess())
	{
		uint64		current_size = GetNeonCurrentClusterSize();

		if (current_size >= ((uint64) max_cluster_size) * 1024 * 1024)
			ereport(ERROR,
					(errcode(ERRCODE_DISK_FULL),
					 errmsg("could not extend file because project size limit (%d MB) has been exceeded",
							max_cluster_size),
					 errhint("This limit is defined by neon.max_cluster_size GUC")));
	}

	/*
	 * If a relation manages to grow to 2^32-1 blocks, refuse to extend it any
	 * more --- we mustn't create a block whose number actually is
	 * InvalidBlockNumber or larger.
	 */
	if ((uint64) blocknum + nblocks >= (uint64) InvalidBlockNumber)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg(NEON_TAG "cannot extend file \"%s\" beyond %u blocks",
						relpath(reln->smgr_rlocator, forkNum),
						InvalidBlockNumber)));

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdzeroextend(reln, forkNum, blocknum, nblocks, skipFsync);
#endif

	/* Don't log any pages if we're not allowed to do so. */
	if (!XLogInsertAllowed())
		return;

	/* ensure we have enough xlog buffers to log max-sized records */
	XLogEnsureRecordSpace(Min(remblocks, (XLR_MAX_BLOCK_ID - 1)), 0);

	/*
	 * Iterate over all the pages. They are collected into batches of
	 * XLR_MAX_BLOCK_ID pages, and a single WAL-record is written for each
	 * batch.
	 */
	while (remblocks > 0)
	{
		int			count = Min(remblocks, XLR_MAX_BLOCK_ID);

		XLogBeginInsert();

		for (int i = 0; i < count; i++)
			XLogRegisterBlock(i, &InfoFromSMgrRel(reln), forkNum, blocknum + i,
							  (char *) buffer.data, REGBUF_FORCE_IMAGE | REGBUF_STANDARD);

		lsn = XLogInsert(RM_XLOG_ID, XLOG_FPI);

		for (int i = 0; i < count; i++)
		{
			lfc_write(InfoFromSMgrRel(reln), forkNum, blocknum + i, buffer.data);
			neon_set_lwlsn_block(lsn, InfoFromSMgrRel(reln), forkNum,
									  blocknum + i);
		}

		blocknum += count;
		remblocks -= count;
	}

	Assert(lsn != 0);

	neon_set_lwlsn_relation(lsn, InfoFromSMgrRel(reln), forkNum);
	set_cached_relsize(InfoFromSMgrRel(reln), forkNum, blocknum);
}
#endif

/*
 *  neon_open() -- Initialize newly-opened relation.
 */
static void
neon_open(SMgrRelation reln)
{
	/*
	 * We don't have anything special to do here. Call mdopen() to let md.c
	 * initialize itself. That's only needed for temporary or unlogged
	 * relations, but it's dirt cheap so do it always to make sure the md
	 * fields are initialized, for debugging purposes if nothing else.
	 */
	mdopen(reln);

	/* no work */
	neon_log(SmgrTrace, "open noop");
}

/*
 *	neon_close() -- Close the specified relation, if it isn't closed already.
 */
static void
neon_close(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * Let md.c close it, if it had it open. Doesn't hurt to do this even for
	 * permanent relations that have no local storage.
	 */
	mdclose(reln, forknum);
}


#if PG_MAJORVERSION_NUM >= 17
/*
 *	neon_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
static bool
neon_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			  int nblocks)
{
	BufferTag	tag;

	switch (reln->smgr_relpersistence)
	{
		case 0:					/* probably shouldn't happen, but ignore it */
		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdprefetch(reln, forknum, blocknum, nblocks);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	tag.spcOid = reln->smgr_rlocator.locator.spcOid;
	tag.dbOid = reln->smgr_rlocator.locator.dbOid;
	tag.relNumber = reln->smgr_rlocator.locator.relNumber;
	tag.forkNum = forknum;

	while (nblocks > 0)
	{
		int		iterblocks = Min(nblocks, PG_IOV_MAX);
		bits8	lfc_present[PG_IOV_MAX / 8] = {0};

		if (lfc_cache_containsv(InfoFromSMgrRel(reln), forknum, blocknum,
								iterblocks, lfc_present) == iterblocks)
		{
			nblocks -= iterblocks;
			blocknum += iterblocks;
			continue;
		}

		tag.blockNum = blocknum;

		communicator_prefetch_register_bufferv(tag, NULL, iterblocks, lfc_present);

		nblocks -= iterblocks;
		blocknum += iterblocks;
	}

	communicator_prefetch_pump_state();

	return false;
}


#else /* PG_MAJORVERSION_NUM >= 17 */
/*
 *	neon_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
static bool
neon_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	BufferTag	tag;

	switch (reln->smgr_relpersistence)
	{
		case 0:					/* probably shouldn't happen, but ignore it */
		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdprefetch(reln, forknum, blocknum);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (lfc_cache_contains(InfoFromSMgrRel(reln), forknum, blocknum))
		return false;

	tag.forkNum = forknum;
	tag.blockNum = blocknum;

	CopyNRelFileInfoToBufTag(tag, InfoFromSMgrRel(reln));

	communicator_prefetch_register_bufferv(tag, NULL, 1, NULL);

	communicator_prefetch_pump_state();

	return false;
}
#endif /* PG_MAJORVERSION_NUM < 17 */


/*
 * neon_writeback() -- Tell the kernel to write pages back to storage.
 *
 * This accepts a range of blocks because flushing several pages at once is
 * considerably more efficient than doing so individually.
 */
static void
neon_writeback(SMgrRelation reln, ForkNumber forknum,
			   BlockNumber blocknum, BlockNumber nblocks)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			/* mdwriteback() does nothing if the file doesn't exist */
			mdwriteback(reln, forknum, blocknum, nblocks);
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwriteback(reln, forknum, blocknum, nblocks);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/*
	 * TODO: WAL sync up to lwLsn for the indicated blocks
	 * Without that sync, writeback doesn't actually guarantee the data is
	 * persistently written, which does seem to be one of the assumed
	 * properties of this smgr API call.
	 */
	neon_log(SmgrTrace, "writeback noop");

	communicator_prefetch_pump_state();

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwriteback(reln, forknum, blocknum, nblocks);
#endif
}

/*
 * While function is defined in the neon extension it's used within neon_test_utils directly.
 * To avoid breaking tests in the runtime please keep function signature in sync.
 */
void
neon_read_at_lsn(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
				 neon_request_lsns request_lsns, void *buffer)
{
	communicator_read_at_lsnv(rinfo, forkNum, blkno, &request_lsns, &buffer, 1, NULL);
}

#if PG_MAJORVERSION_NUM < 17
/*
 *	neon_read() -- Read the specified block from a relation.
 */
#if PG_MAJORVERSION_NUM < 16
static void
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno, char *buffer)
#else
static void
neon_read(SMgrRelation reln, ForkNumber forkNum, BlockNumber blkno, void *buffer)
#endif
{
	neon_request_lsns request_lsns;
	bits8		present;
	void	   *bufferp;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrread() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdread(reln, forkNum, blkno, buffer);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	/* Try to read PS results if they are available */
	communicator_prefetch_pump_state();

	neon_get_request_lsns(InfoFromSMgrRel(reln), forkNum, blkno, &request_lsns, 1);

	present = 0;
	bufferp = buffer;
	if (communicator_prefetch_lookupv(InfoFromSMgrRel(reln), forkNum, blkno, &request_lsns, 1, &bufferp, &present))
	{
		/* Prefetch hit */
		return;
	}

	/* Try to read from local file cache */
	if (lfc_read(InfoFromSMgrRel(reln), forkNum, blkno, buffer))
	{
		MyNeonCounters->file_cache_hits_total++;
		return;
	}

	neon_read_at_lsn(InfoFromSMgrRel(reln), forkNum, blkno, request_lsns, buffer);

	/*
	 * Try to receive prefetch results once again just to make sure we don't leave the smgr code while the OS might still have buffered bytes.
	 */
	communicator_prefetch_pump_state();

#ifdef DEBUG_COMPARE_LOCAL
	if (forkNum == MAIN_FORKNUM && IS_LOCAL_REL(reln))
	{
		char		pageserver_masked[BLCKSZ];
		PGIOAlignedBlock mdbuf;
		PGIOAlignedBlock mdbuf_masked;
		XLogRecPtr  request_lsn = request_lsns.request_lsn;

		mdread(reln, forkNum, blkno, mdbuf.data);

		memcpy(pageserver_masked, buffer, BLCKSZ);
		memcpy(mdbuf_masked.data, mdbuf.data, BLCKSZ);

		if (PageIsNew((Page) mdbuf.data))
		{
			if (!PageIsNew((Page) pageserver_masked))
			{
				neon_log(PANIC, "page is new in MD but not in Page Server at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
					 blkno,
					 RelFileInfoFmt(InfoFromSMgrRel(reln)),
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(buffer));
			}
		}
		else if (PageIsNew((Page) buffer))
		{
			neon_log(PANIC, "page is new in Page Server but not in MD at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
				 blkno,
				 RelFileInfoFmt(InfoFromSMgrRel(reln)),
				 forkNum,
				 (uint32) (request_lsn >> 32), (uint32) request_lsn,
				 hexdump_page(mdbuf.data));
		}
		else if (PageGetSpecialSize(mdbuf.data) == 0)
		{
			/* assume heap */
			RmgrTable[RM_HEAP_ID].rm_mask(mdbuf_masked.data, blkno);
			RmgrTable[RM_HEAP_ID].rm_mask(pageserver_masked, blkno);

			if (memcmp(mdbuf_masked.data, pageserver_masked, BLCKSZ) != 0)
			{
				neon_log(PANIC, "heap buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
					 blkno,
					 RelFileInfoFmt(InfoFromSMgrRel(reln)),
					 forkNum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(mdbuf_masked.data),
					 hexdump_page(pageserver_masked));
			}
		}
		else if (PageGetSpecialSize(mdbuf.data) == MAXALIGN(sizeof(BTPageOpaqueData)))
		{
			if (((BTPageOpaqueData *) PageGetSpecialPointer(mdbuf.data))->btpo_cycleid < MAX_BT_CYCLE_ID)
			{
				/* assume btree */
				RmgrTable[RM_BTREE_ID].rm_mask(mdbuf_masked.data, blkno);
				RmgrTable[RM_BTREE_ID].rm_mask(pageserver_masked, blkno);

				if (memcmp(mdbuf_masked.data, pageserver_masked, BLCKSZ) != 0)
				{
					neon_log(PANIC, "btree buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
						 blkno,
						 RelFileInfoFmt(InfoFromSMgrRel(reln)),
						 forkNum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(mdbuf_masked.data),
						 hexdump_page(pageserver_masked));
				}
			}
		}
	}
#endif
}
#endif /* PG_MAJORVERSION_NUM <= 16 */

#if PG_MAJORVERSION_NUM >= 17
static void
neon_readv(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
		   void **buffers, BlockNumber nblocks)
{
	bits8		read_pages[PG_IOV_MAX / 8];
	neon_request_lsns request_lsns[PG_IOV_MAX];
	int			lfc_result;
	int			prefetch_result;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrread() on rel with unknown persistence");

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdreadv(reln, forknum, blocknum, buffers, nblocks);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (nblocks > PG_IOV_MAX)
		neon_log(ERROR, "Read request too large: %d is larger than max %d",
				 nblocks, PG_IOV_MAX);

	/* Try to read PS results if they are available */
	communicator_prefetch_pump_state();

	neon_get_request_lsns(InfoFromSMgrRel(reln), forknum, blocknum,
						  request_lsns, nblocks);

	memset(read_pages, 0, sizeof(read_pages));

	prefetch_result = communicator_prefetch_lookupv(InfoFromSMgrRel(reln), forknum,
													blocknum, request_lsns, nblocks,
													buffers, read_pages);

	if (prefetch_result == nblocks)
		return;

	/* Try to read from local file cache */
	lfc_result = lfc_readv_select(InfoFromSMgrRel(reln), forknum, blocknum, buffers,
								  nblocks, read_pages);

	if (lfc_result > 0)
		MyNeonCounters->file_cache_hits_total += lfc_result;

	/* Read all blocks from LFC, so we're done */
	if (prefetch_result + lfc_result == nblocks)
		return;

	communicator_read_at_lsnv(InfoFromSMgrRel(reln), forknum, blocknum, request_lsns,
							  buffers, nblocks, read_pages);

	/*
	 * Try to receive prefetch results once again just to make sure we don't leave the smgr code while the OS might still have buffered bytes.
	 */
	communicator_prefetch_pump_state();

#ifdef DEBUG_COMPARE_LOCAL
	if (forknum == MAIN_FORKNUM && IS_LOCAL_REL(reln))
	{
		char		pageserver_masked[BLCKSZ];
		PGIOAlignedBlock mdbuf;
		PGIOAlignedBlock mdbuf_masked;
		XLogRecPtr  request_lsn = request_lsns->request_lsn;

		for (int i = 0; i < nblocks; i++)
		{
			BlockNumber blkno = blocknum + i;
			if (!BITMAP_ISSET(read_pages, i))
				continue;

#if PG_MAJORVERSION_NUM >= 17
			{
				void* mdbuffers[1] = { mdbuf.data };
				mdreadv(reln, forknum, blkno, mdbuffers, 1);
			}
#else
			mdread(reln, forknum, blkno, mdbuf.data);
#endif

			memcpy(pageserver_masked, buffers[i], BLCKSZ);
			memcpy(mdbuf_masked.data, mdbuf.data, BLCKSZ);

			if (PageIsNew((Page) mdbuf.data))
			{
				if (!PageIsNew((Page) pageserver_masked))
				{
					neon_log(PANIC, "page is new in MD but not in Page Server at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
						 blkno,
						 RelFileInfoFmt(InfoFromSMgrRel(reln)),
						 forknum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(buffers[i]));
				}
			}
			else if (PageIsNew((Page) buffers[i]))
			{
				neon_log(PANIC, "page is new in Page Server but not in MD at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n%s\n",
					 blkno,
					 RelFileInfoFmt(InfoFromSMgrRel(reln)),
					 forknum,
					 (uint32) (request_lsn >> 32), (uint32) request_lsn,
					 hexdump_page(mdbuf.data));
			}
			else if (PageGetSpecialSize(mdbuf.data) == 0)
			{
				/* assume heap */
				RmgrTable[RM_HEAP_ID].rm_mask(mdbuf_masked.data, blkno);
				RmgrTable[RM_HEAP_ID].rm_mask(pageserver_masked, blkno);

				if (memcmp(mdbuf_masked.data, pageserver_masked, BLCKSZ) != 0)
				{
					neon_log(PANIC, "heap buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
						 blkno,
						 RelFileInfoFmt(InfoFromSMgrRel(reln)),
						 forknum,
						 (uint32) (request_lsn >> 32), (uint32) request_lsn,
						 hexdump_page(mdbuf_masked.data),
						 hexdump_page(pageserver_masked));
				}
			}
			else if (PageGetSpecialSize(mdbuf.data) == MAXALIGN(sizeof(BTPageOpaqueData)))
			{
				if (((BTPageOpaqueData *) PageGetSpecialPointer(mdbuf.data))->btpo_cycleid < MAX_BT_CYCLE_ID)
				{
					/* assume btree */
					RmgrTable[RM_BTREE_ID].rm_mask(mdbuf_masked.data, blkno);
					RmgrTable[RM_BTREE_ID].rm_mask(pageserver_masked, blkno);
	
					if (memcmp(mdbuf_masked.data, pageserver_masked, BLCKSZ) != 0)
					{
						neon_log(PANIC, "btree buffers differ at blk %u in rel %u/%u/%u fork %u (request LSN %X/%08X):\n------ MD ------\n%s\n------ Page Server ------\n%s\n",
							 blkno,
							 RelFileInfoFmt(InfoFromSMgrRel(reln)),
							 forknum,
							 (uint32) (request_lsn >> 32), (uint32) request_lsn,
							 hexdump_page(mdbuf_masked.data),
							 hexdump_page(pageserver_masked));
					}
				}
			}
		}
	}
#endif
}
#endif

#ifdef DEBUG_COMPARE_LOCAL
static char *
hexdump_page(char *page)
{
	StringInfoData result;

	initStringInfo(&result);

	for (int i = 0; i < BLCKSZ; i++)
	{
		if (i % 8 == 0)
			appendStringInfo(&result, " ");
		if (i % 40 == 0)
			appendStringInfo(&result, "\n");
		appendStringInfo(&result, "%02x", (unsigned char) (page[i]));
	}

	return result.data;
}
#endif

#if PG_MAJORVERSION_NUM < 17
/*
 *	neon_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
static void
#if PG_MAJORVERSION_NUM < 16
neon_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skipFsync)
#else
neon_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const void *buffer, bool skipFsync)
#endif
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
#ifndef DEBUG_COMPARE_LOCAL
			/* This is a bit tricky. Check if the relation exists locally */
			if (mdexists(reln, forknum))
#else
			if (mdexists(reln, INIT_FORKNUM))
#endif
			{
				/* It exists locally. Guess it's unlogged then. */
#if PG_MAJORVERSION_NUM >= 17
				mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
#else
				mdwrite(reln, forknum, blocknum, buffer, skipFsync);
#endif
				/*
				 * We could set relpersistence now that we have determined
				 * that it's local. But we don't dare to do it, because that
				 * would immediately allow reads as well, which shouldn't
				 * happen. We could cache it with a different 'relpersistence'
				 * value, but this isn't performance critical.
				 */
				return;
			}
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			#if PG_MAJORVERSION_NUM >= 17
			mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
			#else
			mdwrite(reln, forknum, blocknum, buffer, skipFsync);
			#endif
			/* Update LFC in case of unlogged index build */
			if (reln == unlogged_build_rel && unlogged_build_phase == UNLOGGED_BUILD_PHASE_2)
				lfc_write(InfoFromSMgrRel(reln), forknum, blocknum, buffer);
			return;
		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_wallog_page(reln, forknum, blocknum, buffer, false);

	lsn = PageGetLSN((Page) buffer);
	neon_log(SmgrTrace, "smgrwrite called for %u/%u/%u.%u blk %u, page LSN: %X/%08X",
		 RelFileInfoFmt(InfoFromSMgrRel(reln)),
		 forknum, blocknum,
		 (uint32) (lsn >> 32), (uint32) lsn);

	lfc_write(InfoFromSMgrRel(reln), forknum, blocknum, buffer);

	communicator_prefetch_pump_state();

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		#if PG_MAJORVERSION_NUM >= 17
		mdwritev(reln, forknum, blocknum, &buffer, 1, skipFsync);
		#else
		mdwrite(reln, forknum, blocknum, buffer, skipFsync);
		#endif
#endif
}
#endif



#if PG_MAJORVERSION_NUM >= 17
static void
neon_writev(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 const void **buffers, BlockNumber nblocks, bool skipFsync)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
#ifndef DEBUG_COMPARE_LOCAL
			/* This is a bit tricky. Check if the relation exists locally */
			if (mdexists(reln, forknum))
#else
			if (mdexists(reln, INIT_FORKNUM))
#endif
			{
				/* It exists locally. Guess it's unlogged then. */
				mdwritev(reln, forknum, blkno, buffers, nblocks, skipFsync);

				/*
				 * We could set relpersistence now that we have determined
				 * that it's local. But we don't dare to do it, because that
				 * would immediately allow reads as well, which shouldn't
				 * happen. We could cache it with a different 'relpersistence'
				 * value, but this isn't performance critical.
				 */
				return;
			}
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdwritev(reln, forknum, blkno, buffers, nblocks, skipFsync);
			/* Update LFC in case of unlogged index build */
			if (reln == unlogged_build_rel && unlogged_build_phase == UNLOGGED_BUILD_PHASE_2)
				lfc_writev(InfoFromSMgrRel(reln), forknum, blkno, buffers, nblocks);
			return;
		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_wallog_pagev(reln, forknum, blkno, nblocks, (const char **) buffers, false);

	lfc_writev(InfoFromSMgrRel(reln), forknum, blkno, buffers, nblocks);

	communicator_prefetch_pump_state();

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdwritev(reln, forknum, blkno, buffers, nblocks, skipFsync);
#endif
}

#endif

/*
 *	neon_nblocks() -- Get the number of blocks stored in a relation.
 */
static BlockNumber
neon_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	BlockNumber n_blocks;
	neon_request_lsns request_lsns;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrnblocks() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			return mdnblocks(reln, forknum);

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	if (get_cached_relsize(InfoFromSMgrRel(reln), forknum, &n_blocks))
	{
		neon_log(SmgrTrace, "cached nblocks for %u/%u/%u.%u: %u blocks",
			 RelFileInfoFmt(InfoFromSMgrRel(reln)),
			 forknum, n_blocks);
		return n_blocks;
	}

	neon_get_request_lsns(InfoFromSMgrRel(reln), forknum,
						  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1);

	n_blocks = communicator_nblocks(InfoFromSMgrRel(reln), forknum, &request_lsns);
	update_cached_relsize(InfoFromSMgrRel(reln), forknum, n_blocks);

	neon_log(SmgrTrace, "neon_nblocks: rel %u/%u/%u fork %u (request LSN %X/%08X): %u blocks",
			 RelFileInfoFmt(InfoFromSMgrRel(reln)),
			 forknum,
			 LSN_FORMAT_ARGS(request_lsns.effective_request_lsn),
			 n_blocks);

	return n_blocks;
}

/*
 *	neon_db_size() -- Get the size of the database in bytes.
 */
int64
neon_dbsize(Oid dbNode)
{
	int64		db_size;
	neon_request_lsns request_lsns;
	NRelFileInfo dummy_node = {0};

	neon_get_request_lsns(dummy_node, MAIN_FORKNUM,
						  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1);

	db_size = communicator_dbsize(dbNode, &request_lsns);

	neon_log(SmgrTrace, "neon_dbsize: db %u (request LSN %X/%08X): %ld bytes",
			 dbNode, LSN_FORMAT_ARGS(request_lsns.effective_request_lsn), db_size);

	return db_size;
}

/*
 *	neon_truncate() -- Truncate relation to specified number of blocks.
 */
static void
neon_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks, BlockNumber nblocks)
{
	XLogRecPtr	lsn;

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrtruncate() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdtruncate(reln, forknum, old_blocks, nblocks);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	set_cached_relsize(InfoFromSMgrRel(reln), forknum, nblocks);

	/*
	 * Truncating a relation drops all its buffers from the buffer cache
	 * without calling smgrwrite() on them. But we must account for that in
	 * our tracking of last-written-LSN all the same: any future smgrnblocks()
	 * request must return the new size after the truncation. We don't know
	 * what the LSN of the truncation record was, so be conservative and use
	 * the most recently inserted WAL record's LSN.
	 */
	lsn = GetXLogInsertRecPtr();
	lsn = nm_adjust_lsn(lsn);

	/*
	 * Flush it, too. We don't actually care about it here, but let's uphold
	 * the invariant that last-written LSN <= flush LSN.
	 */
	XLogFlush(lsn);

	/*
	 * Truncate may affect several chunks of relations. So we should either
	 * update last written LSN for all of them, or update LSN for "dummy"
	 * metadata block. Second approach seems more efficient. If the relation
	 * is extended again later, the extension will update the last-written LSN
	 * for the extended pages, so there's no harm in leaving behind obsolete
	 * entries for the truncated chunks.
	 */
	neon_set_lwlsn_relation(lsn, InfoFromSMgrRel(reln), forknum);

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdtruncate(reln, forknum, old_blocks, nblocks);
#endif
}

/*
 *	neon_immedsync() -- Immediately sync a relation to stable storage.
 *
 * Note that only writes already issued are synced; this routine knows
 * nothing of dirty buffers that may exist inside the buffer manager.  We
 * sync active and inactive segments; smgrDoPendingSyncs() relies on this.
 * Consider a relation skipping WAL.  Suppose a checkpoint syncs blocks of
 * some segment, then mdtruncate() renders that segment inactive.  If we
 * crash before the next checkpoint syncs the newly-inactive segment, that
 * segment may survive recovery, reintroducing unwanted data into the table.
 */
static void
neon_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrimmedsync() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdimmedsync(reln, forknum);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_log(SmgrTrace, "[NEON_SMGR] immedsync noop");

	communicator_prefetch_pump_state();

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdimmedsync(reln, forknum);
#endif
}

#if PG_MAJORVERSION_NUM >= 17
static void
neon_registersync(SMgrRelation reln, ForkNumber forknum)
{
	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgrregistersync() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			mdregistersync(reln, forknum);
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

	neon_log(SmgrTrace, "[NEON_SMGR] registersync noop");

#ifdef DEBUG_COMPARE_LOCAL
	if (IS_LOCAL_REL(reln))
		mdimmedsync(reln, forknum);
#endif
}
#endif


/*
 * neon_start_unlogged_build() -- Starting build operation on a rel.
 *
 * Some indexes are built in two phases, by first populating the table with
 * regular inserts, using the shared buffer cache but skipping WAL-logging,
 * and WAL-logging the whole relation after it's done. Neon relies on the
 * WAL to reconstruct pages, so we cannot use the page server in the
 * first phase when the changes are not logged.
 */
static void
neon_start_unlogged_build(SMgrRelation reln)
{
	/*
	 * Currently, there can be only one unlogged relation build operation in
	 * progress at a time. That's enough for the current usage.
	 */
	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
		neon_log(ERROR, "unlogged relation build is already in progress");
	Assert(unlogged_build_rel == NULL);

	ereport(SmgrTrace,
			(errmsg(NEON_TAG "starting unlogged build of relation %u/%u/%u",
					RelFileInfoFmt(InfoFromSMgrRel(reln)))));

	switch (reln->smgr_relpersistence)
	{
		case 0:
			neon_log(ERROR, "cannot call smgr_start_unlogged_build() on rel with unknown persistence");
			break;

		case RELPERSISTENCE_PERMANENT:
			break;

		case RELPERSISTENCE_TEMP:
		case RELPERSISTENCE_UNLOGGED:
			unlogged_build_rel = reln;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_PERMANENT;
#ifdef DEBUG_COMPARE_LOCAL
			if (!IsParallelWorker())
				mdcreate(reln, INIT_FORKNUM, true);
#endif
			return;

		default:
			neon_log(ERROR, "unknown relpersistence '%c'", reln->smgr_relpersistence);
	}

#if PG_MAJORVERSION_NUM >= 17
	/*
	 * We have to disable this check for pg14-16 because sorted build of GIST index requires
	 * to perform unlogged build several times
	 */
	if (smgrnblocks(reln, MAIN_FORKNUM) != 0)
		neon_log(ERROR, "cannot perform unlogged index build, index is not empty ");
#endif

	unlogged_build_rel = reln;
	unlogged_build_phase = UNLOGGED_BUILD_PHASE_1;

	/* Make the relation look like it's unlogged */
	reln->smgr_relpersistence = RELPERSISTENCE_UNLOGGED;

	/*
	 * Create the local file. In a parallel build, the leader is expected to
	 * call this first and do it.
	 *
	 * FIXME: should we pass isRedo true to create the tablespace dir if it
	 * doesn't exist? Is it needed?
	 */
 	if (!IsParallelWorker())
	{
#ifndef DEBUG_COMPARE_LOCAL
		mdcreate(reln, MAIN_FORKNUM, false);
#else
		mdcreate(reln, INIT_FORKNUM, true);
#endif
	}
}

/*
 * neon_finish_unlogged_build_phase_1()
 *
 * Call this after you have finished populating a relation in unlogged mode,
 * before you start WAL-logging it.
 */
static void
neon_finish_unlogged_build_phase_1(SMgrRelation reln)
{
	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg(NEON_TAG "finishing phase 1 of unlogged build of relation %u/%u/%u",
					RelFileInfoFmt(InfoFromSMgrRel(reln)))));

	if (unlogged_build_phase == UNLOGGED_BUILD_NOT_PERMANENT)
		return;

	Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_1);
	Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

	/*
	 * In a parallel build, (only) the leader process performs the 2nd
	 * phase.
	 */
	if (IsParallelWorker())
	{
		unlogged_build_rel = NULL;
		unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
	}
	else
		unlogged_build_phase = UNLOGGED_BUILD_PHASE_2;
}

/*
 * neon_end_unlogged_build() -- Finish an unlogged rel build.
 *
 * Call this after you have finished WAL-logging a relation that was
 * first populated without WAL-logging.
 *
 * This removes the local copy of the rel, since it's now been fully
 * WAL-logged and is present in the page server.
 */
static void
neon_end_unlogged_build(SMgrRelation reln)
{
	NRelFileInfoBackend rinfob = InfoBFromSMgrRel(reln);

	Assert(unlogged_build_rel == reln);

	ereport(SmgrTrace,
			(errmsg(NEON_TAG "ending unlogged build of relation %u/%u/%u",
					RelFileInfoFmt(InfoFromNInfoB(rinfob)))));

	if (unlogged_build_phase != UNLOGGED_BUILD_NOT_PERMANENT)
	{
		XLogRecPtr recptr;
		BlockNumber nblocks;

		Assert(unlogged_build_phase == UNLOGGED_BUILD_PHASE_2);
		Assert(reln->smgr_relpersistence == RELPERSISTENCE_UNLOGGED);

		/*
		 * Update the last-written LSN cache.
		 *
		 * The relation is still on local disk so we can get the size by
		 * calling mdnblocks() directly. For the LSN, GetXLogInsertRecPtr() is
		 * very conservative. If we could assume that this function is called
		 * from the same backend that WAL-logged the contents, we could use
		 * XactLastRecEnd here. But better safe than sorry.
		 */
		nblocks = mdnblocks(reln, MAIN_FORKNUM);
		recptr = GetXLogInsertRecPtr();

		neon_set_lwlsn_block_range(recptr,
								   InfoFromNInfoB(rinfob),
								   MAIN_FORKNUM, 0, nblocks);
		neon_set_lwlsn_relation(recptr,
								InfoFromNInfoB(rinfob),
								MAIN_FORKNUM);

		/* Make the relation look permanent again */
		reln->smgr_relpersistence = RELPERSISTENCE_PERMANENT;

		/* Remove local copy */
		for (int forknum = 0; forknum <= MAX_FORKNUM; forknum++)
		{
			neon_log(SmgrTrace, "forgetting cached relsize for %u/%u/%u.%u",
				 RelFileInfoFmt(InfoFromNInfoB(rinfob)),
				 forknum);

			forget_cached_relsize(InfoFromNInfoB(rinfob), forknum);
			mdclose(reln, forknum);
#ifndef DEBUG_COMPARE_LOCAL
			/* use isRedo == true, so that we drop it immediately */
			mdunlink(rinfob, forknum, true);
#endif
		}
#ifdef DEBUG_COMPARE_LOCAL
		mdunlink(rinfob, INIT_FORKNUM, true);
#endif
	}
	unlogged_build_rel = NULL;
	unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
}

#define STRPREFIX(str, prefix) (strncmp(str, prefix, strlen(prefix)) == 0)

static int
neon_read_slru_segment(SMgrRelation reln, const char* path, int segno, void* buffer)
{
	XLogRecPtr	request_lsn,
				not_modified_since;
	SlruKind	kind;
	int			n_blocks;
	neon_request_lsns request_lsns;

	/*
	 * Compute a request LSN to use, similar to neon_get_request_lsns() but the
	 * logic is a bit simpler.
	 */
	if (RecoveryInProgress())
	{
		request_lsn = GetXLogReplayRecPtr(NULL);
		if (request_lsn == InvalidXLogRecPtr)
		{
			/*
			 * This happens in neon startup, we start up without replaying any
			 * records.
			 */
			request_lsn = GetRedoStartLsn();
		}
		request_lsn = nm_adjust_lsn(request_lsn);
	}
	else
		request_lsn = UINT64_MAX;

	/*
	 * GetRedoStartLsn() returns LSN of the basebackup. We know that the SLRU
	 * segment has not changed since the basebackup, because in order to
	 * modify it, we would have had to download it already. And once
	 * downloaded, we never evict SLRU segments from local disk.
	 */
	not_modified_since = nm_adjust_lsn(GetRedoStartLsn());

	if (STRPREFIX(path, "pg_xact"))
		kind = SLRU_CLOG;
	else if (STRPREFIX(path, "pg_multixact/members"))
		kind = SLRU_MULTIXACT_MEMBERS;
	else if (STRPREFIX(path, "pg_multixact/offsets"))
		kind = SLRU_MULTIXACT_OFFSETS;
	else
		return -1;

	request_lsns.request_lsn = request_lsn;
	request_lsns.not_modified_since = not_modified_since;
	request_lsns.effective_request_lsn = request_lsn;

	n_blocks = communicator_read_slru_segment(kind, segno, &request_lsns, buffer);

	return n_blocks;
}

static void
AtEOXact_neon(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * Forget about any build we might have had in progress. The local
			 * file will be unlinked by smgrDoPendingDeletes()
			 */
			unlogged_build_rel = NULL;
			unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
			break;

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_PREPARE:
			if (unlogged_build_phase != UNLOGGED_BUILD_NOT_IN_PROGRESS)
			{
				unlogged_build_rel = NULL;
				unlogged_build_phase = UNLOGGED_BUILD_NOT_IN_PROGRESS;
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 (errmsg(NEON_TAG "unlogged index build was not properly finished"))));
			}
			break;
	}
	communicator_reconfigure_timeout_if_needed();
}

static const struct f_smgr neon_smgr =
{
	.smgr_init = neon_init,
	.smgr_shutdown = NULL,
	.smgr_open = neon_open,
	.smgr_close = neon_close,
	.smgr_create = neon_create,
	.smgr_exists = neon_exists,
	.smgr_unlink = neon_unlink,
	.smgr_extend = neon_extend,
#if PG_MAJORVERSION_NUM >= 16
	.smgr_zeroextend = neon_zeroextend,
#endif
#if PG_MAJORVERSION_NUM >= 17
	.smgr_prefetch = neon_prefetch,
	.smgr_readv = neon_readv,
	.smgr_writev = neon_writev,
#else
	.smgr_prefetch = neon_prefetch,
	.smgr_read = neon_read,
	.smgr_write = neon_write,
#endif

	.smgr_writeback = neon_writeback,
	.smgr_nblocks = neon_nblocks,
	.smgr_truncate = neon_truncate,
	.smgr_immedsync = neon_immedsync,
#if PG_MAJORVERSION_NUM >= 17
	.smgr_registersync = neon_registersync,
#endif
	.smgr_start_unlogged_build = neon_start_unlogged_build,
	.smgr_finish_unlogged_build_phase_1 = neon_finish_unlogged_build_phase_1,
	.smgr_end_unlogged_build = neon_end_unlogged_build,

	.smgr_read_slru_segment = neon_read_slru_segment,
};

const f_smgr *
smgr_neon(ProcNumber backend, NRelFileInfo rinfo)
{

	/* Don't use page server for temp relations */
	if (backend != INVALID_PROC_NUMBER)
		return smgr_standard(backend, rinfo);
	else
		return &neon_smgr;
}

void
smgr_init_neon(void)
{
	RegisterXactCallback(AtEOXact_neon, NULL);

	smgr_init_standard();
	neon_init();
	communicator_init();
}


static void
neon_extend_rel_size(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno, XLogRecPtr end_recptr)
{
	BlockNumber relsize;

	/* This is only used in WAL replay */
	Assert(RecoveryInProgress());

	/* Extend the relation if we know its size */
	if (get_cached_relsize(rinfo, forknum, &relsize))
	{
		if (relsize < blkno + 1)
		{
			update_cached_relsize(rinfo, forknum, blkno + 1);
			neon_set_lwlsn_relation(end_recptr, rinfo, forknum);
		}
	}
	else
	{
		/*
		 * Size was not cached. We populate the cache now, with the size of
		 * the relation measured after this WAL record is applied.
		 *
		 * This length is later reused when we open the smgr to read the
		 * block, which is fine and expected.
		 */
		neon_request_lsns request_lsns;

		neon_get_request_lsns(rinfo, forknum,
							  REL_METADATA_PSEUDO_BLOCKNO, &request_lsns, 1);

		relsize = communicator_nblocks(rinfo, forknum, &request_lsns);

		relsize = Max(relsize, blkno + 1);

		set_cached_relsize(rinfo, forknum, relsize);
		neon_set_lwlsn_relation(end_recptr, rinfo, forknum);

		neon_log(SmgrTrace, "Set length to %d", relsize);
	}
}

#define FSM_TREE_DEPTH	((SlotsPerFSMPage >= 1626) ? 3 : 4)

/*
 * TODO: May be it is better to make correspondent function from freespace.c public?
 */
static BlockNumber
get_fsm_physical_block(BlockNumber heapblk)
{
	BlockNumber pages;
	int			leafno;
	int			l;

	/*
	 * Calculate the logical page number of the first leaf page below the
	 * given page.
	 */
	leafno = heapblk / SlotsPerFSMPage;

	/* Count upper level nodes required to address the leaf page */
	pages = 0;
	for (l = 0; l < FSM_TREE_DEPTH; l++)
	{
		pages += leafno + 1;
		leafno /= SlotsPerFSMPage;
	}

	/* Turn the page count into 0-based block number */
	return pages - 1;
}


/*
 * Return whether we can skip the redo for this block.
 *
 * The conditions for skipping the IO are:
 *
 * - The block is not in the shared buffers, and
 * - The block is not in the local file cache
 *
 * ... because any subsequent read of the page requires us to read
 * the new version of the page from the PageServer. We do not
 * check the local file cache; we instead evict the page from LFC: it
 * is cheaper than going through the FS calls to read the page, and
 * limits the number of lock operations used in the REDO process.
 *
 * We have one exception to the rules for skipping IO: We always apply
 * changes to shared catalogs' pages. Although this is mostly out of caution,
 * catalog updates usually result in backends rebuilding their catalog snapshot,
 * which means it's quite likely the modified page is going to be used soon.
 *
 * It is important to note that skipping WAL redo for a page also means
 * the page isn't locked by the redo process, as there is no Buffer
 * being returned, nor is there a buffer descriptor to lock.
 * This means that any IO that wants to read this block needs to wait
 * for the WAL REDO process to finish processing the WAL record before
 * it allows the system to start reading the block, as releasing the
 * block early could lead to phantom reads.
 *
 * For example, REDO for a WAL record that modifies 3 blocks could skip
 * the first block, wait for a lock on the second, and then modify the
 * third block. Without skipping, all blocks would be locked and phantom
 * reads would not occur, but with skipping, a concurrent process could
 * read block 1 with post-REDO contents and read block 3 with pre-REDO
 * contents, where with REDO locking it would wait on block 1 and see
 * block 3 with post-REDO contents only.
 */
static bool
neon_redo_read_buffer_filter(XLogReaderState *record, uint8 block_id)
{
	XLogRecPtr	end_recptr = record->EndRecPtr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
	BlockNumber blkno;
	BufferTag	tag;
	uint32		hash;
	LWLock	   *partitionLock;
	int			buf_id;
	bool		no_redo_needed;

	if (old_redo_read_buffer_filter && old_redo_read_buffer_filter(record, block_id))
		return true;

#if PG_VERSION_NUM < 150000
	if (!XLogRecGetBlockTag(record, block_id, &rinfo, &forknum, &blkno))
		neon_log(PANIC, "failed to locate backup block with ID %d", block_id);
#else
	XLogRecGetBlockTag(record, block_id, &rinfo, &forknum, &blkno);
#endif

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forknum;
	tag.blockNum = blkno;

	hash = BufTableHashCode(&tag);
	partitionLock = BufMappingPartitionLock(hash);

	/*
	 * Lock the partition of shared_buffers so that it can't be updated
	 * concurrently.
	 */
	LWLockAcquire(partitionLock, LW_SHARED);

	/*
	 * Out of an abundance of caution, we always run redo on shared catalogs,
	 * regardless of whether the block is stored in shared buffers. See also
	 * this function's top comment.
	 */
	if (!OidIsValid(NInfoGetDbOid(rinfo)))
	{
		no_redo_needed = false;
	}
	else
	{
		/* Try to find the relevant buffer */
		buf_id = BufTableLookup(&tag, hash);

		no_redo_needed = buf_id < 0;
	}

	/*
	 * we don't have the buffer in memory, update lwLsn past this record, also
	 * evict page from file cache
	 */
	if (no_redo_needed)
	{
		neon_set_lwlsn_block(end_recptr, rinfo, forknum, blkno);
		/*
		 * Redo changes if page exists in LFC.
		 * We should perform this check after assigning LwLSN to prevent
		 * prefetching of some older version of the page by some other backend.
		 */
		no_redo_needed = !lfc_cache_contains(rinfo, forknum, blkno);
	}

	LWLockRelease(partitionLock);

	neon_extend_rel_size(rinfo, forknum, blkno, end_recptr);
	if (forknum == MAIN_FORKNUM)
	{
		neon_extend_rel_size(rinfo, FSM_FORKNUM, get_fsm_physical_block(blkno), end_recptr);
	}
	return no_redo_needed;
}
