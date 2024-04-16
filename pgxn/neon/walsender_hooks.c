/*-------------------------------------------------------------------------
 *
 * walsender_hooks.c
 *
 * Implements XLogReaderRoutine in terms of NeonWALReader. Allows for
 * fetching WAL from safekeepers, which normal xlogreader can't do.
 *
 *-------------------------------------------------------------------------
 */
#include "walsender_hooks.h"
#include "postgres.h"
#include "fmgr.h"
#include "access/xlogdefs.h"
#include "replication/walsender.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "utils/wait_event.h"
#include "utils/guc.h"
#include "postmaster/interrupt.h"

#include "neon_walreader.h"
#include "walproposer.h"

static NeonWALReader *wal_reader = NULL;
extern XLogRecPtr WalSndWaitForWal(XLogRecPtr loc);
extern bool GetDonorShmem(XLogRecPtr *donor_lsn);

static XLogRecPtr
NeonWALReadWaitForWAL(XLogRecPtr loc)
{
	while (!NeonWALReaderUpdateDonor(wal_reader))
	{
		pg_usleep(1000);
		CHECK_FOR_INTERRUPTS();
	}

	return WalSndWaitForWal(loc);
}

static int
NeonWALPageRead(
				XLogReaderState *xlogreader,
				XLogRecPtr targetPagePtr,
				int reqLen,
				XLogRecPtr targetRecPtr,
				char *readBuf)
{
	XLogRecPtr	rem_lsn;

	/* Wait for flush pointer to advance past our request */
	XLogRecPtr	flushptr = NeonWALReadWaitForWAL(targetPagePtr + reqLen);
	int			count;

	if (flushptr < targetPagePtr + reqLen)
		return -1;

	/* Read at most XLOG_BLCKSZ bytes */
	if (targetPagePtr + XLOG_BLCKSZ <= flushptr)
		count = XLOG_BLCKSZ;
	else
		count = flushptr - targetPagePtr;

	/*
	 * Sometimes walsender requests non-monotonic sequences of WAL. If that's
	 * the case, we have to reset streaming from remote at the correct
	 * position.
	 * For example, walsender may try to verify the segment header when trying
	 * to read in the middle of it.
	 */
	rem_lsn = NeonWALReaderGetRemLsn(wal_reader);
	if (rem_lsn != 0 && targetPagePtr != rem_lsn)
	{
		NeonWALReaderResetRemote(wal_reader);
	}

	for (;;)
	{
		NeonWALReadResult res = NeonWALRead(
											wal_reader,
											readBuf,
											targetPagePtr,
											count,
											walprop_pg_get_timeline_id());

		if (res == NEON_WALREAD_SUCCESS)
		{
			/*
			 * We don't actually use these fields ever, but we set it to
			 * conform to invariants outlined by XLogReaderRoutine.
			 */

			xlogreader->seg.ws_tli = NeonWALReaderGetSegment(wal_reader)->ws_tli;
			xlogreader->seg.ws_segno = NeonWALReaderGetSegment(wal_reader)->ws_segno;
			xlogreader->seg.ws_file = NeonWALReaderGetSegment(wal_reader)->ws_file;
			return count;
		}
		if (res == NEON_WALREAD_ERROR)
		{
			elog(ERROR, "[walsender] Failed to read WAL (req_lsn=%X/%X, len=%zu): %s",
				 LSN_FORMAT_ARGS(targetPagePtr),
				 reqLen,
				 NeonWALReaderErrMsg(wal_reader));
			return -1;
		}

		/*
		 * Res is WOULDBLOCK, so we wait on the socket, recreating event set
		 * if necessary
		 */
		{

			pgsocket	sock = NeonWALReaderSocket(wal_reader);
			uint32_t	reader_events = NeonWALReaderEvents(wal_reader);
			WaitEvent	event;
			long		timeout_ms = 1000;

			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
			if (ConfigReloadPending)
			{
				ConfigReloadPending = false;
				ProcessConfigFile(PGC_SIGHUP);
			}

			WaitLatchOrSocket(
							  MyLatch,
							  WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | reader_events,
							  sock,
							  timeout_ms,
							  WAIT_EVENT_WAL_SENDER_MAIN);
		}
	}
}

static void
NeonWALReadSegmentOpen(XLogReaderState *xlogreader, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	neon_wal_segment_open(wal_reader, nextSegNo, tli_p);
	xlogreader->seg.ws_file = NeonWALReaderGetSegment(wal_reader)->ws_file;
}

static void
NeonWALReadSegmentClose(XLogReaderState *xlogreader)
{
	neon_wal_segment_close(wal_reader);
	xlogreader->seg.ws_file = NeonWALReaderGetSegment(wal_reader)->ws_file;
}

void
NeonOnDemandXLogReaderRoutines(XLogReaderRoutine *xlr)
{
	if (!wal_reader)
	{
		XLogRecPtr	epochStartLsn = pg_atomic_read_u64(&GetWalpropShmemState()->propEpochStartLsn);

		if (epochStartLsn == 0)
		{
			elog(ERROR, "Unable to start walsender when propEpochStartLsn is 0!");
		}
		wal_reader = NeonWALReaderAllocate(wal_segment_size, epochStartLsn, "[walsender] ");
	}
	xlr->page_read = NeonWALPageRead;
	xlr->segment_open = NeonWALReadSegmentOpen;
	xlr->segment_close = NeonWALReadSegmentClose;
}
