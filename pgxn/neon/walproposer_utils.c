#include "postgres.h"

#include "access/timeline.h"
#include "access/xlogutils.h"
#include "common/logging.h"
#include "common/ip.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "replication/slot.h"
#include "walproposer_utils.h"
#include "replication/walsender_private.h"

#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"

#include "libpq-fe.h"
#include <netinet/tcp.h>
#include <unistd.h>

/*
 * These variables are used similarly to openLogFile/SegNo,
 * but for walproposer to write the XLOG during recovery. walpropFileTLI is the TimeLineID
 * corresponding the filename of walpropFile.
 */
static int	walpropFile = -1;
static TimeLineID walpropFileTLI = 0;
static XLogSegNo walpropSegNo = 0;

/* START cloned file-local variables and functions from walsender.c */

/*
 * xlogreader used for replication.  Note that a WAL sender doing physical
 * replication does not need xlogreader to read WAL, but it needs one to
 * keep a state of its work.
 */
static XLogReaderState *xlogreader = NULL;

/*
 * These variables keep track of the state of the timeline we're currently
 * sending. sendTimeLine identifies the timeline. If sendTimeLineIsHistoric,
 * the timeline is not the latest timeline on this server, and the server's
 * history forked off from that timeline at sendTimeLineValidUpto.
 */
static TimeLineID sendTimeLine = 0;
static TimeLineID sendTimeLineNextTLI = 0;
static bool sendTimeLineIsHistoric = false;
static XLogRecPtr sendTimeLineValidUpto = InvalidXLogRecPtr;

/*
 * Timestamp of last ProcessRepliesIfAny() that saw a reply from the
 * standby. Set to 0 if wal_sender_timeout doesn't need to be active.
 */
static TimestampTz last_reply_timestamp = 0;

/* Have we sent a heartbeat message asking for reply, since last reply? */
static bool waiting_for_ping_response = false;

static bool streamingDoneSending;
static bool streamingDoneReceiving;

/* Are we there yet? */
static bool WalSndCaughtUp = false;

/* Flags set by signal handlers for later service in main loop */
static volatile sig_atomic_t got_STOPPING = false;

/*
 * How far have we sent WAL already? This is also advertised in
 * MyWalSnd->sentPtr.  (Actually, this is the next WAL location to send.)
 */
static XLogRecPtr sentPtr = InvalidXLogRecPtr;

/*
 * This is set while we are streaming. When not set
 * PROCSIG_WALSND_INIT_STOPPING signal will be handled like SIGTERM. When set,
 * the main loop is responsible for checking got_STOPPING and terminating when
 * it's set (after streaming any remaining WAL).
 */
static volatile sig_atomic_t replication_active = false;

typedef void (*WalSndSendDataCallback) (void);
static void WalSndLoop(WalSndSendDataCallback send_data);
static void XLogSendPhysical(void);
static XLogRecPtr GetStandbyFlushRecPtr(void);

static void WalSndSegmentOpen(XLogReaderState *state, XLogSegNo nextSegNo,
							  TimeLineID *tli_p);

/* END cloned file-level variables and functions from walsender.c */

int
CompareLsn(const void *a, const void *b)
{
	XLogRecPtr	lsn1 = *((const XLogRecPtr *) a);
	XLogRecPtr	lsn2 = *((const XLogRecPtr *) b);

	if (lsn1 < lsn2)
		return -1;
	else if (lsn1 == lsn2)
		return 0;
	else
		return 1;
}

/* Returns a human-readable string corresonding to the SafekeeperState
 *
 * The string should not be freed.
 *
 * The strings are intended to be used as a prefix to "state", e.g.:
 *
 *   elog(LOG, "currently in %s state", FormatSafekeeperState(sk->state));
 *
 * If this sort of phrasing doesn't fit the message, instead use something like:
 *
 *   elog(LOG, "currently in state [%s]", FormatSafekeeperState(sk->state));
 */
char*
FormatSafekeeperState(SafekeeperState state)
{
	char* return_val = NULL;

	switch (state)
	{
		case SS_OFFLINE:
			return_val = "offline";
			break;
		case SS_CONNECTING_READ:
		case SS_CONNECTING_WRITE:
			return_val = "connecting";
			break;
		case SS_WAIT_EXEC_RESULT:
			return_val = "receiving query result";
			break;
		case SS_HANDSHAKE_RECV:
			return_val = "handshake (receiving)";
			break;
		case SS_VOTING:
			return_val = "voting";
			break;
		case SS_WAIT_VERDICT:
			return_val = "wait-for-verdict";
			break;
		case SS_SEND_ELECTED_FLUSH:
			return_val = "send-announcement-flush";
			break;
		case SS_IDLE:
			return_val = "idle";
			break;
		case SS_ACTIVE:
			return_val = "active";
			break;
	}

	Assert(return_val != NULL);

	return return_val;
}

/* Asserts that the provided events are expected for given safekeeper's state */
void
AssertEventsOkForState(uint32 events, Safekeeper* sk)
{
	uint32 expected = SafekeeperStateDesiredEvents(sk->state);

	/* The events are in-line with what we're expecting, under two conditions:
	 *   (a) if we aren't expecting anything, `events` has no read- or
	 *       write-ready component.
	 *   (b) if we are expecting something, there's overlap
	 *       (i.e. `events & expected != 0`)
	 */
	bool events_ok_for_state; /* long name so the `Assert` is more clear later */

	if (expected == WL_NO_EVENTS)
		events_ok_for_state = ((events & (WL_SOCKET_READABLE|WL_SOCKET_WRITEABLE)) == 0);
	else
		events_ok_for_state = ((events & expected) != 0);

	if (!events_ok_for_state)
	{
		/* To give a descriptive message in the case of failure, we use elog and
		 * then an assertion that's guaranteed to fail. */
		elog(WARNING, "events %s mismatched for safekeeper %s:%s in state [%s]",
			 FormatEvents(events), sk->host, sk->port, FormatSafekeeperState(sk->state));
		Assert(events_ok_for_state);
	}
}

/* Returns the set of events a safekeeper in this state should be waiting on
 *
 * This will return WL_NO_EVENTS (= 0) for some events. */
uint32
SafekeeperStateDesiredEvents(SafekeeperState state)
{
	uint32 result = WL_NO_EVENTS;

	/* If the state doesn't have a modifier, we can check the base state */
	switch (state)
	{
		/* Connecting states say what they want in the name */
		case SS_CONNECTING_READ:
			result = WL_SOCKET_READABLE;
			break;
		case SS_CONNECTING_WRITE:
			result = WL_SOCKET_WRITEABLE;
			break;

		/* Reading states need the socket to be read-ready to continue */
		case SS_WAIT_EXEC_RESULT:
		case SS_HANDSHAKE_RECV:
		case SS_WAIT_VERDICT:
			result = WL_SOCKET_READABLE;
			break;

		/* Idle states use read-readiness as a sign that the connection has been
		 * disconnected. */
		case SS_VOTING:
		case SS_IDLE:
			result = WL_SOCKET_READABLE;
			break;

		/* 
		 * Flush states require write-ready for flushing.
		 * Active state does both reading and writing.
		 * 
		 * TODO: SS_ACTIVE sometimes doesn't need to be write-ready. We should
		 * 	check sk->flushWrite here to set WL_SOCKET_WRITEABLE.
		 */
		case SS_SEND_ELECTED_FLUSH:
		case SS_ACTIVE:
			result = WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE;
			break;

		/* The offline state expects no events. */
		case SS_OFFLINE:
			result = WL_NO_EVENTS;
			break;

		default:
			Assert(false);
			break;
	}

	return result;
}

/* Returns a human-readable string corresponding to the event set
 *
 * If the events do not correspond to something set as the `events` field of a `WaitEvent`, the
 * returned string may be meaingless.
 *
 * The string should not be freed. It should also not be expected to remain the same between
 * function calls. */
char*
FormatEvents(uint32 events)
{
	static char return_str[8];

	/* Helper variable to check if there's extra bits */
	uint32 all_flags = WL_LATCH_SET
		| WL_SOCKET_READABLE
		| WL_SOCKET_WRITEABLE
		| WL_TIMEOUT
		| WL_POSTMASTER_DEATH
		| WL_EXIT_ON_PM_DEATH
		| WL_SOCKET_CONNECTED;

	/* The formatting here isn't supposed to be *particularly* useful -- it's just to give an
	 * sense of what events have been triggered without needing to remember your powers of two. */

	return_str[0] = (events & WL_LATCH_SET       ) ? 'L' : '_';
	return_str[1] = (events & WL_SOCKET_READABLE ) ? 'R' : '_';
	return_str[2] = (events & WL_SOCKET_WRITEABLE) ? 'W' : '_';
	return_str[3] = (events & WL_TIMEOUT         ) ? 'T' : '_';
	return_str[4] = (events & WL_POSTMASTER_DEATH) ? 'D' : '_';
	return_str[5] = (events & WL_EXIT_ON_PM_DEATH) ? 'E' : '_';
	return_str[5] = (events & WL_SOCKET_CONNECTED) ? 'C' : '_';

	if (events & (~all_flags))
	{
		elog(WARNING, "Event formatting found unexpected component %d",
				events & (~all_flags));
		return_str[6] = '*';
		return_str[7] = '\0';
	}
	else
		return_str[6] = '\0';

	return (char *) &return_str;
}

/*
 * Convert a character which represents a hexadecimal digit to an integer.
 *
 * Returns -1 if the character is not a hexadecimal digit.
 */
static int
HexDecodeChar(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;

	return -1;
}

/*
 * Decode a hex string into a byte string, 2 hex chars per byte.
 *
 * Returns false if invalid characters are encountered; otherwise true.
 */
bool
HexDecodeString(uint8 *result, char *input, int nbytes)
{
	int			i;

	for (i = 0; i < nbytes; ++i)
	{
		int			n1 = HexDecodeChar(input[i * 2]);
		int			n2 = HexDecodeChar(input[i * 2 + 1]);

		if (n1 < 0 || n2 < 0)
			return false;
		result[i] = n1 * 16 + n2;
	}

	return true;
}

/* --------------------------------
 *		pq_getmsgint32_le	- get a binary 4-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint32
pq_getmsgint32_le(StringInfo msg)
{
	uint32		n32;

	pq_copymsgbytes(msg, (char *) &n32, sizeof(n32));

	return n32;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer in native (LE) order
 * --------------------------------
 */
uint64
pq_getmsgint64_le(StringInfo msg)
{
	uint64		n64;

	pq_copymsgbytes(msg, (char *) &n64, sizeof(n64));

	return n64;
}

/* append a binary [u]int32 to a StringInfo buffer in native (LE) order */
void
pq_sendint32_le(StringInfo buf, uint32 i)
{
	enlargeStringInfo(buf, sizeof(uint32));
	memcpy(buf->data + buf->len, &i, sizeof(uint32));
	buf->len += sizeof(uint32);
}

/* append a binary [u]int64 to a StringInfo buffer in native (LE) order */
void
pq_sendint64_le(StringInfo buf, uint64 i)
{
	enlargeStringInfo(buf, sizeof(uint64));
	memcpy(buf->data + buf->len, &i, sizeof(uint64));
	buf->len += sizeof(uint64);
}

/*
 * Write XLOG data to disk.
 */
void
XLogWalPropWrite(char *buf, Size nbytes, XLogRecPtr recptr)
{
	int			startoff;
	int			byteswritten;

	while (nbytes > 0)
	{
		int			segbytes;

		/* Close the current segment if it's completed */
		if (walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size))
			XLogWalPropClose(recptr);

		if (walpropFile < 0)
		{
			bool		use_existent = true;

			/* Create/use new log file */
			XLByteToSeg(recptr, walpropSegNo, wal_segment_size);
			walpropFile = XLogFileInit(walpropSegNo, &use_existent, false);
			walpropFileTLI = ThisTimeLineID;
		}

		/* Calculate the start offset of the received logs */
		startoff = XLogSegmentOffset(recptr, wal_segment_size);

		if (startoff + nbytes > wal_segment_size)
			segbytes = wal_segment_size - startoff;
		else
			segbytes = nbytes;

		/* OK to write the logs */
		errno = 0;

		byteswritten = pg_pwrite(walpropFile, buf, segbytes, (off_t) startoff);
		if (byteswritten <= 0)
		{
			char		xlogfname[MAXFNAMELEN];
			int			save_errno;

			/* if write didn't set errno, assume no disk space */
			if (errno == 0)
				errno = ENOSPC;

			save_errno = errno;
			XLogFileName(xlogfname, walpropFileTLI, walpropSegNo, wal_segment_size);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write to log segment %s "
							"at offset %u, length %lu: %m",
							xlogfname, startoff, (unsigned long) segbytes)));
		}

		/* Update state for write */
		recptr += byteswritten;

		nbytes -= byteswritten;
		buf += byteswritten;
	}

	/*
	 * Close the current segment if it's fully written up in the last cycle of
	 * the loop.
	 */
	if (walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size))
	{
		XLogWalPropClose(recptr);
	}
}

/*
 * Close the current segment.
 */
void
XLogWalPropClose(XLogRecPtr recptr)
{
	Assert(walpropFile >= 0 && !XLByteInSeg(recptr, walpropSegNo, wal_segment_size));

	if (close(walpropFile) != 0)
	{
		char		xlogfname[MAXFNAMELEN];
		XLogFileName(xlogfname, walpropFileTLI, walpropSegNo, wal_segment_size);

		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close log segment %s: %m",
						xlogfname)));
	}

	walpropFile = -1;
}

/* START of cloned functions from walsender.c */

/*
 * Handle START_REPLICATION command.
 *
 * At the moment, this never returns, but an ereport(ERROR) will take us back
 * to the main loop.
 */
void
StartProposerReplication(StartReplicationCmd *cmd)
{
	XLogRecPtr	FlushPtr;

	if (ThisTimeLineID == 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("IDENTIFY_SYSTEM has not been run before START_REPLICATION")));

	/* create xlogreader for physical replication */
	xlogreader =
		XLogReaderAllocate(wal_segment_size, NULL,
						   XL_ROUTINE(.segment_open = WalSndSegmentOpen,
									  .segment_close = wal_segment_close),
						   NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
					errmsg("out of memory")));

	/*
	 * We assume here that we're logging enough information in the WAL for
	 * log-shipping, since this is checked in PostmasterMain().
	 *
	 * NOTE: wal_level can only change at shutdown, so in most cases it is
	 * difficult for there to be WAL data that we can still see that was
	 * written at wal_level='minimal'.
	 */

	if (cmd->slotname)
	{
		ReplicationSlotAcquire(cmd->slotname, true);
		if (SlotIsLogical(MyReplicationSlot))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cannot use a logical replication slot for physical replication")));

		/*
		 * We don't need to verify the slot's restart_lsn here; instead we
		 * rely on the caller requesting the starting point to use.  If the
		 * WAL segment doesn't exist, we'll fail later.
		 */
	}

	/*
	 * Select the timeline. If it was given explicitly by the client, use
	 * that. Otherwise use the timeline of the last replayed record, which is
	 * kept in ThisTimeLineID.
	 * 
	 * Neon doesn't currently use PG Timelines, but it may in the future, so
	 * we keep this code around to lighten the load for when we need it.
	 */
	if (am_cascading_walsender)
	{
		/* this also updates ThisTimeLineID */
		FlushPtr = GetStandbyFlushRecPtr();
	}
	else
		FlushPtr = GetFlushRecPtr();

	if (cmd->timeline != 0)
	{
		XLogRecPtr	switchpoint;

		sendTimeLine = cmd->timeline;
		if (sendTimeLine == ThisTimeLineID)
		{
			sendTimeLineIsHistoric = false;
			sendTimeLineValidUpto = InvalidXLogRecPtr;
		}
		else
		{
			List	   *timeLineHistory;

			sendTimeLineIsHistoric = true;

			/*
			 * Check that the timeline the client requested exists, and the
			 * requested start location is on that timeline.
			 */
			timeLineHistory = readTimeLineHistory(ThisTimeLineID);
			switchpoint = tliSwitchPoint(cmd->timeline, timeLineHistory,
										 &sendTimeLineNextTLI);
			list_free_deep(timeLineHistory);

			/*
			 * Found the requested timeline in the history. Check that
			 * requested startpoint is on that timeline in our history.
			 *
			 * This is quite loose on purpose. We only check that we didn't
			 * fork off the requested timeline before the switchpoint. We
			 * don't check that we switched *to* it before the requested
			 * starting point. This is because the client can legitimately
			 * request to start replication from the beginning of the WAL
			 * segment that contains switchpoint, but on the new timeline, so
			 * that it doesn't end up with a partial segment. If you ask for
			 * too old a starting point, you'll get an error later when we
			 * fail to find the requested WAL segment in pg_wal.
			 *
			 * XXX: we could be more strict here and only allow a startpoint
			 * that's older than the switchpoint, if it's still in the same
			 * WAL segment.
			 */
			if (!XLogRecPtrIsInvalid(switchpoint) &&
				switchpoint < cmd->startpoint)
			{
				ereport(ERROR,
						(errmsg("requested starting point %X/%X on timeline %u is not in this server's history",
								LSN_FORMAT_ARGS(cmd->startpoint),
								cmd->timeline),
							errdetail("This server's history forked from timeline %u at %X/%X.",
									  cmd->timeline,
									  LSN_FORMAT_ARGS(switchpoint))));
			}
			sendTimeLineValidUpto = switchpoint;
		}
	}
	else
	{
		sendTimeLine = ThisTimeLineID;
		sendTimeLineValidUpto = InvalidXLogRecPtr;
		sendTimeLineIsHistoric = false;
	}

	streamingDoneSending = streamingDoneReceiving = false;

	/* If there is nothing to stream, don't even enter COPY mode */
	if (!sendTimeLineIsHistoric || cmd->startpoint < sendTimeLineValidUpto)
	{
		/*
		 * When we first start replication the standby will be behind the
		 * primary. For some applications, for example synchronous
		 * replication, it is important to have a clear state for this initial
		 * catchup mode, so we can trigger actions when we change streaming
		 * state later. We may stay in this state for a long time, which is
		 * exactly why we want to be able to monitor whether or not we are
		 * still here.
		 */
		WalSndSetState(WALSNDSTATE_CATCHUP);

		/*
		 * Don't allow a request to stream from a future point in WAL that
		 * hasn't been flushed to disk in this server yet.
		 */
		if (FlushPtr < cmd->startpoint)
		{
			ereport(ERROR,
					(errmsg("requested starting point %X/%X is ahead of the WAL flush position of this server %X/%X",
							LSN_FORMAT_ARGS(cmd->startpoint),
							LSN_FORMAT_ARGS(FlushPtr))));
		}

		/* Start streaming from the requested point */
		sentPtr = cmd->startpoint;

		/* Initialize shared memory status, too */
		SpinLockAcquire(&MyWalSnd->mutex);
		MyWalSnd->sentPtr = sentPtr;
		SpinLockRelease(&MyWalSnd->mutex);

		SyncRepInitConfig();

		/* Main loop of walsender */
		replication_active = true;

		WalSndLoop(XLogSendPhysical);

		replication_active = false;
		if (got_STOPPING)
			proc_exit(0);
		WalSndSetState(WALSNDSTATE_STARTUP);

		Assert(streamingDoneSending && streamingDoneReceiving);
	}

	if (cmd->slotname)
		ReplicationSlotRelease();

	/*
	 * Copy is finished now. Send a single-row result set indicating the next
	 * timeline.
	 */
	if (sendTimeLineIsHistoric)
	{
		char		startpos_str[8 + 1 + 8 + 1];
		DestReceiver *dest;
		TupOutputState *tstate;
		TupleDesc	tupdesc;
		Datum		values[2];
		bool		nulls[2];

		snprintf(startpos_str, sizeof(startpos_str), "%X/%X",
				 LSN_FORMAT_ARGS(sendTimeLineValidUpto));

		dest = CreateDestReceiver(DestRemoteSimple);
		MemSet(nulls, false, sizeof(nulls));

		/*
		 * Need a tuple descriptor representing two columns. int8 may seem
		 * like a surprising data type for this, but in theory int4 would not
		 * be wide enough for this, as TimeLineID is unsigned.
		 */
		tupdesc = CreateTemplateTupleDesc(2);
		TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 1, "next_tli",
								  INT8OID, -1, 0);
		TupleDescInitBuiltinEntry(tupdesc, (AttrNumber) 2, "next_tli_startpos",
								  TEXTOID, -1, 0);

		/* prepare for projection of tuple */
		tstate = begin_tup_output_tupdesc(dest, tupdesc, &TTSOpsVirtual);

		values[0] = Int64GetDatum((int64) sendTimeLineNextTLI);
		values[1] = CStringGetTextDatum(startpos_str);

		/* send it to dest */
		do_tup_output(tstate, values, nulls);

		end_tup_output(tstate);
	}

	/* Send CommandComplete message */
	EndReplicationCommand("START_STREAMING");
}

/*
 * Returns the latest point in WAL that has been safely flushed to disk, and
 * can be sent to the standby. This should only be called when in recovery,
 * ie. we're streaming to a cascaded standby.
 *
 * As a side-effect, ThisTimeLineID is updated to the TLI of the last
 * replayed WAL record.
 */
static XLogRecPtr
GetStandbyFlushRecPtr(void)
{
	XLogRecPtr	replayPtr;
	TimeLineID	replayTLI;
	XLogRecPtr	receivePtr;
	TimeLineID	receiveTLI;
	XLogRecPtr	result;

	/*
	 * We can safely send what's already been replayed. Also, if walreceiver
	 * is streaming WAL from the same timeline, we can send anything that it
	 * has streamed, but hasn't been replayed yet.
	 */

	receivePtr = GetWalRcvFlushRecPtr(NULL, &receiveTLI);
	replayPtr = GetXLogReplayRecPtr(&replayTLI);

	ThisTimeLineID = replayTLI;

	result = replayPtr;
	if (receiveTLI == ThisTimeLineID && receivePtr > replayPtr)
		result = receivePtr;

	return result;
}

/* XLogReaderRoutine->segment_open callback */
static void
WalSndSegmentOpen(XLogReaderState *state, XLogSegNo nextSegNo,
				  TimeLineID *tli_p)
{
	char		path[MAXPGPATH];

	/*-------
	 * When reading from a historic timeline, and there is a timeline switch
	 * within this segment, read from the WAL segment belonging to the new
	 * timeline.
	 *
	 * For example, imagine that this server is currently on timeline 5, and
	 * we're streaming timeline 4. The switch from timeline 4 to 5 happened at
	 * 0/13002088. In pg_wal, we have these files:
	 *
	 * ...
	 * 000000040000000000000012
	 * 000000040000000000000013
	 * 000000050000000000000013
	 * 000000050000000000000014
	 * ...
	 *
	 * In this situation, when requested to send the WAL from segment 0x13, on
	 * timeline 4, we read the WAL from file 000000050000000000000013. Archive
	 * recovery prefers files from newer timelines, so if the segment was
	 * restored from the archive on this server, the file belonging to the old
	 * timeline, 000000040000000000000013, might not exist. Their contents are
	 * equal up to the switchpoint, because at a timeline switch, the used
	 * portion of the old segment is copied to the new file.  -------
	 */
	*tli_p = sendTimeLine;
	if (sendTimeLineIsHistoric)
	{
		XLogSegNo	endSegNo;

		XLByteToSeg(sendTimeLineValidUpto, endSegNo, state->segcxt.ws_segsize);
		if (nextSegNo == endSegNo)
			*tli_p = sendTimeLineNextTLI;
	}

	XLogFilePath(path, *tli_p, nextSegNo, state->segcxt.ws_segsize);
	state->seg.ws_file = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file >= 0)
		return;

	/*
	 * If the file is not found, assume it's because the standby asked for a
	 * too old WAL segment that has already been removed or recycled.
	 */
	if (errno == ENOENT)
	{
		char		xlogfname[MAXFNAMELEN];
		int			save_errno = errno;

		XLogFileName(xlogfname, *tli_p, nextSegNo, wal_segment_size);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("requested WAL segment %s has already been removed",
						   xlogfname)));
	}
	else
		ereport(ERROR,
				(errcode_for_file_access(),
					errmsg("could not open file \"%s\": %m",
						   path)));
}


/* Main loop of walsender process that streams the WAL over Copy messages. */
static void
WalSndLoop(WalSndSendDataCallback send_data)
{
	/*
	 * Initialize the last reply timestamp. That enables timeout processing
	 * from hereon.
	 */
	last_reply_timestamp = GetCurrentTimestamp();
	waiting_for_ping_response = false;

	/*
	 * Loop until we reach the end of this timeline or the client requests to
	 * stop streaming.
	 */
	for (;;)
	{
		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Process any requests or signals received recently */
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
			SyncRepInitConfig();
		}

		/* always true */
		if (am_wal_proposer)
		{
			send_data();
			if (WalSndCaughtUp)
			{
				if (MyWalSnd->state == WALSNDSTATE_CATCHUP)
					WalSndSetState(WALSNDSTATE_STREAMING);
				WalProposerPoll();
				WalSndCaughtUp = false;
			}
			continue;
		}
	}
}

/*
 * Send out the WAL in its normal physical/stored form.
 *
 * Read up to MAX_SEND_SIZE bytes of WAL that's been flushed to disk,
 * but not yet sent to the client, and buffer it in the libpq output
 * buffer.
 *
 * If there is no unsent WAL remaining, WalSndCaughtUp is set to true,
 * otherwise WalSndCaughtUp is set to false.
 */
static void
XLogSendPhysical(void)
{
	XLogRecPtr	SendRqstPtr;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	Size		nbytes PG_USED_FOR_ASSERTS_ONLY;

	/* If requested switch the WAL sender to the stopping state. */
	if (got_STOPPING)
		WalSndSetState(WALSNDSTATE_STOPPING);

	if (streamingDoneSending)
	{
		WalSndCaughtUp = true;
		return;
	}

	/* Figure out how far we can safely send the WAL. */
	if (sendTimeLineIsHistoric)
	{
		/*
		 * Streaming an old timeline that's in this server's history, but is
		 * not the one we're currently inserting or replaying. It can be
		 * streamed up to the point where we switched off that timeline.
		 */
		SendRqstPtr = sendTimeLineValidUpto;
	}
	else if (am_cascading_walsender)
	{
		/*
		 * Streaming the latest timeline on a standby.
		 *
		 * Attempt to send all WAL that has already been replayed, so that we
		 * know it's valid. If we're receiving WAL through streaming
		 * replication, it's also OK to send any WAL that has been received
		 * but not replayed.
		 *
		 * The timeline we're recovering from can change, or we can be
		 * promoted. In either case, the current timeline becomes historic. We
		 * need to detect that so that we don't try to stream past the point
		 * where we switched to another timeline. We check for promotion or
		 * timeline switch after calculating FlushPtr, to avoid a race
		 * condition: if the timeline becomes historic just after we checked
		 * that it was still current, it's still be OK to stream it up to the
		 * FlushPtr that was calculated before it became historic.
		 */
		bool		becameHistoric = false;

		SendRqstPtr = GetStandbyFlushRecPtr();

		if (!RecoveryInProgress())
		{
			/*
			 * We have been promoted. RecoveryInProgress() updated
			 * ThisTimeLineID to the new current timeline.
			 */
			am_cascading_walsender = false;
			becameHistoric = true;
		}
		else
		{
			/*
			 * Still a cascading standby. But is the timeline we're sending
			 * still the one recovery is recovering from? ThisTimeLineID was
			 * updated by the GetStandbyFlushRecPtr() call above.
			 */
			if (sendTimeLine != ThisTimeLineID)
				becameHistoric = true;
		}

		if (becameHistoric)
		{
			/*
			 * The timeline we were sending has become historic. Read the
			 * timeline history file of the new timeline to see where exactly
			 * we forked off from the timeline we were sending.
			 */
			List	   *history;

			history = readTimeLineHistory(ThisTimeLineID);
			sendTimeLineValidUpto = tliSwitchPoint(sendTimeLine, history, &sendTimeLineNextTLI);

			Assert(sendTimeLine < sendTimeLineNextTLI);
			list_free_deep(history);

			sendTimeLineIsHistoric = true;

			SendRqstPtr = sendTimeLineValidUpto;
		}
	}
	else
	{
		/*
		 * Streaming the current timeline on a primary.
		 *
		 * Attempt to send all data that's already been written out and
		 * fsync'd to disk.  We cannot go further than what's been written out
		 * given the current implementation of WALRead().  And in any case
		 * it's unsafe to send WAL that is not securely down to disk on the
		 * primary: if the primary subsequently crashes and restarts, standbys
		 * must not have applied any WAL that got lost on the primary.
		 */
		SendRqstPtr = GetFlushRecPtr();
	}

	/*
	 * Record the current system time as an approximation of the time at which
	 * this WAL location was written for the purposes of lag tracking.
	 *
	 * In theory we could make XLogFlush() record a time in shmem whenever WAL
	 * is flushed and we could get that time as well as the LSN when we call
	 * GetFlushRecPtr() above (and likewise for the cascading standby
	 * equivalent), but rather than putting any new code into the hot WAL path
	 * it seems good enough to capture the time here.  We should reach this
	 * after XLogFlush() runs WalSndWakeupProcessRequests(), and although that
	 * may take some time, we read the WAL flush pointer and take the time
	 * very close to together here so that we'll get a later position if it is
	 * still moving.
	 *
	 * Because LagTrackerWrite ignores samples when the LSN hasn't advanced,
	 * this gives us a cheap approximation for the WAL flush time for this
	 * LSN.
	 *
	 * Note that the LSN is not necessarily the LSN for the data contained in
	 * the present message; it's the end of the WAL, which might be further
	 * ahead.  All the lag tracking machinery cares about is finding out when
	 * that arbitrary LSN is eventually reported as written, flushed and
	 * applied, so that it can measure the elapsed time.
	 */
	LagTrackerWrite(SendRqstPtr, GetCurrentTimestamp());

	/*
	 * If this is a historic timeline and we've reached the point where we
	 * forked to the next timeline, stop streaming.
	 *
	 * Note: We might already have sent WAL > sendTimeLineValidUpto. The
	 * startup process will normally replay all WAL that has been received
	 * from the primary, before promoting, but if the WAL streaming is
	 * terminated at a WAL page boundary, the valid portion of the timeline
	 * might end in the middle of a WAL record. We might've already sent the
	 * first half of that partial WAL record to the cascading standby, so that
	 * sentPtr > sendTimeLineValidUpto. That's OK; the cascading standby can't
	 * replay the partial WAL record either, so it can still follow our
	 * timeline switch.
	 */
	if (sendTimeLineIsHistoric && sendTimeLineValidUpto <= sentPtr)
	{
		/* close the current file. */
		if (xlogreader->seg.ws_file >= 0)
			wal_segment_close(xlogreader);

		/* Send CopyDone */
		pq_putmessage_noblock('c', NULL, 0);
		streamingDoneSending = true;

		WalSndCaughtUp = true;

		elog(DEBUG1, "walsender reached end of timeline at %X/%X (sent up to %X/%X)",
			 LSN_FORMAT_ARGS(sendTimeLineValidUpto),
			 LSN_FORMAT_ARGS(sentPtr));
		return;
	}

	/* Do we have any work to do? */
	Assert(sentPtr <= SendRqstPtr);
	if (SendRqstPtr <= sentPtr)
	{
		WalSndCaughtUp = true;
		return;
	}

	/*
	 * Figure out how much to send in one message. If there's no more than
	 * MAX_SEND_SIZE bytes to send, send everything. Otherwise send
	 * MAX_SEND_SIZE bytes, but round back to logfile or page boundary.
	 *
	 * The rounding is not only for performance reasons. Walreceiver relies on
	 * the fact that we never split a WAL record across two messages. Since a
	 * long WAL record is split at page boundary into continuation records,
	 * page boundary is always a safe cut-off point. We also assume that
	 * SendRqstPtr never points to the middle of a WAL record.
	 */
	startptr = sentPtr;
	endptr = startptr;
	endptr += MAX_SEND_SIZE;

	/* if we went beyond SendRqstPtr, back off */
	if (SendRqstPtr <= endptr)
	{
		endptr = SendRqstPtr;
		if (sendTimeLineIsHistoric)
			WalSndCaughtUp = false;
		else
			WalSndCaughtUp = true;
	}
	else
	{
		/* round down to page boundary. */
		endptr -= (endptr % XLOG_BLCKSZ);
		WalSndCaughtUp = false;
	}

	nbytes = endptr - startptr;
	Assert(nbytes <= MAX_SEND_SIZE);

	/* always true */
	if (am_wal_proposer)
	{
		WalProposerBroadcast(startptr, endptr);
	}
	else
	{
		/* code removed for brevity */
	}
	sentPtr = endptr;

	/* Update shared memory status */
	{
		WalSnd	   *walsnd = MyWalSnd;

		SpinLockAcquire(&walsnd->mutex);
		walsnd->sentPtr = sentPtr;
		SpinLockRelease(&walsnd->mutex);
	}

	/* Report progress of XLOG streaming in PS display */
	if (update_process_title)
	{
		char		activitymsg[50];

		snprintf(activitymsg, sizeof(activitymsg), "streaming %X/%X",
				 LSN_FORMAT_ARGS(sentPtr));
		set_ps_display(activitymsg);
	}
}

