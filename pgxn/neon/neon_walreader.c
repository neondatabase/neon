/*
 * Like WALRead, but when WAL segment doesn't exist locally instead of throwing
 * ERROR asynchronously tries to fetch it from the most advanced safekeeper.
 *
 * We can't use libpqwalreceiver as it blocks during connection establishment
 * (and waiting for PQExec result), so use libpqwalproposer instead.
 *
 * TODO: keepalives are currently never sent, so the other side can close the
 * connection prematurely.
 *
 * TODO: close conn if reading takes too long to prevent stuck connections.
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "libpq/pqformat.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

#include "libpq-fe.h"

#include "neon_walreader.h"
#include "walproposer.h"

#define NEON_WALREADER_ERR_MSG_LEN 512

/*
 * Can be called where NeonWALReader *state is available in the context, adds log_prefix.
 */
#define nwr_log(elevel, fmt, ...) elog(elevel, "%s" fmt, state->log_prefix, ## __VA_ARGS__)

static NeonWALReadResult NeonWALReadRemote(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli);
static NeonWALReadResult NeonWALReaderReadMsg(NeonWALReader *state);
static bool NeonWALReadLocal(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli);
static bool is_wal_segment_exists(XLogSegNo segno, int segsize,
								  TimeLineID tli);

/*
 * State of connection to donor safekeeper.
 */
typedef enum
{
	/* no remote connection */
	RS_NONE,
	/* doing PQconnectPoll, need readable socket */
	RS_CONNECTING_READ,
	/* doing PQconnectPoll, need writable socket */
	RS_CONNECTING_WRITE,
	/* Waiting for START_REPLICATION result */
	RS_WAIT_EXEC_RESULT,
	/* replication stream established */
	RS_ESTABLISHED,
} NeonWALReaderRemoteState;

struct NeonWALReader
{
	/*
	 * LSN before which we assume WAL is not available locally. Exists because
	 * though first segment after startup always exists, part before
	 * basebackup LSN is filled with zeros.
	 */
	XLogRecPtr	available_lsn;
	WALSegmentContext segcxt;
	WALOpenSegment seg;
	int			wre_errno;
	/* Explains failure to read, static for simplicity. */
	char		err_msg[NEON_WALREADER_ERR_MSG_LEN];

	/*
	 * Saved info about request in progress, used to check validity of
	 * arguments after resume and remember how far we accomplished it. req_lsn
	 * is 0 if there is no request in progress.
	 */
	XLogRecPtr	req_lsn;
	Size		req_len;
	Size		req_progress;
	char		donor_conninfo[MAXCONNINFO];
	char		donor_name[64]; /* saved donor safekeeper name for logging */
	XLogRecPtr	donor_lsn;
	/* state of connection to safekeeper */
	NeonWALReaderRemoteState rem_state;
	WalProposerConn *wp_conn;

	/*
	 * position in wp_conn recvbuf from which we'll copy WAL next time, or
	 * NULL if there is no unprocessed message
	 */
	char	   *wal_ptr;
	Size		wal_rem_len;	/* how many unprocessed bytes left in recvbuf */

	/*
	 * LSN of wal_ptr position according to walsender to cross check against
	 * read request
	 */
	XLogRecPtr	rem_lsn;

	/* prepended to lines logged by neon_walreader, if provided */
	char		log_prefix[64];
};

/* palloc and initialize NeonWALReader */
NeonWALReader *
NeonWALReaderAllocate(int wal_segment_size, XLogRecPtr available_lsn, char *log_prefix)
{
	NeonWALReader *reader;

	/*
	 * Note: we allocate in TopMemoryContext, reusing the reader for all process
	 * reads.
	 */
	reader = (NeonWALReader *)
		MemoryContextAllocZero(TopMemoryContext, sizeof(NeonWALReader));

	reader->available_lsn = available_lsn;
	reader->seg.ws_file = -1;
	reader->seg.ws_segno = 0;
	reader->seg.ws_tli = 0;
	reader->segcxt.ws_segsize = wal_segment_size;

	reader->rem_state = RS_NONE;

	if (log_prefix)
		strlcpy(reader->log_prefix, log_prefix, sizeof(reader->log_prefix));

	return reader;
}

void
NeonWALReaderFree(NeonWALReader *state)
{
	if (state->seg.ws_file != -1)
		neon_wal_segment_close(state);
	if (state->wp_conn)
		libpqwp_disconnect(state->wp_conn);
	pfree(state);
}

/*
 * Like vanilla WALRead, but if requested position is before available_lsn or
 * WAL segment doesn't exist on disk, it tries to fetch needed segment from the
 * advanced safekeeper.
 *
 * Read 'count' bytes into 'buf', starting at location 'startptr', from WAL
 * fetched from timeline 'tli'.
 *
 * Returns NEON_WALREAD_SUCCESS if succeeded, NEON_WALREAD_ERROR if an error
 * occurs, in which case 'err' has the desciption. Error always closes remote
 * connection, if there was any, so socket subscription should be removed.
 *
 * NEON_WALREAD_WOULDBLOCK means caller should obtain socket to wait for with
 * NeonWALReaderSocket and call NeonWALRead again with exactly the same
 * arguments when NeonWALReaderEvents happen on the socket. Note that per libpq
 * docs during connection establishment (before first successful read) socket
 * underneath might change.
 *
 * Also, eventually walreader should switch from remote to local read; caller
 * should remove subscription to socket then by checking NeonWALReaderEvents
 * after successful read (otherwise next read might reopen the connection with
 * different socket).
 *
 * Reading not monotonically is not supported and will result in error.
 *
 * Caller should be sure that WAL up to requested LSN exists, otherwise
 * NEON_WALREAD_WOULDBLOCK might be always returned.
 */
NeonWALReadResult
NeonWALRead(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli)
{
	/*
	 * If requested data is before known available basebackup lsn or there is
	 * already active remote state, do remote read.
	 */
	if (startptr < state->available_lsn || state->rem_state != RS_NONE)
	{
		return NeonWALReadRemote(state, buf, startptr, count, tli);
	}
	if (NeonWALReadLocal(state, buf, startptr, count, tli))
	{
		return NEON_WALREAD_SUCCESS;
	}
	else if (state->wre_errno == ENOENT)
	{
		nwr_log(LOG, "local read at %X/%X len %zu failed as segment file doesn't exist, attempting remote",
				LSN_FORMAT_ARGS(startptr), count);
		return NeonWALReadRemote(state, buf, startptr, count, tli);
	}
	else
	{
		return NEON_WALREAD_ERROR;
	}
}

/* Do the read from remote safekeeper. */
static NeonWALReadResult
NeonWALReadRemote(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli)
{
	if (state->rem_state == RS_NONE)
	{
		if (!NeonWALReaderUpdateDonor(state))
		{
			snprintf(state->err_msg, sizeof(state->err_msg),
					 "failed to establish remote connection to fetch WAL: no donor available");
			return NEON_WALREAD_ERROR;

		}
		/* no connection yet; start one */
		nwr_log(LOG, "establishing connection to %s, lsn=%X/%X to fetch WAL", state->donor_name, LSN_FORMAT_ARGS(state->donor_lsn));
		state->wp_conn = libpqwp_connect_start(state->donor_conninfo);
		if (PQstatus(state->wp_conn->pg_conn) == CONNECTION_BAD)
		{
			snprintf(state->err_msg, sizeof(state->err_msg),
					 "failed to connect to %s to fetch WAL: immediately failed with %s",
					 state->donor_name, PQerrorMessage(state->wp_conn->pg_conn));
			NeonWALReaderResetRemote(state);
			return NEON_WALREAD_ERROR;
		}
		/* we'll poll immediately */
		state->rem_state = RS_CONNECTING_WRITE;
		return NEON_WALREAD_WOULDBLOCK;
	}

	if (state->rem_state == RS_CONNECTING_READ || state->rem_state == RS_CONNECTING_WRITE)
	{
		switch (PQconnectPoll(state->wp_conn->pg_conn))
		{
			case PGRES_POLLING_FAILED:
				snprintf(state->err_msg, sizeof(state->err_msg),
						 "failed to connect to %s to fetch WAL: poll error: %s",
						 state->donor_name, PQerrorMessage(state->wp_conn->pg_conn));
				NeonWALReaderResetRemote(state);
				return NEON_WALREAD_ERROR;
			case PGRES_POLLING_READING:
				state->rem_state = RS_CONNECTING_READ;
				return NEON_WALREAD_WOULDBLOCK;
			case PGRES_POLLING_WRITING:
				state->rem_state = RS_CONNECTING_WRITE;
				return NEON_WALREAD_WOULDBLOCK;
			case PGRES_POLLING_OK:
				{
					/* connection successfully established */
					char		start_repl_query[128];
					term_t		term = pg_atomic_read_u64(&GetWalpropShmemState()->mineLastElectedTerm);

					/*
					 * Set elected walproposer's term to pull only data from
					 * its history. Note: for logical walsender it means we
					 * might stream WAL not yet committed by safekeepers. It
					 * would be cleaner to fix this.
					 *
					 * mineLastElectedTerm shouldn't be 0 at this point
					 * because we checked above that donor exists and it
					 * appears only after successfull election.
					 */
					Assert(term > 0);
					snprintf(start_repl_query, sizeof(start_repl_query),
							 "START_REPLICATION PHYSICAL %X/%X (term='" UINT64_FORMAT "')",
							 LSN_FORMAT_ARGS(startptr), term);
					nwr_log(LOG, "connection to %s to fetch WAL succeeded, running %s",
							state->donor_name, start_repl_query);
					if (!libpqwp_send_query(state->wp_conn, start_repl_query))
					{
						snprintf(state->err_msg, sizeof(state->err_msg),
								 "failed to send %s query to %s: %s",
								 start_repl_query, state->donor_name, PQerrorMessage(state->wp_conn->pg_conn));
						NeonWALReaderResetRemote(state);
						return NEON_WALREAD_ERROR;
					}
					state->rem_state = RS_WAIT_EXEC_RESULT;
					break;
				}

			default:			/* there is unused PGRES_POLLING_ACTIVE */
				Assert(false);
				return NEON_WALREAD_ERROR;	/* keep the compiler quiet */
		}
	}

	if (state->rem_state == RS_WAIT_EXEC_RESULT)
	{
		switch (libpqwp_get_query_result(state->wp_conn))
		{
			case WP_EXEC_SUCCESS_COPYBOTH:
				state->rem_state = RS_ESTABLISHED;
				break;
			case WP_EXEC_NEEDS_INPUT:
				return NEON_WALREAD_WOULDBLOCK;
			case WP_EXEC_FAILED:
				snprintf(state->err_msg, sizeof(state->err_msg),
						 "get START_REPLICATION result from %s failed: %s",
						 state->donor_name, PQerrorMessage(state->wp_conn->pg_conn));
				NeonWALReaderResetRemote(state);
				return NEON_WALREAD_ERROR;
			default:			/* can't happen */
				snprintf(state->err_msg, sizeof(state->err_msg),
						 "get START_REPLICATION result from %s: unexpected result",
						 state->donor_name);
				NeonWALReaderResetRemote(state);
				return NEON_WALREAD_ERROR;
		}
	}

	Assert(state->rem_state == RS_ESTABLISHED);

	/*
	 * If we had the request before, verify args are the same and advance the
	 * result ptr according to the progress; otherwise register the request.
	 */
	if (state->req_lsn != InvalidXLogRecPtr)
	{
		if (state->req_lsn != startptr || state->req_len != count)
		{
			snprintf(state->err_msg, sizeof(state->err_msg),
					 "args changed during request, was %X/%X %zu, now %X/%X %zu",
					 LSN_FORMAT_ARGS(state->req_lsn), state->req_len, LSN_FORMAT_ARGS(startptr), count);
			NeonWALReaderResetRemote(state);
			return NEON_WALREAD_ERROR;
		}
		nwr_log(DEBUG5, "continuing remote read at req_lsn=%X/%X len=%zu, req_progress=%zu",
				LSN_FORMAT_ARGS(startptr),
				count,
				state->req_progress);
		buf += state->req_progress;
	}
	else
	{
		state->req_lsn = startptr;
		state->req_len = count;
		state->req_progress = 0;
		nwr_log(DEBUG5, "starting remote read req_lsn=%X/%X len=%zu",
				LSN_FORMAT_ARGS(startptr),
				count);
	}

	while (true)
	{
		Size		to_copy;

		/*
		 * If we have no ready data, receive new message.
		 */
		if (state->wal_rem_len == 0 &&

		/*
		 * check for the sake of 0 length reads; walproposer does these for
		 * heartbeats, though generally they shouldn't hit remote source.
		 */
			state->req_len - state->req_progress > 0)
		{
			NeonWALReadResult read_msg_res = NeonWALReaderReadMsg(state);

			if (read_msg_res != NEON_WALREAD_SUCCESS)
				return read_msg_res;
		}

		if (state->req_lsn + state->req_progress != state->rem_lsn)
		{
			snprintf(state->err_msg, sizeof(state->err_msg),
					 "expected remote WAL at %X/%X but got %X/%X. Non monotonic read requests could have caused this. req_lsn=%X/%X len=%zu",
					 LSN_FORMAT_ARGS(state->req_lsn + state->req_progress),
					 LSN_FORMAT_ARGS(state->rem_lsn),
					 LSN_FORMAT_ARGS(state->req_lsn),
					 state->req_len);
			NeonWALReaderResetRemote(state);
			return NEON_WALREAD_ERROR;
		}

		/* We can copy min of (available, requested) bytes. */
		to_copy =
			Min(state->req_len - state->req_progress, state->wal_rem_len);
		memcpy(buf, state->wal_ptr, to_copy);
		state->wal_ptr += to_copy;
		state->wal_rem_len -= to_copy;
		state->rem_lsn += to_copy;
		if (state->wal_rem_len == 0)
			state->wal_ptr = NULL;	/* freed by libpqwalproposer */
		buf += to_copy;
		state->req_progress += to_copy;
		if (state->req_progress == state->req_len)
		{
			XLogSegNo	next_segno;
			XLogSegNo	req_segno;

			XLByteToSeg(state->req_lsn, req_segno, state->segcxt.ws_segsize);
			XLByteToSeg(state->rem_lsn, next_segno, state->segcxt.ws_segsize);

			/*
			 * Request completed. If there is a chance of serving next one
			 * locally, close the connection.
			 */
			if (state->req_lsn < state->available_lsn &&
				state->rem_lsn >= state->available_lsn)
			{
				nwr_log(LOG, "closing remote connection as available_lsn %X/%X crossed and next read at %X/%X is likely to be served locally",
						LSN_FORMAT_ARGS(state->available_lsn), LSN_FORMAT_ARGS(state->rem_lsn));
				NeonWALReaderResetRemote(state);
			}
			else if (state->rem_lsn >= state->available_lsn && next_segno > req_segno &&
					 is_wal_segment_exists(next_segno, state->segcxt.ws_segsize, tli))
			{
				nwr_log(LOG, "closing remote connection as WAL file at next lsn %X/%X exists",
						LSN_FORMAT_ARGS(state->rem_lsn));
				NeonWALReaderResetRemote(state);
			}
			state->req_lsn = InvalidXLogRecPtr;
			state->req_len = 0;
			state->req_progress = 0;

			/* Update the current segment info. */
			state->seg.ws_tli = tli;

			return NEON_WALREAD_SUCCESS;
		}
	}
}

/*
 * Read one WAL message from the stream, sets state->wal_ptr in case of success.
 * Resets remote state in case of failure.
 */
static NeonWALReadResult
NeonWALReaderReadMsg(NeonWALReader *state)
{
	while (true)				/* loop until we get 'w' */
	{
		char	   *copydata_ptr;
		int			copydata_size;
		StringInfoData s;
		char		msg_type;
		int			hdrlen;

		Assert(state->rem_state == RS_ESTABLISHED);
		Assert(state->wal_ptr == NULL && state->wal_rem_len == 0);

		switch (libpqwp_async_read(state->wp_conn,
								   &copydata_ptr,
								   &copydata_size))
		{
			case PG_ASYNC_READ_SUCCESS:
				break;
			case PG_ASYNC_READ_TRY_AGAIN:
				return NEON_WALREAD_WOULDBLOCK;
			case PG_ASYNC_READ_FAIL:
				snprintf(state->err_msg,
						 sizeof(state->err_msg),
						 "req_lsn=%X/%X, req_len=%zu, req_progress=%zu, get copydata failed: %s",
						 LSN_FORMAT_ARGS(state->req_lsn),
						 state->req_len,
						 state->req_progress,
						 PQerrorMessage(state->wp_conn->pg_conn));
				goto err;
		}

		/* put data on StringInfo to parse */
		s.data = copydata_ptr;
		s.len = copydata_size;
		s.cursor = 0;
		s.maxlen = -1;

		if (copydata_size == 0)
		{
			snprintf(state->err_msg,
					 sizeof(state->err_msg),
					 "zero length copydata received");
			goto err;
		}
		msg_type = pq_getmsgbyte(&s);
		switch (msg_type)
		{
			case 'w':
				{
					XLogRecPtr	start_lsn;

					hdrlen = sizeof(int64) + sizeof(int64) + sizeof(int64);
					if (s.len - s.cursor < hdrlen)
					{
						snprintf(state->err_msg,
								 sizeof(state->err_msg),
								 "invalid WAL message received from primary");
						goto err;
					}

					start_lsn = pq_getmsgint64(&s);
					pq_getmsgint64(&s); /* XLogRecPtr	end_lsn; */
					pq_getmsgint64(&s); /* TimestampTz send_time */

					state->rem_lsn = start_lsn;
					state->wal_rem_len = (Size) (s.len - s.cursor);
					state->wal_ptr = (char *) pq_getmsgbytes(&s, s.len - s.cursor);
					nwr_log(DEBUG5, "received WAL msg at %X/%X len %zu",
							LSN_FORMAT_ARGS(state->rem_lsn), state->wal_rem_len);

					return NEON_WALREAD_SUCCESS;
				}
			case 'k':
				{
					XLogRecPtr	end_lsn;
					bool		reply_requested;

					hdrlen = sizeof(int64) + sizeof(int64) + sizeof(char);
					if (s.len - s.cursor < hdrlen)
					{
						snprintf(state->err_msg, sizeof(state->err_msg),
								 "invalid keepalive message received from primary");
						goto err;
					}

					end_lsn = pq_getmsgint64(&s);
					pq_getmsgint64(&s); /* TimestampTz timestamp; */
					reply_requested = pq_getmsgbyte(&s);
					nwr_log(DEBUG5, "received keepalive end_lsn=%X/%X reply_requested=%d",
							LSN_FORMAT_ARGS(end_lsn),
							reply_requested);
					if (end_lsn < state->req_lsn + state->req_len)
					{
						snprintf(state->err_msg, sizeof(state->err_msg),
								 "closing remote connection: requested WAL up to %X/%X, but current donor %s has only up to %X/%X",
								 LSN_FORMAT_ARGS(state->req_lsn + state->req_len), state->donor_name, LSN_FORMAT_ARGS(end_lsn));
						goto err;
					}
					continue;
				}
			default:
				nwr_log(WARNING, "invalid replication message type %d", msg_type);
				continue;
		}
	}
err:
	NeonWALReaderResetRemote(state);
	return NEON_WALREAD_ERROR;
}

/* reset remote connection and request in progress */
void
NeonWALReaderResetRemote(NeonWALReader *state)
{
	state->req_lsn = InvalidXLogRecPtr;
	state->req_len = 0;
	state->req_progress = 0;
	state->rem_state = RS_NONE;
	if (state->wp_conn)
	{
		libpqwp_disconnect(state->wp_conn);
		state->wp_conn = NULL;
	}
	state->donor_name[0] = '\0';
	state->wal_ptr = NULL;
	state->wal_rem_len = 0;
	state->rem_lsn = InvalidXLogRecPtr;
}

/*
 * Return socket of connection to remote source. Must be called only when
 * connection exists (NeonWALReaderEvents returns non zero).
 */
pgsocket
NeonWALReaderSocket(NeonWALReader *state)
{
	if (!state->wp_conn)
		nwr_log(FATAL, "NeonWALReaderSocket is called without active remote connection");
	return PQsocket(state->wp_conn->pg_conn);
}

/*
 * Whether remote connection is established. Once this is done, until successful
 * local read or error socket is stable and user can update socket events
 * instead of readding it each time.
 */
bool
NeonWALReaderIsRemConnEstablished(NeonWALReader *state)
{
	return state->rem_state == RS_ESTABLISHED;
}

/*
 * Returns events user should wait on connection socket or 0 if remote
 * connection is not active.
 */
extern uint32
NeonWALReaderEvents(NeonWALReader *state)
{
	switch (state->rem_state)
	{
		case RS_NONE:
			return 0;
		case RS_CONNECTING_READ:
			return WL_SOCKET_READABLE;
		case RS_CONNECTING_WRITE:
			return WL_SOCKET_WRITEABLE;
		case RS_WAIT_EXEC_RESULT:
		case RS_ESTABLISHED:
			return WL_SOCKET_READABLE;
		default:
			Assert(false);
			return 0;			/* make compiler happy */
	}
}

static bool
NeonWALReadLocal(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	p = buf;
	recptr = startptr;
	nbytes = count;

/* Try to read directly from WAL buffers first. */
#if PG_MAJORVERSION_NUM >= 17
	{
		Size	rbytes;
		rbytes = WALReadFromBuffers(p, recptr, nbytes, tli);
		recptr += rbytes;
		nbytes -= rbytes;
		p += rbytes;
	}
#endif

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;
		XLogSegNo	lastRemovedSegNo;

		startoff = XLogSegmentOffset(recptr, state->segcxt.ws_segsize);

		/*
		 * If the data we want is not in a segment we have open, close what we
		 * have (if anything) and open the next one, using the caller's
		 * provided openSegment callback.
		 */
		if (state->seg.ws_file < 0 ||
			!XLByteInSeg(recptr, state->seg.ws_segno, state->segcxt.ws_segsize) ||
			tli != state->seg.ws_tli)
		{
			XLogSegNo	nextSegNo;

			neon_wal_segment_close(state);

			XLByteToSeg(recptr, nextSegNo, state->segcxt.ws_segsize);
			if (!neon_wal_segment_open(state, nextSegNo, &tli))
			{
				char		fname[MAXFNAMELEN];

				state->wre_errno = errno;

				XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);
				snprintf(state->err_msg, sizeof(state->err_msg), "failed to open WAL segment %s while reading at %X/%X: %s",
						 fname, LSN_FORMAT_ARGS(recptr), strerror(state->wre_errno));
				return false;
			}

			/* This shouldn't happen -- indicates a bug in segment_open */
			Assert(state->seg.ws_file >= 0);

			/* Update the current segment info. */
			state->seg.ws_tli = tli;
			state->seg.ws_segno = nextSegNo;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (state->segcxt.ws_segsize - startoff))
			segbytes = state->segcxt.ws_segsize - startoff;
		else
			segbytes = nbytes;

#ifndef FRONTEND
		pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
#endif

		/* Reset errno first; eases reporting non-errno-affecting errors */
		errno = 0;
		readbytes = pg_pread(state->seg.ws_file, p, segbytes, (off_t) startoff);

#ifndef FRONTEND
		pgstat_report_wait_end();
#endif

		if (readbytes <= 0)
		{
			char		fname[MAXFNAMELEN];

			XLogFileName(fname, state->seg.ws_tli, state->seg.ws_segno, state->segcxt.ws_segsize);

			if (readbytes < 0)
			{
				state->wre_errno = errno;
				snprintf(state->err_msg, sizeof(state->err_msg), "could not read from log segment %s, offset %d: %m: %s",
						 fname, startoff, strerror(state->wre_errno));
			}
			else
			{
				snprintf(state->err_msg, sizeof(state->err_msg), "could not read from log segment %s, offset %d: %m: unexpected EOF",
						 fname, startoff);
			}
			return false;
		}

		/*
		 * Recheck that the segment hasn't been removed while we were reading
		 * it.
		 */
		lastRemovedSegNo = XLogGetLastRemovedSegno();
		if (state->seg.ws_segno <= lastRemovedSegNo)
		{
			char		fname[MAXFNAMELEN];

			state->wre_errno = ENOENT;

			XLogFileName(fname, tli, state->seg.ws_segno, state->segcxt.ws_segsize);
			snprintf(state->err_msg, sizeof(state->err_msg), "WAL segment %s has been removed during the read, lastRemovedSegNo " UINT64_FORMAT,
					 fname, lastRemovedSegNo);
			return false;
		}

		/* Update state for read */
		recptr += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}

	return true;
}

XLogRecPtr
NeonWALReaderGetRemLsn(NeonWALReader *state)
{
	return state->rem_lsn;
}

const WALOpenSegment *
NeonWALReaderGetSegment(NeonWALReader *state)
{
	return &state->seg;
}

/*
 * Copy of vanilla wal_segment_open, but returns false in case of error instead
 * of ERROR, with errno set.
 *
 * XLogReaderRoutine->segment_open callback for local pg_wal files
 */
bool
neon_wal_segment_open(NeonWALReader *state, XLogSegNo nextSegNo,
					  TimeLineID *tli_p)
{
	TimeLineID	tli = *tli_p;
	char		path[MAXPGPATH];

	XLogFilePath(path, tli, nextSegNo, state->segcxt.ws_segsize);
	nwr_log(DEBUG5, "opening %s", path);
	state->seg.ws_file = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file >= 0)
		return true;

	return false;
}

static bool
is_wal_segment_exists(XLogSegNo segno, int segsize, TimeLineID tli)
{
	struct stat stat_buffer;
	char		path[MAXPGPATH];

	XLogFilePath(path, tli, segno, segsize);
	return stat(path, &stat_buffer) == 0;
}

/* copy of vanilla wal_segment_close with NeonWALReader */
void
neon_wal_segment_close(NeonWALReader *state)
{
	if (state->seg.ws_file >= 0)
	{
		close(state->seg.ws_file);
		/* need to check errno? */
		state->seg.ws_file = -1;
	}
}

char *
NeonWALReaderErrMsg(NeonWALReader *state)
{
	return state->err_msg;
}

/*
 * Returns true if there is a donor, and false otherwise
 */
bool
NeonWALReaderUpdateDonor(NeonWALReader *state)
{
	WalproposerShmemState *wps = GetWalpropShmemState();

	SpinLockAcquire(&wps->mutex);
	memcpy(state->donor_name, wps->donor_name, sizeof(state->donor_name));
	memcpy(state->donor_conninfo, wps->donor_conninfo, sizeof(state->donor_conninfo));
	state->donor_lsn = wps->donor_lsn;
	SpinLockRelease(&wps->mutex);
	return state->donor_name[0] != '\0';
}
