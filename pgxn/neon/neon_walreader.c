/*
 * Like WALRead, but returns error instead of throwing ERROR when segment is
 * missing + doesn't attempt to read WAL before specified horizon -- basebackup
 * LSN. Missing WAL should be fetched by peer recovery, or, alternatively, on
 * demand WAL fetching from safekeepers should be implemented in NeonWALReader.
 *
 * We can't use libpqwalreceiver as it blocks during connection establishment
 * (and waiting for PQExec result), so use libpqwalproposer instead.
 *
 * TODO: keepalives are currently never sent, so the other side can close the
 * connection prematurely.
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

#include "libpq-fe.h"

#include "neon_walreader.h"
#include "walproposer.h"

#define NEON_WALREADER_ERR_MSG_LEN	   256

static NeonWALReadResult NeonWALReadRemote(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli);
static void NeonWALReaderResetRemote(NeonWALReader *state);
static bool NeonWALReadLocal(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli);
static bool neon_wal_segment_open(NeonWALReader *state, XLogSegNo nextSegNo, TimeLineID *tli_p);
static void neon_wal_segment_close(NeonWALReader *state);

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
	WalProposer *wp;			/* we learn donor through walproposer */
	char		donor_name[64]; /* saved donor safekeeper name for logging */
	/* state of connection to safekeeper */
	NeonWALReaderRemoteState rem_state;
	WalProposerConn *wp_conn;

	/*
	 * position in recvbuf from which we'll copy WAL next time, or NULL if
	 * there is no unprocessed message
	 */
	char	   *wal_ptr;

	/*
	 * LSN of wal_ptr position according to walsender to cross check against
	 * read request
	 */
	XLogRecPtr	rem_lsn;
};

/* palloc and initialize NeonWALReader */
NeonWALReader *
NeonWALReaderAllocate(int wal_segment_size, XLogRecPtr available_lsn, WalProposer *wp)
{
	NeonWALReader *reader;

	reader = (NeonWALReader *)
		palloc_extended(sizeof(NeonWALReader),
						MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	if (!reader)
		return NULL;

	reader->available_lsn = available_lsn;
	reader->seg.ws_file = -1;
	reader->seg.ws_segno = 0;
	reader->seg.ws_tli = 0;
	reader->segcxt.ws_segsize = wal_segment_size;

	reader->wp = wp;

	return reader;
}

void
NeonWALReaderFree(NeonWALReader *state)
{
	if (state->seg.ws_file != -1)
		neon_wal_segment_close(state);
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
		elog(LOG, "local read failed with segment doesn't exist, attempting remote");
		return NeonWALReadRemote(state, buf, startptr, count, tli);
	}
	else
	{
		return NEON_WALREAD_ERROR;
	}
}

static NeonWALReadResult
NeonWALReadRemote(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli)
{
	if (state->rem_state == RS_NONE)
	{
		/* no connection yet; start one */
		Safekeeper *donor = GetDonor(state->wp);

		if (donor == NULL)
		{
			snprintf(state->err_msg, sizeof(state->err_msg),
					 "failed to establish remote connection to fetch WAL: no donor available");
			return NEON_WALREAD_ERROR;
		}
		snprintf(state->donor_name, sizeof(state->donor_name), "%s:%s", donor->host, donor->port);
		elog(LOG, "establishing connection to %s to fetch WAL", state->donor_name);
		state->wp_conn = libpqwp_connect_start(donor->conninfo);
		if (PQstatus(state->wp_conn->pg_conn) == CONNECTION_BAD)
		{
			snprintf(state->err_msg, sizeof(state->err_msg),
					 "failed to connect to %s:%s to fetch WAL: immediately failed with %s",
					 state->donor_name, PQerrorMessage(state->wp_conn->pg_conn));
			NeonWALReaderResetRemote(state);
			return NEON_WALREAD_ERROR;
		}
		/* we'll poll immediately */
		state->rem_state = RS_CONNECTING_READ;
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

					snprintf(start_repl_query, sizeof(start_repl_query),
							 "START_REPLICATION PHYSICAL %X/%X (term='" UINT64_FORMAT "')",
							 LSN_FORMAT_ARGS(startptr), state->wp->propTerm);
					elog(LOG, "connection to %s to fetch WAL succeeded, running %s",
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
						 "get result from %s failed: %s",
						 state->donor_name, PQerrorMessage(state->wp_conn->pg_conn));
				NeonWALReaderResetRemote(state);
				return NEON_WALREAD_ERROR;
			default:			/* can't happen */
				snprintf(state->err_msg, sizeof(state->err_msg),
						 "get result from %s: unexpected result",
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
		elog(LOG, "moving ptr by %zu bytes restoring progress, req_lsn = %X/%X", state->req_progress, LSN_FORMAT_ARGS(startptr));
		buf += state->req_progress;
	}

	snprintf(state->err_msg, sizeof(state->err_msg), "remote read failed: not implemented");
	return NEON_WALREAD_ERROR;
}

/* reset remote connection and request in progress */
static void
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
		elog(FATAL, "NeonWALReaderSocket is called without active remote connection");
	return PQsocket(state->wp_conn->pg_conn);
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
			break;
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

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

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

		/* Update state for read */
		recptr += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}

	return true;
}

/*
 * Copy of vanilla wal_segment_open, but returns false in case of error instead
 * of ERROR, with errno set.
 *
 * XLogReaderRoutine->segment_open callback for local pg_wal files
 */
static bool
neon_wal_segment_open(NeonWALReader *state, XLogSegNo nextSegNo,
					  TimeLineID *tli_p)
{
	TimeLineID	tli = *tli_p;
	char		path[MAXPGPATH];

	XLogFilePath(path, tli, nextSegNo, state->segcxt.ws_segsize);
	walprop_log(LOG, "opening %s", path);
	state->seg.ws_file = BasicOpenFile(path, O_RDONLY | PG_BINARY);
	if (state->seg.ws_file >= 0)
		return true;

	return false;
}

/* copy of vanilla wal_segment_close with NeonWALReader */
static void
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
