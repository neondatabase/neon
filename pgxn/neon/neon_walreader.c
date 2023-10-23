/*
 * Like WALRead, but returns error instead of throwing ERROR when segment is
 * missing + doesn't attempt to read WAL before specified horizon -- basebackup
 * LSN. Missing WAL should be fetched by peer recovery, or, alternatively, on
 * demand WAL fetching from safekeepers should be implemented in NeonWALReader.
 */
#include "postgres.h"

#include <unistd.h>

#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "storage/fd.h"
#include "utils/wait_event.h"

#include "neon_walreader.h"
#include "walproposer.h"

#define NEON_WALREADER_ERR_MSG_LEN	   128

static void neon_wal_segment_close(NeonWALReader *state);

struct NeonWALReader
{
	/* LSN before */
	XLogRecPtr	available_lsn;
	WALSegmentContext segcxt;
	WALOpenSegment seg;
	int			wre_errno;
	/* Explains failure to read, static for simplicity. */
	char		err_msg[NEON_WALREADER_ERR_MSG_LEN];
};

/* palloc and initialize NeonWALReader */
NeonWALReader *
NeonWALReaderAllocate(int wal_segment_size, XLogRecPtr available_lsn)
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
void
neon_wal_segment_close(NeonWALReader *state)
{
	close(state->seg.ws_file);
	/* need to check errno? */
	state->seg.ws_file = -1;
}

/*
 * Mostly copy of vanilla WALRead, but 1) returns error if requested data before
 * available_lsn 2) returns error is segment is missing instead of throwing
 * ERROR.
 *
 * Read 'count' bytes into 'buf', starting at location 'startptr', from WAL
 * fetched from timeline 'tli'.
 *
 * Returns true if succeeded, false if an error occurs, in which case
 * 'state->errno' shows whether it was missing WAL (ENOENT) or something else,
 * and 'err' the desciption.
 */
bool
NeonWALRead(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	if (startptr < state->available_lsn)
	{
		state->wre_errno = 0;
		snprintf(state->err_msg, sizeof(state->err_msg), "failed to read WAL at %X/%X which is earlier than available %X/%X",
				 LSN_FORMAT_ARGS(startptr), LSN_FORMAT_ARGS(state->available_lsn));
		return false;
	}

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

			if (state->seg.ws_file >= 0)
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

char *
NeonWALReaderErrMsg(NeonWALReader *state)
{
	return state->err_msg;
}
