#include "deps.c"

char	   *wal_acceptors_list;
int			wal_acceptor_reconnect_timeout = 1000;
int			wal_acceptor_connection_timeout = 5000;

// static void
// nwp_register_gucs(void)
// {
// 	DefineCustomStringVariable(
// 							   "neon.safekeepers",
// 							   "List of Neon WAL acceptors (host:port)",
// 							   NULL,	/* long_desc */
// 							   &wal_acceptors_list, /* valueAddr */
// 							   "",	/* bootValue */
// 							   PGC_POSTMASTER,
// 							   GUC_LIST_INPUT,	/* extensions can't use*
// 												 * GUC_LIST_QUOTE */
// 							   NULL, NULL, NULL);

// 	DefineCustomIntVariable(
// 							"neon.safekeeper_reconnect_timeout",
// 							"Timeout for reconnecting to offline wal acceptor.",
// 							NULL,
// 							&wal_acceptor_reconnect_timeout,
// 							1000, 0, INT_MAX,	/* default, min, max */
// 							PGC_SIGHUP, /* context */
// 							GUC_UNIT_MS,	/* flags */
// 							NULL, NULL, NULL);

// 	DefineCustomIntVariable(
// 							"neon.safekeeper_connect_timeout",
// 							"Timeout for connection establishement and it's maintenance against safekeeper",
// 							NULL,
// 							&wal_acceptor_connection_timeout,
// 							5000, 0, INT_MAX,
// 							PGC_SIGHUP,
// 							GUC_UNIT_MS,
// 							NULL, NULL, NULL);
// }

/*
 * Get latest redo apply position.
 *
 * Exported to allow WALReceiver to read the pointer directly.
 */
XLogRecPtr
GetXLogReplayRecPtr(void)
{
	// TODO
    return 0;
}

/*
 * GetFlushRecPtr -- Returns the current flush position, ie, the last WAL
 * position known to be fsync'd to disk.
 */
XLogRecPtr
GetFlushRecPtr(void)
{
    // TODO:
	return 0;
}

/*
 * RedoStartLsn is set only once by startup process, locking is not required
 * after its exit.
 */
XLogRecPtr
GetRedoStartLsn(void)
{
    // TODO:
	return 0;
}

TimestampTz
GetCurrentTimestamp(void)
{
    // TODO:
	return 0;
}

/* typedef in latch.h */
struct WaitEventSet
{
};
typedef struct WaitEventSet WaitEventSet;

typedef struct WaitEvent
{
	int			pos;			/* position in the event data structure */
	uint32		events;			/* triggered events */
	pgsocket	fd;				/* socket fd associated with event */
	void	   *user_data;		/* pointer provided in AddWaitEventToSet */
} WaitEvent;

int	WaitEventSetWait(WaitEventSet *set, long timeout,
							 WaitEvent *occurred_events, int nevents,
							 uint32 wait_event_info)
{
    // TODO:
    return 0;
}

extern PGDLLIMPORT struct Latch *MyLatch;

int	WaitLatchOrSocket(Latch *latch, int wakeEvents,
							  pgsocket sock, long timeout, uint32 wait_event_info)
{
    // TODO:
    return 0;
}

XLogReaderState *
XLogReaderAllocate()
{
    // TODO:
	return NULL;
}

uint64 systemId = 0;

/*
 * This is the default value for wal_segment_size to be used when initdb is run
 * without the --wal-segsize option.  It must be a valid segment size.
 */
#define DEFAULT_XLOG_SEG_SIZE	(16*1024*1024)
int			wal_segment_size = DEFAULT_XLOG_SEG_SIZE;

WaitEventSet *CreateWaitEventSet(int nevents)
{
    // TODO:
    return NULL;
}

void FreeWaitEventSet(WaitEventSet *set)
{
    // TODO:
}

int	AddWaitEventToSet(WaitEventSet *set, uint32 events, pgsocket fd,
							  Latch *latch, void *user_data)
{
    // TODO:
    return 0;
}

void ModifyWaitEvent(WaitEventSet *set, int pos, uint32 events, Latch *latch)
{
    // TODO:   
}

#define SizeOfXLogLongPHD 40UL
#define SizeOfXLogShortPHD 24UL

/*
 * Error information from WALRead that both backend and frontend caller can
 * process.  Currently only errors from pg_pread can be reported.
 */
typedef struct WALReadError
{
	int			wre_errno;		/* errno set by the last pg_pread() */
	int			wre_off;		/* Offset we tried to read from. */
	int			wre_req;		/* Bytes requested to be read. */
	int			wre_read;		/* Bytes read by the last read(). */
} WALReadError;

bool WALRead(XLogReaderState *state,
					char *buf, XLogRecPtr startptr, Size count,
					TimeLineID tli, WALReadError *errinfo)
{
    return false;
}


void PhysicalConfirmReceivedLocation(XLogRecPtr lsn)
{
}

void ResetLatch(Latch *latch)
{
}