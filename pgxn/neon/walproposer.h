#ifndef __NEON_WALPROPOSER_H__
#define __NEON_WALPROPOSER_H__

#include "access/xlogdefs.h"
#include "postgres.h"
#include "port.h"
#include "access/xlog_internal.h"
#include "access/transam.h"
#include "nodes/replnodes.h"
#include "utils/uuid.h"
#include "replication/walreceiver.h"

#define SK_MAGIC 0xCafeCeefu
#define SK_PROTOCOL_VERSION 2

#define MAX_SAFEKEEPERS 32
#define MAX_SEND_SIZE (XLOG_BLCKSZ * 16)	/* max size of a single* WAL
											 * message */
#define XLOG_HDR_SIZE (1 + 8 * 3)	/* 'w' + startPos + walEnd + timestamp */
#define XLOG_HDR_START_POS 1	/* offset of start position in wal sender*
								 * message header */
#define XLOG_HDR_END_POS (1 + 8)	/* offset of end position in wal sender*
									 * message header */

/*
 * In the spirit of WL_SOCKET_READABLE and others, this corresponds to no events having occurred,
 * because all WL_* events are given flags equal to some (1 << i), starting from i = 0
 */
#define WL_NO_EVENTS 0

extern char *wal_acceptors_list;
extern int	wal_acceptor_reconnect_timeout;
extern int	wal_acceptor_connection_timeout;
extern bool am_wal_proposer;

struct WalProposerConn;			/* Defined in libpqwalproposer */
typedef struct WalProposerConn WalProposerConn;

struct WalMessage;
typedef struct WalMessage WalMessage;

/* Possible return values from ReadPGAsync */
typedef enum
{
	/* The full read was successful. buf now points to the data */
	PG_ASYNC_READ_SUCCESS,

	/*
	 * The read is ongoing. Wait until the connection is read-ready, then try
	 * again.
	 */
	PG_ASYNC_READ_TRY_AGAIN,
	/* Reading failed. Check PQerrorMessage(conn) */
	PG_ASYNC_READ_FAIL,
}			PGAsyncReadResult;

/* Possible return values from WritePGAsync */
typedef enum
{
	/* The write fully completed */
	PG_ASYNC_WRITE_SUCCESS,

	/*
	 * The write started, but you'll need to call PQflush some more times to
	 * finish it off. We just tried, so it's best to wait until the connection
	 * is read- or write-ready to try again.
	 *
	 * If it becomes read-ready, call PQconsumeInput and flush again. If it
	 * becomes write-ready, just call PQflush.
	 */
	PG_ASYNC_WRITE_TRY_FLUSH,
	/* Writing failed. Check PQerrorMessage(conn) */
	PG_ASYNC_WRITE_FAIL,
}			PGAsyncWriteResult;

/*
 * WAL safekeeper state, which is used to wait for some event.
 *
 * States are listed here in the order that they're executed.
 *
 * Most states, upon failure, will move back to SS_OFFLINE by calls to
 * ResetConnection or ShutdownConnection.
 */
typedef enum
{
	/*
	 * Does not have an active connection and will stay that way until further
	 * notice.
	 *
	 * Moves to SS_CONNECTING_WRITE by calls to ResetConnection.
	 */
	SS_OFFLINE,

	/*
	 * Connecting states. "_READ" waits for the socket to be available for
	 * reading, "_WRITE" waits for writing. There's no difference in the code
	 * they execute when polled, but we have this distinction in order to
	 * recreate the event set in HackyRemoveWalProposerEvent.
	 *
	 * After the connection is made, "START_WAL_PUSH" query is sent.
	 */
	SS_CONNECTING_WRITE,
	SS_CONNECTING_READ,

	/*
	 * Waiting for the result of the "START_WAL_PUSH" command.
	 *
	 * After we get a successful result, sends handshake to safekeeper.
	 */
	SS_WAIT_EXEC_RESULT,

	/*
	 * Executing the receiving half of the handshake. After receiving, moves
	 * to SS_VOTING.
	 */
	SS_HANDSHAKE_RECV,

	/*
	 * Waiting to participate in voting, but a quorum hasn't yet been reached.
	 * This is an idle state - we do not expect AdvancePollState to be called.
	 *
	 * Moved externally by execution of SS_HANDSHAKE_RECV, when we received a
	 * quorum of handshakes.
	 */
	SS_VOTING,

	/*
	 * Already sent voting information, waiting to receive confirmation from
	 * the node. After receiving, moves to SS_IDLE, if the quorum isn't
	 * reached yet.
	 */
	SS_WAIT_VERDICT,

	/* Need to flush ProposerElected message. */
	SS_SEND_ELECTED_FLUSH,

	/*
	 * Waiting for quorum to send WAL. Idle state. If the socket becomes
	 * read-ready, the connection has been closed.
	 *
	 * Moves to SS_ACTIVE only by call to StartStreaming.
	 */
	SS_IDLE,

	/*
	 * Active phase, when we acquired quorum and have WAL to send or feedback
	 * to read.
	 */
	SS_ACTIVE,
}			SafekeeperState;

/* Consensus logical timestamp. */
typedef uint64 term_t;

/* neon storage node id */
typedef uint64 NNodeId;

/*
 * Proposer <-> Acceptor messaging.
 */

/* Initial Proposer -> Acceptor message */
typedef struct ProposerGreeting
{
	uint64		tag;			/* message tag */
	uint32		protocolVersion;	/* proposer-safekeeper protocol version */
	uint32		pgVersion;
	pg_uuid_t	proposerId;
	uint64		systemId;		/* Postgres system identifier */
	uint8		timeline_id[16];	/* Neon timeline id */
	uint8		tenant_id[16];
	TimeLineID	timeline;
	uint32		walSegSize;
}			ProposerGreeting;

typedef struct AcceptorProposerMessage
{
	uint64		tag;
}			AcceptorProposerMessage;

/*
 * Acceptor -> Proposer initial response: the highest term acceptor voted for.
 */
typedef struct AcceptorGreeting
{
	AcceptorProposerMessage apm;
	term_t		term;
	NNodeId		nodeId;
}			AcceptorGreeting;

/*
 * Proposer -> Acceptor vote request.
 */
typedef struct VoteRequest
{
	uint64		tag;
	term_t		term;
	pg_uuid_t	proposerId;		/* for monitoring/debugging */
}			VoteRequest;

/* Element of term switching chain. */
typedef struct TermSwitchEntry
{
	term_t		term;
	XLogRecPtr	lsn;
}			TermSwitchEntry;

typedef struct TermHistory
{
	uint32		n_entries;
	TermSwitchEntry *entries;
}			TermHistory;

/* Vote itself, sent from safekeeper to proposer */
typedef struct VoteResponse
{
	AcceptorProposerMessage apm;
	term_t		term;
	uint64		voteGiven;

	/*
	 * Safekeeper flush_lsn (end of WAL) + history of term switches allow
	 * proposer to choose the most advanced one.
	 */
	XLogRecPtr	flushLsn;
	XLogRecPtr	truncateLsn;	/* minimal LSN which may be needed for*
								 * recovery of some safekeeper */
	TermHistory termHistory;
	XLogRecPtr	timelineStartLsn;	/* timeline globally starts at this LSN */
}			VoteResponse;

/*
 * Proposer -> Acceptor message announcing proposer is elected and communicating
 * epoch history to it.
 */
typedef struct ProposerElected
{
	uint64		tag;
	term_t		term;
	/* proposer will send since this point */
	XLogRecPtr	startStreamingAt;
	/* history of term switches up to this proposer */
	TermHistory *termHistory;
	/* timeline globally starts at this LSN */
	XLogRecPtr	timelineStartLsn;
}			ProposerElected;

/*
 * Header of request with WAL message sent from proposer to safekeeper.
 */
typedef struct AppendRequestHeader
{
	uint64		tag;
	term_t		term;			/* term of the proposer */

	/*
	 * LSN since which current proposer appends WAL (begin_lsn of its first
	 * record); determines epoch switch point.
	 */
	XLogRecPtr	epochStartLsn;
	XLogRecPtr	beginLsn;		/* start position of message in WAL */
	XLogRecPtr	endLsn;			/* end position of message in WAL */
	XLogRecPtr	commitLsn;		/* LSN committed by quorum of safekeepers */

	/*
	 * minimal LSN which may be needed for recovery of some safekeeper (end
	 * lsn + 1 of last chunk streamed to everyone)
	 */
	XLogRecPtr	truncateLsn;
	pg_uuid_t	proposerId;		/* for monitoring/debugging */
}			AppendRequestHeader;

/*
 * Hot standby feedback received from replica
 */
typedef struct HotStandbyFeedback
{
	TimestampTz ts;
	FullTransactionId xmin;
	FullTransactionId catalog_xmin;
}			HotStandbyFeedback;

typedef struct PageserverFeedback
{
	/* current size of the timeline on pageserver */
	uint64		currentClusterSize;
	/* standby_status_update fields that safekeeper received from pageserver */
	XLogRecPtr	last_received_lsn;
	XLogRecPtr	disk_consistent_lsn;
	XLogRecPtr	remote_consistent_lsn;
	TimestampTz replytime;
}			PageserverFeedback;

typedef struct WalproposerShmemState
{
	slock_t		mutex;
	PageserverFeedback feedback;
	term_t		mineLastElectedTerm;
	pg_atomic_uint64 backpressureThrottlingTime;
}			WalproposerShmemState;

/*
 * Report safekeeper state to proposer
 */
typedef struct AppendResponse
{
	AcceptorProposerMessage apm;

	/*
	 * Current term of the safekeeper; if it is higher than proposer's, the
	 * compute is out of date.
	 */
	term_t		term;
	/* TODO: add comment */
	XLogRecPtr	flushLsn;
	/* Safekeeper reports back his awareness about which WAL is committed, as */
	/* this is a criterion for walproposer --sync mode exit */
	XLogRecPtr	commitLsn;
	HotStandbyFeedback hs;
	/* Feedback received from pageserver includes standby_status_update fields */
	/* and custom neon feedback. */
	/* This part of the message is extensible. */
	PageserverFeedback rf;
}			AppendResponse;

/*  PageserverFeedback is extensible part of the message that is parsed separately */
/*  Other fields are fixed part */
#define APPENDRESPONSE_FIXEDPART_SIZE offsetof(AppendResponse, rf)

/*
 * Descriptor of safekeeper
 */
typedef struct Safekeeper
{
	char const *host;
	char const *port;

	/*
	 * connection string for connecting/reconnecting.
	 *
	 * May contain private information like password and should not be logged.
	 */
	char conninfo[MAXCONNINFO];

	/*
	 * postgres protocol connection to the WAL acceptor
	 *
	 * Equals NULL only when state = SS_OFFLINE. Nonblocking is set once we
	 * reach SS_ACTIVE; not before.
	 */
	WalProposerConn *conn;

	/*
	 * Temporary buffer for the message being sent to the safekeeper.
	 */
	StringInfoData outbuf;

	/*
	 * WAL reader, allocated for each safekeeper.
	 */
	XLogReaderState *xlogreader;

	/*
	 * Streaming will start here; must be record boundary.
	 */
	XLogRecPtr	startStreamingAt;

	bool		flushWrite;		/* set to true if we need to call AsyncFlush,*
								 * to flush pending messages */
	XLogRecPtr	streamingAt;	/* current streaming position */
	AppendRequestHeader appendRequest;	/* request for sending to safekeeper */

	int			eventPos;		/* position in wait event set. Equal to -1 if*
								 * no event */
	SafekeeperState state;		/* safekeeper state machine state */
	TimestampTz latestMsgReceivedAt;        /* when latest msg is received */
	AcceptorGreeting greetResponse; /* acceptor greeting */
	VoteResponse voteResponse;	/* the vote */
	AppendResponse appendResponse;	/* feedback for master */
} Safekeeper;

extern void WalProposerSync(int argc, char *argv[]);
extern void WalProposerMain(Datum main_arg);
extern void WalProposerBroadcast(XLogRecPtr startpos, XLogRecPtr endpos);
extern void WalProposerPoll(void);
extern void ParsePageserverFeedbackMessage(StringInfo reply_message,
											PageserverFeedback *rf);
extern void StartProposerReplication(StartReplicationCmd *cmd);

extern Size WalproposerShmemSize(void);
extern bool WalproposerShmemInit(void);
extern void replication_feedback_set(PageserverFeedback *rf);
extern void replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn);

/* libpqwalproposer hooks & helper type */

/* Re-exported PostgresPollingStatusType */
typedef enum
{
	WP_CONN_POLLING_FAILED = 0,
	WP_CONN_POLLING_READING,
	WP_CONN_POLLING_WRITING,
	WP_CONN_POLLING_OK,

	/*
	 * 'libpq-fe.h' still has PGRES_POLLING_ACTIVE, but says it's unused.
	 * We've removed it here to avoid clutter.
	 */
}			WalProposerConnectPollStatusType;

/* Re-exported and modified ExecStatusType */
typedef enum
{
	/* We received a single CopyBoth result */
	WP_EXEC_SUCCESS_COPYBOTH,

	/*
	 * Any success result other than a single CopyBoth was received. The
	 * specifics of the result were already logged, but it may be useful to
	 * provide an error message indicating which safekeeper messed up.
	 *
	 * Do not expect PQerrorMessage to be appropriately set.
	 */
	WP_EXEC_UNEXPECTED_SUCCESS,

	/*
	 * No result available at this time. Wait until read-ready, then call
	 * again. Internally, this is returned when PQisBusy indicates that
	 * PQgetResult would block.
	 */
	WP_EXEC_NEEDS_INPUT,
	/* Catch-all failure. Check PQerrorMessage. */
	WP_EXEC_FAILED,
}			WalProposerExecStatusType;

/* Re-exported ConnStatusType */
typedef enum
{
	WP_CONNECTION_OK,
	WP_CONNECTION_BAD,

	/*
	 * The original ConnStatusType has many more tags, but requests that they
	 * not be relied upon (except for displaying to the user). We don't need
	 * that extra functionality, so we collect them into a single tag here.
	 */
	WP_CONNECTION_IN_PROGRESS,
}			WalProposerConnStatusType;

/* Re-exported PQerrorMessage */
extern char *walprop_error_message(WalProposerConn *conn);

/* Re-exported PQstatus */
extern WalProposerConnStatusType walprop_status(WalProposerConn *conn);

/* Re-exported PQconnectStart */
extern WalProposerConn * walprop_connect_start(char *conninfo, char *password);

/* Re-exported PQconectPoll */
extern WalProposerConnectPollStatusType walprop_connect_poll(WalProposerConn *conn);

/* Blocking wrapper around PQsendQuery */
extern bool walprop_send_query(WalProposerConn *conn, char *query);

/* Wrapper around PQconsumeInput + PQisBusy + PQgetResult */
extern WalProposerExecStatusType walprop_get_query_result(WalProposerConn *conn);

/* Re-exported PQsocket */
extern pgsocket walprop_socket(WalProposerConn *conn);

/* Wrapper around PQconsumeInput (if socket's read-ready) + PQflush */
extern int	walprop_flush(WalProposerConn *conn);

/* Re-exported PQfinish */
extern void walprop_finish(WalProposerConn *conn);

/*
 * Ergonomic wrapper around PGgetCopyData
 *
 * Reads a CopyData block from a safekeeper, setting *amount to the number
 * of bytes returned.
 *
 * This function is allowed to assume certain properties specific to the
 * protocol with the safekeepers, so it should not be used as-is for any
 * other purpose.
 *
 * Note: If possible, using <AsyncRead> is generally preferred, because it
 * performs a bit of extra checking work that's always required and is normally
 * somewhat verbose.
 */
extern PGAsyncReadResult walprop_async_read(WalProposerConn *conn, char **buf, int *amount);

/*
 * Ergonomic wrapper around PQputCopyData + PQflush
 *
 * Starts to write a CopyData block to a safekeeper.
 *
 * For information on the meaning of return codes, refer to PGAsyncWriteResult.
 */
extern PGAsyncWriteResult walprop_async_write(WalProposerConn *conn, void const *buf, size_t size);

/*
 * Blocking equivalent to walprop_async_write_fn
 *
 * Returns 'true' if successful, 'false' on failure.
 */
extern bool walprop_blocking_write(WalProposerConn *conn, void const *buf, size_t size);

extern uint64 BackpressureThrottlingTime(void);

#endif							/* __NEON_WALPROPOSER_H__ */
