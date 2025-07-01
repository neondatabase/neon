/*
 * Interface to set of libpq wrappers walproposer and neon_walreader need.
 * Similar to libpqwalreceiver, but it has blocking connection establishment and
 * pqexec which don't fit us. Implementation is at walproposer_pg.c.
 */
#ifndef ___LIBPQWALPROPOSER_H__
#define ___LIBPQWALPROPOSER_H__

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
} WalProposerExecStatusType;

/* Possible return values from walprop_async_read */
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
} PGAsyncReadResult;

/* Possible return values from walprop_async_write */
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
} PGAsyncWriteResult;

/*
 * This header is included by walproposer.h to define walproposer_api; if we're
 * building walproposer without pg, ignore libpq part, leaving only interface
 * types.
 */
#ifndef WALPROPOSER_LIB

#include "libpq-fe.h"

/*
 * Sometimes working directly with underlying PGconn is simpler, export the
 * whole thing for simplicity.
 */
typedef struct WalProposerConn
{
	PGconn	   *pg_conn;
	bool		is_nonblocking; /* whether the connection is non-blocking */
	char	   *recvbuf;		/* last received CopyData message from
								 * walprop_async_read */
} WalProposerConn;

extern WalProposerConn *libpqwp_connect_start(char *conninfo);
extern bool libpqwp_send_query(WalProposerConn *conn, char *query);
extern WalProposerExecStatusType libpqwp_get_query_result(WalProposerConn *conn);
extern PGAsyncReadResult libpqwp_async_read(WalProposerConn *conn, char **buf, int *amount);
extern void libpqwp_disconnect(WalProposerConn *conn);

#endif							/* WALPROPOSER_LIB */
#endif							/* ___LIBPQWALPROPOSER_H__ */
