#include "postgres.h"

#include "libpq-fe.h"
#include "neon.h"
#include "walproposer.h"

/* Header in walproposer.h -- Wrapper struct to abstract away the libpq connection */
struct WalProposerConn
{
	PGconn	   *pg_conn;
	bool		is_nonblocking; /* whether the connection is non-blocking */
	char	   *recvbuf;		/* last received data from
								 * walprop_async_read */
};

/* Helper function */
static bool
ensure_nonblocking_status(WalProposerConn *conn, bool is_nonblocking)
{
	/* If we're already correctly blocking or nonblocking, all good */
	if (is_nonblocking == conn->is_nonblocking)
		return true;

	/* Otherwise, set it appropriately */
	if (PQsetnonblocking(conn->pg_conn, is_nonblocking) == -1)
		return false;

	conn->is_nonblocking = is_nonblocking;
	return true;
}

/* Exported function definitions */
char *
walprop_error_message(WalProposerConn *conn)
{
	return PQerrorMessage(conn->pg_conn);
}

WalProposerConnStatusType
walprop_status(WalProposerConn *conn)
{
	switch (PQstatus(conn->pg_conn))
	{
		case CONNECTION_OK:
			return WP_CONNECTION_OK;
		case CONNECTION_BAD:
			return WP_CONNECTION_BAD;
		default:
			return WP_CONNECTION_IN_PROGRESS;
	}
}

WalProposerConn *
walprop_connect_start(char *conninfo, char *password)
{
	WalProposerConn *conn;
	PGconn	   *pg_conn;
	const char *keywords[3];
	const char *values[3];
	int			n;

	/*
	 * Connect using the given connection string. If the
	 * NEON_AUTH_TOKEN environment variable was set, use that as
	 * the password.
	 *
	 * The connection options are parsed in the order they're given, so
	 * when we set the password before the connection string, the
	 * connection string can override the password from the env variable.
	 * Seems useful, although we don't currently use that capability
	 * anywhere.
	 */
	n = 0;
	if (password)
	{
		keywords[n] = "password";
		values[n] = neon_auth_token;
		n++;
	}
	keywords[n] = "dbname";
	values[n] = conninfo;
	n++;
	keywords[n] = NULL;
	values[n] = NULL;
	n++;
	pg_conn = PQconnectStartParams(keywords, values, 1);

	/*
	 * Allocation of a PQconn can fail, and will return NULL. We want to fully
	 * replicate the behavior of PQconnectStart here.
	 */
	if (!pg_conn)
		return NULL;

	/*
	 * And in theory this allocation can fail as well, but it's incredibly
	 * unlikely if we just successfully allocated a PGconn.
	 *
	 * palloc will exit on failure though, so there's not much we could do if
	 * it *did* fail.
	 */
	conn = palloc(sizeof(WalProposerConn));
	conn->pg_conn = pg_conn;
	conn->is_nonblocking = false;	/* connections always start in blocking
									 * mode */
	conn->recvbuf = NULL;
	return conn;
}

WalProposerConnectPollStatusType
walprop_connect_poll(WalProposerConn *conn)
{
	WalProposerConnectPollStatusType return_val;

	switch (PQconnectPoll(conn->pg_conn))
	{
		case PGRES_POLLING_FAILED:
			return_val = WP_CONN_POLLING_FAILED;
			break;
		case PGRES_POLLING_READING:
			return_val = WP_CONN_POLLING_READING;
			break;
		case PGRES_POLLING_WRITING:
			return_val = WP_CONN_POLLING_WRITING;
			break;
		case PGRES_POLLING_OK:
			return_val = WP_CONN_POLLING_OK;
			break;

			/*
			 * There's a comment at its source about this constant being
			 * unused. We'll expect it's never returned.
			 */
		case PGRES_POLLING_ACTIVE:
			elog(FATAL, "Unexpected PGRES_POLLING_ACTIVE returned from PQconnectPoll");

			/*
			 * This return is never actually reached, but it's here to make
			 * the compiler happy
			 */
			return WP_CONN_POLLING_FAILED;

		default:
			Assert(false);
			return_val = WP_CONN_POLLING_FAILED;	/* keep the compiler quiet */
	}

	return return_val;
}

bool
walprop_send_query(WalProposerConn *conn, char *query)
{
	/*
	 * We need to be in blocking mode for sending the query to run without
	 * requiring a call to PQflush
	 */
	if (!ensure_nonblocking_status(conn, false))
		return false;

	/* PQsendQuery returns 1 on success, 0 on failure */
	if (!PQsendQuery(conn->pg_conn, query))
		return false;

	return true;
}

WalProposerExecStatusType
walprop_get_query_result(WalProposerConn *conn)
{
	PGresult   *result;
	WalProposerExecStatusType return_val;

	/* Marker variable if we need to log an unexpected success result */
	char	   *unexpected_success = NULL;

	/* Consume any input that we might be missing */
	if (!PQconsumeInput(conn->pg_conn))
		return WP_EXEC_FAILED;

	if (PQisBusy(conn->pg_conn))
		return WP_EXEC_NEEDS_INPUT;


	result = PQgetResult(conn->pg_conn);

	/*
	 * PQgetResult returns NULL only if getting the result was successful &
	 * there's no more of the result to get.
	 */
	if (!result)
	{
		elog(WARNING, "[libpqwalproposer] Unexpected successful end of command results");
		return WP_EXEC_UNEXPECTED_SUCCESS;
	}

	/* Helper macro to reduce boilerplate */
#define UNEXPECTED_SUCCESS(msg) \
		return_val = WP_EXEC_UNEXPECTED_SUCCESS; \
		unexpected_success = msg; \
		break;


	switch (PQresultStatus(result))
	{
			/* "true" success case */
		case PGRES_COPY_BOTH:
			return_val = WP_EXEC_SUCCESS_COPYBOTH;
			break;

			/* Unexpected success case */
		case PGRES_EMPTY_QUERY:
			UNEXPECTED_SUCCESS("empty query return");
		case PGRES_COMMAND_OK:
			UNEXPECTED_SUCCESS("data-less command end");
		case PGRES_TUPLES_OK:
			UNEXPECTED_SUCCESS("tuples return");
		case PGRES_COPY_OUT:
			UNEXPECTED_SUCCESS("'Copy Out' response");
		case PGRES_COPY_IN:
			UNEXPECTED_SUCCESS("'Copy In' response");
		case PGRES_SINGLE_TUPLE:
			UNEXPECTED_SUCCESS("single tuple return");
		case PGRES_PIPELINE_SYNC:
			UNEXPECTED_SUCCESS("pipeline sync point");

			/* Failure cases */
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
		case PGRES_FATAL_ERROR:
		case PGRES_PIPELINE_ABORTED:
			return_val = WP_EXEC_FAILED;
			break;

		default:
			Assert(false);
			return_val = WP_EXEC_FAILED;	/* keep the compiler quiet */
	}

	if (unexpected_success)
		elog(WARNING, "[libpqwalproposer] Unexpected successful %s", unexpected_success);

	return return_val;
}

pgsocket
walprop_socket(WalProposerConn *conn)
{
	return PQsocket(conn->pg_conn);
}

int
walprop_flush(WalProposerConn *conn)
{
	return (PQflush(conn->pg_conn));
}

void
walprop_finish(WalProposerConn *conn)
{
	if (conn->recvbuf != NULL)
		PQfreemem(conn->recvbuf);
	PQfinish(conn->pg_conn);
	pfree(conn);
}

/*
 * Receive a message from the safekeeper.
 *
 * On success, the data is placed in *buf. It is valid until the next call
 * to this function.
 */
PGAsyncReadResult
walprop_async_read(WalProposerConn *conn, char **buf, int *amount)
{
	int			result;

	if (conn->recvbuf != NULL)
	{
		PQfreemem(conn->recvbuf);
		conn->recvbuf = NULL;
	}

	/* Call PQconsumeInput so that we have the data we need */
	if (!PQconsumeInput(conn->pg_conn))
	{
		*amount = 0;
		*buf = NULL;
		return PG_ASYNC_READ_FAIL;
	}

	/*
	 * The docs for PQgetCopyData list the return values as: 0 if the copy is
	 * still in progress, but no "complete row" is available -1 if the copy is
	 * done -2 if an error occurred (> 0) if it was successful; that value is
	 * the amount transferred.
	 *
	 * The protocol we use between walproposer and safekeeper means that we
	 * *usually* wouldn't expect to see that the copy is done, but this can
	 * sometimes be triggered by the server returning an ErrorResponse (which
	 * also happens to have the effect that the copy is done).
	 */
	switch (result = PQgetCopyData(conn->pg_conn, &conn->recvbuf, true))
	{
		case 0:
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_TRY_AGAIN;
		case -1:
			{
				/*
				 * If we get -1, it's probably because of a server error; the
				 * safekeeper won't normally send a CopyDone message.
				 *
				 * We can check PQgetResult to make sure that the server
				 * failed; it'll always result in PGRES_FATAL_ERROR
				 */
				ExecStatusType status = PQresultStatus(PQgetResult(conn->pg_conn));

				if (status != PGRES_FATAL_ERROR)
					elog(FATAL, "unexpected result status %d after failed PQgetCopyData", status);

				/*
				 * If there was actually an error, it'll be properly reported
				 * by calls to PQerrorMessage -- we don't have to do anything
				 * else
				 */
				*amount = 0;
				*buf = NULL;
				return PG_ASYNC_READ_FAIL;
			}
		case -2:
			*amount = 0;
			*buf = NULL;
			return PG_ASYNC_READ_FAIL;
		default:
			/* Positive values indicate the size of the returned result */
			*amount = result;
			*buf = conn->recvbuf;
			return PG_ASYNC_READ_SUCCESS;
	}
}

PGAsyncWriteResult
walprop_async_write(WalProposerConn *conn, void const *buf, size_t size)
{
	int			result;

	/* If we aren't in non-blocking mode, switch to it. */
	if (!ensure_nonblocking_status(conn, true))
		return PG_ASYNC_WRITE_FAIL;

	/*
	 * The docs for PQputcopyData list the return values as: 1 if the data was
	 * queued, 0 if it was not queued because of full buffers, or -1 if an
	 * error occurred
	 */
	result = PQputCopyData(conn->pg_conn, buf, size);

	/*
	 * We won't get a result of zero because walproposer always empties the
	 * connection's buffers before sending more
	 */
	Assert(result != 0);

	switch (result)
	{
		case 1:
			/* good -- continue */
			break;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			elog(FATAL, "invalid return %d from PQputCopyData", result);
	}

	/*
	 * After queueing the data, we still need to flush to get it to send. This
	 * might take multiple tries, but we don't want to wait around until it's
	 * done.
	 *
	 * PQflush has the following returns (directly quoting the docs): 0 if
	 * sucessful, 1 if it was unable to send all the data in the send queue
	 * yet -1 if it failed for some reason
	 */
	switch (result = PQflush(conn->pg_conn))
	{
		case 0:
			return PG_ASYNC_WRITE_SUCCESS;
		case 1:
			return PG_ASYNC_WRITE_TRY_FLUSH;
		case -1:
			return PG_ASYNC_WRITE_FAIL;
		default:
			elog(FATAL, "invalid return %d from PQflush", result);
	}
}

/*
 * This function is very similar to walprop_async_write. For more
 * information, refer to the comments there.
 */
bool
walprop_blocking_write(WalProposerConn *conn, void const *buf, size_t size)
{
	int			result;

	/* If we are in non-blocking mode, switch out of it. */
	if (!ensure_nonblocking_status(conn, false))
		return false;

	if ((result = PQputCopyData(conn->pg_conn, buf, size)) == -1)
		return false;

	Assert(result == 1);

	/* Because the connection is non-blocking, flushing returns 0 or -1 */

	if ((result = PQflush(conn->pg_conn)) == -1)
		return false;

	Assert(result == 0);
	return true;
}
