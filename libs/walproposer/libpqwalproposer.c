#include "postgres.h"
#include "neon.h"
#include "walproposer.h"

/* Header in walproposer.h -- Wrapper struct to abstract away the libpq connection */
struct WalProposerConn
{
};

/* Helper function */
static bool
ensure_nonblocking_status(WalProposerConn *conn, bool is_nonblocking)
{
	elog(INFO, "not implemented");
    return false;
}

/* Exported function definitions */
char *
walprop_error_message(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
    return NULL;
}

WalProposerConnStatusType
walprop_status(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
    return WP_CONNECTION_OK;
}

WalProposerConn *
walprop_connect_start(char *conninfo)
{
	elog(INFO, "not implemented");
    return NULL;
}

WalProposerConnectPollStatusType
walprop_connect_poll(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
    return WP_CONN_POLLING_OK;
}

bool
walprop_send_query(WalProposerConn *conn, char *query)
{
	elog(INFO, "not implemented");
    return false;
}

WalProposerExecStatusType
walprop_get_query_result(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
    return WP_EXEC_SUCCESS_COPYBOTH;
}

pgsocket
walprop_socket(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
    return 0;
}

int
walprop_flush(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
    return 0;
}

void
walprop_finish(WalProposerConn *conn)
{
	elog(INFO, "not implemented");
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
	elog(INFO, "not implemented");
    return PG_ASYNC_READ_FAIL;
}

PGAsyncWriteResult
walprop_async_write(WalProposerConn *conn, void const *buf, size_t size)
{
	elog(INFO, "not implemented");
    return PG_ASYNC_WRITE_FAIL;
}

/*
 * This function is very similar to walprop_async_write. For more
 * information, refer to the comments there.
 */
bool
walprop_blocking_write(WalProposerConn *conn, void const *buf, size_t size)
{
	elog(INFO, "not implemented");
    return false;
}
