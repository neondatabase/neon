#include "postgres.h"
#include "neon.h"
#include "walproposer.h"
#include "rust_bindings.h"

/* Header in walproposer.h -- Wrapper struct to abstract away the libpq connection */
struct WalProposerConn
{
	int64_t tcp;
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
	elog(INFO, "not implemented: walprop_status");
    return WP_CONNECTION_OK;
}

WalProposerConn *
walprop_connect_start(char *conninfo)
{
	WalProposerConn *conn;

	elog(INFO, "walprop_connect_start: %s", conninfo);
	
	const char *connstr_prefix = "host=node port=";
	Assert(strncmp(conninfo, connstr_prefix, strlen(connstr_prefix)) == 0);

	int nodeId = atoi(conninfo + strlen(connstr_prefix));

	conn = palloc(sizeof(WalProposerConn));
	conn->tcp = sim_open_tcp(nodeId);
	return conn;
}

WalProposerConnectPollStatusType
walprop_connect_poll(WalProposerConn *conn)
{
	elog(INFO, "not implemented: walprop_connect_poll");
    return WP_CONN_POLLING_OK;
}

bool
walprop_send_query(WalProposerConn *conn, char *query)
{
	elog(INFO, "not implemented: walprop_send_query");
    return true;
}

WalProposerExecStatusType
walprop_get_query_result(WalProposerConn *conn)
{
	elog(INFO, "not implemented: walprop_get_query_result");
    return WP_EXEC_SUCCESS_COPYBOTH;
}

pgsocket
walprop_socket(WalProposerConn *conn)
{
	return (pgsocket) conn->tcp;
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
	uintptr_t len;
	char *msg;
	
	msg = sim_msg_get_bytes(&len);
	*buf = msg;
	*amount = len;
	elog(INFO, "walprop_async_read: %d", len);

    return PG_ASYNC_READ_SUCCESS;
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
	elog(INFO, "not implemented: walprop_blocking_write");
	sim_msg_set_bytes(buf, size);
	sim_tcp_send(conn->tcp);
    return true;
}
