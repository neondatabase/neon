import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


# 1. Delete all tenants.
# 2. Enable walreceiver_connect failpoint.
# 3. Create a tenant + timeline.
# 4. Create a postgres so that walreceiver starts
#
# In step 4, the failpoint will trigger after establishing the walreceiver connection task_mgr task.
# The NeonEnvBuilder will scan the log for unexpected errors.
def test_wal_receiver_error_after_spawn(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    for tenant in ps_http.tenant_list():
        ps_http.tenant_detach(tenant["id"])

    pageserver_http = env.pageserver.http_client()
    pageserver_http.configure_failpoints(("walreceiver_handle_connection_post_spawn", "return"))

    tenant_id, _ = env.neon_cli.create_tenant()

    log.info("generate some WAL")
    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    # XXX waiting for the timeout increases the test runtime by tens of seconds
    with pytest.raises(
        Exception, match="page server returned error: Timed out while waiting for WAL record"
    ):
        pg.safe_psql_many(
            queries=[
                "CREATE TABLE t(key int primary key, value text)",
                "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
            ],
        )
    # pageserver will also log the error
    env.pageserver.allowed_errors.append(".*Timed out while waiting for WAL record.*")

    log.info(
        "the log should contain no unexpected error messages, that will be checked by NeonEnvBuilder"
    )
