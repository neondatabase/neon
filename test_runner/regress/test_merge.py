import time
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.types import TimelineId
from fixtures.utils import query_scalar

#
# Merge ancestor branch with the main branch.
#
def test_merge(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # Override defaults: 4M checkpoint_distance, disable background compaction and gc.
    tenant, _ = env.neon_cli.create_tenant()

    main_branch = env.endpoints.create_start("main", tenant_id=tenant)
    main_cur = main_branch.connect().cursor()

    # Create table and insert some data
    main_cur.execute("CREATE TABLE t(x bigint primary key)")
    main_cur.execute("INSERT INTO t values(generate_series(1, 10000))");

    # Create branch ws.
    env.neon_cli.create_branch("ws", "main", tenant_id=tenant)
    ws_branch = env.endpoints.create_start("ws", tenant_id=tenant)
    log.info("postgres is running on 'ws' branch")

    # Merge brnach ws as mergeable:it create logical replication slots and pins WAL
    env.neon_cli.set_mergeable(ws_branch)

    # Insert more data in the branch
    ws_cur = ws_branch.connect().cursor()
    ws_cur.execute("INSERT INTO t values(generate_series(10001, 20000))")

    # Merge ws brnach intp main
    env.neon_cli.merge(ws_branch, main_branch)

    # sleep for some time until changes are applied
    time.sleep(2)

    # Check that changes are merged
    assert query_scalar(main_cur, "SELECT count(*) from t") == 20000
