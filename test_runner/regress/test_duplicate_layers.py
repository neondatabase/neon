import time

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin


# Test duplicate layer detection
#
# This test sets fail point at the end of first compaction phase:
# after flushing new L1 layers but before deletion of L0 layers
# it should cause generation of duplicate L1 layer by compaction after restart.
@pytest.mark.timeout(600)
def test_duplicate_layers(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # Use aggressive compaction and checkpoint settings
    tenant_id, _ = env.neon_cli.create_tenant(
        conf={
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            "compaction_period": "5 s",
            "compaction_threshold": "3",
        }
    )

    pageserver_http.configure_failpoints(("compact-level0-phase1-return-same", "return"))

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    connstr = endpoint.connstr(options="-csynchronous_commit=off")
    pg_bin.run_capture(["pgbench", "-i", "-s1", connstr])

    time.sleep(10)  # let compaction to be performed
    assert env.pageserver.log_contains("compact-level0-phase1-return-same")

    pg_bin.run_capture(["pgbench", "-P1", "-N", "-c5", "-T200", "-Mprepared", connstr])
