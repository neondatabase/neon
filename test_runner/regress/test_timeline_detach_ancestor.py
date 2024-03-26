import random
import threading
import time
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import wait_until_tenant_active
from fixtures.types import Lsn, TimelineId
from fixtures.utils import query_scalar
from performance.test_perf_pgbench import get_scales_matrix
from requests import RequestException
from requests.exceptions import RetryError


def test_ancestor_detach_with_restart_after(neon_env_builder: NeonEnvBuilder):
    """
    Happy path, without gc problems. Does not require live-reparenting
    implementation, because we restart after operation.
    """

    env = neon_env_builder.init_start(initial_tenant_conf={
        "gc_period": "0s",
    })

    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id = env.initial_tenant) as ep:
        ep.safe_psql("CREATE TABLE foo AS SELECT i::bigint FROM generate_series(0, 8191) g(i);")

        # create a single layer for us to remote copy
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)
        client.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(8192, 16383) g(i);")

        branch_at = Lsn(ep.safe_psql("SELECT pg_current_wal_flush_lsn();")[0][0])
        # 16384 rows in branch

        ep.safe_psql("INSERT INTO foo SELECT i::bigint FROM generate_series(16384, 24575) g(i);")
        # 24576 rows in original main

    new_main_id = env.neon_cli.create_branch("new_main", "main", env.initial_tenant, ancestor_start_lsn=branch_at)

    # if this is done, we will see the timeline ancestor delete fail because it has children
    # if this is not done, we see compute startup failure
    # it breaks because of missing previous lsn
    # which the flush_frozen_layer does through Timeline::schedule_uploads

    write_to_branch_before_detach = True

    # does false cause a missing previous lsn?
    # no it's zenith.signal check between prev lsn being "invalid" or "none" without a hack

    if write_to_branch_before_detach:
        with env.endpoints.create_start("new_main", tenant_id = env.initial_tenant) as ep:
            assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384
            # make sure the ep is writable
            ep.safe_psql("CREATE TABLE audit AS SELECT 1 as starts;")

        client.timeline_checkpoint(env.initial_tenant, new_main_id)

    # does gc_cutoff move on the timeline?
    log.info(f"{client.timeline_detail(env.initial_tenant, env.initial_timeline)}")

    client.detach_ancestor(env.initial_tenant, new_main_id)

    env.pageserver.stop()
    env.pageserver.start()

    with env.endpoints.create_start("main", tenant_id = env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 24576

    with env.endpoints.create_start("new_main", tenant_id = env.initial_tenant) as ep:
        assert ep.safe_psql("SELECT count(*) FROM foo;")[0][0] == 16384

    client.timeline_delete(env.initial_tenant, env.initial_timeline)
