from __future__ import annotations

import os
import pdb

import fixtures.pageserver.many_tenants as many_tenants
import pytest
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
)

from performance.pageserver.util import ensure_pageserver_ready_for_benchmarking

"""
Usage:
DEFAULT_PG_VERSION=16 BUILD_TYPE=debug NEON_ENV_BUILDER_USE_OVERLAYFS_FOR_SNAPSHOTS=1 INTERACTIVE=true \
    ./scripts/pytest --timeout 0 test_runner/performance/pageserver/interactive/test_many_small_tenants.py
"""


@pytest.mark.skipif(
    os.environ.get("INTERACTIVE", "false") != "true",
    reason="test is for interactive use only",
)
def test_many_small_tenants(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    _env = setup_env(neon_env_builder, 2)  # vary this to the desired number of tenants
    _pg_bin = pg_bin

    # drop into pdb so that we can debug pageserver interactively, use pdb here
    # For example, to interactively examine pageserver startup behavior, call
    #   _env.pageserver.stop(immediate=True)
    #   _env.pageserver.start()
    # from the pdb shell.
    pdb.set_trace()


def setup_env(
    neon_env_builder: NeonEnvBuilder,
    n_tenants: int,
) -> NeonEnv:
    def setup_template(env: NeonEnv):
        # create our template tenant
        config = {
            "gc_period": "0s",
            "checkpoint_timeout": "10 years",
            "compaction_period": "20 s",
            "compaction_threshold": 10,
            "compaction_target_size": 134217728,
            "checkpoint_distance": 268435456,
            "image_creation_threshold": 3,
        }
        template_tenant, template_timeline = env.create_tenant(set_default=True)
        env.pageserver.tenant_detach(template_tenant)
        env.pageserver.tenant_attach(template_tenant, config)
        ep = env.endpoints.create_start("main", tenant_id=template_tenant)
        ep.safe_psql("create table foo(b text)")
        for _ in range(0, 8):
            ep.safe_psql("insert into foo(b) values ('some text')")
            last_flush_lsn_upload(env, ep, template_tenant, template_timeline)
        ep.stop_and_destroy()
        return (template_tenant, template_timeline, config)

    def doit(neon_env_builder: NeonEnvBuilder) -> NeonEnv:
        return many_tenants.single_timeline(neon_env_builder, setup_template, n_tenants)

    env = neon_env_builder.build_and_use_snapshot(f"many-small-tenants-{n_tenants}", doit)

    env.start()
    ensure_pageserver_ready_for_benchmarking(env, n_tenants)

    return env
