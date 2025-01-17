from __future__ import annotations

import os
from contextlib import closing

from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder


#
# Test starting Postgres with custom options
#
def test_config(neon_simple_env: NeonEnv):
    env = neon_simple_env

    # change config
    endpoint = env.endpoints.create_start("main", config_lines=["log_min_messages=debug1"])

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT setting
                FROM pg_settings
                WHERE
                    source != 'default'
                    AND source != 'override'
                    AND name = 'log_min_messages'
            """
            )

            # check that config change was applied
            assert cur.fetchone() == ("debug1",)


#
# Test that reordering of safekeepers does not restart walproposer
#
def test_safekeepers_reconfigure_reorder(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    env.create_branch("test_safekeepers_reconfigure_reorder")

    endpoint = env.endpoints.create_start("test_safekeepers_reconfigure_reorder")

    old_sks = ""
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW neon.safekeepers")
            res = cur.fetchone()
            assert res is not None, "neon.safekeepers GUC is set"
            old_sks = res[0]

    # Reorder safekeepers
    safekeepers = endpoint.active_safekeepers
    safekeepers = safekeepers[1:] + safekeepers[:1]

    endpoint.reconfigure(safekeepers=safekeepers)

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW neon.safekeepers")
            res = cur.fetchone()
            assert res is not None, "neon.safekeepers GUC is set"
            new_sks = res[0]

            assert new_sks != old_sks, "GUC changes were applied"

    log_path = os.path.join(endpoint.endpoint_path(), "compute.log")
    with open(log_path) as log_file:
        logs = log_file.read()
        # Check that walproposer was not restarted
        assert "restarting walproposer" not in logs
