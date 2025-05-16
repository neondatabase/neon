from __future__ import annotations

import os
from typing import TYPE_CHECKING

from fixtures.metrics import parse_metrics
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv

from fixtures.log_helper import log


def test_compute_reconfigure(neon_simple_env: NeonEnv):
    """
    Test that we can change postgresql.conf settings even if
    skip_pg_catalog_updates=True is set.
    """
    env = neon_simple_env

    TEST_LOG_LINE_PREFIX = "%m [%p] [test_compute_reconfigure]: "

    endpoint = env.endpoints.create_start("main")

    # Check that the log line prefix is not set
    # or different from TEST_LOG_LINE_PREFIX
    with endpoint.cursor() as cursor:
        cursor.execute("SHOW log_line_prefix;")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] != TEST_LOG_LINE_PREFIX

    endpoint.respec_deep(
        **{
            "spec": {
                "skip_pg_catalog_updates": True,
                "cluster": {
                    "settings": [
                        {
                            "name": "log_line_prefix",
                            "vartype": "string",
                            "value": TEST_LOG_LINE_PREFIX,
                        }
                    ]
                },
            },
        }
    )
    endpoint.reconfigure()

    # Check that in logs we see that it was actually reconfigured,
    # not restarted or something else.
    endpoint.log_contains("INFO request{method=POST uri=/configure")

    # In /configure we only send SIGHUP at the end, so in theory
    # it doesn't necessarily mean that Postgres already reloaded
    # the new config; and it may race in some envs.
    # So we wait until we see the log line that the config was changed.
    def check_logs():
        endpoint.log_contains(
            f'[test_compute_reconfigure]: LOG:  parameter "log_line_prefix" changed to "{TEST_LOG_LINE_PREFIX}"'
        )

    wait_until(check_logs)

    # Check that the log line prefix is set
    with endpoint.cursor() as cursor:
        cursor.execute("SHOW log_line_prefix;")
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == TEST_LOG_LINE_PREFIX

    # Check that even after reconfigure and state transitions we still report
    # only the current status.
    client = endpoint.http_client()
    raw_metrics = client.metrics()
    metrics = parse_metrics(raw_metrics)
    samples = metrics.query_all("compute_ctl_up")
    assert len(samples) == 1
    assert samples[0].value == 1
    samples = metrics.query_all("compute_ctl_up", {"status": "running"})
    assert len(samples) == 1
    assert samples[0].value == 1
    # Check that build tag is reported
    build_tag = os.environ.get("BUILD_TAG", "latest")
    samples = metrics.query_all("compute_ctl_up", {"build_tag": build_tag})
    assert len(samples) == 1
    assert samples[0].value == 1


def test_compute_safekeeper_connstrings_duplicate(neon_simple_env: NeonEnv):
    """
    Test that we can change postgresql.conf settings even if
    skip_pg_catalog_updates=True is set.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    # grab the current value of neon.safekeepers
    sk_list = []
    with endpoint.cursor() as cursor:
        cursor.execute("SHOW neon.safekeepers;")
        row = cursor.fetchone()
        assert row is not None

        log.info(f'    initial neon.safekeepers: "{row}"')

        # build a safekeepers list with a duplicate
        sk_list.append(row[0])
        sk_list.append(row[0])

    safekeepers = ",".join(sk_list)
    log.info(f'reconfigure neon.safekeepers: "{safekeepers}"')

    # introduce duplicate entry in neon.safekeepers, on purpose
    endpoint.respec_deep(
        **{
            "spec": {
                "skip_pg_catalog_updates": True,
                "cluster": {
                    "settings": [
                        {
                            "name": "neon.safekeepers",
                            "vartype": "string",
                            "value": safekeepers,
                        }
                    ]
                },
            },
        }
    )

    try:
        endpoint.reconfigure()

        # Check that in logs we see that it was actually reconfigured,
        # not restarted or something else.
        endpoint.log_contains("INFO request{method=POST uri=/configure")

    except Exception as e:
        # we except a failure here
        log.info(f"RAISED: {e}" % e)
