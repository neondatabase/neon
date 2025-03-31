from __future__ import annotations

from typing import TYPE_CHECKING

from fixtures.metrics import parse_metrics
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


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
    samples = metrics.query_all("compute_ctl_up", {"build_tag": "latest"})
    assert len(samples) == 1
    assert samples[0].value == 1
