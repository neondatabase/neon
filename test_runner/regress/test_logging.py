from __future__ import annotations

import os
import uuid

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder
from fixtures.utils import run_only_on_default_postgres, wait_until


@pytest.mark.parametrize("level", ["trace", "debug", "info", "warn", "error"])
@run_only_on_default_postgres("it does not use any postgres functionality")
def test_logging_event_count(neon_env_builder: NeonEnvBuilder, level: str):
    # self-test: make sure the event is logged (i.e., our testing endpoint works)
    log_expected = {
        "trace": False,
        "debug": False,
        "info": True,
        "warn": True,
        "error": True,
    }[level]

    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()
    msg_id = uuid.uuid4().hex

    # NB: the _total suffix is added by our prometheus client
    before = ps_http.get_metric_value("libmetrics_tracing_event_count_total", {"level": level})

    # post the event
    ps_http.post_tracing_event(level, msg_id)
    if log_expected:
        env.pageserver.allowed_errors.append(f".*{msg_id}.*")

    def assert_logged():
        if not log_expected:
            return
        env.pageserver.assert_log_contains(f".*{msg_id}.*")

    wait_until(assert_logged)

    # make sure it's counted
    def assert_metric_value():
        if not log_expected:
            return
        # NB: the _total suffix is added by our prometheus client
        val = ps_http.get_metric_value("libmetrics_tracing_event_count_total", {"level": level})
        val = val or 0.0
        log.info("libmetrics_tracing_event_count: %s", val)
        assert val > (before or 0.0)

    wait_until(assert_metric_value)


def test_base_audit_logging(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create("main")

    endpoint.respec(
        audit_log_level="Log",
    )

    endpoint.start()

    with endpoint.cursor() as cursor:
        cursor.execute("CREATE DATABASE test")

    with endpoint.cursor(dbname="test") as cursor:
        cursor.execute("CREATE TABLE tbl(a int)")
        cursor.execute("DROP TABLE tbl")

    endpoint.stop()

    log_path = os.path.join(endpoint.endpoint_path(), "compute.log")
    with open(log_path) as log_file:
        logs = log_file.read()
        # Check that audit log is present in the logs
        # expected log entries:
        # LOG:  AUDIT: SESSION,1,1,DDL,CREATE TABLE,,,CREATE TABLE tbl(a int),<not logged>
        # LOG:  AUDIT: SESSION,2,1,DDL,DROP TABLE,,,DROP TABLE tbl,<not logged>
        assert "CREATE TABLE tbl" in logs
        assert "DROP TABLE tbl" in logs
