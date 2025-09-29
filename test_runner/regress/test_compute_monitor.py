from __future__ import annotations

import time
from typing import TYPE_CHECKING

from fixtures.metrics import parse_metrics
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_compute_monitor_downtime_calculation(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can detect Postgres going down (unresponsive) and
    reconnect when it comes back online. Also check that the downtime metrics
    are properly emitted.
    """
    TEST_DB = "test_compute_monitor_downtime_calculation"

    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    # Check that default postgres database is present
    with endpoint.cursor() as cursor:
        cursor.execute("SELECT datname FROM pg_database WHERE datname = 'postgres'")
        catalog_db = cursor.fetchone()
        assert catalog_db is not None
        assert len(catalog_db) == 1

        # Create a new database
        cursor.execute(f"CREATE DATABASE {TEST_DB}")

    # Drop database 'postgres'
    with endpoint.cursor(dbname=TEST_DB) as cursor:
        # Use FORCE to terminate all connections to the database
        cursor.execute("DROP DATABASE postgres WITH (FORCE)")

    client = endpoint.http_client()

    def check_metrics_down():
        raw_metrics = client.metrics()
        metrics = parse_metrics(raw_metrics)
        compute_pg_current_downtime_ms = metrics.query_all("compute_pg_current_downtime_ms")
        assert len(compute_pg_current_downtime_ms) == 1
        assert compute_pg_current_downtime_ms[0].value > 0
        compute_pg_downtime_ms_total = metrics.query_all("compute_pg_downtime_ms_total")
        assert len(compute_pg_downtime_ms_total) == 1
        assert compute_pg_downtime_ms_total[0].value > 0

    wait_until(check_metrics_down)

    # Recreate postgres database
    with endpoint.cursor(dbname=TEST_DB) as cursor:
        cursor.execute("CREATE DATABASE postgres")

    # Current downtime should reset to 0, but not total downtime
    def check_metrics_up():
        raw_metrics = client.metrics()
        metrics = parse_metrics(raw_metrics)
        compute_pg_current_downtime_ms = metrics.query_all("compute_pg_current_downtime_ms")
        assert len(compute_pg_current_downtime_ms) == 1
        assert compute_pg_current_downtime_ms[0].value == 0
        compute_pg_downtime_ms_total = metrics.query_all("compute_pg_downtime_ms_total")
        assert len(compute_pg_downtime_ms_total) == 1
        assert compute_pg_downtime_ms_total[0].value > 0

    wait_until(check_metrics_up)

    # Just a sanity check that we log the downtime info
    endpoint.log_contains("downtime_info")


def test_compute_monitor_activity(neon_simple_env: NeonEnv):
    """
    Test compute monitor correctly detects user activity inside Postgres
    and updates last_active timestamp in the /status response.
    """
    TEST_DB = "test_compute_monitor_activity_db"

    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    with endpoint.cursor() as cursor:
        # Create a new database because `postgres` DB is excluded
        # from activity monitoring.
        cursor.execute(f"CREATE DATABASE {TEST_DB}")

    client = endpoint.http_client()

    prev_last_active = None

    def check_last_active():
        nonlocal prev_last_active

        with endpoint.cursor(dbname=TEST_DB) as cursor:
            # Execute some dummy query to generate 'activity'.
            cursor.execute("SELECT * FROM generate_series(1, 10000)")

        status = client.status()
        assert status["last_active"] is not None
        prev_last_active = status["last_active"]

    wait_until(check_last_active)

    assert prev_last_active is not None

    # Sleep for everything to settle down. It's not strictly necessary,
    # but should still remove any potential noise and/or prevent test from passing
    # even if compute monitor is not working.
    time.sleep(3)

    with endpoint.cursor(dbname=TEST_DB) as cursor:
        cursor.execute("SELECT * FROM generate_series(1, 10000)")

    def check_last_active_updated():
        nonlocal prev_last_active

        status = client.status()
        assert status["last_active"] is not None
        assert status["last_active"] != prev_last_active
        assert status["last_active"] > prev_last_active

    wait_until(check_last_active_updated)
