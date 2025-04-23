from __future__ import annotations

from typing import TYPE_CHECKING

from fixtures.metrics import parse_metrics
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_compute_monitor(neon_simple_env: NeonEnv):
    """
    Test that compute_ctl can detect Postgres going down (unresponsive) and
    reconnect when it comes back online. Also check that the downtime metrics
    are properly emitted.
    """
    TEST_DB = "test_compute_monitor"

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
