from __future__ import annotations

pytest_plugins = (
    "fixtures.pg_version",
    "fixtures.parametrize",
    "fixtures.httpserver",
    "fixtures.compute_reconfigure",
    "fixtures.storage_controller_proxy",
    "fixtures.neon_fixtures",
    "fixtures.benchmark_fixture",
    "fixtures.pg_stats",
    "fixtures.compare_fixtures",
    "fixtures.slow",
    "fixtures.flaky",
)
