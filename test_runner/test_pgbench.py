import pytest

pytest_plugins = ("fixtures.zenith_fixtures")


def test_pgbench(zen_simple):
    zen_simple.pg_bin.run_capture(
        'pgbench -h localhost -p 55432 -i postgres'.split())
    zen_simple.pg_bin.run_capture(
        'pgbench -h localhost -p 55432 -c 10 -T 100 -P 1 -M prepared postgres'.split())
