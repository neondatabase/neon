# What performance tests do we have and how we run them

Performance tests are built using infrastructure of our usual python integration tests.

## Tests that are run against local installation

Most of the performance tests run against a local installation. This causes some problems because safekeeper(s) and the pageserver share resources of one single host and one underlying disk.

These tests are run in CI in the same environment as the usual integration tests. So environment may not yield comarable results because this is the machine that CI provider gives us.

## Remote tests

There are a few tests that marked with `pytest.mark.remote_cluster`. These tests do not use local installation and only need a connection string to run. So they can be used for every postgresql compatible database. Currently these tests are run against our staging daily. Staging is not an isolated environment, so it adds to possible noise due to activity of other clusters.

## Noise

All tests run only once. Usually to obtain more consistent performance numbers test is performed multiple times and then some statistics is applied to the results, like min/max/avg/median etc.

## Results collection

Local tests results for main branch and results of daily performance tests are stored in neon cluster deployed in production environment and there is a grafana dashboard that visualizes the results. Here is the [dashboard](https://observer.zenith.tech/d/DGKBm9Jnz/perf-test-results?orgId=1). The main problem with it is the unavailability to point at particular commits though the data for that is available in the database. Needs some tweaking from someone who knows Grafana tricks.

There is also an inconsistency in test naming. Test name should be the same across platforms and results can be differentiated by the platform field. But now platform is sometimes included in test name because of the way how parametrization works in pytest. Ie there is a platform switch in the dashboard with zenith-local-ci and zenith-staging variants. I e some tests under zenith-local-ci value for a platform switch are displayed as `Test test_runner/performance/test_bulk_insert.py::test_bulk_insert[vanilla]` and `Test test_runner/performance/test_bulk_insert.py::test_bulk_insert[zenith]` which is highly confusing.
