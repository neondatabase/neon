# This test spawns pgbench in a thread in the background and concurrently restarts pageserver,
# checking how client is able to transparently restore connection to pageserver
#

from __future__ import annotations

import threading
import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_pageserver_restarts_under_worload(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    env.create_branch("test_pageserver_restarts")
    endpoint = env.endpoints.create_start("test_pageserver_restarts")
    n_restarts = 10
    scale = 10

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", f"-T{n_restarts}", connstr])

    thread = threading.Thread(target=run_pgbench, args=(endpoint.connstr(),), daemon=True)
    thread.start()

    for _ in range(n_restarts):
        # Stop the pageserver gracefully and restart it.
        time.sleep(1)
        env.pageserver.stop()
        env.pageserver.start()

    thread.join()
