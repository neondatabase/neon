import threading
import time
from contextlib import closing

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


# Test updating neon.pageserver_connstring setting on the fly.
#
# This merely changes some whitespace in the connection string, so
# this doesn't prove that the new string actually takes effect. But at
# least the code gets exercised.
def test_pageserver_reconnect(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    env.neon_cli.create_branch("test_pageserver_restarts")
    endpoint = env.endpoints.create_start("test_pageserver_restarts")
    n_reconnects = 1000
    timeout = 0.01
    scale = 10

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", f"-T{int(n_reconnects*timeout)}", connstr])

    thread = threading.Thread(target=run_pgbench, args=(endpoint.connstr(),), daemon=True)
    thread.start()

    with closing(endpoint.connect()) as con:
        with con.cursor() as c:
            c.execute("SELECT setting FROM pg_settings WHERE name='neon.pageserver_connstring'")
            connstring = c.fetchall()[0][0]
            for i in range(n_reconnects):
                time.sleep(timeout)
                c.execute(
                    "alter system set neon.pageserver_connstring=%s",
                    (connstring + (" " * (i % 2)),),
                )
                c.execute("select pg_reload_conf()")

    thread.join()
