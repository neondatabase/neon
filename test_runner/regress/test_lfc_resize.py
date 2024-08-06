import threading
import time
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


#
# Test branching, when a transaction is in prepared state
#
@pytest.mark.timeout(600)
def test_lfc_resize(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    cache_dir = Path(env.repo_dir) / "file_cache"
    cache_dir.mkdir(exist_ok=True)
    env.neon_cli.create_branch("test_lfc_resize", "empty")
    endpoint = env.endpoints.create_start(
        "test_lfc_resize",
        config_lines=[
            f"neon.file_cache_path='{cache_dir}/file.cache'",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
        ],
    )
    n_resize = 10
    scale = 10

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", "-c4", f"-T{n_resize}", "-Mprepared", connstr])

    thread = threading.Thread(target=run_pgbench, args=(endpoint.connstr(),), daemon=True)
    thread.start()

    conn = endpoint.connect()
    cur = conn.cursor()

    for i in range(n_resize):
        cur.execute(f"alter system set neon.file_cache_size_limit='{i*10}MB'")
        cur.execute("select pg_reload_conf()")
        time.sleep(1)

    thread.join()
