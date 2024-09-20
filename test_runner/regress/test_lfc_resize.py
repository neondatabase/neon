import os
import random
import re
import subprocess
import threading
import time

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


#
# Test branching, when a transaction is in prepared state
#
@pytest.mark.timeout(600)
def test_lfc_resize(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.file_cache_path='file.cache'",
            "neon.max_file_cache_size=512MB",
            "neon.file_cache_size_limit=512MB",
        ],
    )
    n_resize = 10
    scale = 100

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", "-c10", f"-T{n_resize}", "-Mprepared", "-S", connstr])

    thread = threading.Thread(target=run_pgbench, args=(endpoint.connstr(),), daemon=True)
    thread.start()

    conn = endpoint.connect()
    cur = conn.cursor()

    for _ in range(n_resize):
        size = random.randint(1, 512)
        cur.execute(f"alter system set neon.file_cache_size_limit='{size}MB'")
        cur.execute("select pg_reload_conf()")
        time.sleep(1)

    cur.execute("alter system set neon.file_cache_size_limit='100MB'")
    cur.execute("select pg_reload_conf()")

    thread.join()

    lfc_file_path = f"{endpoint.pg_data_dir_path()}/file.cache"
    lfc_file_size = os.path.getsize(lfc_file_path)
    res = subprocess.run(["ls", "-sk", lfc_file_path], check=True, text=True, capture_output=True)
    lfc_file_blocks = re.findall("([0-9A-F]+)", res.stdout)[0]
    log.info(f"Size of LFC file {lfc_file_size}, blocks {lfc_file_blocks}")
    assert lfc_file_size <= 512 * 1024 * 1024
    assert int(lfc_file_blocks) <= 128 * 1024
