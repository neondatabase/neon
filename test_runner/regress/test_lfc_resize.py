from __future__ import annotations

import os
import random
import re
import subprocess
import threading
import time
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin


@pytest.mark.timeout(600)
def test_lfc_resize(neon_simple_env: NeonEnv, pg_bin: PgBin):
    """
    Test resizing the Local File Cache
    """
    env = neon_simple_env
    cache_dir = Path(env.repo_dir) / "file_cache"
    cache_dir.mkdir(exist_ok=True)
    env.neon_cli.create_branch("test_lfc_resize", "empty")
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            f"neon.file_cache_path='{cache_dir}/file.cache'",
            "neon.max_file_cache_size=1GB",
            "neon.file_cache_size_limit=1GB",
        ],
    )
    n_resize = 10
    scale = 100

    def run_pgbench(connstr: str):
        log.info(f"Start a pgbench workload on pg {connstr}")
        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])
        pg_bin.run_capture(["pgbench", "-c10", f"-T{n_resize}", "-Mprepared", "-S", connstr])

    # Initializing the pgbench database can be very slow, especially on debug builds.
    connstr = endpoint.connstr(options="-cstatement_timeout=300s")

    thread = threading.Thread(target=run_pgbench, args=(connstr,), daemon=True)
    thread.start()

    conn = endpoint.connect()
    cur = conn.cursor()

    # For as long as pgbench is running, twiddle the LFC size once a second.
    # Note that we launch this immediately, already while the "pgbench -i"
    # initialization step is still running. That's quite a different workload
    # than the actual pgbench benchamark run, so this gives us coverage of both.
    while thread.is_alive():
        size = random.randint(1, 512)
        cur.execute(f"alter system set neon.file_cache_size_limit='{size}MB'")
        cur.execute("select pg_reload_conf()")
        time.sleep(1)
    thread.join()

    # At the end, set it at 100 MB, and perform a final check that the disk usage
    # of the file is in that ballbark.
    #
    # We retry the check a few times, because it might take a while for the
    # system to react to changing the setting and shrinking the file.
    cur.execute("alter system set neon.file_cache_size_limit='100MB'")
    cur.execute("select pg_reload_conf()")
    nretries = 10
    while True:
        lfc_file_path = f"{endpoint.pg_data_dir_path()}/file.cache"
        lfc_file_size = os.path.getsize(lfc_file_path)
        res = subprocess.run(
            ["ls", "-sk", lfc_file_path], check=True, text=True, capture_output=True
        )
        lfc_file_blocks = re.findall("([0-9A-F]+)", res.stdout)[0]
        log.info(f"Size of LFC file {lfc_file_size}, blocks {lfc_file_blocks}")
        assert lfc_file_size <= 512 * 1024 * 1024

        if int(lfc_file_blocks) <= 128 * 1024 or nretries == 0:
            break

        nretries = nretries - 1
        time.sleep(1)

    assert int(lfc_file_blocks) <= 128 * 1024
