# NB: there are benchmarks that double-serve as tests inside the `performance` directory.

import subprocess
from pathlib import Path

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


def test_slow_flush(neon_env_builder: NeonEnvBuilder, neon_binpath: Path):
    def patch_pageserver_toml(config):
        config["page_service_pipelining"] = {
            "mode": "pipelined",
            "max_batch_size": 32,
            "execution": "concurrent-futures",
        }

    neon_env_builder.pageserver_config_override = patch_pageserver_toml
    env = neon_env_builder.init_start()

    log.info("make flush appear slow")

    log.info("filling pipe")
    child = subprocess.Popen(
        [
            neon_binpath / "test_helper_slow_client_reads",
            env.pageserver.connstr(),
            str(env.initial_tenant),
            str(env.initial_timeline),
        ],
        bufsize=0,  # unbuffered
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    buf = child.stdout.read(1)
    if len(buf) != 1:
        raise Exception("unexpected EOF")
    if buf != b"R":
        raise Exception(f"unexpected data: {buf!r}")
    log.info("helper reports pipe filled")

    log.info("try to shut down the tenant")
    env.pageserver.tenant_detach(env.initial_tenant)
