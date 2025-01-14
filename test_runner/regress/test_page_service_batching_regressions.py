# NB: there are benchmarks that double-serve as tests inside the `performance` directory.

import subprocess
from pathlib import Path

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.timeout(30)  # test takes <20s if pageserver impl is correct
@pytest.mark.parametrize("kind", ["pageserver-stop", "tenant-detach"])
def test_slow_flush(neon_env_builder: NeonEnvBuilder, neon_binpath: Path, kind: str):
    def patch_pageserver_toml(config):
        config["page_service_pipelining"] = {
            "mode": "pipelined",
            "max_batch_size": 32,
            "execution": "concurrent-futures",
        }

    neon_env_builder.pageserver_config_override = patch_pageserver_toml
    env = neon_env_builder.init_start()

    log.info("make flush appear slow")

    log.info("sending requests until pageserver accepts no more")
    # TODO: extract this into a helper, like subprocess_capture,
    # so that we capture the stderr from the helper somewhere.
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
    assert child.stdout is not None
    buf = child.stdout.read(1)
    if len(buf) != 1:
        raise Exception("unexpected EOF")
    if buf != b"R":
        raise Exception(f"unexpected data: {buf!r}")
    log.info("helper reports pageserver accepts no more requests")
    log.info(
        "assuming pageserver connection handle is in a state where TCP has backpressured pageserver=>client response flush() into userspace"
    )

    if kind == "pageserver-stop":
        log.info("try to shut down the pageserver cleanly")
        env.pageserver.stop()
    elif kind == "tenant-detach":
        log.info("try to shut down the tenant")
        env.pageserver.tenant_detach(env.initial_tenant)
    else:
        raise ValueError(f"unexpected kind: {kind}")

    log.info("shutdown did not time out, test passed")
