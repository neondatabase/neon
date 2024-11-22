from __future__ import annotations

import socket
import subprocess
from pathlib import Path
from types import TracebackType

import backoff
from fixtures.log_helper import log
from fixtures.neon_fixtures import PgProtocol, VanillaPostgres
from fixtures.port_distributor import PortDistributor


def generate_tls_cert(cn, certout, keyout):
    subprocess.run(
        [
            "openssl",
            "req",
            "-new",
            "-x509",
            "-days",
            "365",
            "-nodes",
            "-out",
            certout,
            "-keyout",
            keyout,
            "-subj",
            f"/CN={cn}",
        ]
    )


class PgSniRouter(PgProtocol):
    def __init__(
        self,
        neon_binpath: Path,
        port: int,
        destination: str,
        tls_cert: Path,
        tls_key: Path,
        test_output_dir: Path,
    ):
        # Must use a hostname rather than IP here, for SNI to work
        host = "localhost"
        super().__init__(host=host, port=port)

        self.host = host
        self.neon_binpath = neon_binpath
        self.port = port
        self.destination = destination
        self.tls_cert = tls_cert
        self.tls_key = tls_key
        self._popen: subprocess.Popen[bytes] | None = None
        self.test_output_dir = test_output_dir

    def start(self) -> PgSniRouter:
        assert self._popen is None
        args = [
            str(self.neon_binpath / "pg_sni_router"),
            *["--listen", f"127.0.0.1:{self.port}"],
            *["--tls-cert", str(self.tls_cert)],
            *["--tls-key", str(self.tls_key)],
            *["--destination", self.destination],
        ]

        router_log_path = self.test_output_dir / "pg_sni_router.log"
        router_log = open(router_log_path, "w")

        self._popen = subprocess.Popen(args, stderr=router_log)
        self._wait_until_ready()
        log.info(f"pg_sni_router started, log file: {router_log_path}")
        return self

    @backoff.on_exception(backoff.expo, OSError, max_time=10)
    def _wait_until_ready(self):
        socket.create_connection((self.host, self.port))

    # Sends SIGTERM to the proxy if it has been started
    def terminate(self):
        if self._popen:
            self._popen.terminate()

    # Waits for proxy to exit if it has been opened with a default timeout of
    # two seconds. Raises subprocess.TimeoutExpired if the proxy does not exit in time.
    def wait_for_exit(self, timeout=2):
        if self._popen:
            self._popen.wait(timeout=2)

    def __enter__(self) -> PgSniRouter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ):
        if self._popen is not None:
            self._popen.terminate()
            try:
                self._popen.wait(timeout=5)
            except subprocess.TimeoutExpired:
                log.warning("failed to gracefully terminate pg_sni_router; killing")
                self._popen.kill()


def test_pg_sni_router(
    vanilla_pg: VanillaPostgres,
    port_distributor: PortDistributor,
    neon_binpath: Path,
    test_output_dir: Path,
):
    generate_tls_cert(
        "endpoint.namespace.localtest.me",
        test_output_dir / "router.crt",
        test_output_dir / "router.key",
    )

    # Start a stand-alone Postgres to test with
    vanilla_pg.start()
    pg_port = vanilla_pg.default_options["port"]

    router_port = port_distributor.get_port()

    with PgSniRouter(
        neon_binpath=neon_binpath,
        port=router_port,
        destination="localtest.me",
        tls_cert=test_output_dir / "router.crt",
        tls_key=test_output_dir / "router.key",
        test_output_dir=test_output_dir,
    ) as router:
        router.start()

        out = router.safe_psql(
            "select 1",
            dbname="postgres",
            sslmode="require",
            host=f"endpoint--namespace--{pg_port}.localtest.me",
            hostaddr="127.0.0.1",
        )
        assert out[0][0] == 1
