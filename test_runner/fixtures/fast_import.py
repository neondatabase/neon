import os
import shutil
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from _pytest.config import Config

from fixtures.log_helper import log
from fixtures.neon_cli import AbstractNeonCli
from fixtures.pg_version import PgVersion


class FastImport(AbstractNeonCli):
    COMMAND = "fast_import"
    cmd: subprocess.CompletedProcess[str] | None = None

    def __init__(
        self,
        extra_env: dict[str, str] | None,
        binpath: Path,
        pg_distrib_dir: Path,
        pg_version: PgVersion,
        workdir: Path,
        cleanup: bool = True,
    ):
        if extra_env is None:
            env_vars = {}
        else:
            env_vars = extra_env.copy()

        if not (binpath / self.COMMAND).exists():
            raise Exception(f"{self.COMMAND} binary not found at '{binpath}'")
        super().__init__(env_vars, binpath)

        pg_dir = pg_distrib_dir / pg_version.v_prefixed
        self.pg_distrib_dir = pg_distrib_dir
        self.pg_version = pg_version
        self.pg_bin = pg_dir / "bin"
        if not (self.pg_bin / "postgres").exists():
            raise Exception(f"postgres binary was not found at '{self.pg_bin}'")
        self.pg_lib = pg_dir / "lib"
        if env_vars.get("LD_LIBRARY_PATH") is not None:
            self.pg_lib = Path(env_vars["LD_LIBRARY_PATH"])
        elif os.getenv("LD_LIBRARY_PATH") is not None:
            self.pg_lib = Path(str(os.getenv("LD_LIBRARY_PATH")))
        if not workdir.exists():
            raise Exception(f"Working directory '{workdir}' does not exist")
        self.workdir = workdir
        self.cleanup = cleanup

    def run_pgdata(
        self,
        s3prefix: str | None = None,
        pg_port: int | None = None,
        source_connection_string: str | None = None,
        interactive: bool = False,
    ):
        return self.run(
            "pgdata",
            s3prefix=s3prefix,
            pg_port=pg_port,
            source_connection_string=source_connection_string,
            interactive=interactive,
        )

    def run_dump_restore(
        self,
        s3prefix: str | None = None,
        source_connection_string: str | None = None,
        destination_connection_string: str | None = None,
    ):
        return self.run(
            "dump-restore",
            s3prefix=s3prefix,
            source_connection_string=source_connection_string,
            destination_connection_string=destination_connection_string,
        )

    def run(
        self,
        command: str,
        s3prefix: str | None = None,
        pg_port: int | None = None,
        source_connection_string: str | None = None,
        destination_connection_string: str | None = None,
        interactive: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        if self.cmd is not None:
            raise Exception("Command already executed")
        args = [
            f"--pg-bin-dir={self.pg_bin}",
            f"--pg-lib-dir={self.pg_lib}",
            f"--working-directory={self.workdir}",
        ]
        if s3prefix is not None:
            args.append(f"--s3-prefix={s3prefix}")
        args.append(command)
        if pg_port is not None:
            args.append(f"--pg-port={pg_port}")
        if source_connection_string is not None:
            args.append(f"--source-connection-string={source_connection_string}")
        if destination_connection_string is not None:
            args.append(f"--destination-connection-string={destination_connection_string}")
        if interactive:
            args.append("--interactive")

        self.cmd = self.raw_cli(args)
        return self.cmd

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.workdir.exists() and self.cleanup:
            shutil.rmtree(self.workdir)


@pytest.fixture(scope="function")
def fast_import(
    pg_version: PgVersion,
    test_output_dir: Path,
    neon_binpath: Path,
    pg_distrib_dir: Path,
    pytestconfig: Config,
) -> Iterator[FastImport]:
    workdir = Path(tempfile.mkdtemp(dir=test_output_dir, prefix="fast_import_"))
    with FastImport(
        None,
        neon_binpath,
        pg_distrib_dir,
        pg_version,
        workdir,
        cleanup=not cast(bool, pytestconfig.getoption("--preserve-database-files")),
    ) as fi:
        yield fi

        if fi.cmd is None:
            return

        # dump stdout & stderr into test log dir
        with open(test_output_dir / "fast_import.stdout", "w") as f:
            f.write(fi.cmd.stdout)
        with open(test_output_dir / "fast_import.stderr", "w") as f:
            f.write(fi.cmd.stderr)

        log.info("Written logs to %s", test_output_dir)
