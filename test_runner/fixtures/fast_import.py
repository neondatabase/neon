import subprocess
from pathlib import Path

from fixtures.neon_cli import AbstractNeonCli


class FastImport(AbstractNeonCli):
    COMMAND = "fast_import"

    def __init__(self, extra_env: dict[str, str] | None, binpath: Path, pg_dir: Path):
        if extra_env is None:
            env_vars = {}
        else:
            env_vars = extra_env.copy()

        if not (binpath / self.COMMAND).exists():
            raise Exception(f"{self.COMMAND} binary not found at '{binpath}'")
        super().__init__(env_vars, binpath)

        self.pg_bin = pg_dir / "bin"
        if not (self.pg_bin / "postgres") .exists():
            raise Exception(f"postgres binary was not found at '{self.pg_bin}'")
        self.pg_lib = pg_dir / "lib"

    def run(self,
            workdir: Path,
            pg_port: int,
            source_connection_string: str | None = None,
            s3prefix: str | None = None,
            interactive: bool = False,
        ) -> subprocess.CompletedProcess[str]:
        args = [
            f"--pg-bin-dir={self.pg_bin}",
            f"--pg-lib-dir={self.pg_lib}",
            f"--pg-port={pg_port}",
            f"--working-directory={workdir}",
        ]
        if source_connection_string is not None:
            args.append(f"--source-connection-string={source_connection_string}")
        if s3prefix is not None:
            args.append(f"--s3-prefix={s3prefix}")
        if interactive:
            args.append("--interactive")

        return self.raw_cli(args)