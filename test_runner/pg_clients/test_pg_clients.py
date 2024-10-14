from __future__ import annotations

import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from fixtures.neon_fixtures import RemotePostgres
from fixtures.utils import subprocess_capture


@pytest.mark.remote_cluster
@pytest.mark.parametrize(
    "client",
    [
        "csharp/npgsql",
        "java/jdbc",
        "rust/tokio-postgres",
        "python/asyncpg",
        "python/pg8000",
        # PostgresClientKitExample does not support SNI or connection options, so it uses workaround D (https://neon.tech/sni)
        # See example itself: test_runner/pg_clients/swift/PostgresClientKitExample/Sources/PostgresClientKitExample/main.swift
        "swift/PostgresClientKitExample",
        "swift/PostgresNIOExample",
        "typescript/postgresql-client",
        "typescript/serverless-driver",
    ],
)
def test_pg_clients(test_output_dir: Path, remote_pg: RemotePostgres, client: str):
    conn_options = remote_pg.conn_options()

    env_file = None
    with NamedTemporaryFile(mode="w", delete=False) as f:
        env_file = f.name
        f.write(
            f"""
            NEON_HOST={conn_options["host"]}
            NEON_DATABASE={conn_options["dbname"]}
            NEON_USER={conn_options["user"]}
            NEON_PASSWORD={conn_options["password"]}
        """
        )

    image_tag = client.lower()
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        raise RuntimeError("docker is required for running this test")

    build_cmd = [docker_bin, "build", "--tag", image_tag, f"{Path(__file__).parent / client}"]
    subprocess_capture(test_output_dir, build_cmd, check=True)

    run_cmd = [docker_bin, "run", "--rm", "--env-file", env_file, image_tag]
    _, output, _ = subprocess_capture(test_output_dir, run_cmd, check=True, capture_stdout=True)

    assert str(output).strip() == "1"
