from __future__ import annotations

import os
import platform
import shutil
import tarfile
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, cast, final

import pytest
import zstandard
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.paths import BASE_DIR
from fixtures.pg_config import PgConfigKey
from fixtures.utils import WITH_SANITIZERS, subprocess_capture
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    from fixtures.httpserver import ListenAddress
    from fixtures.neon_fixtures import (
        NeonEnvBuilder,
    )
    from fixtures.pg_config import PgConfig
    from fixtures.pg_version import PgVersion
    from pytest_httpserver import HTTPServer
    from werkzeug.wrappers.request import Request


# use neon_env_builder_local fixture to override the default neon_env_builder fixture
# and use a test-specific pg_install instead of shared one
@pytest.fixture(scope="function")
def neon_env_builder_local(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_distrib_dir: Path,
) -> NeonEnvBuilder:
    test_local_pginstall = test_output_dir / "pg_install"
    log.info(f"copy {pg_distrib_dir} to {test_local_pginstall}")

    # We can't copy only the version that we are currently testing because other
    # binaries like the storage controller need specific Postgres versions.
    shutil.copytree(pg_distrib_dir, test_local_pginstall)

    neon_env_builder.pg_distrib_dir = test_local_pginstall
    log.info(f"local neon_env_builder.pg_distrib_dir: {neon_env_builder.pg_distrib_dir}")

    return neon_env_builder


@final
class RemoteExtension(StrEnum):
    SQL_ONLY = "test_extension_sql_only"
    WITH_LIB = "test_extension_with_lib"

    @property
    def compressed_tarball_name(self) -> str:
        return f"{self.tarball_name}.zst"

    @property
    def control_file_name(self) -> str:
        return f"{self}.control"

    @property
    def directory(self) -> Path:
        return BASE_DIR / "test_runner" / "regress" / "data" / "test_remote_extensions" / self

    @property
    def shared_library_name(self) -> str:
        return f"{self}.so"

    @property
    def tarball_name(self) -> str:
        return f"{self}.tar"

    def archive_route(self, build_tag: str, arch: str, pg_version: PgVersion) -> str:
        return f"{build_tag}/{arch}/v{pg_version}/extensions/{self.compressed_tarball_name}"

    def build(self, pg_config: PgConfig, output_dir: Path) -> None:
        if self is not RemoteExtension.WITH_LIB:
            return

        cmd: list[str] = [
            *cast("list[str]", pg_config[PgConfigKey.CC]),
            *cast("list[str]", pg_config[PgConfigKey.CPPFLAGS]),
            *["-I", str(cast("Path", pg_config[PgConfigKey.INCLUDEDIR_SERVER]))],
            *cast("list[str]", pg_config[PgConfigKey.CFLAGS]),
            *cast("list[str]", pg_config[PgConfigKey.CFLAGS_SL]),
            *cast("list[str]", pg_config[PgConfigKey.LDFLAGS_EX]),
            *cast("list[str]", pg_config[PgConfigKey.LDFLAGS_SL]),
            "-shared",
            *["-o", str(output_dir / self.shared_library_name)],
            str(self.directory / "src" / f"{self}.c"),
        ]

        subprocess_capture(output_dir, cmd, check=True)

    def control_file_contents(self) -> str:
        with open(self.directory / self.control_file_name, encoding="utf-8") as f:
            return f.read()

    def files(self, output_dir: Path) -> dict[Path, str]:
        files = {
            # self.directory / self.control_file_name: f"share/extension/{self.control_file_name}",
            self.directory / "sql" / f"{self}--1.0.sql": f"share/extension/{self}--1.0.sql",
            self.directory
            / "sql"
            / f"{self}--1.0--1.1.sql": f"share/extension/{self}--1.0--1.1.sql",
        }

        if self is RemoteExtension.WITH_LIB:
            files[output_dir / self.shared_library_name] = f"lib/{self.shared_library_name}"

        return files

    def package(self, output_dir: Path) -> Path:
        tarball = output_dir / self.tarball_name
        with tarfile.open(tarball, "x") as tarf:
            for file, arcname in self.files(output_dir).items():
                tarf.add(file, arcname=arcname)

        return tarball

    def remove(self, output_dir: Path, pg_version: PgVersion) -> None:
        for file in self.files(output_dir).values():
            if file.startswith("share/extension"):
                file = f"share/postgresql/extension/{os.path.basename(file)}"
            if file.startswith("lib"):
                file = f"lib/postgresql/{os.path.basename(file)}"
            (output_dir / "pg_install" / f"v{pg_version}" / file).unlink()


@pytest.mark.parametrize(
    "extension",
    (RemoteExtension.SQL_ONLY, RemoteExtension.WITH_LIB),
    ids=["sql_only", "with_lib"],
)
def test_remote_extensions(
    httpserver: HTTPServer,
    neon_env_builder_local: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
    test_output_dir: Path,
    pg_version: PgVersion,
    pg_config: PgConfig,
    extension: RemoteExtension,
):
    if WITH_SANITIZERS and extension is RemoteExtension.WITH_LIB:
        pytest.skip(
            """
            For this test to work with sanitizers enabled, we would need to
            compile the dummy Postgres extension with the same CFLAGS that we
            compile Postgres and the neon extension with to link the sanitizers.
            """
        )

    # Setup a mock nginx S3 gateway which will return our test extension.
    (host, port) = httpserver_listen_address
    extensions_endpoint = f"http://{host}:{port}/pg-ext-s3-gateway"

    extension.build(pg_config, test_output_dir)
    tarball = extension.package(test_output_dir)

    def handler(request: Request) -> Response:
        log.info(f"request: {request}")

        # Compress tarball
        compressor = zstandard.ZstdCompressor()
        with open(tarball, "rb") as f:
            compressed_data = compressor.compress(f.read())

        return Response(
            compressed_data,
            mimetype="application/octet-stream",
            headers=[
                ("Content-Length", str(len(compressed_data))),
            ],
            direct_passthrough=True,
        )

    # We have decided to use the Go naming convention due to Kubernetes.
    arch = platform.machine()
    match arch:
        case "aarch64":
            arch = "arm64"
        case "x86_64":
            arch = "amd64"
        case _:
            pass

    httpserver.expect_request(
        f"/pg-ext-s3-gateway/{extension.archive_route(build_tag=os.environ.get('BUILD_TAG', 'latest'), arch=arch, pg_version=pg_version)}",
        method="GET",
    ).respond_with_handler(handler)

    # Start a compute node with remote_extension spec
    # and check that it can download the extensions and use them to CREATE EXTENSION.
    env = neon_env_builder_local.init_start()
    env.create_branch("test_remote_extensions")
    endpoint = env.endpoints.create("test_remote_extensions")

    # mock remote_extensions spec
    spec: dict[str, Any] = {
        "public_extensions": [extension],
        "custom_extensions": None,
        "library_index": {
            extension: extension,
        },
        "extension_data": {
            extension: {
                "archive_path": "",
                "control_data": {
                    extension.control_file_name: extension.control_file_contents(),
                },
            },
        },
    }

    endpoint.create_remote_extension_spec(spec)

    endpoint.start(remote_ext_base_url=extensions_endpoint)

    with endpoint.connect() as conn:
        with conn.cursor() as cur:
            # Check that appropriate files were downloaded
            cur.execute(f"CREATE EXTENSION {extension} VERSION '1.0'")
            cur.execute(f"SELECT {extension}.motd()")

    httpserver.check()

    # Check that we properly recorded downloads in the metrics
    client = endpoint.http_client()
    raw_metrics = client.metrics()
    metrics = parse_metrics(raw_metrics)
    remote_ext_requests = metrics.query_all(
        "compute_ctl_remote_ext_requests_total",
        # Check that we properly report the filename in the metrics
        {"filename": extension.compressed_tarball_name},
    )
    assert len(remote_ext_requests) == 1
    for sample in remote_ext_requests:
        assert sample.value == 1

    endpoint.stop()

    # Remove the extension files to force a redownload of the extension.
    extension.remove(test_output_dir, pg_version)

    endpoint.start(remote_ext_base_url=extensions_endpoint)

    # Test that ALTER EXTENSION UPDATE statements also fetch remote extensions.
    with endpoint.connect() as conn:
        with conn.cursor() as cur:
            # Check that appropriate files were downloaded
            cur.execute(f"ALTER EXTENSION {extension} UPDATE TO '1.1'")
            cur.execute(f"SELECT {extension}.fun_fact()")

    # Check that we properly recorded downloads in the metrics
    client = endpoint.http_client()
    raw_metrics = client.metrics()
    metrics = parse_metrics(raw_metrics)
    remote_ext_requests = metrics.query_all(
        "compute_ctl_remote_ext_requests_total",
        # Check that we properly report the filename in the metrics
        {"filename": extension.compressed_tarball_name},
    )
    assert len(remote_ext_requests) == 1
    for sample in remote_ext_requests:
        assert sample.value == 1


# TODO
# 1. Test downloading remote library.
#
# 2. Test a complex extension, which has multiple extensions in one archive
# using postgis as an example
#
# 3.Test that extension is downloaded after endpoint restart,
# when the library is used in the query.
# Run the test with multiple simultaneous connections to an endpoint.
# to ensure that the extension is downloaded only once.
#
# 4. Test that private extensions are only downloaded when they are present in the spec.
#
