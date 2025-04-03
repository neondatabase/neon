from __future__ import annotations

import os
import shutil
import tarfile
from typing import TYPE_CHECKING

import pytest
import zstandard
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from werkzeug.wrappers.response import Response

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    from fixtures.httpserver import ListenAddress
    from fixtures.neon_fixtures import (
        NeonEnvBuilder,
    )
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


def test_remote_extensions(
    httpserver: HTTPServer,
    neon_env_builder_local: NeonEnvBuilder,
    httpserver_listen_address: ListenAddress,
    test_output_dir: Path,
    base_dir: Path,
    pg_version: PgVersion,
):
    # Setup a mock nginx S3 gateway which will return our test extension.
    (host, port) = httpserver_listen_address
    extensions_endpoint = f"http://{host}:{port}/pg-ext-s3-gateway"

    build_tag = os.environ.get("BUILD_TAG", "latest")
    archive_route = f"{build_tag}/v{pg_version}/extensions/test_extension.tar.zst"
    tarball = test_output_dir / "test_extension.tar"
    extension_dir = (
        base_dir / "test_runner" / "regress" / "data" / "test_remote_extensions" / "test_extension"
    )

    # Create tarball
    with tarfile.open(tarball, "x") as tarf:
        tarf.add(
            extension_dir / "sql" / "test_extension--1.0.sql",
            arcname="share/extension/test_extension--1.0.sql",
        )
        tarf.add(
            extension_dir / "sql" / "test_extension--1.0--1.1.sql",
            arcname="share/extension/test_extension--1.0--1.1.sql",
        )

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

    httpserver.expect_request(
        f"/pg-ext-s3-gateway/{archive_route}", method="GET"
    ).respond_with_handler(handler)

    # Start a compute node with remote_extension spec
    # and check that it can download the extensions and use them to CREATE EXTENSION.
    env = neon_env_builder_local.init_start()
    env.create_branch("test_remote_extensions")
    endpoint = env.endpoints.create("test_remote_extensions")

    with open(extension_dir / "test_extension.control", encoding="utf-8") as f:
        control_data = f.read()

    # mock remote_extensions spec
    spec: dict[str, Any] = {
        "public_extensions": ["test_extension"],
        "custom_extensions": None,
        "library_index": {
            "test_extension": "test_extension",
        },
        "extension_data": {
            "test_extension": {
                "archive_path": "",
                "control_data": {
                    "test_extension.control": control_data,
                },
            },
        },
    }

    endpoint.create_remote_extension_spec(spec)

    endpoint.start(remote_ext_config=extensions_endpoint)

    with endpoint.connect() as conn:
        with conn.cursor() as cur:
            # Check that appropriate files were downloaded
            cur.execute("CREATE EXTENSION test_extension VERSION '1.0'")
            cur.execute("SELECT test_extension.motd()")

    httpserver.check()

    # Check that we properly recorded downloads in the metrics
    client = endpoint.http_client()
    raw_metrics = client.metrics()
    metrics = parse_metrics(raw_metrics)
    remote_ext_requests = metrics.query_all(
        "compute_ctl_remote_ext_requests_total",
        # Check that we properly report the filename in the metrics
        {"filename": "test_extension.tar.zst"},
    )
    assert len(remote_ext_requests) == 1
    for sample in remote_ext_requests:
        assert sample.value == 1

    endpoint.stop()

    # Remove the extension files to force a redownload of the extension.
    for file in (
        "test_extension.control",
        "test_extension--1.0.sql",
        "test_extension--1.0--1.1.sql",
    ):
        (
            test_output_dir
            / "pg_install"
            / f"v{pg_version}"
            / "share"
            / "postgresql"
            / "extension"
            / file
        ).unlink()

    endpoint.start(remote_ext_config=extensions_endpoint)

    # Test that ALTER EXTENSION UPDATE statements also fetch remote extensions.
    with endpoint.connect() as conn:
        with conn.cursor() as cur:
            # Check that appropriate files were downloaded
            cur.execute("ALTER EXTENSION test_extension UPDATE TO '1.1'")
            cur.execute("SELECT test_extension.fun_fact()")

    # Check that we properly recorded downloads in the metrics
    client = endpoint.http_client()
    raw_metrics = client.metrics()
    metrics = parse_metrics(raw_metrics)
    remote_ext_requests = metrics.query_all(
        "compute_ctl_remote_ext_requests_total",
        # Check that we properly report the filename in the metrics
        {"filename": "test_extension.tar.zst"},
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
