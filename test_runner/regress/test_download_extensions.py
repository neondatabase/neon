import os
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pg_version import PgVersion
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


# Check that the extension is not already in the share_dir_path_ext
# if it is, skip the test
#
# After the test is done, cleanup the control file and the extension directory
@pytest.fixture(scope="function")
def ext_file_cleanup(pg_bin):
    out = pg_bin.run_capture("pg_config --sharedir".split())
    share_dir_path = Path(f"{out}.stdout").read_text().strip()
    log.info(f"share_dir_path: {share_dir_path}")
    share_dir_path_ext = os.path.join(share_dir_path, "extension")

    log.info(f"share_dir_path_ext: {share_dir_path_ext}")

    # if file is already in the share_dir_path_ext, skip the test
    if os.path.isfile(os.path.join(share_dir_path_ext, "anon.control")):
        log.info("anon.control is already in the share_dir_path_ext, skipping the test")
        yield False
        return
    else:
        yield True

        # cleanup the control file
        if os.path.isfile(os.path.join(share_dir_path_ext, "anon.control")):
            os.unlink(os.path.join(share_dir_path_ext, "anon.control"))
            log.info("anon.control was removed from the share_dir_path_ext")

        # remove the extension directory recursively
        if os.path.isdir(os.path.join(share_dir_path_ext, "anon")):
            directories_to_clean: List[Path] = []
            for f in Path(os.path.join(share_dir_path_ext, "anon")).iterdir():
                if f.is_file():
                    log.info(f"Removing file {f}")
                    f.unlink()
                elif f.is_dir():
                    directories_to_clean.append(f)

            for directory_to_clean in reversed(directories_to_clean):
                if not os.listdir(directory_to_clean):
                    log.info(f"Removing empty directory {directory_to_clean}")
                    directory_to_clean.rmdir()

            os.rmdir(os.path.join(share_dir_path_ext, "anon"))
            log.info("anon directory was removed from the share_dir_path_ext")


def test_remote_extensions(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address,
    pg_version,
    ext_file_cleanup,
):
    if ext_file_cleanup is False:
        log.info("test_remote_extensions skipped")
        return

    if pg_version == PgVersion.V16:
        pytest.skip("TODO: PG16 extension building")

    # setup mock http server
    # that expects request for anon.tar.zst
    # and returns the requested file
    (host, port) = httpserver_listen_address
    extensions_endpoint = f"http://{host}:{port}/pg-ext-s3-gateway"

    archive_path = f"latest/v{pg_version}/extensions/anon.tar.zst"

    def endpoint_handler_build_tag(request: Request) -> Response:
        log.info(f"request: {request}")

        file_name = "anon.tar.zst"
        file_path = f"test_runner/regress/data/extension_test/5670669815/v{pg_version}/extensions/anon.tar.zst"
        file_size = os.path.getsize(file_path)
        fh = open(file_path, "rb")
        return Response(
            fh,
            mimetype="application/octet-stream",
            headers=[
                ("Content-Length", str(file_size)),
                ("Content-Disposition", 'attachment; filename="%s"' % file_name),
            ],
            direct_passthrough=True,
        )

    httpserver.expect_request(
        f"/pg-ext-s3-gateway/{archive_path}", method="GET"
    ).respond_with_handler(endpoint_handler_build_tag)

    # Start a compute node with remote_extension spec
    # and check that it can download the extensions and use them to CREATE EXTENSION.
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)
    endpoint = env.endpoints.create(
        "test_remote_extensions",
        tenant_id=tenant_id,
        config_lines=["log_min_messages=debug3"],
    )

    # mock remote_extensions spec
    spec: Dict[str, Any] = {
        "library_index": {
            "anon": "anon",
        },
        "extension_data": {
            "anon": {
                "archive_path": "",
                "control_data": {
                    "anon.control": "# PostgreSQL Anonymizer (anon) extension\ncomment = 'Data anonymization tools'\ndefault_version = '1.1.0'\ndirectory='extension/anon'\nrelocatable = false\nrequires = 'pgcrypto'\nsuperuser = false\nmodule_pathname = '$libdir/anon'\ntrusted = true\n"
                },
            },
        },
    }
    spec["extension_data"]["anon"]["archive_path"] = archive_path

    endpoint.create_remote_extension_spec(spec)

    endpoint.start(
        remote_ext_config=extensions_endpoint,
    )

    # this is expected to fail if there's no pgcrypto extension, that's ok
    # we just want to check that the extension was downloaded
    try:
        with closing(endpoint.connect()) as conn:
            with conn.cursor() as cur:
                # Check that appropriate files were downloaded
                cur.execute("CREATE EXTENSION anon")
                res = [x[0] for x in cur.fetchall()]
                log.info(res)
    except Exception as err:
        assert "pgcrypto" in str(err), f"unexpected error creating anon extension {err}"

    httpserver.check()


# TODO
# 1. Test downloading remote library.
#
# 2. Test a complex extension, which has multiple extensions in one archive
# using postgis as an example
#
# 3.Test that extension is downloaded after endpoint restart,
# when the library is used in the query.
# Run the test with mutliple simultaneous connections to an endpoint.
# to ensure that the extension is downloaded only once.
#
# 4. Test that private extensions are only downloaded when they are present in the spec.
#
