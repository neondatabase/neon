import os
import shutil
import threading
import uuid
from contextlib import closing
from pathlib import Path

import pytest
from _pytest.config import Config
from fixtures.broker import NeonBroker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import (
    MockS3Server,
    RemoteStorageKind,
    available_s3_storages,
)


def upload_files(env):
    log.info("Uploading test files to mock bucket")
    os.chdir("test_runner/regress/data/extension_test")
    for path in os.walk("."):
        prefix, _, files = path
        for file in files:
            # the [2:] is to remove the leading "./"
            full_path = os.path.join(prefix, file)[2:]

            with open(full_path, "rb") as f:
                log.info(f"UPLOAD {full_path} to ext/{full_path}")
                env.remote_storage_client.upload_fileobj(
                    f,
                    env.ext_remote_storage.bucket_name,
                    f"ext/{full_path}",
                )
    os.chdir("../../../..")


# Test downloading remote extension.
@pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
def test_remote_extensions(
    remote_storage_kind: RemoteStorageKind,
    pg_version: PgVersion,
    pytestconfig: Config,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    mock_s3_server: MockS3Server,
    neon_binpath: Path,
    pg_distrib_dir: Path,
    default_broker: NeonBroker,
    run_id: uuid.UUID,
):
    alt_pgdir = test_output_dir / "pg_install"
    log.info(f"Copying {pg_distrib_dir} (which is big) to {alt_pgdir}.")
    shutil.copytree(pg_distrib_dir, alt_pgdir)

    neon_env_builder = NeonEnvBuilder(
        repo_dir=test_output_dir / "repo",
        port_distributor=port_distributor,
        mock_s3_server=mock_s3_server,
        neon_binpath=neon_binpath,
        pg_distrib_dir=alt_pgdir,
        pg_version=pg_version,
        broker=default_broker,
        run_id=run_id,
        preserve_database_files=pytestconfig.getoption("--preserve-database-files"),
    )

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_extensions",
        enable_remote_extensions=True,
    )
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy
    assert env.remote_storage_client is not None  # satisfy mypy

    # For MOCK_S3 we upload test files.
    # For REAL_S3 we use the files already in the bucket
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        upload_files(env)

    # Start a compute node and check that it can download the extensions
    # and use them to CREATE EXTENSION and LOAD
    endpoint = env.endpoints.create_start(
        "test_remote_extensions",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        # config_lines=["log_min_messages=debug3"],
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # Check that appropriate control files were downloaded
            cur.execute("SELECT * FROM pg_available_extensions")
            all_extensions = [x[0] for x in cur.fetchall()]
            log.info(all_extensions)
            assert "anon" in all_extensions

            # postgis is on real s3 but not mock s3.
            # it's kind of a big file, would rather not upload to github
            if remote_storage_kind == RemoteStorageKind.REAL_S3:
                assert "postgis" in all_extensions
                # this may fail locally if dependency is missing
                # we don't really care about the error,
                # we just want to make sure it downloaded
                try:
                    cur.execute("CREATE EXTENSION postgis")
                except Exception as err:
                    log.info(f"(expected) error creating postgis extension: {err}")
                    # we do not check the error, so this is basically a NO-OP
                    # however checking the log you can make sure that it worked
                    # and also get valuable information about how long loading the extension took

            # If you create the extension it would fail unless you have the pgcrypto extension installed
            cur.execute("LOAD 'anon'")

    shutil.rmtree(alt_pgdir)


# # Test downloading remote library.
# @pytest.mark.parametrize("remote_storage_kind", available_s3_storages())
# def test_remote_library(
#     remote_storage_kind: RemoteStorageKind,
#     pg_version: PgVersion,
#     pytestconfig: Config,
#     test_output_dir: Path,
#     port_distributor: PortDistributor,
#     mock_s3_server: MockS3Server,
#     neon_binpath: Path,
#     pg_distrib_dir: Path,
#     default_broker: NeonBroker,
#     run_id: uuid.UUID,
# ):
#     alt_dir = test_output_dir / "pg_install"
#     log.info(f"Copying {pg_distrib_dir} (which is big) to {alt_dir}.")
#     shutil.copytree(pg_distrib_dir, alt_dir)

#     neon_env_builder = NeonEnvBuilder(
#         repo_dir=test_output_dir / "repo",
#         port_distributor=port_distributor,
#         mock_s3_server=mock_s3_server,
#         neon_binpath=neon_binpath,
#         pg_distrib_dir=alt_dir,
#         pg_version=pg_version,
#         broker=default_broker,
#         run_id=run_id,
#         preserve_database_files=pytestconfig.getoption("--preserve-database-files"),
#     )

#     neon_env_builder.enable_remote_storage(
#         remote_storage_kind=remote_storage_kind,
#         test_name="test_remote_library",
#         enable_remote_extensions=True,
#     )
#     env = neon_env_builder.init_start()
#     tenant_id, _ = env.neon_cli.create_tenant()
#     env.neon_cli.create_timeline("test_remote_library", tenant_id=tenant_id)

#     assert env.ext_remote_storage is not None  # satisfy mypy
#     assert env.remote_storage_client is not None  # satisfy mypy

#     # For MOCK_S3 we upload test files.
#     # For REAL_S3 we use the files already in the bucket
#     if remote_storage_kind == RemoteStorageKind.MOCK_S3:
#         upload_files(env)

#     # and use them to run LOAD library
#     endpoint = env.endpoints.create_start(
#         "test_remote_library",
#         tenant_id=tenant_id,
#         remote_ext_config=env.ext_remote_storage.to_string(),
#         # config_lines=["log_min_messages=debug3"],
#     )
#     with closing(endpoint.connect()) as conn:
#         with conn.cursor() as cur:
#             # try to load library
#             cur.execute("LOAD 'anon'")
#             # test library which name is different from extension name
#             # this may fail locally if dependency is missing
#             # however, it does successfully download the postgis archive
#             if remote_storage_kind == RemoteStorageKind.REAL_S3:
#                 try:
#                     cur.execute("LOAD 'postgis_topology-3'")
#                 except Exception as err:
#                     log.info("error loading postgis_topology-3")
#                     assert "No such file or directory" in str(
#                         err
#                     ), "unexpected error loading postgis_topology-3"


# # Here we test a complex extension
# # which has multiple extensions in one archive
# # using postgis as an example
# @pytest.mark.skipif(
#     RemoteStorageKind.REAL_S3 not in available_s3_storages(),
#     reason="skipping test because real s3 not enabled",
# )
# def test_multiple_extensions_one_archive(
#     remote_storage_kind: RemoteStorageKind,
#     pg_version: PgVersion,
#     pytestconfig: Config,
#     test_output_dir: Path,
#     port_distributor: PortDistributor,
#     mock_s3_server: MockS3Server,
#     neon_binpath: Path,
#     pg_distrib_dir: Path,
#     default_broker: NeonBroker,
#     run_id: uuid.UUID,
# ):
#     alt_dir = test_output_dir / "pg_install"
#     log.info(f"Copying {pg_distrib_dir} (which is big) to {alt_dir}.")
#     shutil.copytree(pg_distrib_dir, alt_dir)

#     neon_env_builder = NeonEnvBuilder(
#         repo_dir=test_output_dir / "repo",
#         port_distributor=port_distributor,
#         mock_s3_server=mock_s3_server,
#         neon_binpath=neon_binpath,
#         pg_distrib_dir=alt_dir,
#         pg_version=pg_version,
#         broker=default_broker,
#         run_id=run_id,
#         preserve_database_files=pytestconfig.getoption("--preserve-database-files"),
#     )

#     neon_env_builder.enable_remote_storage(
#         remote_storage_kind=RemoteStorageKind.REAL_S3,
#         test_name="test_multiple_extensions_one_archive",
#         enable_remote_extensions=True,
#     )
#     env = neon_env_builder.init_start()
#     tenant_id, _ = env.neon_cli.create_tenant()
#     env.neon_cli.create_timeline("test_multiple_extensions_one_archive", tenant_id=tenant_id)

#     assert env.ext_remote_storage is not None  # satisfy mypy
#     assert env.remote_storage_client is not None  # satisfy mypy

#     endpoint = env.endpoints.create_start(
#         "test_multiple_extensions_one_archive",
#         tenant_id=tenant_id,
#         remote_ext_config=env.ext_remote_storage.to_string(),
#     )
#     with closing(endpoint.connect()) as conn:
#         with conn.cursor() as cur:
#             cur.execute("CREATE EXTENSION address_standardizer;")
#             cur.execute("CREATE EXTENSION address_standardizer_data_us;")
#             # execute query to ensure that it works
#             cur.execute(
#                 "SELECT house_num, name, suftype, city, country, state, unit \
#                         FROM standardize_address('us_lex', 'us_gaz', 'us_rules', \
#                         'One Rust Place, Boston, MA 02109');"
#             )
#             res = cur.fetchall()
#             log.info(res)
#             assert len(res) > 0


# # Test that extension is downloaded after endpoint restart,
# # when the library is used in the query.
# #
# # Run the test with mutliple simultaneous connections to an endpoint.
# # to ensure that the extension is downloaded only once.
# #
# def test_extension_download_after_restart(
#     remote_storage_kind: RemoteStorageKind,
#     pg_version: PgVersion,
#     pytestconfig: Config,
#     test_output_dir: Path,
#     port_distributor: PortDistributor,
#     mock_s3_server: MockS3Server,
#     neon_binpath: Path,
#     pg_distrib_dir: Path,
#     default_broker: NeonBroker,
#     run_id: uuid.UUID,
# ):
#     alt_dir = test_output_dir / "pg_install"
#     log.info(f"Copying {pg_distrib_dir} (which is big) to {alt_dir}.")
#     shutil.copytree(pg_distrib_dir, alt_dir)

#     neon_env_builder = NeonEnvBuilder(
#         repo_dir=test_output_dir / "repo",
#         port_distributor=port_distributor,
#         mock_s3_server=mock_s3_server,
#         neon_binpath=neon_binpath,
#         pg_distrib_dir=alt_dir,
#         pg_version=pg_version,
#         broker=default_broker,
#         run_id=run_id,
#         preserve_database_files=pytestconfig.getoption("--preserve-database-files"),
#     )

#     if "15" in pg_version:  # SKIP v15 for now because test set only has extension built for v14
#         return None

#     neon_env_builder.enable_remote_storage(
#         remote_storage_kind=RemoteStorageKind.MOCK_S3,
#         test_name="test_extension_download_after_restart",
#         enable_remote_extensions=True,
#     )
#     env = neon_env_builder.init_start()
#     tenant_id, _ = env.neon_cli.create_tenant()
#     env.neon_cli.create_timeline("test_extension_download_after_restart", tenant_id=tenant_id)

#     assert env.ext_remote_storage is not None  # satisfy mypy
#     assert env.remote_storage_client is not None  # satisfy mypy

#     # For MOCK_S3 we upload test files.
#     upload_files(env)

#     endpoint = env.endpoints.create_start(
#         "test_extension_download_after_restart",
#         tenant_id=tenant_id,
#         remote_ext_config=env.ext_remote_storage.to_string(),
#         config_lines=["log_min_messages=debug3"],
#     )
#     with closing(endpoint.connect()) as conn:
#         with conn.cursor() as cur:
#             cur.execute("CREATE extension pg_buffercache;")
#             cur.execute("SELECT * from pg_buffercache;")
#             res = cur.fetchall()
#             assert len(res) > 0
#             log.info(res)

#     # shutdown compute node
#     endpoint.stop()
#     # remove extension files locally
#     # TODO: fail the test right now, probably we need to bring back the cleanup logic...
#     assert 2 + 2 == 5
#     # cleanup(pg_version)

#     # spin up compute node again (there are no extension files available, because compute is stateless)
#     endpoint = env.endpoints.create_start(
#         "test_extension_download_after_restart",
#         tenant_id=tenant_id,
#         remote_ext_config=env.ext_remote_storage.to_string(),
#         config_lines=["log_min_messages=debug3"],
#     )

#     # connect to compute node and run the query
#     # that will trigger the download of the extension
#     def run_query(endpoint, thread_id: int):
#         log.info("thread_id {%d} starting", thread_id)
#         with closing(endpoint.connect()) as conn:
#             with conn.cursor() as cur:
#                 cur.execute("SELECT * from pg_buffercache;")
#                 res = cur.fetchall()
#                 assert len(res) > 0
#                 log.info("thread_id {%d}, res = %s", thread_id, res)

#     threads = [threading.Thread(target=run_query, args=(endpoint, i)) for i in range(2)]

#     for thread in threads:
#         thread.start()
#     for thread in threads:
#         thread.join()
