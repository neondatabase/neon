import os
import shutil
import subprocess
import threading
import uuid
from contextlib import closing
from pathlib import Path
from typing import Optional

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


# creates the weighted_mean extension and runs test queries to verify it works
def weighted_mean_test(endpoint, message: Optional[str] = None):
    if message is not None:
        log.info(message)
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # Check that appropriate control files were downloaded
            cur.execute("SELECT * FROM pg_available_extensions")
            all_extensions = [x[0] for x in cur.fetchall()]
            log.info(all_extensions)
            assert "weighted_mean" in all_extensions

            # test is from: https://github.com/Kozea/weighted_mean/tree/master/test
            cur.execute("CREATE extension weighted_mean")
            cur.execute(
                "create temp table test as (\
            select a::numeric, b::numeric\
            from\
                generate_series(1, 100) as a(a),\
                generate_series(1, 100) as b(b));"
            )
            cur.execute("select weighted_mean(a,b) from test;")
            x = cur.fetchone()
            log.info(x)
            assert str(x) == "(Decimal('50.5000000000000000'),)"
            cur.execute("update test set b = 0;")
            cur.execute("select weighted_mean(a,b) from test;")
            x = cur.fetchone()
            log.info(x)
            assert str(x) == "(Decimal('0'),)"


# Test downloading remote extension.
def test_remote_extensions(
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
    log.info(f"Copying {pg_distrib_dir} (which is big) to {alt_pgdir}")
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
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_remote_extensions",
        enable_remote_extensions=True,
    )
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_remote_extensions", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy
    upload_files(env)

    # Start a compute node and check that it can download the extensions
    # and use them to CREATE EXTENSION and LOAD
    endpoint = env.endpoints.create_start(
        "test_remote_extensions",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        # config_lines=["log_min_messages=debug3"],
    )
    weighted_mean_test(endpoint)

    # Test that extension is downloaded after endpoint restart,
    # when the library is used in the query.
    #
    # Run the test with mutliple simultaneous connections to an endpoint.
    # to ensure that the extension is downloaded only once.

    # shutdown compute node
    endpoint.stop()
    # remove extension files locally
    SHAREDIR = subprocess.check_output(
        [alt_pgdir / f"{pg_version.v_prefixed}/bin/pg_config", "--sharedir"]
    )
    SHAREDIRPATH = Path(SHAREDIR.decode("utf-8").strip())
    log.info("SHAREDIRPATH: %s", SHAREDIRPATH)
    (SHAREDIRPATH / "extension/weighted_mean--1.0.1.sql").unlink()
    (SHAREDIRPATH / "extension/weighted_mean.control").unlink()

    # spin up compute node again (there are no extension files available, because compute is stateless)
    endpoint = env.endpoints.create_start(
        "test_extension_download_after_restart",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
        config_lines=["log_min_messages=debug3"],
    )

    # connect to compute node and run the query
    # that will trigger the download of the extension
    threads = [
        threading.Thread(target=weighted_mean_test, args=(endpoint, f"this is thread {i}"))
        for i in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # shutil.rmtree(alt_pgdir)


# Here we test a complex extension
# which has multiple extensions in one archive
# using postgis as an example
@pytest.mark.skipif(
    RemoteStorageKind.REAL_S3 not in available_s3_storages(),
    reason="skipping test because real s3 not enabled",
)
def test_multiple_extensions_one_archive(
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
    alt_dir = test_output_dir / "pg_install"
    log.info(f"Copying {pg_distrib_dir} (which is big) to {alt_dir}.")
    shutil.copytree(pg_distrib_dir, alt_dir)

    neon_env_builder = NeonEnvBuilder(
        repo_dir=test_output_dir / "repo",
        port_distributor=port_distributor,
        mock_s3_server=mock_s3_server,
        neon_binpath=neon_binpath,
        pg_distrib_dir=alt_dir,
        pg_version=pg_version,
        broker=default_broker,
        run_id=run_id,
        preserve_database_files=pytestconfig.getoption("--preserve-database-files"),
    )

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.REAL_S3,
        test_name="test_multiple_extensions_one_archive",
        enable_remote_extensions=True,
    )
    env = neon_env_builder.init_start()
    tenant_id, _ = env.neon_cli.create_tenant()
    env.neon_cli.create_timeline("test_multiple_extensions_one_archive", tenant_id=tenant_id)

    assert env.ext_remote_storage is not None  # satisfy mypy

    endpoint = env.endpoints.create_start(
        "test_multiple_extensions_one_archive",
        tenant_id=tenant_id,
        remote_ext_config=env.ext_remote_storage.to_string(),
    )
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION address_standardizer;")
            cur.execute("CREATE EXTENSION address_standardizer_data_us;")
            # execute query to ensure that it works
            cur.execute(
                "SELECT house_num, name, suftype, city, country, state, unit \
                        FROM standardize_address('us_lex', 'us_gaz', 'us_rules', \
                        'One Rust Place, Boston, MA 02109');"
            )
            res = cur.fetchall()
            log.info(res)
            assert len(res) > 0
