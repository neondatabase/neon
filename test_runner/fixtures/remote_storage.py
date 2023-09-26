import enum
import hashlib
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import boto3
from mypy_boto3_s3 import S3Client

from fixtures.log_helper import log
from fixtures.types import TenantId, TimelineId

TIMELINE_INDEX_PART_FILE_NAME = "index_part.json"


@enum.unique
class RemoteStorageUser(str, enum.Enum):
    """
    Instead of using strings for the users, use a more strict enum.
    """

    PAGESERVER = "pageserver"
    EXTENSIONS = "ext"
    SAFEKEEPER = "safekeeper"

    def __str__(self) -> str:
        return self.value


class MockS3Server:
    """
    Starts a mock S3 server for testing on a port given, errors if the server fails to start or exits prematurely.
    Relies that `poetry` and `moto` server are installed, since it's the way the tests are run.

    Also provides a set of methods to derive the connection properties from and the method to kill the underlying server.
    """

    def __init__(
        self,
        port: int,
    ):
        self.port = port

        # XXX: do not use `shell=True` or add `exec ` to the command here otherwise.
        # We use `self.subprocess.kill()` to shut down the server, which would not "just" work in Linux
        # if a process is started from the shell process.
        self.subprocess = subprocess.Popen(["poetry", "run", "moto_server", "s3", f"-p{port}"])
        error = None
        try:
            return_code = self.subprocess.poll()
            if return_code is not None:
                error = f"expected mock s3 server to run but it exited with code {return_code}. stdout: '{self.subprocess.stdout}', stderr: '{self.subprocess.stderr}'"
        except Exception as e:
            error = f"expected mock s3 server to start but it failed with exception: {e}. stdout: '{self.subprocess.stdout}', stderr: '{self.subprocess.stderr}'"
        if error is not None:
            log.error(error)
            self.kill()
            raise RuntimeError("failed to start s3 mock server")

    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def region(self) -> str:
        return "us-east-1"

    def access_key(self) -> str:
        return "test"

    def secret_key(self) -> str:
        return "test"

    def kill(self):
        self.subprocess.kill()


@dataclass
class LocalFsStorage:
    root: Path

    def tenant_path(self, tenant_id: TenantId) -> Path:
        return self.root / "tenants" / str(tenant_id)

    def timeline_path(self, tenant_id: TenantId, timeline_id: TimelineId) -> Path:
        return self.tenant_path(tenant_id) / "timelines" / str(timeline_id)

    def index_path(self, tenant_id: TenantId, timeline_id: TimelineId) -> Path:
        return self.timeline_path(tenant_id, timeline_id) / TIMELINE_INDEX_PART_FILE_NAME

    def index_content(self, tenant_id: TenantId, timeline_id: TimelineId):
        with self.index_path(tenant_id, timeline_id).open("r") as f:
            return json.load(f)

    def to_toml_inline_table(self) -> str:
        return f"local_path='{self.root}'"

    def cleanup(self):
        # no cleanup is done here, because there's NeonEnvBuilder.cleanup_local_storage which will remove everything, including localfs files
        pass

    @staticmethod
    def component_path(repo_dir: Path, user: RemoteStorageUser) -> Path:
        return repo_dir / "local_fs_remote_storage" / str(user)


@dataclass
class S3Storage:
    bucket_name: str
    bucket_region: str
    access_key: str
    secret_key: str
    prefix_in_bucket: str
    client: S3Client
    cleanup: bool
    """Is this MOCK_S3 (false) or REAL_S3 (true)"""
    real: bool
    endpoint: Optional[str] = None

    def access_env_vars(self) -> Dict[str, str]:
        return {
            "AWS_ACCESS_KEY_ID": self.access_key,
            "AWS_SECRET_ACCESS_KEY": self.secret_key,
        }

    def to_string(self) -> str:
        return json.dumps(
            {
                "bucket": self.bucket_name,
                "region": self.bucket_region,
                "endpoint": self.endpoint,
                "prefix": self.prefix_in_bucket,
            }
        )

    def to_toml_inline_table(self) -> str:
        s = [
            f"bucket_name='{self.bucket_name}'",
            f"bucket_region='{self.bucket_region}'",
        ]

        if self.prefix_in_bucket is not None:
            s.append(f"prefix_in_bucket='{self.prefix_in_bucket}'")

        if self.endpoint is not None:
            s.append(f"endpoint='{self.endpoint}'")

        return ",".join(s)

    def do_cleanup(self):
        if not self.cleanup:
            # handles previous keep_remote_storage_contents
            return

        log.info(
            "removing data from test s3 bucket %s by prefix %s",
            self.bucket_name,
            self.prefix_in_bucket,
        )
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=self.prefix_in_bucket,
        )

        # Using Any because DeleteTypeDef (from boto3-stubs) doesn't fit our case
        objects_to_delete: Any = {"Objects": []}
        cnt = 0
        for item in pages.search("Contents"):
            # weirdly when nothing is found it returns [None]
            if item is None:
                break

            objects_to_delete["Objects"].append({"Key": item["Key"]})

            # flush once aws limit reached
            if len(objects_to_delete["Objects"]) >= 1000:
                self.client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete=objects_to_delete,
                )
                objects_to_delete = {"Objects": []}
                cnt += 1

        # flush rest
        if len(objects_to_delete["Objects"]):
            self.client.delete_objects(
                Bucket=self.bucket_name,
                Delete=objects_to_delete,
            )

        log.info(f"deleted {cnt} objects from remote storage")


RemoteStorage = Union[LocalFsStorage, S3Storage]


@enum.unique
class RemoteStorageKind(str, enum.Enum):
    LOCAL_FS = "local_fs"
    MOCK_S3 = "mock_s3"
    REAL_S3 = "real_s3"
    # Pass to tests that are generic to remote storage
    # to ensure the test pass with or without the remote storage
    NOOP = "noop"

    def configure(
        self,
        repo_dir: Path,
        mock_s3_server,
        run_id: str,
        test_name: str,
        user: RemoteStorageUser,
        bucket_name: Optional[str] = None,
        bucket_region: Optional[str] = None,
    ) -> Optional[RemoteStorage]:
        if self == RemoteStorageKind.NOOP:
            return None

        if self == RemoteStorageKind.LOCAL_FS:
            return LocalFsStorage(LocalFsStorage.component_path(repo_dir, user))

        # real_s3 uses this as part of prefix, mock_s3 uses this as part of
        # bucket name, giving all users unique buckets because we have to
        # create them
        test_name = re.sub(r"[_\[\]]", "-", test_name)

        def to_bucket_name(user: str, test_name: str) -> str:
            s = f"{user}-{test_name}"

            if len(s) > 63:
                prefix = s[:30]
                suffix = hashlib.sha256(test_name.encode()).hexdigest()[:32]
                s = f"{prefix}-{suffix}"
                assert len(s) == 63

            return s

        if self == RemoteStorageKind.MOCK_S3:
            # there's a single mock_s3 server for each process running the tests
            mock_endpoint = mock_s3_server.endpoint()
            mock_region = mock_s3_server.region()

            access_key, secret_key = mock_s3_server.access_key(), mock_s3_server.secret_key()

            client = boto3.client(
                "s3",
                endpoint_url=mock_endpoint,
                region_name=mock_region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )

            bucket_name = to_bucket_name(user, test_name)
            log.info(
                f"using mock_s3 bucket name {bucket_name} for user={user}, test_name={test_name}"
            )

            return S3Storage(
                bucket_name=bucket_name,
                endpoint=mock_endpoint,
                bucket_region=mock_region,
                access_key=access_key,
                secret_key=secret_key,
                prefix_in_bucket="",
                client=client,
                cleanup=False,
                real=False,
            )

        assert self == RemoteStorageKind.REAL_S3

        env_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        assert env_access_key, "no aws access key provided"
        env_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        assert env_secret_key, "no aws access key provided"

        # session token is needed for local runs with sso auth
        session_token = os.getenv("AWS_SESSION_TOKEN")

        bucket_name = bucket_name or os.getenv("REMOTE_STORAGE_S3_BUCKET")
        assert bucket_name is not None, "no remote storage bucket name provided"
        bucket_region = bucket_region or os.getenv("REMOTE_STORAGE_S3_REGION")
        assert bucket_region is not None, "no remote storage region provided"

        prefix_in_bucket = f"{run_id}/{test_name}/{user}"

        client = boto3.client(
            "s3",
            region_name=bucket_region,
            aws_access_key_id=env_access_key,
            aws_secret_access_key=env_secret_key,
            aws_session_token=session_token,
        )

        return S3Storage(
            bucket_name=bucket_name,
            bucket_region=bucket_region,
            access_key=env_access_key,
            secret_key=env_secret_key,
            prefix_in_bucket=prefix_in_bucket,
            client=client,
            cleanup=True,
            real=True,
        )


def available_remote_storages() -> List[RemoteStorageKind]:
    remote_storages = [RemoteStorageKind.LOCAL_FS, RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE") is not None:
        remote_storages.append(RemoteStorageKind.REAL_S3)
        log.info("Enabling real s3 storage for tests")
    else:
        log.info("Using mock implementations to test remote storage")
    return remote_storages


def available_s3_storages() -> List[RemoteStorageKind]:
    remote_storages = [RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE") is not None:
        remote_storages.append(RemoteStorageKind.REAL_S3)
        log.info("Enabling real s3 storage for tests")
    else:
        log.info("Using mock implementations to test remote storage")
    return remote_storages


def s3_storage() -> RemoteStorageKind:
    """
    For tests that require a remote storage impl that exposes an S3
    endpoint, but don't want to parametrize over multiple storage types.

    Use real S3 if available, else use MockS3
    """
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE") is not None:
        return RemoteStorageKind.REAL_S3
    else:
        return RemoteStorageKind.MOCK_S3


# serialize as toml inline table
def remote_storage_to_toml_inline_table(remote_storage: RemoteStorage) -> str:
    if not isinstance(remote_storage, (LocalFsStorage, S3Storage)):
        raise Exception("invalid remote storage type")

    return f"{{{remote_storage.to_toml_inline_table()}}}"
