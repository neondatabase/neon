import os
import pytest
import subprocess
import uuid

from fixtures.zenith_fixtures import WalAcceptorFactory, PgBin
from fixtures.utils import lsn_to_hex, mkdir_if_needed

pytest_plugins = ("fixtures.zenith_fixtures")


class ProposerPostgres:
    """Object for running safekeepers sync with walproposer"""
    def __init__(self, pgdata_dir: str, pg_bin: PgBin, timeline_id: str, tenant_id: str):
        self.pgdata_dir: str = pgdata_dir
        self.pg_bin: PgBin = pg_bin
        self.timeline_id: str = timeline_id
        self.tenant_id: str = tenant_id

    def pg_data_dir_path(self) -> str:
        """ Path to data directory """
        return self.pgdata_dir

    def config_file_path(self) -> str:
        """ Path to postgresql.conf """
        return os.path.join(self.pgdata_dir, 'postgresql.conf')

    def create_dir_config(self, wal_acceptors: str):
        """ Create dir and config for running --sync-safekeepers """

        mkdir_if_needed(self.pg_data_dir_path())
        with open(self.config_file_path(), "w") as f:
            f.write("zenith.zenith_timeline = '{}'\n".format(self.timeline_id))
            f.write("zenith.zenith_tenant = '{}'\n".format(self.tenant_id))
            f.write("synchronous_standby_names = '{}'\n".format("walproposer"))
            f.write("wal_acceptors = '{}'\n".format(wal_acceptors))

    def sync_safekeepers(self) -> subprocess.CompletedProcess:
        """
        Run 'postgres --sync-safekeepers'.
        Returns execution result, which is commit_lsn after sync.
        """

        pg_path = os.path.join(self.pg_bin.pg_bin_path, "postgres")
        command = [pg_path, "--sync-safekeepers"]
        env = {
            "PGDATA": self.pg_data_dir_path(),
        }

        print('Running command "{}"'.format(" ".join(command)))
        res = subprocess.run(
            command, env=env, check=True, text=True, stdout=subprocess.PIPE
        )

        return res.stdout.strip("\n ")


# insert wal in all safekeepers and run sync on proposer
def test_sync_safekeepers(repo_dir: str, pg_bin: PgBin, wa_factory: WalAcceptorFactory):
    wa_factory.start_n_new(3)

    timeline_id = uuid.uuid4().hex
    tenant_id = uuid.uuid4().hex

    # write config for proposer
    pgdata_dir = os.path.join(repo_dir, "proposer_pgdata")
    pg = ProposerPostgres(pgdata_dir, pg_bin, timeline_id, tenant_id)
    pg.create_dir_config(wa_factory.get_connstrs())

    # valid lsn, which is not in the segment start, nor in zero segment
    epoch_start_lsn = 0x16B9188  # 0/16B9188
    begin_lsn = epoch_start_lsn

    # append and commit WAL
    lsn_after_append = []
    for i in range(3):
        res = wa_factory.instances[i].append_logical_message(
            tenant_id,
            timeline_id,
            {
                "lm_prefix": "prefix",
                "lm_message": "message",
                "set_commit_lsn": True,
                "term": 2,
                "begin_lsn": begin_lsn,
                "epoch_start_lsn": epoch_start_lsn,
                "truncate_lsn": epoch_start_lsn,
            },
        )
        lsn_hex = lsn_to_hex(res["inserted_wal"]["end_lsn"])
        lsn_after_append.append(lsn_hex)
        print(f"safekeeper[{i}] lsn after append: {lsn_hex}")

    # run sync safekeepers
    lsn_after_sync = pg.sync_safekeepers()
    print(f"lsn after sync = {lsn_after_sync}")

    assert all(lsn_after_sync == lsn for lsn in lsn_after_append)
