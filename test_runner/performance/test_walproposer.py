from pathlib import Path
import os
import subprocess

from fixtures.zenith_fixtures import PgBin, PortDistributor
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from batch_others.test_wal_acceptor import ProposerPostgres, SafekeeperEnv
from fixtures.utils import mkdir_if_needed

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


def calc_avg_sizes(filename, avg_size_block) -> "list[float]":
    sizes = []
    pos = []

    i = 0
    cursum = 0
    curcnt = 0

    with open(filename) as f:
        while True:
            line = f.readline()
            if not line:
                break
            cur = int(line)
            cursum += cur
            curcnt += 1
            i += 1
            if curcnt == avg_size_block:
                sizes.append(cursum / curcnt)
                pos.append(i)
                cursum = 0
                curcnt = 0

        if curcnt > 0:
            sizes.append(cursum / curcnt)
            pos.append(i)

    sizes = list(map(int, sizes))
    return sizes


def test_walproposer_pgbench(test_output_dir: str,
                             port_distributor: PortDistributor,
                             pg_bin: PgBin,
                             zenbenchmark: ZenithBenchmarker):
    # Create the environment in the test-specific output dir
    repo_dir = Path(os.path.join(test_output_dir, "repo"))

    env = SafekeeperEnv(
        repo_dir,
        port_distributor,
        pg_bin,
        num_safekeepers=1,
    )

    with env:
        env.init()
        assert env.postgres is not None

        pgbench_env = {
            "PGHOST": env.postgres.listen_addr,
            "PGPORT": str(env.postgres.port),
            "PGDATA": env.postgres.pg_data_dir_path(),
            "PGUSER": "zenith_admin",
            "PGDATABASE": "postgres"
        }

        run_pgbench(pg_bin, zenbenchmark, repo_dir, env.postgres.log_path(), pgbench_env)


def test_vanilla_pgbench(test_output_dir: str,
                         port_distributor: PortDistributor,
                         pg_bin: PgBin,
                         zenbenchmark: ZenithBenchmarker):
    # Create the environment in the test-specific output dir
    repo_dir = Path(os.path.join(test_output_dir, "repo"))
    mkdir_if_needed(repo_dir)

    pgdata_master = os.path.join(repo_dir, "pgdata_master")
    pgdata_slave = os.path.join(repo_dir, "pgdata_slave")

    master = ProposerPostgres(pgdata_master,
                              pg_bin,
                              '',
                              '',
                              "127.0.0.1",
                              port_distributor.get_port())

    master.initdb()
    mkdir_if_needed(master.pg_data_dir_path())
    with open(master.config_file_path(), "w") as f:
        cfg = [
            "max_wal_senders=10\n",
            "wal_log_hints=on\n",
            "max_replication_slots=10\n",
            "hot_standby=on\n",
            "shared_buffers=2GB\n",
            "min_wal_size=20GB\n",
            "max_wal_size=40GB\n",
            "checkpoint_timeout=60min\n",
            "log_checkpoints=on\n",
            "fsync=off\n",
            "max_connections=100\n",
            "wal_sender_timeout=0\n",
            "wal_level=replica\n",
            f"listen_addresses = '{master.listen_addr}'\n",
            f"port = '{master.port}'\n",
            "wal_keep_size=10TB\n",
            "shared_preload_libraries=zenith\n",
            "zenith.page_server_connstring=''\n",
            "synchronous_standby_names = 'ANY 1 (s1)'\n",
            "synchronous_commit=on\n",
        ]

        f.writelines(cfg)

    with open(os.path.join(pgdata_master, "pg_hba.conf"), "w") as f:
        cfg = """
host     all             all             0.0.0.0/0               trust
host     all             all             ::/0                    trust
local    all             all                                     trust
host     all             all        127.0.0.1/32                 trust
host     all             all        ::1/128                      trust
host      replication     all             0.0.0.0/0               trust
host      replication     all             ::/0                    trust

# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     all                                     trust
host    replication     all             127.0.0.1/32            trust
host    replication     all             ::1/128                 trust
"""
        f.writelines(cfg)

    master_env = {
        "PGHOST": master.listen_addr,
        "PGPORT": str(master.port),
        "PGDATA": master.pg_data_dir_path(),
        "PGUSER": "zenith_admin",
        "PGDATABASE": "postgres"
    }

    master.start()
    master.safe_psql("SELECT pg_create_physical_replication_slot('s1');")
    pg_bin.run(["pg_basebackup", "-D", pgdata_slave], env=master_env)

    slave = ProposerPostgres(pgdata_slave, pg_bin, '', '', "127.0.0.1", port_distributor.get_port())

    with open(slave.config_file_path(), "w") as f:
        cfg = [
            "max_wal_senders=10\n",
            "wal_log_hints=on\n",
            "max_replication_slots=10\n",
            "hot_standby=on\n",
            "shared_buffers=2GB\n",
            "min_wal_size=20GB\n",
            "max_wal_size=40GB\n",
            "checkpoint_timeout=60min\n",
            "log_checkpoints=on\n",
            "fsync=on\n",
            "max_connections=100\n",
            "wal_sender_timeout=0\n",
            "wal_level=replica\n",
            f"listen_addresses = '{slave.listen_addr}'\n",
            f"port = '{slave.port}'\n",
            "wal_keep_size=10TB\n",
            "shared_preload_libraries=zenith\n",
            "zenith.page_server_connstring=''\n",
            "synchronous_standby_names = 'ANY 1 (s1)'\n",
            "primary_slot_name = 's1'\n",
            f"primary_conninfo = 'application_name=s1 user=zenith_admin host={master.listen_addr} channel_binding=disable port={master.port} sslmode=disable sslcompression=0 sslsni=1 ssl_min_protocol_version=TLSv1.2 gssencmode=disable krbsrvname=postgres target_session_attrs=any'\n",
        ]

        f.writelines(cfg)

    with open(os.path.join(pgdata_slave, "standby.signal"), "w") as f:
        pass

    slave.start()

    run_pgbench(pg_bin, zenbenchmark, repo_dir, master.log_path(), master_env)

    slave.stop()
    master.stop()


def run_pgbench(pg_bin: PgBin,
                zenbenchmark: ZenithBenchmarker,
                repo_dir: str,
                log_path: str,
                pgbench_env: dict,
                scale=200,
                conns_count=32,
                pgbench_time=60,
                block=5000):
    cmd = ["pgbench", "-i", "-s", str(scale)]
    basepath = pg_bin.run_capture(cmd, pgbench_env)
    pgbench_init = basepath + '.stderr'

    with open(pgbench_init, 'r') as stdout_f:
        stdout = stdout_f.readlines()
        stats_str = stdout[-1]
        log.info(stats_str)

    init_seconds = float(stats_str.split()[2])

    zenbenchmark.record("pgbench_init", init_seconds, unit="s", report=MetricReport.LOWER_IS_BETTER)

    cmd = [
        "pgbench",
        "-c",
        str(conns_count),
        "-N",
        "-P",
        "1",
        "-T",
        str(pgbench_time),
    ]
    basepath = pg_bin.run_capture(cmd, pgbench_env)
    pgbench_run = basepath + '.stdout'

    with open(pgbench_run, 'r') as stdout_f:
        stdout = stdout_f.readlines()
        stats_str = stdout[-1]
        log.info(stats_str)

    pgbench_tps = float(stats_str.split()[2])

    zenbenchmark.record("tps_pgbench", pgbench_tps, unit="", report=MetricReport.HIGHER_IS_BETTER)

    pg_log = log_path

    sizes_log = str(repo_dir.joinpath("sizes.log"))

    cmd = [
        "bash",
        "-c",
        f"grep 'sending message' {pg_log} | awk '{{print $9}}' | grep -v -Fx '0' > {sizes_log}"
    ]
    subprocess.run(cmd, cwd=repo_dir)

    avg_sizes = calc_avg_sizes(sizes_log, block)
    log.info(f"avg_sizes in blocks of {block} messages = {avg_sizes}")

    zenbenchmark.record("walmessage_size_begin",
                        avg_sizes[0],
                        unit="b",
                        report=MetricReport.HIGHER_IS_BETTER)

    zenbenchmark.record("walmessage_size_end",
                        avg_sizes[-1],
                        unit="b",
                        report=MetricReport.HIGHER_IS_BETTER)
