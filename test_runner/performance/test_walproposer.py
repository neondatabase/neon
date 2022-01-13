from pathlib import Path
import os
import subprocess

from fixtures.zenith_fixtures import PgBin, PortDistributor
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from batch_others.test_wal_acceptor import SafekeeperEnv

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


def test_walproposer_pgbench(test_output_dir: str,
                             port_distributor: PortDistributor,
                             pg_bin: PgBin,
                             zenbenchmark: ZenithBenchmarker):
    scale = 50
    conns_count = 32
    pgbench_time = 30
    block = 5000

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

        cmd = ["pgbench", "-i", "-s", str(scale)]
        basepath = pg_bin.run_capture(cmd, pgbench_env)
        pgbench_init = basepath + '.stderr'

        with open(pgbench_init, 'r') as stdout_f:
            stdout = stdout_f.readlines()
            stats_str = stdout[-1]
            log.info(stats_str)

        init_seconds = float(stats_str.split()[2])

        zenbenchmark.record("pgbench_init",
                            init_seconds,
                            unit="s",
                            report=MetricReport.LOWER_IS_BETTER)

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

        zenbenchmark.record("tps_pgbench",
                            pgbench_tps,
                            unit="",
                            report=MetricReport.HIGHER_IS_BETTER)

        pg_log = env.postgres.log_path()

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
