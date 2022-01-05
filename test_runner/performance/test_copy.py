from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from io import BufferedReader, RawIOBase
from itertools import repeat

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


class CopyTestData(RawIOBase):
    def __init__(self, rows: int):
        self.rows = rows
        self.rownum = 0
        self.linebuf = None
        self.ptr = 0

    def readable(self):
        return True

    def readinto(self, b):
        if self.linebuf is None or self.ptr == len(self.linebuf):
            if self.rownum >= self.rows:
                # No more rows, return EOF
                return 0
            self.linebuf = f"{self.rownum}\tSomewhat long string to consume some space.\n".encode()
            self.ptr = 0
            self.rownum += 1

        # Number of bytes to read in this call
        l = min(len(self.linebuf) - self.ptr, len(b))

        b[:l] = self.linebuf[self.ptr:(self.ptr + l)]
        self.ptr += l
        return l


def copy_test_data(rows: int):
    return BufferedReader(CopyTestData(rows))


#
# COPY performance tests.
#
def test_copy(zenith_simple_env: ZenithEnv, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_copy", "empty"])

    pg = env.postgres.create_start('test_copy')
    log.info("postgres is running on 'test_copy' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the pageserver 'checkpoint' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

            cur.execute("create table copytest (i int, t text);")

            # Load data with COPY, recording the time and I/O it takes.
            #
            # Since there's no data in the table previously, this extends it.
            with zenbenchmark.record_pageserver_writes(env.pageserver,
                                                       'copy_extend_pageserver_writes'):
                with zenbenchmark.record_duration('copy_extend'):
                    cur.copy_from(copy_test_data(1000000), 'copytest')
                    # Flush the layers from memory to disk. This is included in the reported
                    # time and I/O
                    pscur.execute(f"checkpoint {env.initial_tenant} {timeline}")

            # Delete most rows, and VACUUM to make the space available for reuse.
            with zenbenchmark.record_pageserver_writes(env.pageserver, 'delete_pageserver_writes'):
                with zenbenchmark.record_duration('delete'):
                    cur.execute("delete from copytest where i % 100 <> 0;")
                    # Flush the layers from memory to disk. This is included in the reported
                    # time and I/O
                    pscur.execute(f"checkpoint {env.initial_tenant} {timeline}")

            with zenbenchmark.record_pageserver_writes(env.pageserver, 'vacuum_pageserver_writes'):
                with zenbenchmark.record_duration('vacuum'):
                    cur.execute("vacuum copytest")
                    # Flush the layers from memory to disk. This is included in the reported
                    # time and I/O
                    pscur.execute(f"checkpoint {env.initial_tenant} {timeline}")

            # Load data into the table again. This time, this will use the space free'd
            # by the VACUUM.
            #
            # This will also clear all the VM bits.
            with zenbenchmark.record_pageserver_writes(env.pageserver,
                                                       'copy_reuse_pageserver_writes'):
                with zenbenchmark.record_duration('copy_reuse'):
                    cur.copy_from(copy_test_data(1000000), 'copytest')

                    # Flush the layers from memory to disk. This is included in the reported
                    # time and I/O
                    pscur.execute(f"checkpoint {env.initial_tenant} {timeline}")

            # Record peak memory usage
            zenbenchmark.record("peak_mem",
                                zenbenchmark.get_peak_mem(env.pageserver) / 1024,
                                'MB',
                                report=MetricReport.LOWER_IS_BETTER)

            # Report disk space used by the repository
            timeline_size = zenbenchmark.get_timeline_size(env.repo_dir,
                                                           env.initial_tenant,
                                                           timeline)
            zenbenchmark.record('size',
                                timeline_size / (1024 * 1024),
                                'MB',
                                report=MetricReport.LOWER_IS_BETTER)
