from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare
from io import BufferedReader, RawIOBase
from itertools import repeat

pytest_plugins = (
    "fixtures.zenith_fixtures",
    "fixtures.benchmark_fixture",
    "fixtures.compare_fixtures",
)


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
def test_copy(zenith_with_baseline: PgCompare):
    env = zenith_with_baseline

    # Get the timeline ID of our branch. We need it for the pageserver 'checkpoint' command
    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table copytest (i int, t text);")

            # Load data with COPY, recording the time and I/O it takes.
            #
            # Since there's no data in the table previously, this extends it.
            with env.record_pageserver_writes('copy_extend_pageserver_writes'):
                with env.record_duration('copy_extend'):
                    cur.copy_from(copy_test_data(1000000), 'copytest')
                    env.flush()

            # Delete most rows, and VACUUM to make the space available for reuse.
            with env.record_pageserver_writes('delete_pageserver_writes'):
                with env.record_duration('delete'):
                    cur.execute("delete from copytest where i % 100 <> 0;")
                    env.flush()

            with env.record_pageserver_writes('vacuum_pageserver_writes'):
                with env.record_duration('vacuum'):
                    cur.execute("vacuum copytest")
                    env.flush()

            # Load data into the table again. This time, this will use the space free'd
            # by the VACUUM.
            #
            # This will also clear all the VM bits.
            with env.record_pageserver_writes('copy_reuse_pageserver_writes'):
                with env.record_duration('copy_reuse'):
                    cur.copy_from(copy_test_data(1000000), 'copytest')
                    env.flush()

            env.report_peak_memory_use()
            env.report_size()
