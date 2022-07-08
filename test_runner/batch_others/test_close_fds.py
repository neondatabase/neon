from contextlib import closing
import shutil
import time
import subprocess
import os.path

from cached_property import threading
from fixtures.neon_fixtures import NeonEnv
from fixtures.log_helper import log


def lsof_path() -> str:
    path_output = shutil.which("lsof")
    if path_output is None:
        raise RuntimeError('lsof not found in PATH')
    else:
        return path_output


# Makes sure that `pageserver.pid` is only held by `pageserve` command, not other commands.
# This is to test the changes in https://github.com/neondatabase/neon/pull/1834.
def test_lsof_pageserver_pid(neon_simple_env: NeonEnv):
    env = neon_simple_env

    def start_workload():
        env.neon_cli.create_branch("test_lsof_pageserver_pid")
        pg = env.postgres.create_start("test_lsof_pageserver_pid")
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE foo as SELECT x FROM generate_series(1,100000) x")
                cur.execute("update foo set x=x+1")

    workload_thread = threading.Thread(target=start_workload, args=())
    workload_thread.start()

    path = os.path.join(env.repo_dir, "pageserver.pid")
    lsof = lsof_path()
    while workload_thread.is_alive():
        res = subprocess.run([lsof, path],
                             check=False,
                             universal_newlines=True,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        # parse the `lsof` command's output to get only the list of commands
        commands = [line.split(' ')[0] for line in res.stdout.strip().split('\n')[1:]]
        if len(commands) > 0:
            log.info(f"lsof commands: {commands}")
            assert commands == ['pageserve']

        time.sleep(1.0)
