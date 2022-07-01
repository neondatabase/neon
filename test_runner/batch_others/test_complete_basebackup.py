from fixtures.neon_fixtures import VanillaPostgres
from fixtures.utils import mkdir_if_needed, subprocess_capture
import os
import shutil


def to_rel_path(tablespace, filenode):
    if tablespace == 0:
        return f"global/{filenode}"
    else:
        # TODO doesn't seem right. We need dbnode here
        return f"base/{tablespace}/{filenode}"


def get_rel_paths(pg_bin, restored_dir):
    """Return list of relation paths"""
    port = "55439"  # Probably free
    with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
        vanilla_pg.configure([f"port={port}"])
        vanilla_pg.start()

        # Not using this, but seems relevant
        query = "SELECT oid, dattablespace FROM pg_database"
        result = vanilla_pg.safe_psql(query, user="cloud_admin")
        print("AAA")
        print(result)
        # [(13134, 1663), (1, 1663), (13133, 1663)]

        query = "SELECT reltablespace, relfilenode FROM pg_class"
        result = vanilla_pg.safe_psql(query, user="cloud_admin")
        paths = [to_rel_path(*row) for row in result]

        # Check for duplicates
        from collections import Counter
        duplicates = {
            x: count
            for x, count in Counter(paths).items()
            if count > 1
        }

        # Sus: currently seeing {'global/0': 154, 'base/1664/0': 43}
        if duplicates:
            print(f"Warning: found duplicate paths: {duplicates}")

        return paths


def test_complete(pg_bin):
    # Specify directories
    work_dir = "/home/bojan/src/neondatabase/neon/test_output/test_import_from_pageserver/"
    base_tar = os.path.join(work_dir, "psql_2.stdout")
    output_tar = os.path.join(work_dir, "psql_2-completed.stdout")

    # Unpack the base tar
    restored_dir = os.path.join(work_dir, "restored")
    shutil.rmtree(restored_dir, ignore_errors=True)
    os.mkdir(restored_dir, 0o750)
    subprocess_capture(work_dir, ["tar", "-xf", base_tar, "-C", restored_dir])

    # Get the nodes
    paths = get_rel_paths(pg_bin, restored_dir)

    for path in paths:
        absolute_path = os.path.join(restored_dir, path)
        print(path, os.path.exists(absolute_path))
        # TODO touch if not exists. But first get correct paths. Something's off now

    # Pack completed tar, being careful to preserve relative file names
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(work_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)
