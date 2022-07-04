from fixtures.neon_fixtures import VanillaPostgres
from fixtures.utils import mkdir_if_needed, subprocess_capture
import os
import shutil
from pathlib import Path


def get_rel_paths(pg_bin, restored_dir):
    """Return list of relation paths"""
    port = "55439"  # Probably free
    with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
        vanilla_pg.configure([f"port={port}"])
        vanilla_pg.start()

        query = "select pg_relation_filepath(oid) from pg_class"
        result = vanilla_pg.safe_psql(query, user="cloud_admin")
        return [
            row[0]
            for row in result
            if row[0] is not None  # TODO why is it None sometimes?
        ]


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

    # TODO rm empty fiiles, just to see if they get recreated

    # Get the nodes
    paths = get_rel_paths(pg_bin, restored_dir)

    # Touch files that don't exist
    for path in paths:
        absolute_path = os.path.join(restored_dir, path)
        exists = os.path.exists(absolute_path)
        if not exists:
            print("Touching file ", absolute_path)
            Path(absolute_path).touch()

    # Pack completed tar, being careful to preserve relative file names
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(work_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)
