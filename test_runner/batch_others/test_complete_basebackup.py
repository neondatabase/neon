from fixtures.neon_fixtures import VanillaPostgres
from fixtures.utils import mkdir_if_needed, subprocess_capture
import os
import shutil


def get_nodes(pg_bin, restored_dir):
    port = "55439"  # Probably free
    with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
        # TODO make port an optional argument
        vanilla_pg.configure([
            f"port={port}",
        ])
        vanilla_pg.start()

        query = "SELECT relfilenode FROM pg_class"
        result = vanilla_pg.safe_psql(query, user="cloud_admin")
        nodes = [row[0] for row in result]
        return nodes


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
    nodes = get_nodes(pg_bin, restored_dir)
    print(nodes)

    # Add missing empty rel files
    # TODO:
    # 1. Convert nodes to file paths
    # 2. Add files inside restored_dir

    # Pack completed tar, being careful to preserve relative file names
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(work_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)
