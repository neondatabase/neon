from fixtures.neon_fixtures import VanillaPostgres
from fixtures.utils import mkdir_if_needed, subprocess_capture
import os
import shutil
from pathlib import Path


template0_files = [3430, 3256, 3429, 3118, 4171, 4165, 12960, 3576, 6102, 826, 3466, 12965, 12955, 4173, 3439,
    2830, 4169, 1418, 3501, 4153, 4159, 2604, 4145, 4155, 1417, 2832, 2613, 2620, 3381, 6175, 6104, 12970, 4157,
    4143, 2611, 2224, 6106, 4167, 2336, 4163, 2995, 3350, 4147, 2328, 2834, 3598, 4151, 4149, 3596]


def get_rel_paths(pg_bin, restored_dir):
    """Yeild list of relation paths"""
    port = "55439"  # Probably free
    with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
        vanilla_pg.configure([f"port={port}"])
        vanilla_pg.start()

        query = "select oid, datname from pg_database"
        oid_dbname_pairs = vanilla_pg.safe_psql(query, user="cloud_admin")

        for oid, database in oid_dbname_pairs:
            if database == "template0":
                # We can't connect to template0, but it's always the same
                for rel in template0_files:
                    yield f"base/{oid}/{rel}"
                continue

            query = "select pg_relation_filepath(oid) from pg_class"
            result = vanilla_pg.safe_psql(query, user="cloud_admin", dbname=database)
            for row in result:
                filepath = row[0]
                if filepath is not None:
                    yield filepath


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

    # Find empty files
    empty_files = []
    for root, dirs, files in os.walk(restored_dir):
        for name in files:
            file_path = os.path.join(root, name)
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                empty_files.append(file_path)

    # Delete empty files (just to see if they get recreated)
    for empty_file in empty_files:
        os.remove(empty_file)

    # Get the nodes
    paths = list(get_rel_paths(pg_bin, restored_dir))

    # Touch files that don't exist
    for path in paths:
        absolute_path = os.path.join(restored_dir, path)
        exists = os.path.exists(absolute_path)
        if not exists:
            print("Touching file ", absolute_path)
            Path(absolute_path).touch()

    # Check that all deleted files are back
    unimportant_files = {
        "postgresql.auto.conf",
        "pg_ident.conf",
    }
    for empty_file in empty_files:
        file_name = os.path.basename(empty_file)
        if file_name in unimportant_files:
            continue

        exists = os.path.exists(empty_file)
        if not exists:
            print(f"Deleted empty file {empty_file} was not recreated")
            assert(exists)

    # Pack completed tar, being careful to preserve relative file names
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(work_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)
