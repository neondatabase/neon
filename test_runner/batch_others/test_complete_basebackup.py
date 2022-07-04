from fixtures.neon_fixtures import VanillaPostgres
from fixtures.utils import mkdir_if_needed, subprocess_capture
import os
import shutil
from pathlib import Path


def get_rel_paths(pg_bin, restored_dir):
    """Yeild list of relation paths"""
    port = "55439"  # Probably free
    with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
        vanilla_pg.configure([f"port={port}"])
        vanilla_pg.start()

        query = "select datname from pg_database"
        result = vanilla_pg.safe_psql(query, user="cloud_admin")
        databases = [row[0] for row in result]

        for database in databases:
            if database == "template0":
                continue  # TODO this one doesn't take connections

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
    print(paths)

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

    # Pack completed tar, being careful to preserve relative file names
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(work_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)
