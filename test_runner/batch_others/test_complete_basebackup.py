from fixtures.neon_fixtures import VanillaPostgres
from fixtures.utils import mkdir_if_needed, subprocess_capture
import os
import shutil
from pathlib import Path
import tempfile


def get_rel_paths(log_dir, pg_bin, base_tar):
    """Yeild list of relation paths"""
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", base_tar, "-C", restored_dir])

        port = "55439"  # Probably free
        with VanillaPostgres(restored_dir, pg_bin, port, init=False) as vanilla_pg:
            vanilla_pg.configure([f"port={port}"])
            vanilla_pg.start()

            # Create database based on template0 because we can't connect to template0
            query = "create database template0copy template template0"
            vanilla_pg.safe_psql(query, user="cloud_admin")
            vanilla_pg.safe_psql("CHECKPOINT", user="cloud_admin")

            # Get all databases
            query = "select oid, datname from pg_database"
            oid_dbname_pairs = vanilla_pg.safe_psql(query, user="cloud_admin")
            template0_oid = [
                oid
                for (oid, database) in oid_dbname_pairs
                if database == "template0"
            ][0]

            # Get rel paths for each database
            for oid, database in oid_dbname_pairs:
                if database == "template0":
                    # We can't connect to template0
                    continue

                query = "select relname, pg_relation_filepath(oid) from pg_class"
                result = vanilla_pg.safe_psql(query, user="cloud_admin", dbname=database)
                for relname, filepath in result:
                    if filepath is not None:

                        if database == "template0copy":
                            # Add all template0copy paths to template0
                            prefix = f"base/{oid}/"
                            if filepath.startswith(prefix):
                                suffix = filepath[len(prefix):]
                                yield f"base/{template0_oid}/{suffix}"
                            elif filepath.startswith("global"):
                                print(f"skipping {database} global file {filepath}")
                            else:
                                raise AssertionError
                        else:
                            yield filepath


def pack_base(log_dir, restored_dir, output_tar):
    tmp_tar_name = "tmp.tar"
    tmp_tar_path = os.path.join(restored_dir, tmp_tar_name)
    cmd = ["tar", "-cf", tmp_tar_name] + os.listdir(restored_dir)
    subprocess_capture(log_dir, cmd, cwd=restored_dir)
    shutil.move(tmp_tar_path, output_tar)


def get_files_in_tar(log_dir, tar):
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", tar, "-C", restored_dir])

        # Find empty files
        empty_files = []
        for root, dirs, files in os.walk(restored_dir):
            for name in files:
                file_path = os.path.join(root, name)
                yield file_path[len(restored_dir) + 1:]


def corrupt(log_dir, base_tar, output_tar):
    """Remove all empty files and repackage. Return paths of files removed."""
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", base_tar, "-C", restored_dir])

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

        # Repackage
        pack_base(log_dir, restored_dir, output_tar)

        # Return relative paths
        return {
            empty_file[len(restored_dir) + 1:]
            for empty_file in empty_files
        }


def touch_missing_rels(log_dir, corrupt_tar, output_tar, paths):
    with tempfile.TemporaryDirectory() as restored_dir:
        # Unpack the base tar
        subprocess_capture(log_dir, ["tar", "-xf", corrupt_tar, "-C", restored_dir])

        # Touch files that don't exist
        for path in paths:
            absolute_path = os.path.join(restored_dir, path)
            exists = os.path.exists(absolute_path)
            if not exists:
                Path(absolute_path).touch()

        # Repackage
        pack_base(log_dir, restored_dir, output_tar)


def test_complete(pg_bin):
    # Specify directories
    # TODO make this a standalone script, with these as inputs
    work_dir = "/home/bojan/src/neondatabase/neon/test_output/test_import_from_pageserver/"
    base_tar = os.path.join(work_dir, "psql_2.stdout")
    output_tar = os.path.join(work_dir, "psql_2-completed.stdout")

    # Create new base tar with missing empty files
    corrupt_tar = os.path.join(work_dir, "psql_2-corrupted.stdout")
    deleted_files = corrupt(work_dir, base_tar, corrupt_tar)
    assert len(set(get_files_in_tar(work_dir, base_tar)) -
               set(get_files_in_tar(work_dir, corrupt_tar))) > 0

    # Reconstruct paths from the corrupted tar, assert it covers everything important
    reconstructed_paths = set(get_rel_paths(work_dir, pg_bin, corrupt_tar))
    paths_missed = deleted_files - reconstructed_paths
    assert paths_missed.issubset({
        "postgresql.auto.conf",
        "pg_ident.conf",
    })

    # Recreate the correct tar by touching files, compare with original tar
    touch_missing_rels(work_dir, corrupt_tar, output_tar, reconstructed_paths)
    paths_missed = (set(get_files_in_tar(work_dir, base_tar)) -
                    set(get_files_in_tar(work_dir, output_tar)))
    assert paths_missed.issubset({
        "postgresql.auto.conf",
        "pg_ident.conf",
    })
