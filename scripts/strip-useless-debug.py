#!/usr/bin/env python3

# Strip useless .debug_pubnames and .debug_pubtypes from all binaries.
# They bloat the binaries, and are not used by modern debuggers anyway.
# This makes the resulting binaries about 30% smaller, and also makes
# the cargo cache smaller.
#
# See also https://github.com/rust-lang/rust/issues/46034
#
# Usage:
#     ./scripts/strip-useless-debug.py target
#
#
# Why is this script needed?
# --------------------------
#
# The simplest way to do this would be just:
#
#     find target -executable -type f -size +0 | \
#       xargs -IPATH objcopy -R .debug_pubnames -R .debug_pubtypes -p PATH
#
# However, objcopy is not very fast, so we want to run it in parallel.
# That would be straightforward to do with the xargs -P option, except
# that the rust target directory contains hard links. Running objcopy
# on multiple paths that are hardlinked to the same underlying file
# doesn't work, because one objcopy could be overwriting the file while
# the other one is trying to read it.
#
# To work around that, this script scans the target directory and
# collects paths of all executables, except that when multiple paths
# point to the same underlying inode, i.e. if two paths are hard links
# to the same file, only one of the paths is collected. Then, it runs
# objcopy on each of the unique files.
#
# There's one more subtle problem with hardlinks. The GNU objcopy man
# page says that:
#
#    If you do not specify outfile, objcopy creates a temporary file and
#    destructively renames the result with the name of infile.
#
# That is a problem: renaming over the file will create a new inode
# for the path, and leave the other hardlinked paths unchanged. We
# want to modify all the hard linked copies, and we also don't want to
# remove the hard linking, as that saves a lot of space. In testing,
# at least some versions of GNU objcopy seem to actually behave
# differently if the file has hard links, copying over the file
# instead of renaming if it has. So that text in the man page isn't
# totally accurate. But that's hardly something we should rely on:
# llvm-objcopy for example always renames. To avoid that problem, we
# specify a temporary file as the destination, and copy it over the
# original file in this python script. That way, it is independent of
# objcopy's behavior.

import argparse
import asyncio
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


async def main():
    parser = argparse.ArgumentParser(
        description="Strip useless .debug_pubnames and .debug_putypes sections from binaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-j", metavar="NUM", default=os.cpu_count(), type=int, help="number of parallel processes"
    )
    parser.add_argument("target", type=Path, help="target directory")

    args = parser.parse_args()
    max_parallel_processes = args.j
    target_dir = args.target

    # Collect list of executables in the target dir. Make note of the inode of
    # each path, and only record one path with the same inode. This ensures that
    # if there are hard links in the directory tree, we only remember one path
    # for each underlying file.
    inode_paths = {}  # inode -> path dictionary

    def onerror(err):
        raise err

    for currentpath, folders, files in os.walk(target_dir, onerror=onerror):
        for file in files:
            path = os.path.join(currentpath, file)
            if os.access(path, os.X_OK):
                stat = os.stat(path)
                # If multiple paths ar hardlinked to the same underlying file,
                # only we remember the first one that we see. It's arbitrary
                # which one we will see first, but that's ok.
                #
                # Skip empty files while we're at it. There are some .lock files
                # in the target directory that are marked as executable, but are
                # are binaries so objcopy would complain about them.
                if stat.st_size > 0:
                    prev = inode_paths.get(stat.st_ino)
                    if prev:
                        print(f"{path} is a hard link to {prev}, skipping")
                    else:
                        inode_paths[stat.st_ino] = path

    # This function runs "objcopy -R .debug_pubnames -R .debug_pubtypes" on a file.
    #
    # Returns (original size, new size)
    async def run_objcopy(path) -> (int, int):
        stat = os.stat(path)
        orig_size = stat.st_size
        if orig_size == 0:
            return (0, 0)

        # Write the output to a temp file first, and then copy it over the original.
        # objcopy could modify the file in place, but that's not reliable with hard
        # links. (See comment at beginning of this file.)
        with tempfile.NamedTemporaryFile() as tmpfile:
            cmd = [
                "objcopy",
                "-R",
                ".debug_pubnames",
                "-R",
                ".debug_pubtypes",
                "-p",
                path,
                tmpfile.name,
            ]
            proc = await asyncio.create_subprocess_exec(*cmd)
            rc = await proc.wait()
            if rc != 0:
                raise subprocess.CalledProcessError(rc, cmd)

            # If the file got smaller, copy it over the original.
            # Otherwise keep the original
            stat = os.stat(tmpfile.name)
            new_size = stat.st_size
            if new_size < orig_size:
                with open(path, "wb") as orig_dst:
                    shutil.copyfileobj(tmpfile, orig_dst)
                    return (orig_size, new_size)
            else:
                return (orig_size, orig_size)

    # convert the inode->path dictionary into plain list of paths.
    paths = []
    for path in inode_paths.values():
        paths.append(path)

    # Launch worker processes to process the list of files
    before_total = 0
    after_total = 0

    async def runner_subproc():
        nonlocal before_total
        nonlocal after_total
        while len(paths) > 0:
            path = paths.pop()

            (before_size, after_size) = await run_objcopy(path)
            before_total += before_size
            after_total += after_size

            print(f"{path}: {before_size} to {after_size} bytes")

    active_workers = []
    for i in range(max_parallel_processes):
        active_workers.append(asyncio.create_task(runner_subproc()))
    done, () = await asyncio.wait(active_workers)

    # all done!
    print(f"total size before {before_total} after: {after_total}")


if __name__ == "__main__":
    asyncio.run(main())
