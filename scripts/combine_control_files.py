#! /usr/bin/env python3
# Script to generate ext_index.json metadata file
# that stores content of the control files and location of extension archives
# for all extensions in /extensions subdir.
import json
import os
import sys
from os.path import isdir, join

if __name__ == "__main__":
    pg_version = sys.argv[1]
    assert pg_version in ("v14", "v15")

    build_numbers = {}
    with open("build_numbers.txt", "r") as f:
        for line in f.readlines():
            ext_name, build_num = line.split(" ")
            build_numbers[ext_name] = build_num.strip()

    ext_index = {}
    os.chdir("extensions")
    for extension in os.listdir("."):
        if isdir(extension):
            control_data = {}
            for control_file in os.listdir(extension):
                if ".control" not in control_file:
                    continue
                with open(join(extension, control_file), "r") as f:
                    control_data[control_file] = f.read()
            ext_index[extension] = {
                "control_data": control_data,
                "archive_path": f"{build_numbers[extension]}/{pg_version}/extensions/{extension}.tar.zst",
            }

    os.chdir("..")
    with open("ext_index.json", "w") as f:
        json.dump(ext_index, f)
