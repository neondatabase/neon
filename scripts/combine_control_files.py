#! /usr/bin/env python3
# Script to generate ext_index.json metadata file
# that stores content of the control files and location of extension archives
# for all extensions in extensions subdir.
import json
from pathlib import Path
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="generate ext_index.json")
    parser.add_argument("pg_version", type=str, choices=["v14", "v15"], help="pg_version")
    parser.add_argument("BUILD_TAG", type=str, help="BUILD_TAG for this compute image")
    args = parser.parse_args()
    pg_version = args.pg_version

    build_numbers = {}
    with open("build_numbers.txt", "r") as f:
        for line in f.readlines():
            ext_name, build_num = line.split(" ")
            build_numbers[ext_name] = build_num.strip()

    ext_index = {}
    EXT_PATH = Path("extensions")
    for extension in EXT_PATH.iterdir():
        if extension.is_dir():
            control_data = {}
            for control_file in extension.iterdir():
                if control_file.suffix != ".control":
                    continue
                with open(control_file, "r") as f:
                    control_data[control_file.name] = f.read()
            ext_index[extension.name] = {
                "control_data": control_data,
                "archive_path": f"{build_numbers[extension.name]}/{pg_version}/extensions/{extension.name}.tar.zst",
            }

    with open("ext_index.json", "w") as f:
        json.dump(ext_index, f)
