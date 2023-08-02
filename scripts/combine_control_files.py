#! /usr/bin/env python3
# Script to generate ext_index.json metadata file
# that stores content of the control files and location of extension archives
# for all extensions in extensions subdir.
import argparse
import json
import subprocess
from pathlib import Path

"""
# ext_index.json example:
{
    "public_extensions": [
        "anon"
    ],
    "library_index": {
        "anon": "anon",
        // for more complex extensions like postgis
        // we might have something like:
        // address_standardizer: postgis
        // postgis_tiger: postgis
    },
    "extension_data": {
        "anon": {
            "control_data": {
                "anon.control": "# PostgreSQL Anonymizer (anon) extension \ncomment = 'Data anonymization tools' \ndefault_version = '1.1.0' \ndirectory='extension/anon' \nrelocatable = false \nrequires = 'pgcrypto' \nsuperuser = false \nmodule_pathname = '$libdir/anon' \ntrusted = true \n"
            },
            "archive_path": "5648391853/v15/extensions/anon.tar.zst"
        }
    }
}
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="generate ext_index.json")
    parser.add_argument("pg_version", type=str, choices=["v14", "v15"], help="pg_version")
    parser.add_argument("BUILD_TAG", type=str, help="BUILD_TAG for this compute image")
    parser.add_argument("--public_extensions", type=str, help="list of public extensions")
    args = parser.parse_args()
    pg_version = args.pg_version
    BUILD_TAG = args.BUILD_TAG
    public_ext_list = args.public_extensions.split(",")

    ext_index = {}
    library_index = {}
    EXT_PATH = Path("extensions")
    for extension in EXT_PATH.iterdir():
        if extension.is_dir():
            control_data = {}
            for control_file in extension.glob("*.control"):
                if control_file.suffix != ".control":
                    continue
                with open(control_file, "r") as f:
                    control_data[control_file.name] = f.read()
            ext_index[extension.name] = {
                "control_data": control_data,
                "archive_path": f"{BUILD_TAG}/{pg_version}/extensions/{extension.name}.tar.zst",
            }
        elif extension.suffix == ".zst":
            file_list = (
                str(subprocess.check_output(["tar", "tf", str(extension)]), "utf-8")
                .strip()
                .split("\n")
            )
            for file in file_list:
                if file.endswith(".so") and file.startswith("lib/"):
                    lib_name = file[4:-3]
                    library_index[lib_name] = extension.name.replace(".tar.zst", "")

    all_data = {
        "public_extensions": public_ext_list,
        "library_index": library_index,
        "extension_data": ext_index,
    }
    with open("ext_index.json", "w") as f:
        json.dump(all_data, f)
