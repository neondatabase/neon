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
        "kq_imcx": "kq_imcx"
        // would be more complicated for something like postgis where multiple library names all map to postgis
    },
    "extension_data": {
        "kq_imcx": {
            "control_data": {
                "kq_imcx.control": "# This file is generated content from add_postgresql_extension.\n# No point in modifying it, it will be overwritten anyway.\n\n# Default version, always set\ndefault_version = '0.1'\n\n# Module pathname generated from target shared library name. Use\n# MODULE_PATHNAME in script file.\nmodule_pathname = '$libdir/kq_imcx.so'\n\n# Comment for extension. Set using COMMENT option. Can be set in\n# script file as well.\ncomment = 'ketteQ In-Memory Calendar Extension (IMCX)'\n\n# Encoding for script file. Set using ENCODING option.\n#encoding = ''\n\n# Required extensions. Set using REQUIRES option (multi-valued).\n#requires = ''\ntrusted = true\n"
            },
            "archive_path": "5648391853/v15/extensions/kq_imcx.tar.zst"
        },
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
                if ".so" in file:
                    lib_name = file.replace("lib/", "").replace(".so", "")
                    library_index[lib_name] = extension.name.replace(".tar.zst", "")

    all_data = {
        "public_extensions": public_ext_list,
        "library_index": library_index,
        "extension_data": ext_index,
    }
    with open("ext_index.json", "w") as f:
        json.dump(all_data, f)
