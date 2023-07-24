import json
import os
import sys
from os.path import isdir, join

# import shutil

pg_version = sys.argv[1]
#  build_tag = sys.argv[2]
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
            "archive_path": f"{build_numbers[extension]}/{pg_version}/{extension}.tar.zstd",
        }

        # TODO: uncomment this to enable de-duplication:
        #  if build_numbers[extension] != build_tag:
        #      shutil.rmtree(extension)
os.chdir("..")
with open("ext_index.json", "w") as f:
    json.dump(ext_index, f)
