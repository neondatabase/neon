import json
import os

index = {}
os.chdir("control_files")
for prefix in os.listdir("."):
    for file in os.listdir(prefix):
        with open(fname, "r") as f:
            ext_name = fname.replace(".control", "")
            control = f.read()
            index[ext_name] = {
                "path": f"{prefix}/{ext_name}.tar.gz",
                "control": control
            }

