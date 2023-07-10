import json
import os

index = {}
os.chdir("control_files")
for prefix in os.listdir("."):
    for file in os.listdir(prefix):
        with open(os.path.join(prefix, file), "r") as f:
            ext_name = file.replace(".control", "")
            control = f.read()
            index[ext_name] = {
                "path": f"{prefix}/{ext_name}.tar.gz",
                "control": control
            }

with open("../control_index.json", "w") as f:
    json.dump(index, f)

