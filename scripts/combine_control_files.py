import json
import os

ext_index = {}
os.chdir("control_files")
for prefix in os.listdir("."):
    ext_index[prefix] = {}
    for file in os.listdir(prefix):
        with open(os.path.join(prefix, file), "r") as f:
            ext_name = file.replace(".control", "")
            control = f.read()
            ext_index[prefix][ext_name] = {"path": f"extensions/{prefix}/{ext_name}.tar.gz", "control": control}

with open("../ext_index.json", "w") as f:
    json.dump(ext_index, f)

