import json
import os

with open("SOMETHING", "w") as f:
    f.write("SOMETHING")

# enable custom extensions for specific tenants
enabled_extensions = {"123454321": ["anon"], "public": ["embedding"]}

control_data = {}
os.chdir("control_files")
for control_file in os.listdir("."):
    ext_name = control_file.replace(".control", "")
    with open(control_file, "r") as f:
        control_data[ext_name] = f.read()

all_data = {"enabled_extensions": enabled_extensions, "control_data": control_data}

with open("ext_index.json", "w") as f:
    json.dump(all_data, f)
