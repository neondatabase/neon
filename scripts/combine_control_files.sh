#!/bin/bash

# Enable custom extensions for specific tenants
declare -A enabled_extensions
enabled_extensions["123454321"]=("anon")
enabled_extensions["public"]=("embedding")

# Initialize control data
declare -A control_data

# Read control files
for control_file in control_files/*.control; do
  ext_name=$(basename "$control_file" .control)
  control_data["$ext_name"]=$(cat "$control_file")
done

# Construct all data
all_data='{"enabled_extensions":'"$(declare -p enabled_extensions)"', "control_data":'"$(declare -p control_data)"'}'

# Write all data to ext_index.json
echo "$all_data" > ext_index.json

# const fs = require('fs');

# // enable custom extensions for specific tenants
# const enabledExtensions = {
#   "123454321": ["anon"],
#   "public": ["embedding"]
# };

# const controlData = {};
# const controlFiles = fs.readdirSync("control_files");

# controlFiles.forEach(controlFile => {
#   const extName = controlFile.replace(".control", "");
#   const controlFilePath = `control_files/${controlFile}`;
#   const fileData = fs.readFileSync(controlFilePath, "utf8");
#   controlData[extName] = fileData;
# });

# const allData = {
#   enabledExtensions: enabledExtensions,
#   controlData: controlData
# };

# const jsonData = JSON.stringify(allData, null, 2);
# fs.writeFileSync("ext_index.json", jsonData);


# //  import json
# //  import os

# //  # enable custom extensions for specific tenants
# //  enabled_extensions = {"123454321": ["anon"], "public": ["embedding"]}

# //  control_data = {}
# //  for control_file in os.listdir("control_files"):
# //      ext_name = control_file.replace(".control", "")
# //      with open(control_file, "r") as f:
# //          control_data[ext_name] = f.read()

# //  all_data = {"enabled_extensions": enabled_extensions, "control_data": control_data}

# //  with open("ext_index.json", "w") as f:
# //      json.dump(all_data, f)
