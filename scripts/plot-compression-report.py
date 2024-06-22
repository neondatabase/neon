#!/usr/bin/env -S python3 -u

import argparse
import json
import os
from pprint import pprint

import matplotlib.pyplot as plt

parser = argparse.ArgumentParser(prog="compression-report")
parser.add_argument("dir")
args = parser.parse_args()

files = []
for file_name in os.listdir(args.dir):
    if not file_name.endswith(".json"):
        continue
    file_path = os.path.join(args.dir, file_name)
    with open(file_path) as json_str:
        json_data = json.load(json_str)
    files.append((file_name, json_data))
#pprint(files)

extra_zstd_lines = True
dc = 2 # data column to use (1 for sizes, 2 for time)
sort_by = "ZstdHigh"
files.sort(key=lambda file_data: [x for x in file_data[1] if x[0] == sort_by][0][dc])


x_axis = []
data_baseline = []
data_lz4 = []
data_zstd = []
data_zstd_low = []
data_zstd_high = []

for idx, f in enumerate(files):
    file_data = f[1]
    #pprint(file_data)

    x_axis.append(idx)
    data_baseline.append([x for x in file_data if x[0] is None][0][dc])
    data_lz4.append([x for x in file_data if x[0] == "LZ4"][0][dc])
    data_zstd.append([x for x in file_data if x[0] == "Zstd"][0][dc])
    if extra_zstd_lines:
        data_zstd_low.append([x for x in file_data if x[0] == "ZstdLow"][0][dc])
        data_zstd_high.append([x for x in file_data if x[0] == "ZstdHigh"][0][dc])

plt.plot(x_axis, data_baseline, "x", markeredgewidth=2, label="baseline")
plt.plot(x_axis, data_lz4, "x", markeredgewidth=2, label="lz4")
plt.plot(x_axis, data_zstd, "x", markeredgewidth=2, label="Zstd")
if extra_zstd_lines:
    plt.plot(x_axis, data_zstd_low, "x", markeredgewidth=2, label="ZstdLow")
    plt.plot(x_axis, data_zstd_high, "x", markeredgewidth=2, label="ZstdHigh")

# plt.style.use('_mpl-gallery')
plt.ylim(bottom=0)
plt.legend(loc="upper left")

figure_path = os.path.join(args.dir, "figure.png")
print(f"saving figure to {figure_path}")
plt.savefig(figure_path)
plt.show()
