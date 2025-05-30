# Tool to convert the JSON output from running a perf test with `--out-dir` to a CSV that
# can be easily pasted into a spreadsheet for quick viz & analysis.
# Check the `./README.md` in this directory for `--out-dir`.
#
# TODO: add the pytest.mark.parametrize to the json and make them columns here
# https://github.com/neondatabase/neon/issues/11878

import csv
import json
import os
import sys


def json_to_csv(json_file):
    with open(json_file) as f:
        data = json.load(f)

    # Collect all possible metric names to form headers
    all_metrics = set()
    for result in data.get("result", []):
        for metric in result.get("data", []):
            all_metrics.add(metric["name"])

    # Sort metrics for consistent output
    metrics = sorted(list(all_metrics))

    # Create headers
    headers = ["suit"] + metrics

    # Prepare rows
    rows = []
    for result in data.get("result", []):
        row = {"suit": result["suit"]}

        # Initialize all metrics to empty
        for metric in metrics:
            row[metric] = ""

        # Fill in available metrics
        for item in result.get("data", []):
            row[item["name"]] = item["value"]

        rows.append(row)

    # Write to stdout as CSV
    writer = csv.DictWriter(sys.stdout, fieldnames=headers)
    writer.writeheader()
    writer.writerows(rows)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {os.path.basename(__file__)} <json_file>")
        sys.exit(1)

    json_file = sys.argv[1]
    json_to_csv(json_file)
