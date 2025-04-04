import json
import os
import subprocess

image_map = os.getenv("IMAGE_MAP")
if not image_map:
    raise ValueError("IMAGE_MAP environment variable is not set")

try:
    parsed_image_map: dict[str, list[str]] = json.loads(image_map)
except json.JSONDecodeError as e:
    raise ValueError("Failed to parse IMAGE_MAP as JSON") from e

failures = []

pending = [(source, target) for source, targets in parsed_image_map.items() for target in targets]

while len(pending) > 0:
    if len(failures) > 10:
        print("Error: more than 10 failures!")
        for failure in failures:
            print(f'"{failure[0]}" failed with the following output:')
            print(failure[1])
        raise RuntimeError("Retry limit reached.")

    source, target = pending.pop(0)
    cmd = ["docker", "buildx", "imagetools", "create", "-t", target, source]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    if result.returncode != 0:
        failures.append((" ".join(cmd), result.stdout))
        pending.append((source, target))

if len(failures) > 0 and (github_output := os.getenv("GITHUB_OUTPUT")):
    with open(github_output, "a") as f:
        f.write("slack_notify=true\n")
