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

for source, targets in parsed_image_map.items():
    for target in targets:
        cmd = ["docker", "buildx", "imagetools", "create", "-t", target, source]
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        if result.returncode != 0:
            print(f"Error: {result.stdout}")
            raise RuntimeError(f"Command failed: {' '.join(cmd)}")
