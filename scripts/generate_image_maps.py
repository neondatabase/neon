import itertools
import json
import os

build_tag = os.environ["BUILD_TAG"]
branch = os.environ["BRANCH"]
dev_acr = os.environ["DEV_ACR"]
prod_acr = os.environ["PROD_ACR"]

components = {
    "neon": ["neon"],
    "compute": [
        "compute-node-v14",
        "compute-node-v15",
        "compute-node-v16",
        "compute-node-v17",
        "vm-compute-node-v14",
        "vm-compute-node-v15",
        "vm-compute-node-v16",
        "vm-compute-node-v17",
    ],
}

registries = {
    "dev": [
        "docker.io/neondatabase",
        "369495373322.dkr.ecr.eu-central-1.amazonaws.com",
        f"{dev_acr}.azurecr.io/neondatabase",
    ],
    "prod": [
        "093970136003.dkr.ecr.eu-central-1.amazonaws.com",
        f"{prod_acr}.azurecr.io/neondatabase",
    ],
}

outputs: dict[str, dict[str, list[str]]] = {}

target_tags = [build_tag, "latest"] if branch == "main" else [build_tag]
target_stages = ["dev", "prod"] if branch.startswith("release") else ["dev"]

for component_name, component_images in components.items():
    for stage in target_stages:
        outputs[f"{component_name}-{stage}"] = dict(
            [
                (
                    f"docker.io/neondatabase/{component_image}:{build_tag}",
                    [
                        f"{combo[0]}/{component_image}:{combo[1]}"
                        for combo in itertools.product(registries[stage], target_tags)
                    ],
                )
                for component_image in component_images
            ]
        )

with open(os.environ["GITHUB_OUTPUT"], "a") as f:
    for key, value in outputs.items():
        f.write(f"{key}={json.dumps(value)}\n")
