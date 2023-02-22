import textwrap
import requests
import argparse
import json

p = argparse.ArgumentParser()
p.add_argument("--tenant", required=True)
p.add_argument("--merge-existing-with", type=str)
p.add_argument("--endpoint", type=str, default='http://localhost:9898')
args = p.parse_args()

merge_existing_with = {}
if args.merge_existing_with is not None:
    merge_existing_with = json.loads(args.merge_existing_with)
    assert isinstance(merge_existing_with, dict)

class Client:
    def __init__(self, endpoint) -> None:
        self.endpoint = endpoint

    def get(self, rel_url, **kwargs):
        resp = requests.get(self.endpoint + rel_url, **kwargs)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            print("API ERROR: " + resp.text)
            raise
        return resp.json()
    def put(self, rel_url, **kwargs):
        resp = requests.put(self.endpoint + rel_url, **kwargs)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            print("API ERROR: " + resp.text)
            raise
        return resp.json()

client = Client(args.endpoint)

config = client.get(f"/v1/tenant/{args.tenant}/config")

def comparable_json(obj):
    j = json.dumps(obj, indent=' ', sort_keys=True)
    return textwrap.indent(j, '  ')

before = comparable_json(config)
print(f"BEFORE:\n{before}")

overrides = config['tenant_specific_overrides']

updated = {**overrides, **merge_existing_with}

client.put("/v1/tenant/config", json={**updated, "tenant_id": args.tenant})

new_config = client.get(f"/v1/tenant/{args.tenant}/config")
after = comparable_json(new_config)
print(f"AFTER:\n{after}")
