import csv
import logging
import sys
import textwrap
import requests
import argparse
import json


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

class AppException(RuntimeError):
    pass

def do_one(tenant, endpoint, merge_existing_with, check_tenant_exists=True):
    global verbose
    client = Client(endpoint)

    if check_tenant_exists:
        tenants = client.get(f"/v1/tenant")
        matching_tenant = [ t for t in tenants if t['id'] == tenant ]
        if len(matching_tenant) == 0:
            raise AppException(f"no tenant {tenant} on pageserver {endpoint}")
        elif len(matching_tenant) > 1:
            raise AppException(f"multiple ({len(matching_tenant)}) tenants with id {tenant} on pageserver {endpoint}")
        else:
            pass

    config = client.get(f"/v1/tenant/{tenant}/config")

    def comparable_json(obj):
        j = json.dumps(obj, indent=' ', sort_keys=True)
        return textwrap.indent(j, '  ')

    if verbose:
        before = comparable_json(config)
        print(f"BEFORE:\n{before}")

    overrides = config['tenant_specific_overrides']

    updated = {**overrides, **merge_existing_with}

    client.put("/v1/tenant/config", json={**updated, "tenant_id": tenant})
        

    if verbose:
        new_config = client.get(f"/v1/tenant/{tenant}/config")
        after = comparable_json(new_config)
        print(f"AFTER:\n{after}")

def do_csv(csv_file, merge_existing_with):
    succeeded = []
    failed = []
    for n, line in enumerate(csv.reader(csv_file)):
        if n == 0:
            # skip header row
            continue
        if len(line) != 2:
            logging.warn(f"skipping line {n+1}: {line}")
            continue
        tenant_id = line[0]
        pageserver = line[1]
        try:
            do_one(tenant_id, f"http://{pageserver}:9898", merge_existing_with, check_tenant_exists=False)
            logging.info(f"succeeded to configure tenant {tenant_id}")
            succeeded += [tenant_id]
        except Exception as e:
            logging.exception(f"failed to configure tenant {tenant_id}")
            failed += [tenant_id]

    print(json.dumps({
        "succeeded": succeeded,
        "failed": failed,
    }, indent=' ', sort_keys=True))

verbose = False

def main():
    global verbose
    
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    p = argparse.ArgumentParser()
    p.add_argument("--merge-existing-with", type=str)
    p.add_argument("--verbose", action='store_true')
    subcommands = p.add_subparsers(dest="subcommand")
    one_tenant_parser = subcommands.add_parser("one", help='change config of one tenant, specified via CLI flags')
    one_tenant_parser.add_argument("--tenant", required=True)
    one_tenant_parser.add_argument("--endpoint", type=str, default='http://localhost:9898')
    csv_parser = subcommands.add_parser("csv", help='batch reconfigure tenants specified in a csv file')
    csv_parser.add_argument('csv_file', type=argparse.FileType())
    args = p.parse_args()

    verbose = args.verbose

    merge_existing_with = {}
    if args.merge_existing_with is not None:
        merge_existing_with = json.loads(args.merge_existing_with)
        assert isinstance(merge_existing_with, dict)

    ({
        'one': lambda: do_one(args.tenant, args.endpoint, merge_existing_with),
        'csv': lambda: do_csv(args.csv_file, merge_existing_with),
    }[args.subcommand])()

if __name__ == '__main__':
    main()
