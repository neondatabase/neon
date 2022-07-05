#
# Simple script to export nodes from one pageserver
# and import them into another page server
#
from os import path
import os
import requests
import uuid
import subprocess
import argparse
from pathlib import Path

# directory to save exported tar files to
basepath = path.dirname(path.abspath(__file__))


class NeonPageserverApiException(Exception):
    pass


class NeonPageserverHttpClient(requests.Session):
    def __init__(self, host, port):
        super().__init__()
        self.host = host
        self.port = port

    def verbose_error(self, res: requests.Response):
        try:
            res.raise_for_status()
        except requests.RequestException as e:
            try:
                msg = res.json()['msg']
            except:
                msg = ''
            raise NeonPageserverApiException(msg) from e

    def check_status(self):
        self.get(f"http://{self.host}:{self.port}/v1/status").raise_for_status()

    def tenant_list(self):
        res = self.get(f"http://{self.host}:{self.port}/v1/tenant")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json

    def tenant_create(self, new_tenant_id: uuid.UUID, ok_if_exists):
        res = self.post(
            f"http://{self.host}:{self.port}/v1/tenant",
            json={
                'new_tenant_id': new_tenant_id.hex,
            },
        )

        if res.status_code == 409:
            if ok_if_exists:
                print(f'could not create tenant: already exists for id {new_tenant_id}')
            else:
                res.raise_for_status()
        elif res.status_code == 201:
            print(f'created tenant {new_tenant_id}')
        else:
            self.verbose_error(res)

        return new_tenant_id

    def timeline_list(self, tenant_id: uuid.UUID):
        res = self.get(f"http://{self.host}:{self.port}/v1/tenant/{tenant_id.hex}/timeline")
        self.verbose_error(res)
        res_json = res.json()
        assert isinstance(res_json, list)
        return res_json


def main(args: argparse.Namespace):
    old_pageserver_host = args.old_pageserver_host
    new_pageserver_host = args.new_pageserver_host
    tenants = args.tenants

    old_http_client = NeonPageserverHttpClient(old_pageserver_host, args.old_pageserver_http_port)
    old_http_client.check_status()
    old_pageserver_connstr = f"postgresql://{old_pageserver_host}:{args.old_pageserver_pg_port}"

    new_http_client = NeonPageserverHttpClient(new_pageserver_host, args.new_pageserver_http_port)
    new_http_client.check_status()
    new_pageserver_connstr = f"postgresql://{new_pageserver_host}:{args.new_pageserver_pg_port}"

    psql_env = {**os.environ, 'LD_LIBRARY_PATH': '/usr/local/lib/'}

    for tenant_id in tenants:
        print(f"Tenant: {tenant_id}")
        timelines = old_http_client.timeline_list(uuid.UUID(tenant_id))
        print(f"Timelines: {timelines}")

        # Create tenant in new pageserver
        if args.only_import is False:
            new_http_client.tenant_create(uuid.UUID(tenant_id), args.ok_if_exists)

        for timeline in timelines:

            # Export timelines from old pageserver
            if args.only_import is False:
                query = f"fullbackup {timeline['tenant_id']} {timeline['timeline_id']} {timeline['local']['last_record_lsn']}"

                cmd = ["psql", "--no-psqlrc", old_pageserver_connstr, "-c", query]
                print(f"Running: {cmd}")

                tar_filename = path.join(basepath,
                                         f"{timeline['tenant_id']}_{timeline['timeline_id']}.tar")
                stderr_filename = path.join(
                    basepath, f"{timeline['tenant_id']}_{timeline['timeline_id']}.stderr")

                with open(tar_filename, 'w') as stdout_f:
                    with open(stderr_filename, 'w') as stderr_f:
                        print(f"(capturing output to {tar_filename})")
                        subprocess.run(cmd, stdout=stdout_f, stderr=stderr_f, env=psql_env)

                print(f"Done export: {tar_filename}")

            # Import timelines to new pageserver
            psql_path = Path(args.psql_path)
            import_cmd = f"import basebackup {timeline['tenant_id']} {timeline['timeline_id']} {timeline['local']['last_record_lsn']} {timeline['local']['last_record_lsn']}"
            tar_filename = path.join(basepath,
                                     f"{timeline['tenant_id']}_{timeline['timeline_id']}.tar")
            full_cmd = rf"""cat {tar_filename} | {psql_path} {new_pageserver_connstr} -c '{import_cmd}' """

            stderr_filename2 = path.join(
                basepath, f"import_{timeline['tenant_id']}_{timeline['timeline_id']}.stderr")
            stdout_filename = path.join(
                basepath, f"import_{timeline['tenant_id']}_{timeline['timeline_id']}.stdout")

            print(f"Running: {full_cmd}")

            with open(stdout_filename, 'w') as stdout_f:
                with open(stderr_filename2, 'w') as stderr_f:
                    print(f"(capturing output to {stdout_filename})")
                    subprocess.run(full_cmd,
                                   stdout=stdout_f,
                                   stderr=stderr_f,
                                   env=psql_env,
                                   shell=True)

                    print(f"Done import")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--tenant-id',
        dest='tenants',
        required=True,
        nargs='+',
        help='Id of the tenant to migrate. You can pass multiple arguments',
    )
    parser.add_argument(
        '--from-host',
        dest='old_pageserver_host',
        required=True,
        help='Host of the pageserver to migrate data from',
    )
    parser.add_argument(
        '--from-http-port',
        dest='old_pageserver_http_port',
        required=False,
        type=int,
        default=9898,
        help='HTTP port of the pageserver to migrate data from. Default: 9898',
    )
    parser.add_argument(
        '--from-pg-port',
        dest='old_pageserver_pg_port',
        required=False,
        type=int,
        default=6400,
        help='pg port of the pageserver to migrate data from. Default: 6400',
    )
    parser.add_argument(
        '--to-host',
        dest='new_pageserver_host',
        required=True,
        help='Host of the pageserver to migrate data to',
    )
    parser.add_argument(
        '--to-http-port',
        dest='new_pageserver_http_port',
        required=False,
        default=9898,
        type=int,
        help='HTTP port of the pageserver to migrate data to. Default: 9898',
    )
    parser.add_argument(
        '--to-pg-port',
        dest='new_pageserver_pg_port',
        required=False,
        default=6400,
        type=int,
        help='pg port of the pageserver to migrate data to. Default: 6400',
    )
    parser.add_argument(
        '--ignore-tenant-exists',
        dest='ok_if_exists',
        required=False,
        help=
        'Ignore error if we are trying to create the tenant that already exists. It can be dangerous if existing tenant already contains some data.',
    )
    parser.add_argument(
        '--psql-path',
        dest='psql_path',
        required=False,
        default='/usr/local/bin/psql',
        help='Path to the psql binary. Default: /usr/local/bin/psql',
    )
    parser.add_argument(
        '--only-import',
        dest='only_import',
        required=False,
        default=False,
        action='store_true',
        help='Skip export and tenant creation part',
    )
    args = parser.parse_args()
    main(args)
