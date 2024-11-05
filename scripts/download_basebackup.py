#!/usr/bin/env python3
#
# Script to download the basebackup from a pageserver to a tar file.
#
# This can be useful in disaster recovery.
#

from __future__ import annotations

import argparse

import psycopg2
from psycopg2.extensions import connection as PgConnection


def main(args: argparse.Namespace):
    pageserver_connstr = args.pageserver_connstr
    tenant_id = args.tenant
    timeline_id = args.timeline
    lsn = args.lsn
    output_path = args.output_path

    psconn: PgConnection = psycopg2.connect(pageserver_connstr)
    psconn.autocommit = True

    with open(output_path, "wb", encoding="utf-8") as output, psconn.cursor() as pscur:
        pscur.copy_expert(f"basebackup {tenant_id} {timeline_id} {lsn}", output)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--tenant-id",
        dest="tenant",
        required=True,
        help="Id of the tenant",
    )
    parser.add_argument(
        "--timeline-id",
        dest="timeline",
        required=True,
        help="Id of the timeline",
    )
    parser.add_argument(
        "--lsn",
        dest="lsn",
        required=True,
        help="LSN to take the basebackup at",
    )
    parser.add_argument(
        "--pageserver-connstr",
        dest="pageserver_connstr",
        required=True,
        help="libpq connection string of the pageserver",
    )
    parser.add_argument(
        "--output",
        dest="output_path",
        required=True,
        help="output path to write the basebackup to",
    )
    args = parser.parse_args()
    main(args)
