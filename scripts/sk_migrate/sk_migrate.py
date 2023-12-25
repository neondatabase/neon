import argparse
import sys
import psycopg2
import psycopg2.extras
import os
import requests

def migrate_project(conn, from_sk: dict[str, any], to_sk: dict[str, any], project_id: str, dry_run=True):
    print("###############################################################")

    with conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
        project = cur.fetchone()

    if project is None:
        print("Project with id {} does not exist".format(project_id))
        return
    
    assert project['deleted'] == False, "Project with id {} is deleted".format(project_id)

    with conn.cursor() as cur:
        cur.execute("SELECT safekeeper_id FROM projects_safekeepers WHERE project_id = %s", (project_id, ))
        sk_ids = list(map(lambda x: x[0], cur.fetchall()))
        assert from_sk['id'] in sk_ids
        assert to_sk['id'] not in sk_ids

    with conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM branches WHERE project_id = %s AND deleted = 'f'", (project_id, ))
        branches = cur.fetchall()

    for branch in branches:
        if branch['deleted'] != False:
            continue

        tenant_id = project['tenant_id']
        timeline_id = branch['timeline_id']
        print("tenant_id: {}, timeline_id: {}".format(tenant_id, timeline_id))
        print(f"Migrating from {from_sk['host']} to {to_sk['host']}, project={project_id}, branch={branch['id']}, deleted={branch['deleted']}")

        print(list(sk_ids))

        sk_hosts = list(map(
            lambda x: f"http://{safekeepers[x]['host']}:{safekeepers[x]['http_port']}",
            filter(lambda x: x != from_sk['id'], sk_ids)
        ))

        # make HTTP request to /pull_timeline
        # url = f"http://{to_sk['host']}:{to_sk['http_port']}/v1/tenant/{tenant_id}/timeline/{timeline_id}"
        url = f"http://{to_sk['host']}:{to_sk['http_port']}/v1/pull_timeline"
        body = {
            "tenant_id": str(tenant_id),
            "timeline_id": str(timeline_id),
            "http_hosts": sk_hosts,
        }
        print(body)

        print("Making HTTP request to {}".format(url), flush=True)
        if not dry_run:
            response = requests.post(url, json=body)
        # response = requests.get(url)

            if response.status_code != 200 and f"error decoding response body: missing field `tenant_id` at line 1 column 104" in response.text:
                print(f"WARN: Skipping branch {branch['id']} because it's empty on all safekeepers")
                continue

            if response.status_code != 200 and f"Timeline {timeline_id} already exists" in response.text:
                print(f"WARN: Skipping timeline {timeline_id} because it is already exists (was migrated earlier)")
                continue

            if response.status_code != 200:
                print("ERROR: {}".format(response.text))
                return
            print(response.text)

    print(f"Updating safekeeper {from_sk['id']} -> {to_sk['id']} for project={project_id} in the database")
    if not dry_run:
        with conn.cursor() as cur:
            cur.execute("UPDATE projects_safekeepers SET safekeeper_id = %s WHERE project_id = %s AND safekeeper_id = %s RETURNING *", (to_sk['id'], project_id, from_sk['id']))
            print(cur.fetchone())
            conn.commit()

def find_projects(sk_from_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT p.id FROM projects p, projects_safekeepers ps WHERE ps.project_id = p.id AND NOT p.deleted AND ps.safekeeper_id = %s", (sk_from_id, ))
        project_ids = list(map(lambda x: x[0], cur.fetchall()))
        return project_ids


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='migrate sk')
    parser.add_argument("-d", help="database URL", type=str, required=True)
    parser.add_argument("--from-sk", help="from sk id as in the cplane db", type=int, required=True)
    parser.add_argument("--to-sk", help="to sk id as in the cplane db", type=int, required=True)
    parser.add_argument("--not-dry-run", help="", action='store_true')
    parser.add_argument("--project-id", help="project to migrate", type=str, default=None)
    args = parser.parse_args()

    # Connect to postgresql database
    conn = psycopg2.connect(args.d)

    safekeepers = dict()

    # We need to fetch all objects from "safekeepers" table and store them in "safekeepers" list
    # Create cursor
    cur = conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    # Execute query
    cur.execute("SELECT * FROM safekeepers")
    # Fetch all rows
    rows = cur.fetchall()
    # Close cursor
    cur.close()

    # Iterate over rows
    for row in rows:
        safekeepers[row['id']] = row

    # Print all safekeepers
    # print(safekeepers)

    assert args.from_sk in safekeepers, "Safekeeper with id {} does not exist".format(args.from_sk)
    from_sk_hostname = safekeepers[args.from_sk]['host']
    assert safekeepers[args.from_sk]['active'] == False, "Safekeeper with id {} should be inactive".format(args.from_sk)

    assert args.to_sk in safekeepers, "Safekeeper with id {} does not exist".format(args.to_sk)
    to_sk_hostname = safekeepers[args.to_sk]['host']
    assert safekeepers[args.to_sk]['active'] == True, "Safekeeper with id {} should be active".format(args.to_sk)

    print(f"migrating from id {args.from_sk} {from_sk_hostname} to {args.to_sk} {to_sk_hostname}")

    if args.project_id is not None:
        project_ids = [args.project_id]
    else:
        project_ids = find_projects(args.from_sk)
    print(project_ids)

    for project_id in project_ids:
        migrate_project(conn, safekeepers[args.from_sk], safekeepers[args.to_sk], project_id)