import os
import shutil
import sys
import time
import json
from pathlib import Path

# Dry run is the default, i.e. not do anything
act = len(sys.argv) == 2 and sys.argv[1] == '--act'
print('act=', act)

move_to_deleted = True

# result of GET 'https://console.neon.tech/api/v1/admin/projects?order=asc&sort=id&show_deleted=true'
with open('projects.json') as f:
    projects = json.load(f)

projects = projects['data']
deleted_projects = [p for p in projects if p["deleted"]]
active_projects = [p for p in projects if not p["deleted"]]
print(f"deleted {len(deleted_projects)}, active {len(active_projects)}, total {len(projects)}")
# print(deleted_projects)

for project_to_delete in deleted_projects:
    tenant_id = project_to_delete['tenant']
    tenant_dir = Path(f"/storage/safekeeper/data/{tenant_id}")
    timeline_dir = tenant_dir / project_to_delete['timeline_id']
     
    if not os.path.exists(timeline_dir):
        continue

    if move_to_deleted:
        # move to deleted/
        tenant_dir_deleted = (Path(f"/storage/safekeeper/data/deleted/") / tenant_id)
        tenant_dir_deleted.mkdir(parents=True, exist_ok=True)
        print(f"moving {timeline_dir} to {tenant_dir_deleted}")
        if act:
            shutil.move(timeline_dir, tenant_dir_deleted)
    else:
        print(f"removing {timeline_dir}")
        if act:
            shutil.rmtree(timeline_dir)

    if len(os.listdir(tenant_dir)) == 0:
        print(f"removing empty tenant directory {tenant_dir}")
        shutil.rmtree(tenant_dir)