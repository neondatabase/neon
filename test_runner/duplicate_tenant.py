# Usage from top of repo:
#  poetry run python3 test_runner/duplicate_tenant.py b97965931096047b2d54958756baee7b 10
from queue import Queue
import sys
import threading

import requests
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.types import TenantId

initial_tenant = sys.argv[1]
ncopies = int(sys.argv[2])
numthreads = int(sys.argv[3])


# class DuckTypedNeonEnv:
#     pass


# cli = NeonCli(DuckTypedNeonEnv())

q = Queue()
for i in range(0, ncopies):
    q.put(i)

for i in range(0, numthreads):
    q.put(None)


def create():
    while True:
        if q.get() == None:
            break
        new_tenant = TenantId.generate()
        res = requests.post(
            f"http://localhost:9898/v1/tenant/{initial_tenant}/duplicate",
            json={"new_tenant_id": str(new_tenant)},
        )
        res.raise_for_status()


for i in range(0, numthreads):
    threading.Thread(target=create).start()
