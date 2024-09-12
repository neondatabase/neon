#!/usr/bin/env python3

import sys
import time

import requests

tenants_r = requests.get("http://localhost:9898/v1/tenant")
tenants_r.raise_for_status()
tenant = tenants_r.json()[0]
tenant_id = tenant["id"]

timelines_r = requests.get(f"http://localhost:9898/v1/tenant/{tenant_id}/timeline")
timelines_r.raise_for_status()
timeline = timelines_r.json()[0]
default_timeline_id = timeline["timeline_id"]

# print(f"Using MQ timeline {tenant_id}/{timeline_id}")

if sys.argv[1] == "consume":
    topic_name = sys.argv[2]
    offset = int(sys.argv[3])

    try:
        timeline_id = sys.argv[4]
    except IndexError:
        timeline_id = default_timeline_id

    while True:
        try:
            response = requests.get(
                f"http://localhost:9898/v1/tenant/{tenant_id}/timeline/{timeline_id}/event_consume/{topic_name}/{offset}"
            )
            if response.status_code == 200:
                print(response.content)
                stringized = "".join(map(chr, response.json()["payload"]))
                print(stringized)
                offset += 1
            elif response.status_code == 408:
                # Proceed to start another long-poll
                pass
            else:
                print(response.content)
                response.raise_for_status()
        except Exception as e:
            print(e)
            time.sleep(5)

elif sys.argv[1] == "produce":
    topic_name = sys.argv[2]
    data = sys.argv[3]

    try:
        timeline_id = sys.argv[4]
    except IndexError:
        timeline_id = default_timeline_id

    response = requests.post(
        f"http://localhost:9898/v1/tenant/{tenant_id}/timeline/{timeline_id}/event_produce/{topic_name}",
        data=data,
    )
    if response.status_code != 200:
        print(response.content)
        response.raise_for_status()
