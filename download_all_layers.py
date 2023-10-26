import requests

tenants = requests.get("http://localhost:15003/v1/tenant")
tenants.raise_for_status()
tenants = tenants.json()

for tenant in tenants:
    id = tenant["id"]
    timelines = requests.get(f"http://localhost:15003/v1/tenant/{id}/timeline")
    timelines.raise_for_status()
    for timeline in timelines.json():
        tid = timeline["tenant_id"]
        tlid = timeline["timeline_id"]
        layers = requests.get(f"http://localhost:15003/v1/tenant/{tid}/timeline/{tlid}/layer")
        layers.raise_for_status()
        layers = layers.json()
        for l in layers["historic_layers"]:
            if l["remote"] == False:
                requests.get(f"http://localhost:15003/v1/tenant/{tid}/timeline/{tlid}/layer/{l['layer_file_name']}")

