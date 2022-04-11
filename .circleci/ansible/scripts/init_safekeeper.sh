#!/bin/sh

# get instance id from meta-data service
INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)

# store fqdn hostname in var
HOST=$(hostname -f)


cat <<EOF | tee /tmp/payload
{
  "version": 1,
  "host": "${HOST}",
  "port": 6500,
  "region_id": {{ console_region_id }},
  "instance_id": "${INSTANCE_ID}",
  "http_host": "${HOST}",
  "http_port": 7676
}
EOF

# check if safekeeper already registered or not
if ! curl -sf -X PATCH -d '{}' {{ console_mgmt_base_url }}/api/v1/safekeepers/${INSTANCE_ID} -o /dev/null; then

    # not registered, so register it now
    ID=$(curl -sf -X POST {{ console_mgmt_base_url }}/api/v1/safekeepers -d@/tmp/payload | jq -r '.ID')

    # init safekeeper
    sudo -u safekeeper /usr/local/bin/safekeeper --id ${ID} --init -D /storage/safekeeper/data
fi
