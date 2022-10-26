#!/bin/bash
set -eux

PG_VERSION=${PG_VERSION:-14}

SPEC_FILE_ORG=/var/db/postgres/specs/spec.json
SPEC_FILE=/tmp/spec.json

echo "Waiting pageserver become ready."
while ! nc -z pageserver 6400; do
     sleep 1;
done
echo "Page server is ready."

echo "Create a tenant and timeline"
PARAMS=(
     -sb 
     -X POST
     -H "Content-Type: application/json"
     -d "{}"
     http://pageserver:9898/v1/tenant/
)
tenant_id=$(curl "${PARAMS[@]}" | sed 's/"//g')

PARAMS=(
     -sb 
     -X POST
     -H "Content-Type: application/json"
     -d "{\"tenant_id\":\"${tenant_id}\", \"pg_version\": ${PG_VERSION}}"
     "http://pageserver:9898/v1/tenant/${tenant_id}/timeline/"
)
result=$(curl "${PARAMS[@]}")
echo $result | jq .

echo "Overwrite tenant id and timeline id in spec file"
tenant_id=$(echo ${result} | jq -r .tenant_id)
timeline_id=$(echo ${result} | jq -r .timeline_id)

sed "s/TENANT_ID/${tenant_id}/" ${SPEC_FILE_ORG} > ${SPEC_FILE}
sed -i "s/TIMELINE_ID/${timeline_id}/" ${SPEC_FILE}

cat ${SPEC_FILE}

echo "Start compute node"
/usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute \
     -C "postgresql://cloud_admin@localhost:55433/postgres"  \
     -b /usr/local/bin/postgres                              \
     -S ${SPEC_FILE}
