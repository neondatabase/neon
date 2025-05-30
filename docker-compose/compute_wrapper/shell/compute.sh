#!/bin/bash
set -eux

# Generate a random tenant or timeline ID
#
# Takes a variable name as argument. The result is stored in that variable.
generate_id() {
    local -n resvar=$1
    printf -v resvar '%08x%08x%08x%08x' $SRANDOM $SRANDOM $SRANDOM $SRANDOM
}

PG_VERSION=${PG_VERSION:-14}

CONFIG_FILE_ORG=/var/db/postgres/configs/config.json
CONFIG_FILE=/tmp/config.json

# Test that the first library path that the dynamic loader looks in is the path
# that we use for custom compiled software
first_path="$(ldconfig --verbose 2>/dev/null \
    | grep --invert-match ^$'\t' \
    | cut --delimiter=: --fields=1 \
    | head --lines=1)"
test "$first_path" == '/usr/local/lib'

echo "Waiting pageserver become ready."
while ! nc -z pageserver 6400; do
     sleep 1;
done
echo "Page server is ready."

cp ${CONFIG_FILE_ORG} ${CONFIG_FILE}

 if [ -n "${TENANT_ID:-}" ] && [ -n "${TIMELINE_ID:-}" ]; then
   tenant_id=${TENANT_ID}
   timeline_id=${TIMELINE_ID}
else
  echo "Check if a tenant present"
  PARAMS=(
       -X GET
       -H "Content-Type: application/json"
       "http://pageserver:9898/v1/tenant"
  )
  tenant_id=$(curl "${PARAMS[@]}" | jq -r .[0].id)
  if [ -z "${tenant_id}" ] || [ "${tenant_id}" = null ]; then
    echo "Create a tenant"
    generate_id tenant_id
    PARAMS=(
         -X PUT
         -H "Content-Type: application/json"
         -d "{\"mode\": \"AttachedSingle\", \"generation\": 1, \"tenant_conf\": {}}"
        "http://pageserver:9898/v1/tenant/${tenant_id}/location_config"
    )
    result=$(curl "${PARAMS[@]}")
    echo $result | jq .
  fi

  echo "Check if a timeline present"
  PARAMS=(
       -X GET
       -H "Content-Type: application/json"
       "http://pageserver:9898/v1/tenant/${tenant_id}/timeline"
  )
  timeline_id=$(curl "${PARAMS[@]}" | jq -r .[0].timeline_id)
  if [ -z "${timeline_id}" ] || [ "${timeline_id}" = null ]; then
    generate_id timeline_id
    PARAMS=(
        -sbf
        -X POST
        -H "Content-Type: application/json"
        -d "{\"new_timeline_id\": \"${timeline_id}\", \"pg_version\": ${PG_VERSION}}"
        "http://pageserver:9898/v1/tenant/${tenant_id}/timeline/"
    )
    result=$(curl "${PARAMS[@]}")
    echo $result | jq .
  fi
fi

if [[ ${PG_VERSION} -ge 17 ]]; then
  ulid_extension=pgx_ulid
else
  ulid_extension=ulid
fi
echo "Adding pgx_ulid"
shared_libraries=$(jq -r '.spec.cluster.settings[] | select(.name=="shared_preload_libraries").value' ${CONFIG_FILE})
sed -i "s/${shared_libraries}/${shared_libraries},${ulid_extension}/" ${CONFIG_FILE}
echo "Overwrite tenant id and timeline id in spec file"
sed -i "s/TENANT_ID/${tenant_id}/" ${CONFIG_FILE}
sed -i "s/TIMELINE_ID/${timeline_id}/" ${CONFIG_FILE}

cat ${CONFIG_FILE}

echo "Start compute node"
/usr/local/bin/compute_ctl --pgdata /var/db/postgres/compute \
     -C "postgresql://cloud_admin@localhost:55433/postgres"  \
     -b /usr/local/bin/postgres                              \
     --compute-id "compute-$RANDOM"                          \
     --config "$CONFIG_FILE"
