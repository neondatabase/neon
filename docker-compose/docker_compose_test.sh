#!/usr/bin/env bash

# A basic test to ensure Docker images are built correctly.
# Build a wrapper around the compute, start all services and runs a simple SQL query.
# Repeats the process for all currenly supported Postgres versions.

# Implicitly accepts `REPOSITORY` and `TAG` env vars that are passed into the compose file
# Their defaults point at DockerHub `neondatabase/neon:latest` image.`,
# to verify custom image builds (e.g pre-published ones).
#
# A test script for postgres extensions
# Currently supports only v16+
#
set -eux -o pipefail

cd "$(dirname "${0}")"
export COMPOSE_FILE='docker-compose.yml'
export COMPOSE_PROFILES=test-extensions
export PARALLEL_COMPUTES=${PARALLEL_COMPUTES:-1}
READY_MESSAGE="All computes are started"
COMPUTES=()
for i in $(seq 1 "${PARALLEL_COMPUTES}"); do
  COMPUTES+=("compute${i}")
done
cp docker-compose.yml docker-compose.yml.bak
trap 'mv docker-compose.yml.bak docker-compose.yml' EXIT
if [[ ${PARALLEL_COMPUTES} -gt 1 ]]; then
  for i in $(seq 2 "${PARALLEL_COMPUTES}"); do
    yq -i ".services.compute${i} = ( .services.compute1 | (del .build) | (del .ports)) | .services.compute${i}.depends_on = [\"compute1\"]" docker-compose.yml
  done
fi
cd "$(dirname "${0}")"
PSQL_OPTION="-h localhost -U cloud_admin -p 55433 -d postgres"

function cleanup() {
    echo "show container information"
    docker ps
    echo "stop containers..."
    docker compose down
}

for pg_version in ${TEST_VERSION_ONLY-14 15 16 17}; do
    pg_version=${pg_version/v/}
    echo "clean up containers if exist"
    cleanup
    PG_TEST_VERSION=$((pg_version < 16 ? 16 : pg_version))
    PG_VERSION=${pg_version} PG_TEST_VERSION=${PG_TEST_VERSION} docker compose up --quiet-pull --build -d
    echo "wait until the compute is ready. timeout after 60s. "
    cnt=0
    while sleep 3; do
        # check timeout
        (( cnt += 3 ))
        if [[ ${cnt} -gt 60 ]]; then
            echo "timeout before the compute is ready."
            exit 1
        fi
        if docker compose logs compute_is_ready | grep -q "${READY_MESSAGE}"; then
            echo "OK. The compute is ready to connect."
            echo "execute simple queries."
            for compute in "${COMPUTES[@]}"; do
              docker compose exec "${compute}" /bin/bash -c "psql ${PSQL_OPTION} -c 'SELECT 1'"
            done
            break
        fi
    done

    if [[ ${pg_version} -ge 16 ]]; then
        TMPDIR=$(mktemp -d)
        trap 'rm -rf ${TMPDIR}' EXIT
        mkdir "${TMPDIR}"/{pg_hint_plan-src,file_fdw,postgis-src}
        docker compose cp neon-test-extensions:/ext-src/postgis-src/raster/test "${TMPDIR}/postgis-src/test"
        docker compose cp neon-test-extensions:/ext-src/postgis-src/regress/00-regress-install "${TMPDIR}/postgis-src/00-regress-install"
        docker compose cp neon-test-extensions:/ext-src/pg_hint_plan-src/data "${TMPDIR}/pg_hint_plan-src/data"
        docker compose cp neon-test-extensions:/postgres/contrib/file_fdw/data "${TMPDIR}/file_fdw/data"

        for compute in "${COMPUTES[@]}"; do
          # This is required for the pg_hint_plan test, to prevent flaky log message causing the test to fail
          # It cannot be moved to Dockerfile now because the database directory is created after the start of the container
          echo Adding dummy config on "${compute}"
          docker compose exec "${compute}" touch /var/db/postgres/compute/compute_ctl_temp_override.conf
          # Prepare for the PostGIS test
          docker compose exec "${compute}" mkdir -p /tmp/pgis_reg/pgis_reg_tmp /ext-src/postgis-src/raster /ext-src/postgis-src/regress /ext-src/postgis-src/regress/00-regress-install
          docker compose cp "${TMPDIR}/postgis-src/test" "${compute}":/ext-src/postgis-src/raster/test
          docker compose cp "${TMPDIR}/postgis-src/00-regress-install" "${compute}":/ext-src/postgis-src/regress
          # The following block copies the files for the pg_hintplan test to the compute node for the extension test in an isolated docker-compose environment
          docker compose cp "${TMPDIR}/pg_hint_plan-src/data" "${compute}":/ext-src/pg_hint_plan-src/
          # The following block does the same for the contrib/file_fdw test
          docker compose cp "${TMPDIR}/file_fdw/data" "${compute}":/postgres/contrib/file_fdw/data
        done
        # Apply patches
        docker compose exec -T neon-test-extensions bash -c "(cd /postgres && patch -p1)" <"../compute/patches/contrib_pg${pg_version}.patch"
        # We are running tests now
        rm -f testout.txt testout_contrib.txt
        # We want to run the longest tests first to better utilize parallelization and reduce overall test time.
        # Tests listed in the RUN_FIRST variable will be run before others.
        # If parallelization is not used, this environment variable will be ignored.

        docker compose exec -e USE_PGXS=1 -e SKIP=timescaledb-src,rdkit-src,pg_jsonschema-src,kq_imcx-src,wal2json_2_5-src,rag_jina_reranker_v1_tiny_en-src,rag_bge_small_en_v15-src \
        -e RUN_FIRST=hll-src,postgis-src,pgtap-src -e PARALLEL_COMPUTES="${PARALLEL_COMPUTES}" \
        neon-test-extensions /run-tests.sh /ext-src | tee testout.txt && EXT_SUCCESS=1 || EXT_SUCCESS=0
        docker compose exec -e SKIP=start-scripts,postgres_fdw,ltree_plpython,jsonb_plpython,jsonb_plperl,hstore_plpython,hstore_plperl,dblink,bool_plperl \
        -e PARALLEL_COMPUTES="${PARALLEL_COMPUTES}" \
        neon-test-extensions /run-tests.sh /postgres/contrib | tee testout_contrib.txt && CONTRIB_SUCCESS=1 || CONTRIB_SUCCESS=0
        if [[ ${EXT_SUCCESS} -eq 0 || ${CONTRIB_SUCCESS} -eq 0 ]]; then
            CONTRIB_FAILED=
            FAILED=
            [[ ${EXT_SUCCESS} -eq 0 ]] && FAILED=$(tail -1 testout.txt | awk '{for(i=1;i<=NF;i++){print "/ext-src/"$i;}}')
            [[ ${CONTRIB_SUCCESS} -eq 0 ]] && CONTRIB_FAILED=$(tail -1 testout_contrib.txt | awk '{for(i=0;i<=NF;i++){print "/postgres/contrib/"$i;}}')
            for d in ${FAILED} ${CONTRIB_FAILED}; do
                docker compose exec neon-test-extensions bash -c 'for file in $(find '"${d}"' -name regression.diffs -o -name regression.out); do cat ${file}; done' || [[ ${?} -eq 1 ]]
            done
        exit 1
        fi
    fi
done
