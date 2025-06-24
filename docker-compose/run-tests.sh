#!/usr/bin/env bash
set -x

if [[ -v BENCHMARK_CONNSTR ]]; then
  uri_no_proto="${BENCHMARK_CONNSTR#postgres://}"
  uri_no_proto="${uri_no_proto#postgresql://}"
  if [[ $uri_no_proto == *\?* ]]; then
    base="${uri_no_proto%%\?*}"       # before '?'
  else
    base="$uri_no_proto"
  fi
  if [[ $base =~ ^([^:]+):([^@]+)@([^:/]+):?([0-9]*)/(.+)$ ]]; then
    export PGUSER="${BASH_REMATCH[1]}"
    export PGPASSWORD="${BASH_REMATCH[2]}"
    export PGHOST="${BASH_REMATCH[3]}"
    export PGPORT="${BASH_REMATCH[4]:-5432}"
    export PGDATABASE="${BASH_REMATCH[5]}"
    echo export PGUSER="${BASH_REMATCH[1]}"
    echo export PGPASSWORD="${BASH_REMATCH[2]}"
    echo export PGHOST="${BASH_REMATCH[3]}"
    echo export PGPORT="${BASH_REMATCH[4]:-5432}"
    echo export PGDATABASE="${BASH_REMATCH[5]}"
  else
    echo "Invalid PostgreSQL base URI"
    exit 1
  fi
fi
REGULAR_USER=false
RUN_PARALLEL=false
while getopts pr arg; do
  case ${arg} in
  r)
    REGULAR_USER=true
    shift $((OPTIND-1))
    ;;
  p)
    RUN_PARALLEL=true
    shift $((OPTIND-1))
    ;;
  *) :
    ;;
  esac
done

extdir=${1}

cd "${extdir}" || exit 2
FAILED=
export FAILED_FILE=/tmp/failed
rm -f ${FAILED_FILE}
mapfile -t LIST < <( (echo -e "${SKIP//","/"\n"}"; ls) | sort | uniq -u)
if [[ ${RUN_PARALLEL} = true ]]; then
  # Avoid errors if RUN_FIRST is not defined
  RUN_FIRST=${RUN_FIRST:-}
  # Move entries listed in the RUN_FIRST variable to the beginning
  ORDERED_LIST=$(printf "%s\n" "${LIST[@]}" | grep -x -Ff <(echo -e "${RUN_FIRST//,/$'\n'}"); printf "%s\n" "${LIST[@]}" | grep -vx -Ff <(echo -e "${RUN_FIRST//,/$'\n'}"))
  parallel -j3 "[[ -d {} ]] || exit 0
                export PGHOST=pcompute{%}
                if ! psql -c 'select 1'>/dev/null; then
                  exit 1
                fi
                echo Running on \${PGHOST}
                if [[ -f ${extdir}/{}/neon-test.sh ]]; then
                  echo Running from script
                  ${extdir}/{}/neon-test.sh || echo {} >> ${FAILED_FILE};
                else
                  echo Running using make;
                  USE_PGXS=1 make -C {} installcheck || echo {} >> ${FAILED_FILE};
                fi" ::: ${ORDERED_LIST}
  [[ ! -f ${FAILED_FILE} ]] && exit 0
else
  for d in "${LIST[@]}"; do
      [ -d "${d}" ] || continue
      if ! psql -w -c "select 1" >/dev/null; then
        FAILED="${d} ${FAILED}"
        break
      fi
      if [[ ${REGULAR_USER} = true ]] && [ -f "${d}"/regular-test.sh ]; then
        "${d}/regular-test.sh" || FAILED="${d} ${FAILED}"
        continue
      fi

      if [ -f "${d}/neon-test.sh" ]; then
        "${d}/neon-test.sh" || FAILED="${d} ${FAILED}"
      else
        USE_PGXS=1 make -C "${d}" installcheck || FAILED="${d} ${FAILED}"
      fi
  done
  [[ -z ${FAILED} ]]  && exit 0
fi
for d in ${FAILED} $([[ ! -f ${FAILED_FILE} ]] || cat ${FAILED_FILE}); do
  cat "$(find $d -name regression.diffs)"
done
for postgis_diff in /tmp/pgis_reg/*_diff; do
  echo "${postgis_diff}:"
  cat "${postgis_diff}"
done
echo "${FAILED}"
cat ${FAILED_FILE}
exit 1
