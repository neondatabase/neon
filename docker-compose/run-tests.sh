#!/bin/bash
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
while getopts r arg; do
  case $arg in
  r)
    REGULAR_USER=true
    shift $((OPTIND-1))
    ;;
  *) :
    ;;
  esac
done

extdir=${1}

cd "${extdir}" || exit 2
FAILED=
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls) | sort | uniq -u)
for d in ${LIST}; do
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
[ -z "${FAILED}" ] && exit 0
for d in ${FAILED}; do
  cat "$(find $d -name regression.diffs)"
done
for postgis_diff in /tmp/pgis_reg/*_diff; do
  echo "${postgis_diff}:"
  cat "${postgis_diff}"
done
echo "${FAILED}"
exit 1
