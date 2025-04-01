#!/bin/bash
set -x

if [[ -v BENCHMARK_CONNSTR ]] && [[ ${BENCHMARK_CONNSTR} =~ ^postgres(ql)?:\/\/([^:]+):([^@]+)@([^:/]+):?([0-9]*)\/(.+)$ ]]; then
  export PGUSER="${BASH_REMATCH[2]}"
  export PGPASSWORD="${BASH_REMATCH[3]}"
  export PGHOST="${BASH_REMATCH[4]}"
  export PGPORT="${BASH_REMATCH[5]:-5432}"
  export PGDATABASE="${BASH_REMATCH[6]}"
fi

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
    if [ -f "${d}/neon-test.sh" ]; then
       "${d}/neon-test.sh" || FAILED="${d} ${FAILED}"
    else
       USE_PGXS=1 make -C "${d}" installcheck || FAILED="${d} ${FAILED}"
    fi
done
[ -z "${FAILED}" ] && exit 0
echo "${FAILED}"
exit 1
