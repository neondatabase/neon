#!/bin/bash
set -x

cd /ext-src
FAILED=
LIST=$((echo ${SKIP} | sed 's/,/\n/g'; ls -d *-src) | sort | uniq -u)
for d in ${LIST}
do
       [ -d ${d} ] || continue
    psql -c "select 1" >/dev/null || break
       make -C ${d} installcheck || FAILED="${d} ${FAILED}"
done
[ -z "${FAILED}" ] && exit 0
echo ${FAILED}
exit 1