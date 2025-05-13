#!/bin/bash
export DATABASE=${1:-contrib_regression}
psql -c "ALTER DATABASE ${DATABASE} SET neon.allow_unstable_extensions='on'" \
     -c "ALTER DATABASE ${DATABASE} SET DateStyle='Postgres,MDY'" \
     -c "ALTER DATABASE ${DATABASE} SET TimeZone='America/Los_Angeles'" \
