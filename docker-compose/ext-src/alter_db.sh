#!/bin/bash
# We need these settings to get the expected output results.
# We cannot use the environment variables e.g. PGTZ due to
# https://github.com/neondatabase/neon/issues/1287
export DATABASE=${1:-contrib_regression}
psql -c "ALTER DATABASE ${DATABASE} SET neon.allow_unstable_extensions='on'" \
     -c "ALTER DATABASE ${DATABASE} SET DateStyle='Postgres,MDY'" \
     -c "ALTER DATABASE ${DATABASE} SET TimeZone='America/Los_Angeles'" \
