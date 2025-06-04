#!/bin/bash
set -ex
cd "$(dirname "${0}")"
dropdb --if-exist contrib_regression
createdb contrib_regression
psql -d contrib_regression -c "ALTER DATABASE contrib_regression SET TimeZone='UTC'" \
     -c "ALTER DATABASE contrib_regression SET DateStyle='ISO, MDY'" \
     -c "CREATE EXTENSION postgis SCHEMA public" \
     -c "CREATE EXTENSION postgis_topology" \
     -c "CREATE EXTENSION postgis_tiger_geocoder CASCADE" \
     -c "CREATE EXTENSION postgis_raster SCHEMA public" \
     -c "CREATE EXTENSION postgis_sfcgal SCHEMA public"
patch -p1 <"postgis-common-${PG_VERSION}.patch"
patch -p1 <"postgis-regular-${PG_VERSION}.patch"
psql -d contrib_regression -f raster_outdb_template.sql
trap 'patch -R -p1 <postgis-regular-${PG_VERSION}.patch && patch -R -p1 <"postgis-common-${PG_VERSION}.patch"' EXIT
POSTGIS_REGRESS_DB=contrib_regression RUNTESTFLAGS=--nocreate make installcheck-base