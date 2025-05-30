EXTENSION = pgx_ulid

PGFILEDESC = "pgx_ulid - ULID type for PostgreSQL"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_REGRESS = $(dir $(PGXS))/../../src/test/regress/pg_regress
PG_MAJOR_VERSION := $(word 2, $(subst ., , $(shell $(PG_CONFIG) --version)))
ifeq ($(shell test $(PG_MAJOR_VERSION) -lt 17; echo $$?),0)
  REGRESS = 00_ulid_generation 01_ulid_conversions 03_ulid_errors
  EXTNAME = ulid
else
  REGRESS = 00_ulid_generation 01_ulid_conversions 02_ulid_conversions 03_ulid_errors
  EXTNAME = pgx_ulid
endif

.PHONY: installcheck
installcheck: regression-test

regression-test:
	dropdb --if-exists contrib_regression
	createdb contrib_regression
	../alter_db.sh
	psql -d contrib_regression -c "CREATE EXTENSION $(EXTNAME)"
	$(PG_REGRESS) --inputdir=. --outputdir=. --use-existing --dbname=contrib_regression $(REGRESS)
