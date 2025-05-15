EXTENSION = pg_jsonschema
DATA = pg_jsonschema--1.0.sql
REGRESS = jsonschema_valid_api  jsonschema_edge_cases

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_REGRESS := $(dir $(PGXS))../../src/test/regress/pg_regress
.PHONY installcheck:
installcheck:
	dropdb --if-exists contrib_regression
	createdb contrib_regression
	psql -d contrib_regression -c "CREATE EXTENSION $(EXTENSION)"
	$(PG_REGRESS) --use-existing --dbname=contrib_regression $(REGRESS)
