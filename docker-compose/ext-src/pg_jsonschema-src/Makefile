EXTENSION = pg_jsonschema
DATA = pg_jsonschema--1.0.sql
REGRESS = jsonschema_valid_api  jsonschema_edge_cases
REGRESS_OPTS = --load-extension=pg_jsonschema

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
