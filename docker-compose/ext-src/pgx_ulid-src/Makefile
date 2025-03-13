EXTENSION = pgx_ulid

PGFILEDESC = "pgx_ulid - ULID type for PostgreSQL"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

# Test configuration
REGRESS = 00_ulid_generation 01_ulid_conversions 02_ulid_errors
REGRESS_OPTS = --inputdir=sql --outputdir=expected

include $(PGXS)
