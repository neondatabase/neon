EXTENSION = pgx_ulid

PGFILEDESC = "pgx_ulid - ULID type for PostgreSQL"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_MAJOR_VERSION := $(word 2, $(subst ., , $(shell $(PG_CONFIG) --version)))
ifeq ($(shell test $(PG_MAJOR_VERSION) -lt 17; echo $$?),0)
  REGRESS_OPTS = --load-extension=ulid
else
  REGRESS_OPTS = --load-extension=pgx_ulid
endif

# Test configuration
REGRESS = 00_ulid_generation 01_ulid_conversions 02_ulid_errors

include $(PGXS)
