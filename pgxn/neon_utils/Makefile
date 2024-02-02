# pgxs/neon_utils/Makefile


MODULE_big = neon_utils
OBJS = \
	$(WIN32RES) \
	neon_utils.o

EXTENSION = neon_utils
DATA = neon_utils--1.0.sql
PGFILEDESC = "neon_utils - small useful functions"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
