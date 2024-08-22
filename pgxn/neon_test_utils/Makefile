# pgxs/neon_test_utils/Makefile


MODULE_big = neon_test_utils
OBJS = \
	$(WIN32RES) \
	neontest.o

EXTENSION = neon_test_utils
DATA = neon_test_utils--1.3.sql
PGFILEDESC = "neon_test_utils - helpers for neon testing and debugging"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
