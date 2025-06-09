MODULE_big = lsm3
OBJS = lsm3.o
PGFILEDESC = "lsm3 - MVCC storage with undo log"

EXTENSION = lsm3
DATA = lsm3--1.0.sql

REGRESS = test
REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/lsm3/lsm3.conf


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
