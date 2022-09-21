# pgxn/remotexact/Makefile

MODULE_big = remotexact
OBJS = \
	$(WIN32RES)\
	apply.o \
	remotexact.o \
	rwset.o \
	funcs.o 

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = remotexact
PGFILEDESC = "remotexact - remote transactions for multi-master PostgreSQL"
DATA = remotexact--1.0.sql


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
