# pgxs/neon/Makefile


MODULE_big = neon
OBJS = \
	$(WIN32RES) \
	file_cache.o \
	libpagestore.o \
	libpqwalproposer.o \
	neon.o \
	neon_lwlc.o \
	pagestore_smgr.o \
	relsize_cache.o \
	walproposer.o \
	walproposer_utils.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = neon
DATA = neon--1.0.sql
PGFILEDESC = "neon - cloud storage for PostgreSQL"


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
