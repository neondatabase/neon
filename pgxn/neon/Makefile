# pgxs/neon/Makefile


MODULE_big = neon
OBJS = \
	$(WIN32RES) \
	file_cache.o \
	libpagestore.o \
	libpqwalproposer.o \
	neon.o \
	pagestore_smgr.o \
	relsize_cache.o \
	walproposer.o \
	walproposer_utils.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq)
PG_LIBS_INTERNAL = $(libpq)
SHLIB_LINK_INTERNAL = $(libpq)
EXTENSION = neon
DATA = neon--1.0.sql
PGFILEDESC = "neon - cloud storage for PostgreSQL"

# PROGRAM = boop

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
