# pgxs/neon/Makefile


MODULE_big = neon_rmgr
OBJS = \
	$(WIN32RES) \
	neon_rmgr.o \
	neon_rmgr_decode.o \
	neon_rmgr_desc.o


EXTENSION = neon_rmgr
DATA = 
PGFILEDESC = "Neon WAL Resource Manager - custom WAL records used to make Neon work (since PG 16)"


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
