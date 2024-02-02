# pgxs/neon_walredo/Makefile

MODULE_big = neon_walredo
OBJS = \
	$(WIN32RES) \
	inmem_smgr.o \
	walredoproc.o \

# This really should be guarded by $(with_libseccomp), but I couldn't
# make that work with pgxs. So we always compile it, but its contents
# are wrapped in #ifdef HAVE_LIBSECCOMP instead.
OBJS += seccomp.o

PGFILEDESC = "neon_walredo - helper process that runs in Neon pageserver"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifeq ($(with_libseccomp),yes)
SHLIB_LINK += -lseccomp
endif
