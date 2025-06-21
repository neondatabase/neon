# This makefile assumes that 'pg_config' is in the path, or is passed in the
# PG_CONFIG variable.
#
# This is used in two different ways:
#
# 1. The main makefile calls this, when you invoke the `make neon-pg-ext-%`
#    target. It passes PG_CONFIG pointing to pg_install/%/bin/pg_config.
#    This is a VPATH build; the current directory is build/pgxn-%, and
#    the path to the Makefile is passed with the -f argument.
#
# 2. compute-node.Dockerfile invokes this to build the compute extensions
#    for the specific Postgres version. It relies on pg_config already
#    being in $(PATH).

srcdir = $(dir $(firstword $(MAKEFILE_LIST)))

PG_CONFIG = pg_config

subdirs = neon neon_rmgr neon_walredo neon_utils neon_test_utils

.PHONY: install install-compute install-storage $(subdirs)
install: $(subdirs)
install-compute: neon neon_utils neon_test_utils neon_rmgr
install-storage: neon_rmgr neon_walredo

$(subdirs): %:
	mkdir -p $*
	$(MAKE) PG_CONFIG=$(PG_CONFIG) -C $* -f $(abspath $(srcdir)/$@/Makefile) install
