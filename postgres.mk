# Sub-makefile for compiling PostgreSQL as part of Neon. This is
# included from the main Makefile, and is not meant to be called
# directly.
#
# CI workflows and Dockerfiles can take advantage of the following
# properties for caching:
#
# - Compiling the targets in this file only builds the PostgreSQL sources
#   under the vendor/ subdirectory, nothing else from the repository.
# - All outputs go to POSTGRES_INSTALL_DIR (by default 'pg_install',
#   see parent Makefile)
# - intermediate build artifacts go to BUILD_DIR
#
#
# Variables passed from the parent Makefile that control what gets
# installed and where:
# - POSTGRES_VERSIONS
# - POSTGRES_INSTALL_DIR
# - BUILD_DIR
#
# Variables passed from the parent Makefile that affect the build
# process and the resulting binaries:
# - PG_CONFIGURE_OPTS
# - PG_CFLAGS
# - PG_LDFLAGS
# - EXTRA_PATH_OVERRIDES

###
### Main targets
###
### These are called from the main Makefile, and can also be called
### directly from command line

# Compile and install a specific PostgreSQL version
postgres-install-%: postgres-configure-% \
		  postgres-headers-install-% # to prevent `make install` conflicts with neon's `postgres-headers`

# Install the PostgreSQL header files into $(POSTGRES_INSTALL_DIR)/<version>/include
#
# This is implicitly part of the 'postgres-install-%' target, but this can be handy
# if you want to install just the headers without building PostgreSQL, e.g. for building
# extensions.
postgres-headers-install-%: postgres-configure-%
	+@echo "Installing PostgreSQL $* headers"
	$(MAKE) -C $(BUILD_DIR)/$*/src/include MAKELEVEL=0 install

# Run Postgres regression tests
postgres-check-%: postgres-install-%
	$(MAKE) -C $(BUILD_DIR)/$* MAKELEVEL=0 check

###
### Shorthands for the main targets, for convenience
###

# Same as the above main targets, but for all supported PostgreSQL versions
# For example, 'make postgres-install' is equivalent to
# 'make postgres-install-v14 postgres-install-v15 postgres-install-v16 postgres-install-v17'
all_version_targets=postgres-install postgres-headers-install postgres-check
.PHONY: $(all_version_targets)
$(all_version_targets): postgres-%: $(foreach pg_version,$(POSTGRES_VERSIONS),postgres-%-$(pg_version))

.PHONY: postgres
postgres: postgres-install

.PHONY: postgres-headers
postgres-headers: postgres-headers-install

# 'postgres-v17' is an alias for 'postgres-install-v17' etc.
$(foreach pg_version,$(POSTGRES_VERSIONS),postgres-$(pg_version)): postgres-%: postgres-install-%

###
### Intermediate targets
###
### These are not intended to be called directly, but are dependencies for the
### main targets.

# Run 'configure'
$(BUILD_DIR)/%/config.status:
	mkdir -p $(BUILD_DIR)
	test -e $(BUILD_DIR)/CACHEDIR.TAG || echo "$(CACHEDIR_TAG_CONTENTS)" > $(BUILD_DIR)/CACHEDIR.TAG

	+@echo "Configuring Postgres $* build"
	@test -s $(ROOT_PROJECT_DIR)/vendor/postgres-$*/configure || { \
		echo "\nPostgres submodule not found in $(ROOT_PROJECT_DIR)/vendor/postgres-$*/, execute "; \
		echo "'git submodule update --init --recursive --depth 2 --progress .' in project root.\n"; \
		exit 1; }
	mkdir -p $(BUILD_DIR)/$*

	VERSION=$*; \
	EXTRA_VERSION=$$(cd $(ROOT_PROJECT_DIR)/vendor/postgres-$$VERSION && git rev-parse HEAD); \
	(cd $(BUILD_DIR)/$$VERSION && \
	env PATH="$(EXTRA_PATH_OVERRIDES):$$PATH" $(ROOT_PROJECT_DIR)/vendor/postgres-$$VERSION/configure \
		CFLAGS='$(PG_CFLAGS)' LDFLAGS='$(PG_LDFLAGS)' \
		$(PG_CONFIGURE_OPTS) --with-extra-version=" ($$EXTRA_VERSION)" \
		--prefix=$(abspath $(POSTGRES_INSTALL_DIR))/$$VERSION > configure.log)

# nicer alias to run 'configure'.
#
# This tries to accomplish this rule:
#
# postgres-configure-%: $(BUILD_DIR)/%/config.status
#
# XXX: I'm not sure why the above rule doesn't work directly. But this accomplishses
# the same thing
$(foreach pg_version,$(POSTGRES_VERSIONS),postgres-configure-$(pg_version)): postgres-configure-%: FORCE $(BUILD_DIR)/%/config.status

# Compile and install PostgreSQL (and a few contrib modules used in tests)
postgres-install-%: postgres-configure-% \
		  postgres-headers-install-% # to prevent `make install` conflicts with neon's `postgres-headers-install`
	+@echo "Compiling PostgreSQL $*"
	$(MAKE) -C $(BUILD_DIR)/$* MAKELEVEL=0 install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_prewarm install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_buffercache install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_visibility install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pageinspect install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_trgm install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/amcheck install
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/test_decoding install

.PHONY: FORCE
FORCE:
