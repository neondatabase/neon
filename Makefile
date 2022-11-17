ROOT_PROJECT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Where to install Postgres, default is ./pg_install, maybe useful for package managers
POSTGRES_INSTALL_DIR ?= $(ROOT_PROJECT_DIR)/pg_install/

#
# We differentiate between release / debug build types using the BUILD_TYPE
# environment variable.
#
BUILD_TYPE ?= debug
ifeq ($(BUILD_TYPE),release)
	PG_CONFIGURE_OPTS = --enable-debug --with-openssl
	PG_CFLAGS = -O2 -g3 $(CFLAGS)
	# Unfortunately, `--profile=...` is a nightly feature
	CARGO_BUILD_FLAGS += --release
else ifeq ($(BUILD_TYPE),debug)
	PG_CONFIGURE_OPTS = --enable-debug --with-openssl --enable-cassert --enable-depend
	PG_CFLAGS = -O0 -g3 $(CFLAGS)
else
	$(error Bad build type '$(BUILD_TYPE)', see Makefile for options)
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	# Seccomp BPF is only available for Linux
	PG_CONFIGURE_OPTS += --with-libseccomp
else ifeq ($(UNAME_S),Darwin)
	# macOS with brew-installed openssl requires explicit paths
	# It can be configured with OPENSSL_PREFIX variable
	OPENSSL_PREFIX ?= $(shell brew --prefix openssl@3)
	PG_CONFIGURE_OPTS += --with-includes=$(OPENSSL_PREFIX)/include --with-libraries=$(OPENSSL_PREFIX)/lib
	# macOS already has bison and flex in the system, but they are old and result in postgres-v14 target failure
	# brew formulae are keg-only and not symlinked into HOMEBREW_PREFIX, force their usage
	EXTRA_PATH_OVERRIDES += $(shell brew --prefix bison)/bin/:$(shell brew --prefix flex)/bin/:
endif

# Use -C option so that when PostgreSQL "make install" installs the
# headers, the mtime of the headers are not changed when there have
# been no changes to the files. Changing the mtime triggers an
# unnecessary rebuild of 'postgres_ffi'.
PG_CONFIGURE_OPTS += INSTALL='$(ROOT_PROJECT_DIR)/scripts/ninstall.sh -C'

# Choose whether we should be silent or verbose
CARGO_BUILD_FLAGS += --$(if $(filter s,$(MAKEFLAGS)),quiet,verbose)
# Fix for a corner case when make doesn't pass a jobserver
CARGO_BUILD_FLAGS += $(filter -j1,$(MAKEFLAGS))

# This option has a side effect of passing make jobserver to cargo.
# However, we shouldn't do this if `make -n` (--dry-run) has been asked.
CARGO_CMD_PREFIX += $(if $(filter n,$(MAKEFLAGS)),,+)
# Force cargo not to print progress bar
CARGO_CMD_PREFIX += CARGO_TERM_PROGRESS_WHEN=never CI=1

#
# Top level Makefile to build Neon and PostgreSQL
#
.PHONY: all
all: neon postgres neon-pg-ext

### Neon Rust bits
#
# The 'postgres_ffi' depends on the Postgres headers.
.PHONY: neon
neon: postgres-v14-headers postgres-v15-headers
	+@echo "Compiling Neon"
	$(CARGO_CMD_PREFIX) cargo build $(CARGO_BUILD_FLAGS)

### PostgreSQL parts
# The rules are duplicated for Postgres v14 and 15. We may want to refactor
# to avoid the duplication in the future, but it's tolerable for now.
#
$(POSTGRES_INSTALL_DIR)/build/v14/config.status:
	+@echo "Configuring Postgres v14 build"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/v14
	(cd $(POSTGRES_INSTALL_DIR)/build/v14 && \
	env PATH="$(EXTRA_PATH_OVERRIDES):$$PATH" $(ROOT_PROJECT_DIR)/vendor/postgres-v14/configure \
		CFLAGS='$(PG_CFLAGS)' \
		$(PG_CONFIGURE_OPTS) \
		--prefix=$(abspath $(POSTGRES_INSTALL_DIR))/v14 > configure.log)

$(POSTGRES_INSTALL_DIR)/build/v15/config.status:
	+@echo "Configuring Postgres v15 build"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/v15
	(cd $(POSTGRES_INSTALL_DIR)/build/v15 && \
	env PATH="$(EXTRA_PATH_OVERRIDES):$$PATH" $(ROOT_PROJECT_DIR)/vendor/postgres-v15/configure \
		CFLAGS='$(PG_CFLAGS)' \
		$(PG_CONFIGURE_OPTS) \
		--prefix=$(abspath $(POSTGRES_INSTALL_DIR))/v15 > configure.log)

# nicer alias to run 'configure'
.PHONY: postgres-v14-configure
postgres-v14-configure: $(POSTGRES_INSTALL_DIR)/build/v14/config.status

.PHONY: postgres-v15-configure
postgres-v15-configure: $(POSTGRES_INSTALL_DIR)/build/v15/config.status

# Install the PostgreSQL header files into $(POSTGRES_INSTALL_DIR)/<version>/include
.PHONY: postgres-v14-headers
postgres-v14-headers: postgres-v14-configure
	+@echo "Installing PostgreSQL v14 headers"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/src/include MAKELEVEL=0 install

.PHONY: postgres-v15-headers
postgres-v15-headers: postgres-v15-configure
	+@echo "Installing PostgreSQL v15 headers"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/src/include MAKELEVEL=0 install

# Compile and install PostgreSQL
.PHONY: postgres-v14
postgres-v14: postgres-v14-configure \
		  postgres-v14-headers # to prevent `make install` conflicts with neon's `postgres-headers`
	+@echo "Compiling PostgreSQL v14"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14 MAKELEVEL=0 install
	+@echo "Compiling libpq v14"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/src/interfaces/libpq install
	+@echo "Compiling pg_buffercache v14"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/pg_buffercache install
	+@echo "Compiling pageinspect v14"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/pageinspect install

.PHONY: postgres-v15
postgres-v15: postgres-v15-configure \
		  postgres-v15-headers # to prevent `make install` conflicts with neon's `postgres-headers`
	+@echo "Compiling PostgreSQL v15"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15 MAKELEVEL=0 install
	+@echo "Compiling libpq v15"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/src/interfaces/libpq install
	+@echo "Compiling pg_buffercache v15"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/contrib/pg_buffercache install
	+@echo "Compiling pageinspect v15"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/contrib/pageinspect install

# shorthand to build all Postgres versions
postgres: postgres-v14 postgres-v15

.PHONY: postgres-v14-clean
postgres-v14-clean:
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14 MAKELEVEL=0 clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/pg_buffercache clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/pageinspect clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/src/interfaces/libpq clean

.PHONY: postgres-v15-clean
postgres-v15-clean:
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15 MAKELEVEL=0 clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/contrib/pg_buffercache clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/contrib/pageinspect clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v15/src/interfaces/libpq clean

neon-pg-ext-v14: postgres-v14
	+@echo "Compiling neon v14"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-v14
	(cd $(POSTGRES_INSTALL_DIR)/build/neon-v14 && \
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v14/bin/pg_config CFLAGS='$(PG_CFLAGS) $(COPT)' \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon/Makefile install)
	+@echo "Compiling neon_walredo v14"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-walredo-v14
	(cd $(POSTGRES_INSTALL_DIR)/build/neon-walredo-v14 && \
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v14/bin/pg_config CFLAGS='$(PG_CFLAGS) $(COPT)' \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon_walredo/Makefile install)
	+@echo "Compiling neon_test_utils" v14
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-test-utils-v14
	(cd $(POSTGRES_INSTALL_DIR)/build/neon-test-utils-v14 && \
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v14/bin/pg_config CFLAGS='$(PG_CFLAGS) $(COPT)' \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon_test_utils/Makefile install)

neon-pg-ext-v15: postgres-v15
	+@echo "Compiling neon v15"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-v15
	(cd $(POSTGRES_INSTALL_DIR)/build/neon-v15 && \
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v15/bin/pg_config CFLAGS='$(PG_CFLAGS) $(COPT)' \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon/Makefile install)
	+@echo "Compiling neon_walredo v15"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-walredo-v15
	(cd $(POSTGRES_INSTALL_DIR)/build/neon-walredo-v15 && \
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v15/bin/pg_config CFLAGS='$(PG_CFLAGS) $(COPT)' \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon_walredo/Makefile install)
	+@echo "Compiling neon_test_utils" v15
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-test-utils-v15
	(cd $(POSTGRES_INSTALL_DIR)/build/neon-test-utils-v15 && \
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v15/bin/pg_config CFLAGS='$(PG_CFLAGS) $(COPT)' \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon_test_utils/Makefile install)

.PHONY: neon-pg-ext-clean
	$(MAKE) -C $(ROOT_PROJECT_DIR)/pgxn/neon clean
	$(MAKE) -C $(ROOT_PROJECT_DIR)/pgxn/neon_test_utils clean

neon-pg-ext: neon-pg-ext-v14 neon-pg-ext-v15
postgres-headers: postgres-v14-headers postgres-v15-headers
postgres-clean: postgres-v14-clean postgres-v15-clean

# This doesn't remove the effects of 'configure'.
.PHONY: clean
clean:
	cd $(POSTGRES_INSTALL_DIR)/build/v14 && $(MAKE) clean
	cd $(POSTGRES_INSTALL_DIR)/build/v15 && $(MAKE) clean
	$(CARGO_CMD_PREFIX) cargo clean
	cd pgxn/neon && $(MAKE) clean
	cd pgxn/neon_test_utils && $(MAKE) clean

# This removes everything
.PHONY: distclean
distclean:
	rm -rf $(POSTGRES_INSTALL_DIR)
	$(CARGO_CMD_PREFIX) cargo clean

.PHONY: fmt
fmt:
	./pre-commit.py --fix-inplace

.PHONY: setup-pre-commit-hook
setup-pre-commit-hook:
	ln -s -f $(ROOT_PROJECT_DIR)/pre-commit.py .git/hooks/pre-commit
