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

# Seccomp BPF is only available for Linux
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	PG_CONFIGURE_OPTS += --with-libseccomp
endif


# macOS with brew-installed openssl requires explicit paths
# It can be configured with OPENSSL_PREFIX variable
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    OPENSSL_PREFIX ?= $(shell brew --prefix openssl@3)
    PG_CONFIGURE_OPTS += --with-includes=$(OPENSSL_PREFIX)/include --with-libraries=$(OPENSSL_PREFIX)/lib
endif

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
all: neon postgres

### Neon Rust bits
#
# The 'postgres_ffi' depends on the Postgres headers.
.PHONY: neon
neon: postgres-headers
	+@echo "Compiling Neon"
	$(CARGO_CMD_PREFIX) cargo build $(CARGO_BUILD_FLAGS)

### PostgreSQL parts
$(POSTGRES_INSTALL_DIR)/build/v14/config.status:
	+@echo "Configuring Postgres v14 build"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/v14
	(cd $(POSTGRES_INSTALL_DIR)/build/v14 && \
	$(ROOT_PROJECT_DIR)/vendor/postgres-v14/configure CFLAGS='$(PG_CFLAGS)' \
		$(PG_CONFIGURE_OPTS) \
		--prefix=$(abspath $(POSTGRES_INSTALL_DIR))/v14 > configure.log)

# nicer alias to run 'configure'
.PHONY: postgres-v14-configure
postgres-v14-configure: $(POSTGRES_INSTALL_DIR)/build/v14/config.status


# Install the PostgreSQL header files into $(POSTGRES_INSTALL_DIR)/<version>/include
.PHONY: postgres-v14-headers
postgres-v14-headers: postgres-v14-configure
	+@echo "Installing PostgreSQL v14 headers"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/src/include MAKELEVEL=0 install

# Compile and install PostgreSQL and contrib/neon
.PHONY: postgres-v14
postgres-v14: postgres-v14-configure \
		  postgres-v14-headers # to prevent `make install` conflicts with neon's `postgres-headers`
	+@echo "Compiling PostgreSQL"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14 MAKELEVEL=0 install
	+@echo "Compiling contrib/neon"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/neon install
	+@echo "Compiling contrib/neon_test_utils"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/neon_test_utils install
	+@echo "Compiling pg_buffercache"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/pg_buffercache install
	+@echo "Compiling pageinspect"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v14/contrib/pageinspect install

# shorthand to build all Postgres versions
postgres: postgres-v14

postgres-headers: postgres-v14-headers


# This doesn't remove the effects of 'configure'.
.PHONY: clean
clean:
	cd $(POSTGRES_INSTALL_DIR)/build/v14 && $(MAKE) clean
	$(CARGO_CMD_PREFIX) cargo clean

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
