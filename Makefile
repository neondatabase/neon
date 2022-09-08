ROOT_PROJECT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Where to install Postgres, default is ./pg_install, maybe useful for package managers
POSTGRES_INSTALL_DIR ?= $(ROOT_PROJECT_DIR)/pg_install/
POSTGRES_VERSIONS := 14 15
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
all: neon postgres neon-pg-ext

### Neon Rust bits
#
# The 'postgres_ffi' depends on the Postgres headers.
.PHONY: neon
neon: $(POSTGRES_VERSIONS:%=postgres-v%-headers)
	+@echo "Compiling Neon"
	$(CARGO_CMD_PREFIX) cargo build $(CARGO_BUILD_FLAGS)

### PostgreSQL parts
$(POSTGRES_INSTALL_DIR)/build/%/config.status:
	+@echo "Configuring Postgres $* build"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/$*
	(cd $(POSTGRES_INSTALL_DIR)/build/$* && \
	$(ROOT_PROJECT_DIR)/vendor/postgres-$*/configure CFLAGS='$(PG_CFLAGS)' \
		$(PG_CONFIGURE_OPTS) \
		--prefix=$(abspath $(POSTGRES_INSTALL_DIR))/$* > configure.log)

# nicer alias to run 'configure'
postgres-%-configure:
	$(MAKE) $(POSTGRES_INSTALL_DIR)/build/$*/config.status

# Install the PostgreSQL header files into $(POSTGRES_INSTALL_DIR)/<version>/include
postgres-%-headers: postgres-%-configure
	+@echo "Installing PostgreSQL $* headers"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/src/include MAKELEVEL=0 install

# Compile and install PostgreSQL
postgres-%: postgres-%-configure
#		  postgres-%-headers # to prevent `make install` conflicts with neon's `postgres-headers`
	+@echo "Compiling PostgreSQL $*"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$* MAKELEVEL=0 install
	+@echo "Compiling libpq $*"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/src/interfaces/libpq install
	+@echo "Compiling pg_buffercache $*"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/contrib/pg_buffercache install
	+@echo "Compiling pageinspect $*"
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/contrib/pageinspect install

# shorthand to build all Postgres versions
postgres: $(POSTGRES_VERSIONS:%=postgres-v%)

postgres-%-clean:
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$* MAKELEVEL=0 clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/contrib/pg_buffercache clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/contrib/pageinspect clean
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/$*/src/interfaces/libpq clean

neon-pg-ext-%: postgres-%
	cp -a $(ROOT_PROJECT_DIR)/pgxn/neon $(ROOT_PROJECT_DIR)/pgxn/neon-$*
	cp -a $(ROOT_PROJECT_DIR)/pgxn/neon_test_utils $(ROOT_PROJECT_DIR)/pgxn/neon_test_utils-$*
	+@echo "Compiling neon $*"
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-$*
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/$*/bin/pg_config \
		-C $(ROOT_PROJECT_DIR)/pgxn/neon-$* install
	@echo "Compiling neon_test_utils" $*
	mkdir -p $(POSTGRES_INSTALL_DIR)/build/neon-test-utils-$*
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/$*/bin/pg_config \
		-C $(ROOT_PROJECT_DIR)/pgxn/neon_test_utils-$* install

neon-pg-ext: $(POSTGRES_VERSIONS:%=neon-pg-ext-v%)
postgres-headers: $(POSTGRES_VERSIONS:%=postgres-v%-headers)
postgres-clean: $(POSTGRES_VERSIONS:%=postgres-v%-clean)

# This doesn't remove the effects of 'configure'.
.PHONY: clean
clean:
	$(MAKE) -C $(POSTGRES_INSTALL_DIR)/build/v* || true
	$(CARGO_CMD_PREFIX) cargo clean || true
	find $(ROOT_PROJECT_DIR) -type f \( -name '*.o' -o -name '*.so' \) -exec $(RM) '{}' \;

# This removes everything
.PHONY: distclean
distclean:
	rm -rf $(POSTGRES_INSTALL_DIR)
	rm -rf $(ROOT_PROJECT_DIR)/pgxn/neon*-v1*
	$(CARGO_CMD_PREFIX) cargo clean

.PHONY: fmt
fmt:
	./pre-commit.py --fix-inplace

.PHONY: setup-pre-commit-hook
setup-pre-commit-hook:
	ln -s -f $(ROOT_PROJECT_DIR)/pre-commit.py .git/hooks/pre-commit
