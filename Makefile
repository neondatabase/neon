ROOT_PROJECT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Where to install Postgres, default is ./tmp_install, maybe useful for package managers
POSTGRES_INSTALL_DIR ?= $(ROOT_PROJECT_DIR)/tmp_install

# Seccomp BPF is only available for Linux
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	SECCOMP = --with-libseccomp
else
	SECCOMP =
endif

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

# macOS with brew-installed openssl requires explicit paths
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
    PG_CONFIGURE_OPTS += --with-includes=$(HOMEBREW_PREFIX)/opt/openssl/include --with-libraries=$(HOMEBREW_PREFIX)/opt/openssl/lib
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
#
# Postgres is built in the 'build' directory, and installed into
# $(POSTGRES_INSTALL_DIR), which defaults to 'tmp_install'
#
build/config.status:
	+@echo "Configuring postgres build"
	mkdir -p build
	(cd build && \
	$(ROOT_PROJECT_DIR)/vendor/postgres/configure CFLAGS='$(PG_CFLAGS)' \
		$(PG_CONFIGURE_OPTS) \
		$(SECCOMP) \
		--prefix=$(abspath $(POSTGRES_INSTALL_DIR)) > configure.log)

# nicer alias for running 'configure'
.PHONY: postgres-configure
postgres-configure: build/config.status

# Install the PostgreSQL header files into $(POSTGRES_INSTALL_DIR)/include
.PHONY: postgres-headers
postgres-headers: postgres-configure
	+@echo "Installing PostgreSQL headers"
	$(MAKE) -C build/src/include MAKELEVEL=0 install

# Compile and install PostgreSQL and contrib/neon
.PHONY: postgres
postgres: postgres-configure \
		  postgres-headers # to prevent `make install` conflicts with neon's `postgres-headers`
	+@echo "Compiling PostgreSQL"
	$(MAKE) -C build MAKELEVEL=0 install
	+@echo "Compiling contrib/neon"
	$(MAKE) -C build/contrib/neon install
	+@echo "Compiling contrib/neon_test_utils"
	$(MAKE) -C build/contrib/neon_test_utils install
	+@echo "Compiling pg_buffercache"
	$(MAKE) -C build/contrib/pg_buffercache install
	+@echo "Compiling pageinspect"
	$(MAKE) -C build/contrib/pageinspect install


.PHONY: postgres-clean
postgres-clean:
	$(MAKE) -C build MAKELEVEL=0 clean

# This doesn't remove the effects of 'configure'.
.PHONY: clean
clean:
	cd build && $(MAKE) clean
	$(CARGO_CMD_PREFIX) cargo clean

# This removes everything
.PHONY: distclean
distclean:
	rm -rf build $(POSTGRES_INSTALL_DIR)
	$(CARGO_CMD_PREFIX) cargo clean

.PHONY: fmt
fmt:
	./pre-commit.py --fix-inplace

.PHONY: setup-pre-commit-hook
setup-pre-commit-hook:
	ln -s -f $(ROOT_PROJECT_DIR)/pre-commit.py .git/hooks/pre-commit
