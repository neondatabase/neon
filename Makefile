ROOT_PROJECT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Where to install Postgres, default is ./pg_install, maybe useful for package
# managers.
POSTGRES_INSTALL_DIR ?= $(ROOT_PROJECT_DIR)/pg_install/

# Supported PostgreSQL versions
POSTGRES_VERSIONS = v17 v16 v15 v14

# CARGO_BUILD_FLAGS: Extra flags to pass to `cargo build`. `--locked`
# and `--features testing` are popular examples.
#
# CARGO_PROFILE: Set to override the cargo profile to use. By default,
# it is derived from BUILD_TYPE.

# All intermediate build artifacts are stored here.
BUILD_DIR := build

ICU_PREFIX_DIR := /usr/local/icu

#
# We differentiate between release / debug build types using the BUILD_TYPE
# environment variable.
#
BUILD_TYPE ?= debug
WITH_SANITIZERS ?= no
PG_CFLAGS = -fsigned-char
ifeq ($(BUILD_TYPE),release)
	PG_CONFIGURE_OPTS = --enable-debug --with-openssl
	PG_CFLAGS += -O2 -g3 $(CFLAGS)
	PG_LDFLAGS = $(LDFLAGS)
	CARGO_PROFILE ?= --profile=release
else ifeq ($(BUILD_TYPE),debug)
	PG_CONFIGURE_OPTS = --enable-debug --with-openssl --enable-cassert --enable-depend
	PG_CFLAGS += -O0 -g3 $(CFLAGS)
	PG_LDFLAGS = $(LDFLAGS)
	CARGO_PROFILE ?= --profile=dev
else
	$(error Bad build type '$(BUILD_TYPE)', see Makefile for options)
endif

ifeq ($(WITH_SANITIZERS),yes)
	PG_CFLAGS += -fsanitize=address -fsanitize=undefined -fno-sanitize-recover
	COPT += -Wno-error # to avoid failing on warnings induced by sanitizers
	PG_LDFLAGS = -fsanitize=address -fsanitize=undefined -static-libasan -static-libubsan $(LDFLAGS)
	export CC := gcc
	export ASAN_OPTIONS := detect_leaks=0
endif

ifeq ($(shell test -e /home/nonroot/.docker_build && echo -n yes),yes)
	# Exclude static build openssl, icu for local build (MacOS, Linux)
	# Only keep for build type release and debug
	PG_CONFIGURE_OPTS += --with-icu
	PG_CONFIGURE_OPTS += ICU_CFLAGS='-I/$(ICU_PREFIX_DIR)/include -DU_STATIC_IMPLEMENTATION'
	PG_CONFIGURE_OPTS += ICU_LIBS='-L$(ICU_PREFIX_DIR)/lib -L$(ICU_PREFIX_DIR)/lib64 -licui18n -licuuc -licudata -lstdc++ -Wl,-Bdynamic -lm'
endif

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	# Seccomp BPF is only available for Linux
	ifneq ($(WITH_SANITIZERS),yes)
		PG_CONFIGURE_OPTS += --with-libseccomp
	endif
else ifeq ($(UNAME_S),Darwin)
	PG_CFLAGS += -DUSE_PREFETCH
	ifndef DISABLE_HOMEBREW
		# macOS with brew-installed openssl requires explicit paths
		# It can be configured with OPENSSL_PREFIX variable
		OPENSSL_PREFIX := $(shell brew --prefix openssl@3)
		PG_CONFIGURE_OPTS += --with-includes=$(OPENSSL_PREFIX)/include --with-libraries=$(OPENSSL_PREFIX)/lib
		PG_CONFIGURE_OPTS += PKG_CONFIG_PATH=$(shell brew --prefix icu4c)/lib/pkgconfig
		# macOS already has bison and flex in the system, but they are old and result in postgres-v14 target failure
		# brew formulae are keg-only and not symlinked into HOMEBREW_PREFIX, force their usage
		EXTRA_PATH_OVERRIDES += $(shell brew --prefix bison)/bin/:$(shell brew --prefix flex)/bin/:
	endif
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

CACHEDIR_TAG_CONTENTS := "Signature: 8a477f597d28d172789f06886806bc55"

#
# Top level Makefile to build Neon and PostgreSQL
#
.PHONY: all
all: neon postgres-install neon-pg-ext

### Neon Rust bits
#
# The 'postgres_ffi' crate depends on the Postgres headers.
.PHONY: neon
neon: postgres-headers-install walproposer-lib cargo-target-dir
	+@echo "Compiling Neon"
	$(CARGO_CMD_PREFIX) cargo build $(CARGO_BUILD_FLAGS) $(CARGO_PROFILE)

.PHONY: cargo-target-dir
cargo-target-dir:
	# https://github.com/rust-lang/cargo/issues/14281
	mkdir -p target
	test -e target/CACHEDIR.TAG || echo "$(CACHEDIR_TAG_CONTENTS)" > target/CACHEDIR.TAG

.PHONY: neon-pg-ext-%
neon-pg-ext-%: postgres-install-%
	+@echo "Compiling neon-specific Postgres extensions for $*"
	mkdir -p $(BUILD_DIR)/pgxn-$*
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/$*/bin/pg_config COPT='$(COPT)' \
		-C $(BUILD_DIR)/pgxn-$*\
		-f $(ROOT_PROJECT_DIR)/pgxn/Makefile  install

# Build walproposer as a static library. walproposer source code is located
# in the pgxn/neon directory.
#
# We also need to include libpgport.a and libpgcommon.a, because walproposer
# uses some functions from those libraries.
#
# Some object files are removed from libpgport.a and libpgcommon.a because
# they depend on openssl and other libraries that are not included in our
# Rust build.
.PHONY: walproposer-lib
walproposer-lib: neon-pg-ext-v17
	+@echo "Compiling walproposer-lib"
	mkdir -p $(BUILD_DIR)/walproposer-lib
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v17/bin/pg_config COPT='$(COPT)' \
		-C $(BUILD_DIR)/walproposer-lib \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon/Makefile walproposer-lib
	cp $(POSTGRES_INSTALL_DIR)/v17/lib/libpgport.a $(BUILD_DIR)/walproposer-lib
	cp $(POSTGRES_INSTALL_DIR)/v17/lib/libpgcommon.a $(BUILD_DIR)/walproposer-lib
	$(AR) d $(BUILD_DIR)/walproposer-lib/libpgport.a \
		pg_strong_random.o
	$(AR) d $(BUILD_DIR)/walproposer-lib/libpgcommon.a \
		checksum_helper.o \
		cryptohash_openssl.o \
		hmac_openssl.o \
		md5_common.o \
		parse_manifest.o \
		scram-common.o
ifeq ($(UNAME_S),Linux)
	$(AR) d $(BUILD_DIR)/walproposer-lib/libpgcommon.a \
		pg_crc32c.o
endif

# Shorthand to call neon-pg-ext-% target for all Postgres versions
.PHONY: neon-pg-ext
neon-pg-ext: $(foreach pg_version,$(POSTGRES_VERSIONS),neon-pg-ext-$(pg_version))

# This removes everything
.PHONY: distclean
distclean:
	$(RM) -r $(POSTGRES_INSTALL_DIR) $(BUILD_DIR)
	$(CARGO_CMD_PREFIX) cargo clean

.PHONY: fmt
fmt:
	./pre-commit.py --fix-inplace

postgres-%-pg-bsd-indent: postgres-%
	+@echo "Compiling pg_bsd_indent"
	$(MAKE) -C $(BUILD_DIR)/$*/src/tools/pg_bsd_indent/

# Create typedef list for the core. Note that generally it should be combined with
# buildfarm one to cover platform specific stuff.
# https://wiki.postgresql.org/wiki/Running_pgindent_on_non-core_code_or_development_code
postgres-%-typedefs.list: postgres-%
	$(ROOT_PROJECT_DIR)/vendor/postgres-$*/src/tools/find_typedef $(POSTGRES_INSTALL_DIR)/$*/bin > $@

# Indent postgres. See src/tools/pgindent/README for details.
.PHONY: postgres-%-pgindent
postgres-%-pgindent: postgres-%-pg-bsd-indent postgres-%-typedefs.list
	+@echo merge with buildfarm typedef to cover all platforms
	+@echo note: I first tried to download from pgbuildfarm.org, but for unclear reason e.g. \
		REL_16_STABLE list misses PGSemaphoreData
	# wget -q -O - "http://www.pgbuildfarm.org/cgi-bin/typedefs.pl?branch=REL_16_STABLE" |\
	# cat - postgres-$*-typedefs.list | sort | uniq > postgres-$*-typedefs-full.list
	cat $(ROOT_PROJECT_DIR)/vendor/postgres-$*/src/tools/pgindent/typedefs.list |\
		cat - postgres-$*-typedefs.list | sort | uniq > postgres-$*-typedefs-full.list
	+@echo note: you might want to run it on selected files/dirs instead.
	INDENT=$(BUILD_DIR)/$*/src/tools/pg_bsd_indent/pg_bsd_indent \
		$(ROOT_PROJECT_DIR)/vendor/postgres-$*/src/tools/pgindent/pgindent --typedefs postgres-$*-typedefs-full.list \
		$(ROOT_PROJECT_DIR)/vendor/postgres-$*/src/ \
		--excludes $(ROOT_PROJECT_DIR)/vendor/postgres-$*/src/tools/pgindent/exclude_file_patterns
	$(RM) pg*.BAK

# Indent pxgn/neon.
.PHONY: neon-pgindent
neon-pgindent: postgres-v17-pg-bsd-indent neon-pg-ext-v17
	$(MAKE) PG_CONFIG=$(POSTGRES_INSTALL_DIR)/v17/bin/pg_config COPT='$(COPT)' \
		FIND_TYPEDEF=$(ROOT_PROJECT_DIR)/vendor/postgres-v17/src/tools/find_typedef \
		INDENT=$(BUILD_DIR)/v17/src/tools/pg_bsd_indent/pg_bsd_indent \
		PGINDENT_SCRIPT=$(ROOT_PROJECT_DIR)/vendor/postgres-v17/src/tools/pgindent/pgindent \
		-C $(BUILD_DIR)/neon-v17 \
		-f $(ROOT_PROJECT_DIR)/pgxn/neon/Makefile pgindent


.PHONY: setup-pre-commit-hook
setup-pre-commit-hook:
	ln -s -f $(ROOT_PROJECT_DIR)/pre-commit.py .git/hooks/pre-commit

# Targets for building PostgreSQL are defined in postgres.mk.
#
# But if the caller has indicated that PostgreSQL is already
# installed, by setting the PG_INSTALL_CACHED variable, skip it.
ifdef PG_INSTALL_CACHED
postgres-install: skip-install
$(foreach pg_version,$(POSTGRES_VERSIONS),postgres-install-$(pg_version)): skip-install
postgres-headers-install:
	+@echo "Skipping installation of PostgreSQL headers because PG_INSTALL_CACHED is set"
skip-install:
	+@echo "Skipping PostgreSQL installation because PG_INSTALL_CACHED is set"

else
include postgres.mk
endif
