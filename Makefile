ROOT_PROJECT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Where to install Postgres, default is ./pg_install, maybe useful for package
# managers.
POSTGRES_INSTALL_DIR ?= $(ROOT_PROJECT_DIR)/pg_install/

# CARGO_BUILD_FLAGS: Extra flags to pass to `cargo build`. `--locked`
# and `--features testing` are popular examples.
#
# CARGO_PROFILE: You can also set to override the cargo profile to
# use. By default, it is derived from BUILD_TYPE.

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
all: neon postgres neon-pg-ext

### Neon Rust bits
#
# The 'postgres_ffi' depends on the Postgres headers.
.PHONY: neon
neon: postgres-headers walproposer-lib cargo-target-dir
	+@echo "Compiling Neon"
	$(CARGO_CMD_PREFIX) cargo build $(CARGO_BUILD_FLAGS) $(CARGO_PROFILE)
.PHONY: cargo-target-dir
cargo-target-dir:
	# https://github.com/rust-lang/cargo/issues/14281
	mkdir -p target
	test -e target/CACHEDIR.TAG || echo "$(CACHEDIR_TAG_CONTENTS)" > target/CACHEDIR.TAG

### PostgreSQL parts
# Some rules are duplicated for Postgres v14 and 15. We may want to refactor
# to avoid the duplication in the future, but it's tolerable for now.
#
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

# nicer alias to run 'configure'
# Note: I've been unable to use templates for this part of our configuration.
# I'm not sure why it wouldn't work, but this is the only place (apart from
# the "build-all-versions" entry points) where direct mention of PostgreSQL
# versions is used.
.PHONY: postgres-configure-v17
postgres-configure-v17: $(BUILD_DIR)/v17/config.status
.PHONY: postgres-configure-v16
postgres-configure-v16: $(BUILD_DIR)/v16/config.status
.PHONY: postgres-configure-v15
postgres-configure-v15: $(BUILD_DIR)/v15/config.status
.PHONY: postgres-configure-v14
postgres-configure-v14: $(BUILD_DIR)/v14/config.status

# Install just the PostgreSQL header files into $(POSTGRES_INSTALL_DIR)/<version>/include
#
# This is implicitly included in the 'postgres-%' rule, but this can be handy if you
# want to just install the headers without building PostgreSQL, e.g. for building
# extensions.
.PHONY: postgres-headers-%
postgres-headers-%: postgres-configure-%
	+@echo "Installing PostgreSQL $* headers"
	$(MAKE) -C $(BUILD_DIR)/$*/src/include MAKELEVEL=0 install

# Compile and install PostgreSQL
.PHONY: postgres-%
postgres-%: postgres-configure-% \
		  postgres-headers-% # to prevent `make install` conflicts with neon's `postgres-headers`
	+@echo "Compiling PostgreSQL $*"
	$(MAKE) -C $(BUILD_DIR)/$* MAKELEVEL=0 install
	+@echo "Compiling pg_prewarm $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_prewarm install
	+@echo "Compiling pg_buffercache $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_buffercache install
	+@echo "Compiling pg_visibility $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_visibility install
	+@echo "Compiling pageinspect $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pageinspect install
	+@echo "Compiling pg_trgm $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/pg_trgm install
	+@echo "Compiling amcheck $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/amcheck install
	+@echo "Compiling test_decoding $*"
	$(MAKE) -C $(BUILD_DIR)/$*/contrib/test_decoding install

.PHONY: postgres-check-%
postgres-check-%: postgres-%
	$(MAKE) -C $(BUILD_DIR)/$* MAKELEVEL=0 check

.PHONY: neon-pg-ext-%
neon-pg-ext-%: postgres-%
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

.PHONY: neon-pg-ext
neon-pg-ext: \
	neon-pg-ext-v14 \
	neon-pg-ext-v15 \
	neon-pg-ext-v16 \
	neon-pg-ext-v17

# shorthand to build all Postgres versions
.PHONY: postgres
postgres: \
	postgres-v14 \
	postgres-v15 \
	postgres-v16 \
	postgres-v17

.PHONY: postgres-headers
postgres-headers: \
	postgres-headers-v14 \
	postgres-headers-v15 \
	postgres-headers-v16 \
	postgres-headers-v17

.PHONY: postgres-check
postgres-check: \
	postgres-check-v14 \
	postgres-check-v15 \
	postgres-check-v16 \
	postgres-check-v17

# This removes everything
.PHONY: distclean
distclean:
	$(RM) -r $(POSTGRES_INSTALL_DIR)
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
