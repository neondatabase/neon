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
	PG_CONFIGURE_OPTS = --enable-debug
	PG_CFLAGS = -O2 -g3 $(CFLAGS)
else ifeq ($(BUILD_TYPE),debug)
	PG_CONFIGURE_OPTS = --enable-debug --enable-cassert --enable-depend
	PG_CFLAGS = -O0 -g3 $(CFLAGS)
else
$(error Bad build type `$(BUILD_TYPE)', see Makefile for options)
endif

#
# Top level Makefile to build Zenith and PostgreSQL
#
.PHONY: all
all: zenith postgres

# We don't want to run 'cargo build' in parallel with the postgres build,
# because interleaving cargo build output with postgres build output looks
# confusing. Also, 'cargo build' is parallel on its own, so it would be too
# much parallelism. (Recursive invocation of postgres target still gets any
# '-j' flag from the command line, so 'make -j' is still useful.)
.NOTPARALLEL:

### Zenith Rust bits
#
# The 'postgres_ffi' depends on the Postgres headers.
.PHONY: zenith
zenith: postgres-headers
	cargo build

### PostgreSQL parts
tmp_install/build/config.status:
	+@echo "Configuring postgres build"
	mkdir -p tmp_install/build
	(cd tmp_install/build && \
	../../vendor/postgres/configure CFLAGS='$(PG_CFLAGS)' \
		$(PG_CONFIGURE_OPTS) \
		$(SECCOMP) \
		--prefix=$(abspath tmp_install) > configure.log)

# nicer alias for running 'configure'
.PHONY: postgres-configure
postgres-configure: tmp_install/build/config.status

# Install the PostgreSQL header files into tmp_install/include
.PHONY: postgres-headers
postgres-headers: postgres-configure
	+@echo "Installing PostgreSQL headers"
	$(MAKE) -C tmp_install/build/src/include MAKELEVEL=0 install


# Compile and install PostgreSQL and contrib/zenith
.PHONY: postgres
postgres: postgres-configure
	+@echo "Compiling PostgreSQL"
	$(MAKE) -C tmp_install/build MAKELEVEL=0 install
	+@echo "Compiling contrib/zenith"
	$(MAKE) -C tmp_install/build/contrib/zenith install
	+@echo "Compiling contrib/zenith_test_utils"
	$(MAKE) -C tmp_install/build/contrib/zenith_test_utils install

.PHONY: postgres-clean
postgres-clean:
	$(MAKE) -C tmp_install/build MAKELEVEL=0 clean

# This doesn't remove the effects of 'configure'.
.PHONY: clean
clean:
	cd tmp_install/build && $(MAKE) clean
	cargo clean

# This removes everything
.PHONY: distclean
distclean:
	rm -rf tmp_install
	cargo clean

.PHONY: fmt
fmt:
	./pre-commit.py --fix-inplace

.PHONY: setup-pre-commit-hook
setup-pre-commit-hook:
	ln -s -f ../../pre-commit.py .git/hooks/pre-commit
