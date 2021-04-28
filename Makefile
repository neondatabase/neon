#
# Top level Makefile to build Zenith and PostgreSQL
#
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
zenith: postgres-headers
	cargo build

### PostgreSQL parts
tmp_install/build/config.status:
	+@echo "Configuring postgres build"
	mkdir -p tmp_install/build
	(cd tmp_install/build && \
	../../vendor/postgres/configure CFLAGS='-O0' --enable-debug --enable-cassert \
	    --enable-depend --with-libxml --prefix=$(abspath tmp_install) > configure.log)

# nicer alias for running 'configure'
postgres-configure: tmp_install/build/config.status

# Install the PostgreSQL header files into tmp_install/include
postgres-headers: postgres-configure
	+@echo "Installing PostgreSQL headers"
	$(MAKE) -C tmp_install/build/src/include MAKELEVEL=0 install

# Compile and install PostgreSQL
postgres: postgres-configure
	+@echo "Compiling PostgreSQL"
	$(MAKE) -C tmp_install/build MAKELEVEL=0 install

postgres-clean:
	$(MAKE) -C tmp_install/build MAKELEVEL=0 clean

# This doesn't remove the effects of 'configure'.
clean:
	cd tmp_install/build && ${MAKE} clean
	cargo clean

# This removes everything
distclean:
	rm -rf tmp_install
	cargo clean

.PHONY: postgres-configure postgres postgres-headers zenith
