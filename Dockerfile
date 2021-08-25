#
# Docker image for console integration testing.
#

#
# Build Postgres separately --- this layer will be rebuilt only if one of
# mentioned paths will get any changes.
#
FROM zenithdb/build:buster AS pg-build
WORKDIR /zenith
COPY ./vendor/postgres vendor/postgres
COPY ./Makefile Makefile
RUN make -j $(getconf _NPROCESSORS_ONLN) -s postgres

#
# Calculate cargo dependencies.
# This will always run, but only generate recipe.json with list of dependencies without
# installing them.
#
FROM zenithdb/build:buster AS cargo-deps-inspect
WORKDIR /zenith
COPY . .
RUN cargo chef prepare --recipe-path /zenith/recipe.json

#
# Build cargo dependencies.
# This temp cantainner should be rebuilt only if recipe.json was changed.
#
FROM zenithdb/build:buster AS deps-build
WORKDIR /zenith
COPY --from=pg-build /zenith/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY --from=cargo-deps-inspect /usr/local/cargo/bin/cargo-chef /usr/local/cargo/bin/
COPY --from=cargo-deps-inspect /zenith/recipe.json recipe.json
RUN ROCKSDB_LIB_DIR=/usr/lib/ cargo chef cook --release --recipe-path recipe.json

#
# Build zenith binaries
#
FROM zenithdb/build:buster AS build
WORKDIR /zenith
COPY . .
# Copy cached dependencies
COPY --from=pg-build /zenith/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY --from=deps-build /zenith/target target
COPY --from=deps-build /usr/local/cargo/ /usr/local/cargo/
RUN cargo build --release

#
# Copy binaries to resulting image.
#
FROM debian:buster-slim
WORKDIR /data

RUN apt-get update && apt-get -yq install librocksdb-dev libseccomp-dev openssl && \
    mkdir zenith_install

COPY --from=build /zenith/target/release/pageserver /usr/local/bin
COPY --from=build /zenith/target/release/wal_acceptor /usr/local/bin
COPY --from=build /zenith/target/release/proxy /usr/local/bin
COPY --from=pg-build /zenith/tmp_install postgres_install
COPY docker-entrypoint.sh /docker-entrypoint.sh

# Remove build artifacts (~ 500 MB)
RUN rm -rf postgres_install/build && \
    # 'Install' Postgres binaries locally
    cp -r postgres_install/* /usr/local/ && \
    # Prepare an archive of Postgres binaries (should be around 11 MB)
    # and keep it inside container for an ease of deploy pipeline.
    cd postgres_install && tar -czf /data/postgres_install.tar.gz . && cd .. && \
    rm -rf postgres_install

RUN useradd -d /data zenith && chown -R zenith:zenith /data

VOLUME ["/data"]
USER zenith
EXPOSE 6400
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["pageserver"]
