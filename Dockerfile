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
ENV BUILD_TYPE release
RUN make -j $(getconf _NPROCESSORS_ONLN) -s postgres
RUN rm -rf postgres_install/build

#
# Build zenith binaries
#
# TODO: build cargo deps as separate layer. We used cargo-chef before but that was
# net time waste in a lot of cases. Copying Cargo.lock with empty lib.rs should do the work.
#
FROM zenithdb/build:buster AS build
WORKDIR /zenith
COPY --from=pg-build /zenith/tmp_install/include/postgresql/server tmp_install/include/postgresql/server

COPY . .
RUN cargo build --release

#
# Copy binaries to resulting image.
#
FROM debian:buster-slim
WORKDIR /data

RUN apt-get update && apt-get -yq install libreadline-dev libseccomp-dev openssl ca-certificates && \
    mkdir zenith_install

COPY --from=build /zenith/target/release/pageserver /usr/local/bin
COPY --from=build /zenith/target/release/safekeeper /usr/local/bin
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
