# Build Postgres
FROM zimg/rust:1.58 AS pg-build
WORKDIR /pg

USER root

COPY vendor/postgres vendor/postgres
COPY Makefile Makefile

ENV BUILD_TYPE release
RUN set -e \
    && mold -run make -j $(nproc) -s postgres \
    && rm -rf tmp_install/build \
    && tar -C tmp_install -czf /postgres_install.tar.gz .

# Build zenith binaries
FROM zimg/rust:1.58 AS build
ARG GIT_VERSION=local

ARG CACHEPOT_BUCKET=zenith-rust-cachepot
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

COPY --from=pg-build /pg/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY . .

# Show build caching stats to check if it was used in the end.
# Has to be the part of the same RUN since cachepot daemon is killed in the end of this RUN, losing the compilation stats.
RUN set -e \
    && sudo -E "PATH=$PATH" mold -run cargo build --release \
    && cachepot -s

# Build final image
#
FROM debian:bullseye-slim
WORKDIR /data

RUN set -e \
    && apt-get update \
    && apt-get install -y \
        libreadline-dev \
        libseccomp-dev \
        openssl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && useradd -d /data zenith \
    && chown -R zenith:zenith /data

COPY --from=build --chown=zenith:zenith /home/circleci/project/target/release/pageserver /usr/local/bin
COPY --from=build --chown=zenith:zenith /home/circleci/project/target/release/safekeeper /usr/local/bin
COPY --from=build --chown=zenith:zenith /home/circleci/project/target/release/proxy      /usr/local/bin

COPY --from=pg-build /pg/tmp_install/         /usr/local/
COPY --from=pg-build /postgres_install.tar.gz /data/

COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/data"]
USER zenith
EXPOSE 6400
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["pageserver"]
