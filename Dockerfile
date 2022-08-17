# Build Postgres
FROM 369495373322.dkr.ecr.eu-central-1.amazonaws.com/rust:pinned AS pg-build
WORKDIR /home/nonroot

COPY vendor/postgres vendor/postgres
COPY Makefile Makefile

ENV BUILD_TYPE release
RUN set -e \
    && mold -run make -j $(nproc) -s postgres \
    && rm -rf tmp_install/build \
    && tar -C tmp_install -czf /home/nonroot/postgres_install.tar.gz .

# Build zenith binaries
FROM 369495373322.dkr.ecr.eu-central-1.amazonaws.com/rust:pinned AS build
WORKDIR /home/nonroot
ARG GIT_VERSION=local

# Enable https://github.com/paritytech/cachepot to cache Rust crates' compilation results in Docker builds.
# Set up cachepot to use an AWS S3 bucket for cache results, to reuse it between `docker build` invocations.
# cachepot falls back to local filesystem if S3 is misconfigured, not failing the build
ARG RUSTC_WRAPPER=cachepot
ENV AWS_REGION=eu-central-1
ENV CACHEPOT_S3_KEY_PREFIX=cachepot
ARG CACHEPOT_BUCKET=neon-github-dev
#ARG AWS_ACCESS_KEY_ID
#ARG AWS_SECRET_ACCESS_KEY

COPY --from=pg-build /home/nonroot/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY . .

# Show build caching stats to check if it was used in the end.
# Has to be the part of the same RUN since cachepot daemon is killed in the end of this RUN, losing the compilation stats.
RUN set -e \
    && mold -run cargo build --release \
    && cachepot -s

# Build final image
#
FROM debian:bullseye-slim
WORKDIR /data

RUN set -e \
    && apt update \
    && apt install -y \
        libreadline-dev \
        libseccomp-dev \
        openssl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && useradd -d /data zenith \
    && chown -R zenith:zenith /data

COPY --from=build --chown=zenith:zenith /home/nonroot/target/release/pageserver /usr/local/bin
COPY --from=build --chown=zenith:zenith /home/nonroot/target/release/safekeeper /usr/local/bin
COPY --from=build --chown=zenith:zenith /home/nonroot/target/release/proxy      /usr/local/bin

COPY --from=pg-build /home/nonroot/tmp_install/ /usr/local/
COPY --from=pg-build /home/nonroot/postgres_install.tar.gz /data/

COPY docker-entrypoint.sh /docker-entrypoint.sh

VOLUME ["/data"]
USER zenith
EXPOSE 6400
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["pageserver"]
