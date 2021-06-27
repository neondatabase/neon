#
# Docker image for console integration testing.
#
# We may also reuse it in CI to unify installation process and as a general binaries building
# tool for production servers.
#
# Dynamic linking is used for librocksdb and libstdc++ bacause librocksdb-sys calls
# bindgen with "dynamic" feature flag. This also prevents usage of dockerhub alpine-rust
# images which are statically linked and have guards against any dlopen. I would rather
# prefer all static binaries so we may change the way librocksdb-sys builds or wait until
# we will have our own storage and drop rockdb dependency.
#
# Cargo-chef is used to separate dependencies building from main binaries building. This
# way `docker build` will download and install dependencies only of there are changes to
# out Cargo.toml files.
#


#
# build postgres separately -- this layer will be rebuilt only if one of
# mentioned paths will get any changes
#
FROM alpine:3.13 as pg-build
RUN apk add --update clang llvm compiler-rt compiler-rt-static lld musl-dev binutils \
                     make bison flex readline-dev zlib-dev perl linux-headers
WORKDIR zenith
COPY ./vendor/postgres vendor/postgres
COPY ./Makefile Makefile
# Build using clang and lld
RUN CC='clang' LD='lld' CFLAGS='-fuse-ld=lld --rtlib=compiler-rt' make postgres -j4

#
# Calculate cargo dependencies.
# This will always run, but only generate recipe.json with list of dependencies without
# installing them.
#
FROM alpine:20210212 as cargo-deps-inspect
RUN apk add --update rust cargo
RUN cargo install cargo-chef
WORKDIR zenith
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

#
# Build cargo dependencies.
# This temp cantainner would be build only if recipe.json was changed.
#
FROM alpine:20210212 as deps-build
RUN apk add --update rust cargo openssl-dev clang build-base
# rust-rocksdb can be built against system-wide rocksdb -- that saves about
# 10 minutes during build. Rocksdb apk package is in testing now, but use it
# anyway. In case of any troubles we can download and build rocksdb here manually
# (to cache it as a docker layer).
RUN apk --no-cache --update --repository https://dl-cdn.alpinelinux.org/alpine/edge/testing add rocksdb-dev
WORKDIR zenith
COPY --from=pg-build /zenith/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY --from=cargo-deps-inspect /root/.cargo/bin/cargo-chef /root/.cargo/bin/
COPY --from=cargo-deps-inspect /zenith/recipe.json recipe.json
RUN ROCKSDB_LIB_DIR=/usr/lib/ cargo chef cook --release --recipe-path recipe.json

#
# Build zenith binaries
#
FROM alpine:20210212 as build
RUN apk add --update rust cargo openssl-dev clang build-base
RUN apk --no-cache --update --repository https://dl-cdn.alpinelinux.org/alpine/edge/testing add rocksdb-dev
WORKDIR zenith
COPY . .
# Copy cached dependencies
COPY --from=pg-build /zenith/tmp_install/include/postgresql/server tmp_install/include/postgresql/server
COPY --from=deps-build /zenith/target target
COPY --from=deps-build /root/.cargo /root/.cargo
RUN cargo build --release

#
# Copy binaries to resulting image.
# build-base hare to provide libstdc++ (it will also bring gcc, but leave it this way until we figure
# out how to statically link rocksdb or avoid it at all).
#
FROM alpine:3.13
RUN apk add --update openssl build-base
RUN apk --no-cache --update --repository https://dl-cdn.alpinelinux.org/alpine/edge/testing add rocksdb
COPY --from=build /zenith/target/release/pageserver /usr/local/bin
COPY --from=build /zenith/target/release/wal_acceptor /usr/local/bin
COPY --from=build /zenith/target/release/proxy /usr/local/bin
COPY --from=pg-build /zenith/tmp_install /usr/local
COPY docker-entrypoint.sh /docker-entrypoint.sh

RUN addgroup zenith && adduser -h /data -D -G zenith zenith
VOLUME ["/data"]
WORKDIR /data
USER zenith
EXPOSE 6400
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["pageserver"]
