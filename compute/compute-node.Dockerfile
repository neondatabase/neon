#
# This Dockerfile builds the compute image. It is built multiple times to produce
# different images for each PostgreSQL major version.
#
# We use Debian as the base for all the steps. The production images use Debian bookworm
# for v17, and Debian bullseye for older PostgreSQL versions.
#
# ## Intermediary layers
#
# build-tools:   This contains Rust compiler toolchain and other tools needed at compile
#                time. This is also used for the storage builds. This image is defined in
#                build-tools.Dockerfile.
#
# build-deps:    Contains C compiler, other build tools, and compile-time dependencies
#                needed to compile PostgreSQL and most extensions. (Some extensions need
#                extra tools and libraries that are not included in this image. They are
#                installed in the extension-specific build stages.)
#
# pg-build:      Result of compiling PostgreSQL. The PostgreSQL binaries are copied from
#                this to the final image. This is also used as the base for compiling all
#                the extensions.
#
# compute-tools: This contains compute_ctl, the launcher program that starts Postgres
#                in Neon. It also contains a few other tools that are built from the
#                sources from this repository and used in compute VMs: 'fast_import' and
#                'local_proxy'
#
# ## Extensions
#
# By convention, the build of each extension consists of two layers:
#
# {extension}-src:   Contains the source tarball, possible neon-specific patches, and
#                    the extracted tarball with the patches applied. All of these are
#                    under the /ext-src/ directory.
#
# {extension}-build: Contains the installed extension files, under /usr/local/pgsql
#                    (in addition to the PostgreSQL binaries inherited from the pg-build
#                    image). A few extensions need extra libraries or other files
#                    installed elsewhere in the filesystem. They are installed by ONBUILD
#                    directives.
#
# These are merged together into two layers:
#
# all-extensions:    All the extension -build layers merged together
#
# extension-tests:   All the extension -src layers merged together. This is used by the
#                    extension tests. The tests are executed against the compiled image,
#                    but the tests need test scripts, expected result files etc. from the
#                    original sources, which are not included in the binary image.
#
# ## Extra components
#
# These are extra included in the compute image, but are not directly used by PostgreSQL
# itself.
#
# pgbouncer:         pgbouncer and its configuration
#
# sql_exporter:      Metrics exporter daemon.
#
# postgres_exporter: Another metrics exporter daemon, for different sets of metrics.
#
# The configuration files for the metrics exporters are under etc/ directory. We use
# a templating system to handle variations between different PostgreSQL versions,
# building slightly different config files for each PostgreSQL version.
#
#
# ## Final image
#
# The final image puts together the PostgreSQL binaries (pg-build), the compute tools
# (compute-tools), all the extensions (all-extensions) and the extra components into
# one image.
#
# VM image: The final image built by this dockerfile isn't actually the final image that
# we use in computes VMs. There's an extra step that adds some files and makes other
# small adjustments, and builds the QCOV2 filesystem image suitable for using in a VM.
# That step is done by the 'vm-builder' tool. See the vm-compute-node-image job in the
# build_and_test.yml github workflow for how that's done.

ARG PG_VERSION
ARG REPOSITORY=neondatabase
ARG IMAGE=build-tools
ARG TAG=pinned
ARG BUILD_TAG
ARG DEBIAN_VERSION=bookworm
ARG DEBIAN_FLAVOR=${DEBIAN_VERSION}-slim

# Here are the INDEX DIGESTS for the images we use.
# You can get them following next steps for now:
# 1. Get an authentication token from DockerHub:
#    TOKEN=$(curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/debian:pull" | jq -r .token)
# 2. Using that token, query index for the given tag:
#    curl -s -H "Authorization: Bearer $TOKEN" \
#       -H "Accept: application/vnd.docker.distribution.manifest.list.v2+json" \
#       "https://registry.hub.docker.com/v2/library/debian/manifests/bullseye-slim" \
#       -I | grep -i docker-content-digest
# 3. As a next step, TODO(fedordikarev): create script and schedule workflow to run these checks
#    and updates on regular bases and in automated way.
ARG BOOKWORM_SLIM_SHA=sha256:40b107342c492725bc7aacbe93a49945445191ae364184a6d24fedb28172f6f7
ARG BULLSEYE_SLIM_SHA=sha256:e831d9a884d63734fe3dd9c491ed9a5a3d4c6a6d32c5b14f2067357c49b0b7e1

# Here we use ${var/search/replace} syntax, to check
# if base image is one of the images, we pin image index for.
# If var will match one the known images, we will replace it with the known sha.
# If no match, than value will be unaffected, and will process with no-pinned image.
ARG BASE_IMAGE_SHA=debian:${DEBIAN_FLAVOR}
ARG BASE_IMAGE_SHA=${BASE_IMAGE_SHA/debian:bookworm-slim/debian@$BOOKWORM_SLIM_SHA}
ARG BASE_IMAGE_SHA=${BASE_IMAGE_SHA/debian:bullseye-slim/debian@$BULLSEYE_SLIM_SHA}

# By default, build all PostgreSQL extensions. For quick local testing when you don't
# care about the extensions, pass EXTENSIONS=none or EXTENSIONS=minimal
ARG EXTENSIONS=all

#########################################################################################
#
# Layer "build-deps"
#
#########################################################################################
FROM $BASE_IMAGE_SHA AS build-deps
ARG DEBIAN_VERSION

# Use strict mode for bash to catch errors early
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# By default, /bin/sh used in debian images will treat '\n' as eol,
# but as we use bash as SHELL, and built-in echo in bash requires '-e' flag for that.
RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries && \
    echo -e "retry_connrefused = on\ntimeout=15\ntries=5\nretry-on-host-error=on\n" > /root/.wgetrc && \
    echo -e "--retry-connrefused\n--connect-timeout 15\n--retry 5\n--max-time 300\n" > /root/.curlrc

RUN case $DEBIAN_VERSION in \
      # Version-specific installs for Bullseye (PG14-PG16):
      # The h3_pg extension needs a cmake 3.20+, but Debian bullseye has 3.18.
      # Install newer version (3.25) from backports.
      # libstdc++-10-dev is required for plv8
      bullseye) \
        echo "deb http://deb.debian.org/debian bullseye-backports main" > /etc/apt/sources.list.d/bullseye-backports.list; \
        VERSION_INSTALLS="cmake/bullseye-backports cmake-data/bullseye-backports libstdc++-10-dev"; \
      ;; \
      # Version-specific installs for Bookworm (PG17):
      bookworm) \
        VERSION_INSTALLS="cmake libstdc++-12-dev"; \
      ;; \
      *) \
        echo "Unknown Debian version ${DEBIAN_VERSION}" && exit 1 \
      ;; \
    esac && \
    apt update &&  \
    apt install --no-install-recommends --no-install-suggests -y \
    ninja-build git autoconf automake libtool build-essential bison flex libreadline-dev \
    zlib1g-dev libxml2-dev libcurl4-openssl-dev libossp-uuid-dev wget ca-certificates pkg-config libssl-dev \
    libicu-dev libxslt1-dev liblz4-dev libzstd-dev zstd curl unzip \
    $VERSION_INSTALLS \
    && apt clean && rm -rf /var/lib/apt/lists/*

#########################################################################################
#
# Layer "pg-build"
# Build Postgres from the neon postgres repository.
#
#########################################################################################
FROM build-deps AS pg-build
ARG PG_VERSION
COPY vendor/postgres-${PG_VERSION:?} postgres
RUN cd postgres && \
    export CONFIGURE_CMD="./configure CFLAGS='-O2 -g3' --enable-debug --with-openssl --with-uuid=ossp \
    --with-icu --with-libxml --with-libxslt --with-lz4" && \
    if [ "${PG_VERSION:?}" != "v14" ]; then \
        # zstd is available only from PG15
        export CONFIGURE_CMD="${CONFIGURE_CMD} --with-zstd"; \
    fi && \
    eval $CONFIGURE_CMD && \
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s install && \
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s -C contrib/ install && \
    # Install headers
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s -C src/include install && \
    make MAKELEVEL=0 -j $(getconf _NPROCESSORS_ONLN) -s -C src/interfaces/libpq install && \
    # Enable some of contrib extensions
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/autoinc.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/dblink.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/postgres_fdw.control && \
    file=/usr/local/pgsql/share/extension/postgres_fdw--1.0.sql && [ -e $file ] && \
    echo 'GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO neon_superuser;' >> $file && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/bloom.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/earthdistance.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/insert_username.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/intagg.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/moddatetime.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_stat_statements.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pgrowlocks.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pgstattuple.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/refint.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/xml2.control && \
    # We need to grant EXECUTE on pg_stat_statements_reset() to neon_superuser.
    # In vanilla postgres this function is limited to Postgres role superuser.
    # In neon we have neon_superuser role that is not a superuser but replaces superuser in some cases.
    # We could add the additional grant statements to the postgres repository but it would be hard to maintain,
    # whenever we need to pick up a new postgres version and we want to limit the changes in our postgres fork,
    # so we do it here.
    for file in /usr/local/pgsql/share/extension/pg_stat_statements--*.sql; do \
        filename=$(basename "$file"); \
        # Note that there are no downgrade scripts for pg_stat_statements, so we \
        # don't have to modify any downgrade paths or (much) older versions: we only \
        # have to make sure every creation of the pg_stat_statements_reset function \
        # also adds execute permissions to the neon_superuser.
        case $filename in \
          pg_stat_statements--1.4.sql) \
            # pg_stat_statements_reset is first created with 1.4
            echo 'GRANT EXECUTE ON FUNCTION pg_stat_statements_reset() TO neon_superuser;' >> $file; \
            ;; \
          pg_stat_statements--1.6--1.7.sql) \
            # Then with the 1.6-1.7 migration it is re-created with a new signature, thus add the permissions back
            echo 'GRANT EXECUTE ON FUNCTION pg_stat_statements_reset(Oid, Oid, bigint) TO neon_superuser;' >> $file; \
            ;; \
          pg_stat_statements--1.10--1.11.sql) \
            # Then with the 1.10-1.11 migration it is re-created with a new signature again, thus add the permissions back
            echo 'GRANT EXECUTE ON FUNCTION pg_stat_statements_reset(Oid, Oid, bigint, boolean) TO neon_superuser;' >> $file; \
            ;; \
        esac; \
    done;

# Set PATH for all the subsequent build steps
ENV PATH="/usr/local/pgsql/bin:$PATH"

#########################################################################################
#
# Layer "postgis-build"
# Build PostGIS from the upstream PostGIS mirror.
#
#########################################################################################
FROM build-deps AS postgis-src
ARG DEBIAN_VERSION
ARG PG_VERSION

# Postgis 3.5.0 requires SFCGAL 1.4+
#
# It would be nice to update all versions together, but we must solve the SFCGAL dependency first.
# SFCGAL > 1.3 requires CGAL > 5.2, Bullseye's libcgal-dev is 5.2
# and also we must check backward compatibility with older versions of PostGIS.
#
# Use new version only for v17
WORKDIR /ext-src
RUN case "${DEBIAN_VERSION}" in \
    "bookworm") \
        export SFCGAL_VERSION=1.4.1 \
        export SFCGAL_CHECKSUM=1800c8a26241588f11cddcf433049e9b9aea902e923414d2ecef33a3295626c3 \
    ;; \
    "bullseye") \
        export SFCGAL_VERSION=1.3.10 \
        export SFCGAL_CHECKSUM=4e39b3b2adada6254a7bdba6d297bb28e1a9835a9f879b74f37e2dab70203232 \
    ;; \
    *) \
        echo "unexpected PostgreSQL version" && exit 1 \
    ;; \
    esac && \
    wget https://gitlab.com/sfcgal/SFCGAL/-/archive/v${SFCGAL_VERSION}/SFCGAL-v${SFCGAL_VERSION}.tar.gz -O SFCGAL.tar.gz && \
    echo "${SFCGAL_CHECKSUM} SFCGAL.tar.gz" | sha256sum --check && \
    mkdir sfcgal-src && cd sfcgal-src && tar xzf ../SFCGAL.tar.gz --strip-components=1 -C .

# Postgis 3.5.0 supports v17
WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
    "v17") \
        export POSTGIS_VERSION=3.5.0 \
        export POSTGIS_CHECKSUM=ca698a22cc2b2b3467ac4e063b43a28413f3004ddd505bdccdd74c56a647f510 \
    ;; \
    "v14" | "v15" | "v16") \
        export POSTGIS_VERSION=3.3.3 \
        export POSTGIS_CHECKSUM=74eb356e3f85f14233791013360881b6748f78081cc688ff9d6f0f673a762d13 \
    ;; \
    *) \
        echo "unexpected PostgreSQL version" && exit 1 \
    ;; \
    esac && \
    wget https://download.osgeo.org/postgis/source/postgis-${POSTGIS_VERSION}.tar.gz -O postgis.tar.gz && \
    echo "${POSTGIS_CHECKSUM} postgis.tar.gz" | sha256sum --check && \
    mkdir postgis-src && cd postgis-src && tar xzf ../postgis.tar.gz --strip-components=1 -C .

# This is reused for pgrouting
FROM pg-build AS postgis-build-deps
RUN apt update && \
    apt install --no-install-recommends --no-install-suggests -y \
    gdal-bin libboost-dev libboost-thread-dev libboost-filesystem-dev \
    libboost-system-dev libboost-iostreams-dev libboost-program-options-dev libboost-timer-dev \
    libcgal-dev libgdal-dev libgmp-dev libmpfr-dev libopenscenegraph-dev libprotobuf-c-dev \
    protobuf-c-compiler xsltproc \
    && apt clean && rm -rf /var/lib/apt/lists/*

FROM postgis-build-deps AS postgis-build
COPY --from=postgis-src /ext-src/ /ext-src/
WORKDIR /ext-src/sfcgal-src
RUN cmake -DCMAKE_BUILD_TYPE=Release -GNinja . && ninja -j $(getconf _NPROCESSORS_ONLN) && \
    DESTDIR=/sfcgal ninja install -j $(getconf _NPROCESSORS_ONLN) && \
    ninja clean && cp -R /sfcgal/* /

WORKDIR /ext-src/postgis-src
RUN ./autogen.sh && \
    ./configure --with-sfcgal=/usr/local/bin/sfcgal-config && \
    make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    cd extensions/postgis && \
    make clean && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/postgis.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/postgis_raster.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/postgis_sfcgal.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/postgis_tiger_geocoder.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/postgis_topology.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/address_standardizer.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/address_standardizer_data_us.control && \
    mkdir -p /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/postgis.control /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/postgis_raster.control /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/postgis_sfcgal.control /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/postgis_tiger_geocoder.control /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/postgis_topology.control /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/address_standardizer.control /extensions/postgis && \
    cp /usr/local/pgsql/share/extension/address_standardizer_data_us.control /extensions/postgis

#########################################################################################
#
# Layer "pgrouting-build"
# Build pgrouting. Note: This depends on the postgis-build-deps layer built above
#
#########################################################################################

# Uses versioned libraries, i.e. libpgrouting-3.4
# and may introduce function signature changes between releases
# i.e. release 3.5.0 has new signature for pg_dijkstra function
#
# Use new version only for v17
# last release v3.6.2 - Mar 30, 2024
FROM build-deps AS pgrouting-src
ARG DEBIAN_VERSION
ARG PG_VERSION
WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
    "v17") \
        export PGROUTING_VERSION=3.6.2 \
        export PGROUTING_CHECKSUM=f4a1ed79d6f714e52548eca3bb8e5593c6745f1bde92eb5fb858efd8984dffa2 \
    ;; \
    "v14" | "v15" | "v16") \
        export PGROUTING_VERSION=3.4.2 \
        export PGROUTING_CHECKSUM=cac297c07d34460887c4f3b522b35c470138760fe358e351ad1db4edb6ee306e \
    ;; \
    *) \
        echo "unexpected PostgreSQL version" && exit 1 \
    ;; \
    esac && \
    wget https://github.com/pgRouting/pgrouting/archive/v${PGROUTING_VERSION}.tar.gz -O pgrouting.tar.gz && \
    echo "${PGROUTING_CHECKSUM} pgrouting.tar.gz" | sha256sum --check && \
    mkdir pgrouting-src && cd pgrouting-src && tar xzf ../pgrouting.tar.gz --strip-components=1 -C .

FROM postgis-build-deps AS pgrouting-build
COPY --from=pgrouting-src /ext-src/ /ext-src/
WORKDIR /ext-src/pgrouting-src
RUN mkdir build && cd build && \
    cmake -GNinja -DCMAKE_BUILD_TYPE=Release .. && \
    ninja -j $(getconf _NPROCESSORS_ONLN) && \
    ninja -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pgrouting.control

#########################################################################################
#
# Layer "plv8-build"
# Build plv8
#
#########################################################################################
FROM build-deps AS plv8-src
ARG PG_VERSION
WORKDIR /ext-src

COPY compute/patches/plv8-3.1.10.patch .

# plv8 3.2.3 supports v17
# last release v3.2.3 - Sep 7, 2024
#
# clone the repo instead of downloading the release tarball because plv8 has submodule dependencies
# and the release tarball doesn't include them
#
# Use new version only for v17
# because since v3.2, plv8 doesn't include plcoffee and plls extensions
RUN case "${PG_VERSION:?}" in \
    "v17") \
        export PLV8_TAG=v3.2.3 \
    ;; \
    "v14" | "v15" | "v16") \
        export PLV8_TAG=v3.1.10 \
    ;; \
    *) \
        echo "unexpected PostgreSQL version" && exit 1 \
    ;; \
    esac && \
    git clone --recurse-submodules --depth 1 --branch ${PLV8_TAG} https://github.com/plv8/plv8.git plv8-src && \
    tar -czf plv8.tar.gz --exclude .git plv8-src && \
    cd plv8-src && \
    if [[ "${PG_VERSION:?}" < "v17" ]]; then patch -p1 < /ext-src/plv8-3.1.10.patch; fi

FROM pg-build AS plv8-build
ARG PG_VERSION
RUN apt update && \
    apt install --no-install-recommends --no-install-suggests -y \
    ninja-build python3-dev libncurses5 binutils clang \
    && apt clean && rm -rf /var/lib/apt/lists/*

COPY --from=plv8-src /ext-src/ /ext-src/
WORKDIR /ext-src/plv8-src
RUN \
    # generate and copy upgrade scripts
    make generate_upgrades && \
    cp upgrade/* /usr/local/pgsql/share/extension/ && \
    make DOCKER=1 -j $(getconf _NPROCESSORS_ONLN) install && \
    rm -rf /plv8-* && \
    find /usr/local/pgsql/ -name "plv8-*.so" | xargs strip && \
    # don't break computes with installed old version of plv8
    cd /usr/local/pgsql/lib/ && \
    case "${PG_VERSION:?}" in \
    "v17") \
        ln -s plv8-3.2.3.so plv8-3.1.8.so && \
        ln -s plv8-3.2.3.so plv8-3.1.5.so && \
        ln -s plv8-3.2.3.so plv8-3.1.10.so \
    ;; \
    "v14" | "v15" | "v16") \
        ln -s plv8-3.1.10.so plv8-3.1.5.so && \
        ln -s plv8-3.1.10.so plv8-3.1.8.so \
    ;; \
    esac && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/plv8.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/plcoffee.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/plls.control

#########################################################################################
#
# Layer "h3-pg-build"
# Build h3_pg
#
#########################################################################################
FROM build-deps AS h3-pg-src
ARG PG_VERSION
WORKDIR /ext-src

# not version-specific
# last release v4.1.0 - Jan 18, 2023
RUN mkdir -p /h3/usr/ && \
    wget https://github.com/uber/h3/archive/refs/tags/v4.1.0.tar.gz -O h3.tar.gz && \
    echo "ec99f1f5974846bde64f4513cf8d2ea1b8d172d2218ab41803bf6a63532272bc h3.tar.gz" | sha256sum --check && \
    mkdir h3-src && cd h3-src && tar xzf ../h3.tar.gz --strip-components=1 -C .

# not version-specific
# last release v4.1.3 - Jul 26, 2023
WORKDIR /ext-src
RUN wget https://github.com/zachasme/h3-pg/archive/refs/tags/v4.1.3.tar.gz -O h3-pg.tar.gz && \
    echo "5c17f09a820859ffe949f847bebf1be98511fb8f1bd86f94932512c00479e324 h3-pg.tar.gz" | sha256sum --check && \
    mkdir h3-pg-src && cd h3-pg-src && tar xzf ../h3-pg.tar.gz --strip-components=1 -C .

FROM pg-build AS h3-pg-build
COPY --from=h3-pg-src /ext-src/ /ext-src/
WORKDIR /ext-src/h3-src
RUN mkdir build && cd build && \
    cmake .. -GNinja -DBUILD_BENCHMARKS=0 -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_FUZZERS=0 -DBUILD_FILTERS=0 -DBUILD_GENERATORS=0 -DBUILD_TESTING=0 \
    && ninja -j $(getconf _NPROCESSORS_ONLN) && \
    DESTDIR=/h3 ninja install && \
    cp -R /h3/usr / && \
    rm -rf build

WORKDIR /ext-src/h3-pg-src
RUN ls -l && \
    make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/h3.control && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/h3_postgis.control

#########################################################################################
#
# Layer "postgresql-unit-build"
# compile unit extension
#
#########################################################################################
FROM build-deps AS postgresql-unit-src
ARG PG_VERSION

# not version-specific
# last release 7.9 - Sep 15, 2024
WORKDIR /ext-src
RUN wget https://github.com/df7cb/postgresql-unit/archive/refs/tags/7.9.tar.gz -O postgresql-unit.tar.gz && \
    echo "e46de6245dcc8b2c2ecf29873dbd43b2b346773f31dd5ce4b8315895a052b456 postgresql-unit.tar.gz" | sha256sum --check && \
    mkdir postgresql-unit-src && cd postgresql-unit-src && tar xzf ../postgresql-unit.tar.gz --strip-components=1 -C .

FROM pg-build AS postgresql-unit-build
COPY --from=postgresql-unit-src /ext-src/ /ext-src/
WORKDIR /ext-src/postgresql-unit-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    # unit extension's "create extension" script relies on absolute install path to fill some reference tables.
    # We move the extension from '/usr/local/pgsql/' to '/usr/local/'  after it is build. So we need to adjust the path.
    # This one-liner removes pgsql/ part of the path.
    # NOTE: Other extensions that rely on MODULEDIR variable after building phase will need the same fix.
    find /usr/local/pgsql/share/extension/ -name "unit*.sql" -print0 | xargs -0 sed -i "s|pgsql/||g" && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/unit.control

#########################################################################################
#
# Layer "pgvector-build"
# compile pgvector extension
#
#########################################################################################
FROM build-deps AS pgvector-src
ARG PG_VERSION

WORKDIR /ext-src
COPY compute/patches/pgvector.patch .

# By default, pgvector Makefile uses `-march=native`. We don't want that,
# because we build the images on different machines than where we run them.
# Pass OPTFLAGS="" to remove it.
#
# vector >0.7.4 supports v17
# last release v0.8.0 - Oct 30, 2024
RUN wget https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.0.tar.gz -O pgvector.tar.gz && \
    echo "867a2c328d4928a5a9d6f052cd3bc78c7d60228a9b914ad32aa3db88e9de27b0 pgvector.tar.gz" | sha256sum --check && \
    mkdir pgvector-src && cd pgvector-src && tar xzf ../pgvector.tar.gz --strip-components=1 -C . && \
    wget https://github.com/pgvector/pgvector/raw/refs/tags/v0.7.4/sql/vector.sql -O ./sql/vector--0.7.4.sql && \
    echo "10218d05dc02299562252a9484775178b14a1d8edb92a2d1672ef488530f7778 ./sql/vector--0.7.4.sql" | sha256sum --check && \
    patch -p1 < /ext-src/pgvector.patch

FROM pg-build AS pgvector-build
COPY --from=pgvector-src /ext-src/ /ext-src/
WORKDIR /ext-src/pgvector-src
RUN make -j $(getconf _NPROCESSORS_ONLN) OPTFLAGS="" && \
    make -j $(getconf _NPROCESSORS_ONLN) OPTFLAGS="" install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/vector.control

#########################################################################################
#
# Layer "pgjwt-build"
# compile pgjwt extension
#
#########################################################################################
FROM build-deps AS pgjwt-src
ARG PG_VERSION

# not version-specific
# doesn't use releases, last commit f3d82fd - Mar 2, 2023
WORKDIR /ext-src
RUN wget https://github.com/michelp/pgjwt/archive/f3d82fd30151e754e19ce5d6a06c71c20689ce3d.tar.gz -O pgjwt.tar.gz && \
    echo "dae8ed99eebb7593b43013f6532d772b12dfecd55548d2673f2dfd0163f6d2b9 pgjwt.tar.gz" | sha256sum --check && \
    mkdir pgjwt-src && cd pgjwt-src && tar xzf ../pgjwt.tar.gz --strip-components=1 -C .

FROM pg-build AS pgjwt-build
COPY --from=pgjwt-src /ext-src/ /ext-src/
WORKDIR /ext-src/pgjwt-src
RUN make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pgjwt.control

#########################################################################################
#
# Layer "hypopg-build"
# compile hypopg extension
#
#########################################################################################
FROM build-deps AS hypopg-src
ARG PG_VERSION

# HypoPG 1.4.1 supports v17
# last release 1.4.1 - Apr 28, 2024
WORKDIR /ext-src
RUN wget https://github.com/HypoPG/hypopg/archive/refs/tags/1.4.1.tar.gz -O hypopg.tar.gz && \
    echo "9afe6357fd389d8d33fad81703038ce520b09275ec00153c6c89282bcdedd6bc hypopg.tar.gz" | sha256sum --check && \
    mkdir hypopg-src && cd hypopg-src && tar xzf ../hypopg.tar.gz --strip-components=1 -C .

FROM pg-build AS hypopg-build
COPY --from=hypopg-src /ext-src/ /ext-src/
WORKDIR /ext-src/hypopg-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/hypopg.control

#########################################################################################
#
# Layer "pg_hashids-build"
# compile pg_hashids extension
#
#########################################################################################
FROM build-deps AS pg_hashids-src
ARG PG_VERSION

# not version-specific
# last release v1.2.1 -Jan 12, 2018
WORKDIR /ext-src
RUN wget https://github.com/iCyberon/pg_hashids/archive/refs/tags/v1.2.1.tar.gz -O pg_hashids.tar.gz && \
    echo "74576b992d9277c92196dd8d816baa2cc2d8046fe102f3dcd7f3c3febed6822a pg_hashids.tar.gz" | sha256sum --check && \
    mkdir pg_hashids-src && cd pg_hashids-src && tar xzf ../pg_hashids.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_hashids-build
COPY --from=pg_hashids-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_hashids-src
RUN make -j $(getconf _NPROCESSORS_ONLN) USE_PGXS=1 && \
    make -j $(getconf _NPROCESSORS_ONLN) install USE_PGXS=1 && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_hashids.control

#########################################################################################
#
# Layer "rum-build"
# compile rum extension
#
#########################################################################################
FROM build-deps AS rum-src
ARG PG_VERSION

WORKDIR /ext-src
COPY compute/patches/rum.patch .

# supports v17 since https://github.com/postgrespro/rum/commit/cb1edffc57736cd2a4455f8d0feab0d69928da25
# doesn't use releases since 1.3.13 - Sep 19, 2022
# use latest commit from the master branch
RUN wget https://github.com/postgrespro/rum/archive/cb1edffc57736cd2a4455f8d0feab0d69928da25.tar.gz -O rum.tar.gz && \
    echo "65e0a752e99f4c3226400c9b899f997049e93503db8bf5c8072efa136d32fd83 rum.tar.gz" | sha256sum --check && \
    mkdir rum-src && cd rum-src && tar xzf ../rum.tar.gz --strip-components=1 -C . && \
    patch -p1 < /ext-src/rum.patch

FROM pg-build AS rum-build
COPY --from=rum-src /ext-src/ /ext-src/
WORKDIR /ext-src/rum-src
RUN make -j $(getconf _NPROCESSORS_ONLN) USE_PGXS=1 && \
    make -j $(getconf _NPROCESSORS_ONLN) install USE_PGXS=1 && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/rum.control

#########################################################################################
#
# Layer "pgtap-build"
# compile pgTAP extension
#
#########################################################################################
FROM build-deps AS pgtap-src
ARG PG_VERSION

# pgtap 1.3.3 supports v17
# last release v1.3.3 - Apr 8, 2024
WORKDIR /ext-src
RUN wget https://github.com/theory/pgtap/archive/refs/tags/v1.3.3.tar.gz -O pgtap.tar.gz && \
    echo "325ea79d0d2515bce96bce43f6823dcd3effbd6c54cb2a4d6c2384fffa3a14c7 pgtap.tar.gz" | sha256sum --check && \
    mkdir pgtap-src && cd pgtap-src && tar xzf ../pgtap.tar.gz --strip-components=1 -C .

FROM pg-build AS pgtap-build
COPY --from=pgtap-src /ext-src/ /ext-src/
WORKDIR /ext-src/pgtap-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pgtap.control

#########################################################################################
#
# Layer "ip4r-build"
# compile ip4r extension
#
#########################################################################################
FROM build-deps AS ip4r-src
ARG PG_VERSION

# not version-specific
# last release v2.4.2 - Jul 29, 2023
WORKDIR /ext-src
RUN wget https://github.com/RhodiumToad/ip4r/archive/refs/tags/2.4.2.tar.gz -O ip4r.tar.gz && \
    echo "0f7b1f159974f49a47842a8ab6751aecca1ed1142b6d5e38d81b064b2ead1b4b ip4r.tar.gz" | sha256sum --check && \
    mkdir ip4r-src && cd ip4r-src && tar xzf ../ip4r.tar.gz --strip-components=1 -C .

FROM pg-build AS ip4r-build
COPY --from=ip4r-src /ext-src/ /ext-src/
WORKDIR /ext-src/ip4r-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/ip4r.control

#########################################################################################
#
# Layer "prefix-build"
# compile Prefix extension
#
#########################################################################################
FROM build-deps AS prefix-src
ARG PG_VERSION

# not version-specific
# last release v1.2.10  - Jul 5, 2023
WORKDIR /ext-src
RUN wget https://github.com/dimitri/prefix/archive/refs/tags/v1.2.10.tar.gz -O prefix.tar.gz && \
    echo "4342f251432a5f6fb05b8597139d3ccde8dcf87e8ca1498e7ee931ca057a8575 prefix.tar.gz" | sha256sum --check && \
    mkdir prefix-src && cd prefix-src && tar xzf ../prefix.tar.gz --strip-components=1 -C .

FROM pg-build AS prefix-build
COPY --from=prefix-src /ext-src/ /ext-src/
WORKDIR /ext-src/prefix-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/prefix.control

#########################################################################################
#
# Layer "hll-build"
# compile hll extension
#
#########################################################################################
FROM build-deps AS hll-src
ARG PG_VERSION

# not version-specific
# last release v2.18 - Aug 29, 2023
WORKDIR /ext-src
RUN wget https://github.com/citusdata/postgresql-hll/archive/refs/tags/v2.18.tar.gz -O hll.tar.gz && \
    echo "e2f55a6f4c4ab95ee4f1b4a2b73280258c5136b161fe9d059559556079694f0e hll.tar.gz" | sha256sum --check && \
    mkdir hll-src && cd hll-src && tar xzf ../hll.tar.gz --strip-components=1 -C .

FROM pg-build AS hll-build
COPY --from=hll-src /ext-src/ /ext-src/
WORKDIR /ext-src/hll-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/hll.control

#########################################################################################
#
# Layer "plpgsql_check-build"
# compile plpgsql_check extension
#
#########################################################################################
FROM build-deps AS plpgsql_check-src
ARG PG_VERSION

# plpgsql_check v2.7.11 supports v17
# last release v2.7.11 - Sep 16, 2024
WORKDIR /ext-src
RUN wget https://github.com/okbob/plpgsql_check/archive/refs/tags/v2.7.11.tar.gz -O plpgsql_check.tar.gz && \
    echo "208933f8dbe8e0d2628eb3851e9f52e6892b8e280c63700c0f1ce7883625d172 plpgsql_check.tar.gz" | sha256sum --check && \
    mkdir plpgsql_check-src && cd plpgsql_check-src && tar xzf ../plpgsql_check.tar.gz --strip-components=1 -C .

FROM pg-build AS plpgsql_check-build
COPY --from=plpgsql_check-src /ext-src/ /ext-src/
WORKDIR /ext-src/plpgsql_check-src
RUN make -j $(getconf _NPROCESSORS_ONLN) USE_PGXS=1 && \
    make -j $(getconf _NPROCESSORS_ONLN) install USE_PGXS=1 && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/plpgsql_check.control

#########################################################################################
#
# Layer "timescaledb-build"
# compile timescaledb extension
#
#########################################################################################
FROM build-deps AS timescaledb-src
ARG PG_VERSION

WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
      "v14" | "v15") \
        export TIMESCALEDB_VERSION=2.10.1 \
        export TIMESCALEDB_CHECKSUM=6fca72a6ed0f6d32d2b3523951ede73dc5f9b0077b38450a029a5f411fdb8c73 \
        ;; \
      "v16") \
        export TIMESCALEDB_VERSION=2.13.0 \
        export TIMESCALEDB_CHECKSUM=584a351c7775f0e067eaa0e7277ea88cab9077cc4c455cbbf09a5d9723dce95d \
        ;; \
      "v17") \
        export TIMESCALEDB_VERSION=2.17.1 \
        export TIMESCALEDB_CHECKSUM=6277cf43f5695e23dae1c5cfeba00474d730b66ed53665a84b787a6bb1a57e28 \
        ;; \
    esac && \
    wget https://github.com/timescale/timescaledb/archive/refs/tags/${TIMESCALEDB_VERSION}.tar.gz -O timescaledb.tar.gz && \
    echo "${TIMESCALEDB_CHECKSUM} timescaledb.tar.gz" | sha256sum --check && \
    mkdir timescaledb-src && cd timescaledb-src && tar xzf ../timescaledb.tar.gz --strip-components=1 -C .

FROM pg-build AS timescaledb-build
COPY --from=timescaledb-src /ext-src/ /ext-src/
WORKDIR /ext-src/timescaledb-src
RUN ./bootstrap -DSEND_TELEMETRY_DEFAULT:BOOL=OFF -DUSE_TELEMETRY:BOOL=OFF -DAPACHE_ONLY:BOOL=ON -DCMAKE_BUILD_TYPE=Release && \
    cd build && \
    make -j $(getconf _NPROCESSORS_ONLN) && \
    make install -j $(getconf _NPROCESSORS_ONLN) && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/timescaledb.control

#########################################################################################
#
# Layer "pg_hint_plan-build"
# compile pg_hint_plan extension
#
#########################################################################################
FROM build-deps AS pg_hint_plan-src
ARG PG_VERSION

# version-specific, has separate releases for each version
WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
      "v14") \
        export PG_HINT_PLAN_VERSION=14_1_4_1 \
        export PG_HINT_PLAN_CHECKSUM=c3501becf70ead27f70626bce80ea401ceac6a77e2083ee5f3ff1f1444ec1ad1 \
        ;; \
      "v15") \
        export PG_HINT_PLAN_VERSION=15_1_5_0 \
        export PG_HINT_PLAN_CHECKSUM=564cbbf4820973ffece63fbf76e3c0af62c4ab23543142c7caaa682bc48918be \
        ;; \
      "v16") \
        export PG_HINT_PLAN_VERSION=16_1_6_0 \
        export PG_HINT_PLAN_CHECKSUM=fc85a9212e7d2819d4ae4ac75817481101833c3cfa9f0fe1f980984e12347d00 \
        ;; \
      "v17") \
        export PG_HINT_PLAN_VERSION=17_1_7_0 \
        export PG_HINT_PLAN_CHECKSUM=06dd306328c67a4248f48403c50444f30959fb61ebe963248dbc2afb396fe600 \
        ;; \
      *) \
        echo "Export the valid PG_HINT_PLAN_VERSION variable" && exit 1 \
        ;; \
    esac && \
    wget https://github.com/ossc-db/pg_hint_plan/archive/refs/tags/REL${PG_HINT_PLAN_VERSION}.tar.gz -O pg_hint_plan.tar.gz && \
    echo "${PG_HINT_PLAN_CHECKSUM} pg_hint_plan.tar.gz" | sha256sum --check && \
    mkdir pg_hint_plan-src && cd pg_hint_plan-src && tar xzf ../pg_hint_plan.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_hint_plan-build
COPY --from=pg_hint_plan-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_hint_plan-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make install -j $(getconf _NPROCESSORS_ONLN) && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/pg_hint_plan.control


#########################################################################################
#
# Layer "pg_cron-build"
# compile pg_cron extension
#
#########################################################################################
FROM build-deps AS pg_cron-src
ARG PG_VERSION

# This is an experimental extension that we do not support on prod yet.
# !Do not remove!
# We set it in shared_preload_libraries and computes will fail to start if library is not found.
WORKDIR /ext-src
COPY compute/patches/pg_cron.patch .
RUN wget https://github.com/citusdata/pg_cron/archive/refs/tags/v1.6.4.tar.gz -O pg_cron.tar.gz && \
    echo "52d1850ee7beb85a4cb7185731ef4e5a90d1de216709d8988324b0d02e76af61 pg_cron.tar.gz" | sha256sum --check && \
    mkdir pg_cron-src && cd pg_cron-src && tar xzf ../pg_cron.tar.gz --strip-components=1 -C . && \
    patch < /ext-src/pg_cron.patch

FROM pg-build AS pg_cron-build
COPY --from=pg_cron-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_cron-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_cron.control

#########################################################################################
#
# Layer "rdkit-build"
# compile rdkit extension
#
#########################################################################################
FROM build-deps AS rdkit-src
ARG PG_VERSION

# rdkit Release_2024_09_1 supports v17
# last release Release_2024_09_1 - Sep 27, 2024
#
# Use new version only for v17
# because Release_2024_09_1 has some backward incompatible changes
# https://github.com/rdkit/rdkit/releases/tag/Release_2024_09_1

WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
    "v17") \
        export RDKIT_VERSION=Release_2024_09_1 \
        export RDKIT_CHECKSUM=034c00d6e9de323506834da03400761ed8c3721095114369d06805409747a60f \
    ;; \
    "v14" | "v15" | "v16") \
        export RDKIT_VERSION=Release_2023_03_3 \
        export RDKIT_CHECKSUM=bdbf9a2e6988526bfeb8c56ce3cdfe2998d60ac289078e2215374288185e8c8d \
    ;; \
    *) \
        echo "unexpected PostgreSQL version" && exit 1 \
    ;; \
    esac && \
    wget https://github.com/rdkit/rdkit/archive/refs/tags/${RDKIT_VERSION}.tar.gz -O rdkit.tar.gz && \
    echo "${RDKIT_CHECKSUM} rdkit.tar.gz" | sha256sum --check && \
    mkdir rdkit-src && cd rdkit-src && tar xzf ../rdkit.tar.gz --strip-components=1 -C .

FROM pg-build AS rdkit-build
RUN apt update && \
    apt install --no-install-recommends --no-install-suggests -y \
        libboost-iostreams1.74-dev \
        libboost-regex1.74-dev \
        libboost-serialization1.74-dev \
        libboost-system1.74-dev \
        libeigen3-dev \
        libboost-all-dev \
    && apt clean && rm -rf /var/lib/apt/lists/*

COPY --from=rdkit-src /ext-src/ /ext-src/
WORKDIR /ext-src/rdkit-src

# XXX: /usr/local/pgsql/bin is already in PATH, and that should be enough to find
# pg_config. For some reason the rdkit cmake script doesn't work with just that,
# however. By also adding /usr/local/pgsql, it works, which is weird because there
# are no executables in that directory.
ENV PATH="/usr/local/pgsql:$PATH"
RUN cmake \
        -D RDK_BUILD_CAIRO_SUPPORT=OFF \
        -D RDK_BUILD_INCHI_SUPPORT=ON \
        -D RDK_BUILD_AVALON_SUPPORT=ON \
        -D RDK_BUILD_PYTHON_WRAPPERS=OFF \
        -D RDK_BUILD_DESCRIPTORS3D=OFF \
        -D RDK_BUILD_FREESASA_SUPPORT=OFF \
        -D RDK_BUILD_COORDGEN_SUPPORT=ON \
        -D RDK_BUILD_MOLINTERCHANGE_SUPPORT=OFF \
        -D RDK_BUILD_YAEHMOP_SUPPORT=OFF \
        -D RDK_BUILD_STRUCTCHECKER_SUPPORT=OFF \
        -D RDK_TEST_MULTITHREADED=OFF \
        -D RDK_BUILD_CPP_TESTS=OFF \
        -D RDK_USE_URF=OFF \
        -D RDK_BUILD_PGSQL=ON \
        -D RDK_PGSQL_STATIC=ON \
        -D PostgreSQL_CONFIG=pg_config \
        -D PostgreSQL_INCLUDE_DIR=`pg_config --includedir` \
        -D PostgreSQL_TYPE_INCLUDE_DIR=`pg_config --includedir-server` \
        -D PostgreSQL_LIBRARY_DIR=`pg_config --libdir` \
        -D RDK_INSTALL_INTREE=OFF \
        -D RDK_INSTALL_COMIC_FONTS=OFF \
        -D RDK_BUILD_FREETYPE_SUPPORT=OFF \
        -D CMAKE_BUILD_TYPE=Release \
        -GNinja \
        . && \
    ninja -j $(getconf _NPROCESSORS_ONLN) && \
    ninja -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/rdkit.control

#########################################################################################
#
# Layer "pg_uuidv7-build"
# compile pg_uuidv7 extension
#
#########################################################################################
FROM build-deps AS pg_uuidv7-src
ARG PG_VERSION

# not version-specific
# last release v1.6.0 - Oct 9, 2024
WORKDIR /ext-src
RUN wget https://github.com/fboulnois/pg_uuidv7/archive/refs/tags/v1.6.0.tar.gz -O pg_uuidv7.tar.gz && \
    echo "0fa6c710929d003f6ce276a7de7a864e9d1667b2d78be3dc2c07f2409eb55867 pg_uuidv7.tar.gz" | sha256sum --check && \
    mkdir pg_uuidv7-src && cd pg_uuidv7-src && tar xzf ../pg_uuidv7.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_uuidv7-build
COPY --from=pg_uuidv7-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_uuidv7-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_uuidv7.control

#########################################################################################
#
# Layer "pg_roaringbitmap-build"
# compile pg_roaringbitmap extension
#
#########################################################################################
FROM build-deps AS pg_roaringbitmap-src
ARG PG_VERSION

# not version-specific
# last release v0.5.4 - Jun 28, 2022
WORKDIR /ext-src
RUN wget https://github.com/ChenHuajun/pg_roaringbitmap/archive/refs/tags/v0.5.4.tar.gz -O pg_roaringbitmap.tar.gz && \
    echo "b75201efcb1c2d1b014ec4ae6a22769cc7a224e6e406a587f5784a37b6b5a2aa pg_roaringbitmap.tar.gz" | sha256sum --check && \
    mkdir pg_roaringbitmap-src && cd pg_roaringbitmap-src && tar xzf ../pg_roaringbitmap.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_roaringbitmap-build
COPY --from=pg_roaringbitmap-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_roaringbitmap-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/roaringbitmap.control

#########################################################################################
#
# Layer "pg_semver-build"
# compile pg_semver extension
#
#########################################################################################
FROM build-deps AS pg_semver-src
ARG PG_VERSION

# Release 0.40.0 breaks backward compatibility with previous versions
# see release note https://github.com/theory/pg-semver/releases/tag/v0.40.0
# Use new version only for v17
#
# last release v0.40.0 - Jul 22, 2024
WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
    "v17") \
        export SEMVER_VERSION=0.40.0 \
        export SEMVER_CHECKSUM=3e50bcc29a0e2e481e7b6d2bc937cadc5f5869f55d983b5a1aafeb49f5425cfc \
    ;; \
    "v14" | "v15" | "v16") \
        export SEMVER_VERSION=0.32.1 \
        export SEMVER_CHECKSUM=fbdaf7512026d62eec03fad8687c15ed509b6ba395bff140acd63d2e4fbe25d7 \
    ;; \
    *) \
        echo "unexpected PostgreSQL version" && exit 1 \
    ;; \
    esac && \
    wget https://github.com/theory/pg-semver/archive/refs/tags/v${SEMVER_VERSION}.tar.gz -O pg_semver.tar.gz && \
    echo "${SEMVER_CHECKSUM} pg_semver.tar.gz" | sha256sum --check && \
    mkdir pg_semver-src && cd pg_semver-src && tar xzf ../pg_semver.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_semver-build
COPY --from=pg_semver-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_semver-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/semver.control

#########################################################################################
#
# Layer "pg_embedding-build"
# compile pg_embedding extension
#
#########################################################################################
FROM build-deps AS pg_embedding-src
ARG PG_VERSION

# This is our extension, support stopped in favor of pgvector
# TODO: deprecate it
WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
      "v14" | "v15") \
        export PG_EMBEDDING_VERSION=0.3.5 \
        export PG_EMBEDDING_CHECKSUM=0e95b27b8b6196e2cf0a0c9ec143fe2219b82e54c5bb4ee064e76398cbe69ae9 \
        ;; \
      *) \
        echo "pg_embedding not supported on this PostgreSQL version. Use pgvector instead." && exit 0;; \
    esac && \
    wget https://github.com/neondatabase/pg_embedding/archive/refs/tags/${PG_EMBEDDING_VERSION}.tar.gz -O pg_embedding.tar.gz && \
    echo "${PG_EMBEDDING_CHECKSUM} pg_embedding.tar.gz" | sha256sum --check && \
    mkdir pg_embedding-src && cd pg_embedding-src && tar xzf ../pg_embedding.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_embedding-build
COPY --from=pg_embedding-src /ext-src/ /ext-src/
WORKDIR /ext-src/
RUN  if [ -d pg_embedding-src ]; then \
        cd pg_embedding-src && \
        make -j $(getconf _NPROCESSORS_ONLN) && \
        make -j $(getconf _NPROCESSORS_ONLN) install; \
    fi

#########################################################################################
#
# Layer "pg_anon-build"
# compile anon extension
#
#########################################################################################
FROM build-deps AS pg_anon-src
ARG PG_VERSION

# This is an experimental extension, never got to real production.
# !Do not remove! It can be present in shared_preload_libraries and compute will fail to start if library is not found.
WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in "v17") \
    echo "postgresql_anonymizer does not yet support PG17" && exit 0;; \
    esac && \
    wget  https://github.com/neondatabase/postgresql_anonymizer/archive/refs/tags/neon_1.1.1.tar.gz -O pg_anon.tar.gz && \
    echo "321ea8d5c1648880aafde850a2c576e4a9e7b9933a34ce272efc839328999fa9  pg_anon.tar.gz" | sha256sum --check && \
    mkdir pg_anon-src && cd pg_anon-src && tar xzf ../pg_anon.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_anon-build
COPY --from=pg_anon-src /ext-src/ /ext-src/
WORKDIR /ext-src
RUN if [ -d pg_anon-src ]; then \
        cd pg_anon-src && \
        make -j $(getconf _NPROCESSORS_ONLN) install && \
        echo 'trusted = true' >> /usr/local/pgsql/share/extension/anon.control; \
    fi

#########################################################################################
#
# Layer "pg build with nonroot user and cargo installed"
# This layer is base and common for layers with `pgrx`
#
#########################################################################################
FROM pg-build AS pg-build-nonroot-with-cargo
ARG PG_VERSION

RUN apt update && \
    apt install --no-install-recommends --no-install-suggests -y curl libclang-dev && \
    apt clean && rm -rf /var/lib/apt/lists/* && \
    useradd -ms /bin/bash nonroot -b /home

ENV HOME=/home/nonroot
ENV PATH="/home/nonroot/.cargo/bin:$PATH"
USER nonroot
WORKDIR /home/nonroot

# See comment on the top of the file regading `echo` and `\n`
RUN echo -e "--retry-connrefused\n--connect-timeout 15\n--retry 5\n--max-time 300\n" > /home/nonroot/.curlrc

RUN curl -sSO https://static.rust-lang.org/rustup/dist/$(uname -m)-unknown-linux-gnu/rustup-init && \
    chmod +x rustup-init && \
    ./rustup-init -y --no-modify-path --profile minimal --default-toolchain stable && \
    rm rustup-init

#########################################################################################
#
# Layer "rust extensions"
# This layer is used to build `pgrx` deps
#
#########################################################################################
FROM pg-build-nonroot-with-cargo AS rust-extensions-build
ARG PG_VERSION

RUN case "${PG_VERSION:?}" in \
        'v17') \
            echo 'v17 is not supported yet by pgrx. Quit' && exit 0;; \
    esac && \
    cargo install --locked --version 0.11.3 cargo-pgrx && \
    /bin/bash -c 'cargo pgrx init --pg${PG_VERSION:1}=/usr/local/pgsql/bin/pg_config'

USER root

#########################################################################################
#
# Layer "rust extensions pgrx12"
#
# pgrx started to support Postgres 17 since version 12,
# but some older extension aren't compatible with it.
# This layer should be used as a base for new pgrx extensions,
# and eventually get merged with `rust-extensions-build`
#
#########################################################################################
FROM pg-build-nonroot-with-cargo AS rust-extensions-build-pgrx12
ARG PG_VERSION

RUN cargo install --locked --version 0.12.9 cargo-pgrx && \
    /bin/bash -c 'cargo pgrx init --pg${PG_VERSION:1}=/usr/local/pgsql/bin/pg_config'

USER root

#########################################################################################
#
# Layers "pg-onnx-build" and "pgrag-build"
# Compile "pgrag" extensions
#
#########################################################################################

FROM build-deps AS pgrag-src
ARG PG_VERSION

WORKDIR /ext-src
RUN wget https://github.com/microsoft/onnxruntime/archive/refs/tags/v1.18.1.tar.gz -O onnxruntime.tar.gz && \
    mkdir onnxruntime-src && cd onnxruntime-src && tar xzf ../onnxruntime.tar.gz --strip-components=1 -C . && \
    echo "#nothing to test here" > neon-test.sh

RUN wget https://github.com/neondatabase-labs/pgrag/archive/refs/tags/v0.0.0.tar.gz -O pgrag.tar.gz &&  \
    echo "2cbe394c1e74fc8bcad9b52d5fbbfb783aef834ca3ce44626cfd770573700bb4 pgrag.tar.gz" | sha256sum --check && \
    mkdir pgrag-src && cd pgrag-src && tar xzf ../pgrag.tar.gz --strip-components=1 -C .

FROM rust-extensions-build-pgrx12 AS pgrag-build
COPY --from=pgrag-src /ext-src/ /ext-src/

# Install build-time dependencies
# cmake 3.26 or higher is required, so installing it using pip (bullseye-backports has cmake 3.25).
# Install it using virtual environment, because Python 3.11 (the default version on Debian 12 (Bookworm)) complains otherwise
WORKDIR /ext-src/onnxruntime-src
RUN apt update && apt install --no-install-recommends --no-install-suggests -y \
    python3 python3-pip python3-venv protobuf-compiler && \
    apt clean && rm -rf /var/lib/apt/lists/* && \
    python3 -m venv venv && \
    . venv/bin/activate && \
    python3 -m pip install cmake==3.30.5

RUN . venv/bin/activate && \
    ./build.sh --config Release --parallel --cmake_generator Ninja \
    --skip_submodule_sync --skip_tests --allow_running_as_root

WORKDIR /ext-src/pgrag-src
RUN cd exts/rag && \
    sed -i 's/pgrx = "0.12.6"/pgrx = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml && \
    cargo pgrx install --release && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/rag.control

RUN cd exts/rag_bge_small_en_v15 && \
    sed -i 's/pgrx = "0.12.6"/pgrx = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml && \
    ORT_LIB_LOCATION=/ext-src/onnxruntime-src/build/Linux \
        REMOTE_ONNX_URL=http://pg-ext-s3-gateway/pgrag-data/bge_small_en_v15.onnx \
        cargo pgrx install --release --features remote_onnx && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/rag_bge_small_en_v15.control

RUN cd exts/rag_jina_reranker_v1_tiny_en && \
    sed -i 's/pgrx = "0.12.6"/pgrx = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml && \
    ORT_LIB_LOCATION=/ext-src/onnxruntime-src/build/Linux \
        REMOTE_ONNX_URL=http://pg-ext-s3-gateway/pgrag-data/jina_reranker_v1_tiny_en.onnx \
        cargo pgrx install --release --features remote_onnx && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/rag_jina_reranker_v1_tiny_en.control


#########################################################################################
#
# Layer "pg_jsonschema-build"
# Compile "pg_jsonschema" extension
#
#########################################################################################

FROM build-deps AS pg_jsonschema-src
ARG PG_VERSION
# last release v0.3.3 - Oct 16, 2024
WORKDIR /ext-src
RUN wget https://github.com/supabase/pg_jsonschema/archive/refs/tags/v0.3.3.tar.gz -O pg_jsonschema.tar.gz && \
    echo "40c2cffab4187e0233cb8c3bde013be92218c282f95f4469c5282f6b30d64eac pg_jsonschema.tar.gz" | sha256sum --check && \
    mkdir pg_jsonschema-src && cd pg_jsonschema-src && tar xzf ../pg_jsonschema.tar.gz --strip-components=1 -C .

FROM rust-extensions-build-pgrx12 AS pg_jsonschema-build
COPY --from=pg_jsonschema-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_jsonschema-src
RUN \
    # see commit 252b3685a27a0f4c31a0f91e983c6314838e89e8
    # `unsafe-postgres` feature allows to build pgx extensions
    # against postgres forks that decided to change their ABI name (like us).
    # With that we can build extensions without forking them and using stock
    # pgx. As this feature is new few manual version bumps were required.
    sed -i 's/pgrx = "0.12.6"/pgrx = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml && \
    sed -i 's/pgrx-tests = "0.12.6"/pgrx-tests = "0.12.9"/g' Cargo.toml && \
    cargo pgrx install --release && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/pg_jsonschema.control

#########################################################################################
#
# Layer "pg_graphql-build"
# Compile "pg_graphql" extension
#
#########################################################################################

FROM build-deps AS pg_graphql-src
ARG PG_VERSION

# last release v1.5.9 - Oct 16, 2024
WORKDIR /ext-src
COPY compute/patches/pg_graphql.patch .
RUN wget https://github.com/supabase/pg_graphql/archive/refs/tags/v1.5.9.tar.gz -O pg_graphql.tar.gz && \
    echo "cf768385a41278be1333472204fc0328118644ae443182cf52f7b9b23277e497 pg_graphql.tar.gz" | sha256sum --check && \
    mkdir pg_graphql-src && cd pg_graphql-src && tar xzf ../pg_graphql.tar.gz --strip-components=1 -C . && \
    sed -i 's/pgrx = "=0.12.6"/pgrx = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml && \
    sed -i 's/pgrx-tests = "=0.12.6"/pgrx-tests = "=0.12.9"/g' Cargo.toml && \
    patch -p1 < /ext-src/pg_graphql.patch


FROM rust-extensions-build-pgrx12 AS pg_graphql-build
COPY --from=pg_graphql-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_graphql-src
RUN cargo pgrx install --release && \
    # it's needed to enable extension because it uses untrusted C language
    sed -i 's/superuser = false/superuser = true/g' /usr/local/pgsql/share/extension/pg_graphql.control && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/pg_graphql.control

#########################################################################################
#
# Layer "pg_tiktoken-build"
# Compile "pg_tiktoken" extension
#
#########################################################################################

FROM build-deps AS pg_tiktoken-src
ARG PG_VERSION

# doesn't use releases
# 9118dd4549b7d8c0bbc98e04322499f7bf2fa6f7 - on Oct 29, 2024
WORKDIR /ext-src
RUN wget https://github.com/kelvich/pg_tiktoken/archive/9118dd4549b7d8c0bbc98e04322499f7bf2fa6f7.tar.gz -O pg_tiktoken.tar.gz && \
    echo "a5bc447e7920ee149d3c064b8b9f0086c0e83939499753178f7d35788416f628 pg_tiktoken.tar.gz" | sha256sum --check && \
    mkdir pg_tiktoken-src && cd pg_tiktoken-src && tar xzf ../pg_tiktoken.tar.gz --strip-components=1 -C . && \
    sed -i 's/pgrx = { version = "=0.12.6",/pgrx = { version = "0.12.9",/g' Cargo.toml && \
    sed -i 's/pgrx-tests = "=0.12.6"/pgrx-tests = "0.12.9"/g' Cargo.toml

FROM rust-extensions-build-pgrx12 AS pg_tiktoken-build
COPY --from=pg_tiktoken-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_tiktoken-src
RUN cargo pgrx install --release && \
    echo "trusted = true" >> /usr/local/pgsql/share/extension/pg_tiktoken.control

#########################################################################################
#
# Layer "pgx_ulid-build"
# Compile "pgx_ulid" extension for v16 and below
#
#########################################################################################

FROM build-deps AS pgx_ulid-src
ARG PG_VERSION

WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
    "v14" | "v15" | "v16") \
        ;; \
    *) \
        echo "skipping the version of pgx_ulid for $PG_VERSION" && exit 0 \
        ;; \
    esac && \
    wget https://github.com/pksunkara/pgx_ulid/archive/refs/tags/v0.1.5.tar.gz -O pgx_ulid.tar.gz && \
    echo "9d1659a2da65af0133d5451c454de31b37364e3502087dadf579f790bc8bef17  pgx_ulid.tar.gz" | sha256sum --check && \
    mkdir pgx_ulid-src && cd pgx_ulid-src && tar xzf ../pgx_ulid.tar.gz --strip-components=1 -C . && \
    sed -i 's/pgrx       = "^0.11.2"/pgrx = { version = "=0.11.3", features = [ "unsafe-postgres" ] }/g' Cargo.toml

FROM rust-extensions-build AS pgx_ulid-build
COPY --from=pgx_ulid-src /ext-src/ /ext-src/
WORKDIR /ext-src/
RUN if [ -d pgx_ulid-src ]; then \
        cd pgx_ulid-src && \
        cargo pgrx install --release && \
        echo 'trusted = true' >> /usr/local/pgsql/share/extension/ulid.control; \
    fi

#########################################################################################
#
# Layer "pgx_ulid-pgrx12-build"
# Compile "pgx_ulid" extension for v17 and up
#
#########################################################################################

FROM build-deps AS pgx_ulid-pgrx12-src
ARG PG_VERSION

WORKDIR /ext-src
RUN case "${PG_VERSION:?}" in \
    "v17") \
        ;; \
    *) \
        echo "skipping the version of pgx_ulid for $PG_VERSION" && exit 0 \
        ;; \
    esac && \
    wget https://github.com/pksunkara/pgx_ulid/archive/refs/tags/v0.2.0.tar.gz -O pgx_ulid.tar.gz && \
    echo "cef6a9a2e5e7bd1a10a18989286586ee9e6c1c06005a4055cff190de41bf3e9f pgx_ulid.tar.gz" | sha256sum --check && \
    mkdir pgx_ulid-src && cd pgx_ulid-src && tar xzf ../pgx_ulid.tar.gz --strip-components=1 -C . && \
    sed -i 's/pgrx       = "^0.12.7"/pgrx       = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml

FROM rust-extensions-build-pgrx12 AS pgx_ulid-pgrx12-build
ARG PG_VERSION
WORKDIR /ext-src
COPY --from=pgx_ulid-pgrx12-src /ext-src/ /ext-src/
RUN if [ -d pgx_ulid-src ]; then \
        cd pgx_ulid-src && \
        cargo pgrx install --release && \
        echo 'trusted = true' >> /usr/local/pgsql/share/extension/pgx_ulid.control; \
    fi

#########################################################################################
#
# Layer "pg_session_jwt-build"
# Compile "pg_session_jwt" extension
#
#########################################################################################

FROM build-deps AS pg_session_jwt-src
ARG PG_VERSION

# NOTE: local_proxy depends on the version of pg_session_jwt
# Do not update without approve from proxy team
# Make sure the version is reflected in proxy/src/serverless/local_conn_pool.rs
WORKDIR /ext-src
RUN wget https://github.com/neondatabase/pg_session_jwt/archive/refs/tags/v0.2.0.tar.gz -O pg_session_jwt.tar.gz && \
    echo "5ace028e591f2e000ca10afa5b1ca62203ebff014c2907c0ec3b29c36f28a1bb pg_session_jwt.tar.gz" | sha256sum --check && \
    mkdir pg_session_jwt-src && cd pg_session_jwt-src && tar xzf ../pg_session_jwt.tar.gz --strip-components=1 -C . && \
    sed -i 's/pgrx = "0.12.6"/pgrx = { version = "0.12.9", features = [ "unsafe-postgres" ] }/g' Cargo.toml && \
    sed -i 's/version = "0.12.6"/version = "0.12.9"/g' pgrx-tests/Cargo.toml && \
    sed -i 's/pgrx = "=0.12.6"/pgrx = { version = "=0.12.9", features = [ "unsafe-postgres" ] }/g' pgrx-tests/Cargo.toml && \
    sed -i 's/pgrx-macros = "=0.12.6"/pgrx-macros = "=0.12.9"/g' pgrx-tests/Cargo.toml && \
    sed -i 's/pgrx-pg-config = "=0.12.6"/pgrx-pg-config = "=0.12.9"/g' pgrx-tests/Cargo.toml

FROM rust-extensions-build-pgrx12 AS pg_session_jwt-build
COPY --from=pg_session_jwt-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_session_jwt-src
RUN cargo pgrx install --release

#########################################################################################
#
# Layer "wal2json-build"
# Compile "wal2json" extension
#
#########################################################################################

FROM build-deps AS wal2json-src
ARG PG_VERSION

# wal2json wal2json_2_6 supports v17
# last release wal2json_2_6 - Apr 25, 2024
WORKDIR /ext-src
RUN wget https://github.com/eulerto/wal2json/archive/refs/tags/wal2json_2_6.tar.gz -O wal2json.tar.gz && \
    echo "18b4bdec28c74a8fc98a11c72de38378a760327ef8e5e42e975b0029eb96ba0d wal2json.tar.gz" | sha256sum --check && \
    mkdir wal2json-src && cd wal2json-src && tar xzf ../wal2json.tar.gz --strip-components=1 -C .

FROM pg-build AS wal2json-build
COPY --from=wal2json-src /ext-src/ /ext-src/
WORKDIR /ext-src/wal2json-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install

#########################################################################################
#
# Layer "pg_ivm"
# compile pg_ivm extension
#
#########################################################################################
FROM build-deps AS pg_ivm-src
ARG PG_VERSION

# pg_ivm v1.9 supports v17
# last release v1.9 - Jul 31
WORKDIR /ext-src
RUN wget https://github.com/sraoss/pg_ivm/archive/refs/tags/v1.9.tar.gz -O pg_ivm.tar.gz && \
    echo "59e15722939f274650abf637f315dd723c87073496ca77236b044cb205270d8b pg_ivm.tar.gz" | sha256sum --check && \
    mkdir pg_ivm-src && cd pg_ivm-src && tar xzf ../pg_ivm.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_ivm-build
COPY --from=pg_ivm-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_ivm-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_ivm.control

#########################################################################################
#
# Layer "pg_partman"
# compile pg_partman extension
#
#########################################################################################
FROM build-deps AS pg_partman-src
ARG PG_VERSION

# should support v17 https://github.com/pgpartman/pg_partman/discussions/693
# last release 5.1.0  Apr 2, 2024
WORKDIR /ext-src
RUN wget https://github.com/pgpartman/pg_partman/archive/refs/tags/v5.1.0.tar.gz -O pg_partman.tar.gz && \
    echo "3e3a27d7ff827295d5c55ef72f07a49062d6204b3cb0b9a048645d6db9f3cb9f pg_partman.tar.gz" | sha256sum --check && \
    mkdir pg_partman-src && cd pg_partman-src && tar xzf ../pg_partman.tar.gz --strip-components=1 -C .

FROM pg-build AS pg_partman-build
COPY --from=pg_partman-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_partman-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_partman.control

#########################################################################################
#
# Layer "pg_mooncake"
# compile pg_mooncake extension
#
#########################################################################################
FROM build-deps AS pg_mooncake-src
ARG PG_VERSION
WORKDIR /ext-src
RUN wget https://github.com/Mooncake-Labs/pg_mooncake/releases/download/v0.1.2/pg_mooncake-0.1.2.tar.gz -O pg_mooncake.tar.gz && \
    echo "4550473784fcdd2e1e18062bc01eb9c286abd27cdf5e11a4399be6c0a426ba90 pg_mooncake.tar.gz" | sha256sum --check && \
    mkdir pg_mooncake-src && cd pg_mooncake-src && tar xzf ../pg_mooncake.tar.gz --strip-components=1 -C . && \
    echo "make -f pg_mooncake-src/Makefile.build installcheck TEST_DIR=./test SQL_DIR=./sql SRC_DIR=./src" > neon-test.sh && \
    chmod a+x neon-test.sh

FROM rust-extensions-build AS pg_mooncake-build
COPY --from=pg_mooncake-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_mooncake-src
RUN make release -j $(getconf _NPROCESSORS_ONLN) && \
    make install -j $(getconf _NPROCESSORS_ONLN) && \
    echo 'trusted = true' >> /usr/local/pgsql/share/extension/pg_mooncake.control

#########################################################################################
#
# Layer "pg_repack"
# compile pg_repack extension
#
#########################################################################################

FROM build-deps AS pg_repack-src
ARG PG_VERSION
WORKDIR /ext-src
RUN wget https://github.com/reorg/pg_repack/archive/refs/tags/ver_1.5.2.tar.gz -O pg_repack.tar.gz && \
    echo '4516cad42251ed3ad53ff619733004db47d5755acac83f75924cd94d1c4fb681 pg_repack.tar.gz' | sha256sum --check && \
    mkdir pg_repack-src && cd pg_repack-src && tar xzf ../pg_repack.tar.gz --strip-components=1 -C .

FROM rust-extensions-build AS pg_repack-build
COPY --from=pg_repack-src /ext-src/ /ext-src/
WORKDIR /ext-src/pg_repack-src
RUN make -j $(getconf _NPROCESSORS_ONLN) && \
    make -j $(getconf _NPROCESSORS_ONLN) install

#########################################################################################
#
# Layer "neon-ext-build"
# compile neon extensions
#
#########################################################################################
FROM pg-build AS neon-ext-build
ARG PG_VERSION

COPY pgxn/ pgxn/
RUN make -j $(getconf _NPROCESSORS_ONLN) \
        -C pgxn/neon \
        -s install && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        -C pgxn/neon_utils \
        -s install && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        -C pgxn/neon_test_utils \
        -s install && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        -C pgxn/neon_rmgr \
        -s install

#########################################################################################
#
# Layer "extensions-none"
#
#########################################################################################
FROM build-deps AS extensions-none

RUN mkdir /usr/local/pgsql

#########################################################################################
#
# Layer "extensions-minimal"
#
# This subset of extensions includes the extensions that we have in
# shared_preload_libraries by default.
#
#########################################################################################
FROM build-deps AS extensions-minimal

COPY --from=pgrag-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=timescaledb-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_cron-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_partman-build /usr/local/pgsql/ /usr/local/pgsql/

#########################################################################################
#
# Layer "extensions-all"
# Bundle together all the extensions
#
#########################################################################################
FROM build-deps AS extensions-all

# Public extensions
COPY --from=postgis-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=postgis-build /sfcgal/* /
COPY --from=pgrouting-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=plv8-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=h3-pg-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=h3-pg-build /h3/usr /
COPY --from=postgresql-unit-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pgvector-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pgjwt-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pgrag-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_jsonschema-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_graphql-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_tiktoken-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=hypopg-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_hashids-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=rum-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pgtap-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=ip4r-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=prefix-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=hll-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=plpgsql_check-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=timescaledb-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_hint_plan-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_cron-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pgx_ulid-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pgx_ulid-pgrx12-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_session_jwt-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=rdkit-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_uuidv7-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_roaringbitmap-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_semver-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_embedding-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=wal2json-build /usr/local/pgsql /usr/local/pgsql
COPY --from=pg_anon-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_ivm-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_partman-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_mooncake-build /usr/local/pgsql/ /usr/local/pgsql/
COPY --from=pg_repack-build /usr/local/pgsql/ /usr/local/pgsql/

#########################################################################################
#
# Layer "neon-pg-ext-build"
# Includes Postgres and all the extensions chosen by EXTENSIONS arg.
#
#########################################################################################
FROM extensions-${EXTENSIONS} AS neon-pg-ext-build

#########################################################################################
#
# Compile the Neon-specific `compute_ctl`, `fast_import`, and `local_proxy` binaries
#
#########################################################################################
FROM $REPOSITORY/$IMAGE:$TAG AS compute-tools
ARG BUILD_TAG
ENV BUILD_TAG=$BUILD_TAG

USER nonroot
# Copy entire project to get Cargo.* files with proper dependencies for the whole project
COPY --chown=nonroot . .
RUN --mount=type=cache,uid=1000,target=/home/nonroot/.cargo/registry \
    --mount=type=cache,uid=1000,target=/home/nonroot/.cargo/git \
    --mount=type=cache,uid=1000,target=/home/nonroot/target \
    mold -run cargo build --locked --profile release-line-debug-size-lto --bin compute_ctl --bin fast_import --bin local_proxy && \
    mkdir target-bin && \
    cp target/release-line-debug-size-lto/compute_ctl \
       target/release-line-debug-size-lto/fast_import \
       target/release-line-debug-size-lto/local_proxy \
       target-bin

#########################################################################################
#
# Layer "pgbouncer"
#
#########################################################################################

FROM $BASE_IMAGE_SHA AS pgbouncer
RUN set -e \
    && echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries \
    && apt update \
    && apt install --no-install-suggests --no-install-recommends -y \
        build-essential \
        git \
        ca-certificates \
        autoconf \
        automake \
        libevent-dev \
        libtool \
        pkg-config \
    && apt clean && rm -rf /var/lib/apt/lists/*

# Use `dist_man_MANS=` to skip manpage generation (which requires python3/pandoc)
ENV PGBOUNCER_TAG=pgbouncer_1_22_1
RUN set -e \
    && git clone --recurse-submodules --depth 1 --branch ${PGBOUNCER_TAG} https://github.com/pgbouncer/pgbouncer.git pgbouncer \
    && cd pgbouncer \
    && ./autogen.sh \
    && ./configure --prefix=/usr/local/pgbouncer --without-openssl \
    && make -j $(nproc) dist_man_MANS= \
    && make install dist_man_MANS=

#########################################################################################
#
# Layer "exporters"
#
#########################################################################################
FROM build-deps AS exporters
ARG TARGETARCH
# Keep sql_exporter version same as in build-tools.Dockerfile and
# test_runner/regress/test_compute_metrics.py
# See comment on the top of the file regading `echo`, `-e` and `\n`
RUN if [ "$TARGETARCH" = "amd64" ]; then\
        postgres_exporter_sha256='027e75dda7af621237ff8f5ac66b78a40b0093595f06768612b92b1374bd3105';\
        pgbouncer_exporter_sha256='c9f7cf8dcff44f0472057e9bf52613d93f3ffbc381ad7547a959daa63c5e84ac';\
        sql_exporter_sha256='38e439732bbf6e28ca4a94d7bc3686d3fa1abdb0050773d5617a9efdb9e64d08';\
    else\
        postgres_exporter_sha256='131a376d25778ff9701a4c81f703f179e0b58db5c2c496e66fa43f8179484786';\
        pgbouncer_exporter_sha256='217c4afd7e6492ae904055bc14fe603552cf9bac458c063407e991d68c519da3';\
        sql_exporter_sha256='11918b00be6e2c3a67564adfdb2414fdcbb15a5db76ea17d1d1a944237a893c6';\
    fi\
    && curl -sL https://github.com/prometheus-community/postgres_exporter/releases/download/v0.16.0/postgres_exporter-0.16.0.linux-${TARGETARCH}.tar.gz\
     | tar xzf - --strip-components=1 -C.\
    && curl -sL https://github.com/prometheus-community/pgbouncer_exporter/releases/download/v0.10.2/pgbouncer_exporter-0.10.2.linux-${TARGETARCH}.tar.gz\
     | tar xzf - --strip-components=1 -C.\
    && curl -sL https://github.com/burningalchemist/sql_exporter/releases/download/0.17.0/sql_exporter-0.17.0.linux-${TARGETARCH}.tar.gz\
     | tar xzf - --strip-components=1 -C.\
    && echo "${postgres_exporter_sha256} postgres_exporter" | sha256sum -c -\
    && echo "${pgbouncer_exporter_sha256} pgbouncer_exporter" | sha256sum -c -\
    && echo "${sql_exporter_sha256} sql_exporter" | sha256sum -c -

#########################################################################################
#
# Layer "awscli"
#
#########################################################################################
FROM build-deps AS awscli
ARG TARGETARCH
RUN set -ex; \
    if [ "${TARGETARCH}" = "amd64" ]; then \
        TARGETARCH_ALT="x86_64"; \
        CHECKSUM="c9a9df3770a3ff9259cb469b6179e02829687a464e0824d5c32d378820b53a00"; \
    elif [ "${TARGETARCH}" = "arm64" ]; then \
        TARGETARCH_ALT="aarch64"; \
        CHECKSUM="8181730be7891582b38b028112e81b4899ca817e8c616aad807c9e9d1289223a"; \
    else \
        echo "Unsupported architecture: ${TARGETARCH}"; exit 1; \
    fi; \
    curl --retry 5 -L "https://awscli.amazonaws.com/awscli-exe-linux-${TARGETARCH_ALT}-2.17.5.zip" -o /tmp/awscliv2.zip; \
    echo "${CHECKSUM}  /tmp/awscliv2.zip" | sha256sum -c -; \
    unzip /tmp/awscliv2.zip -d /tmp/awscliv2; \
    /tmp/awscliv2/aws/install; \
    rm -rf /tmp/awscliv2.zip /tmp/awscliv2

#########################################################################################
#
# Clean up postgres folder before inclusion
#
#########################################################################################
FROM neon-ext-build AS postgres-cleanup-layer

COPY --from=neon-pg-ext-build /usr/local/pgsql /usr/local/pgsql

# Remove binaries from /bin/ that we won't use (or would manually copy & install otherwise)
RUN cd /usr/local/pgsql/bin && rm -f ecpg raster2pgsql shp2pgsql pgtopo_export pgtopo_import pgsql2shp

# Remove headers that we won't need anymore - we've completed installation of all extensions
RUN rm -r /usr/local/pgsql/include

# Remove static postgresql libraries - all compilation is finished, so we
# can now remove these files - they must be included in other binaries by now
# if they were to be used by other libraries.
RUN rm /usr/local/pgsql/lib/lib*.a

#########################################################################################
#
# Preprocess the sql_exporter configuration files
#
#########################################################################################
FROM $REPOSITORY/$IMAGE:$TAG AS sql_exporter_preprocessor
ARG PG_VERSION

USER nonroot

COPY --chown=nonroot compute compute

RUN make PG_VERSION="${PG_VERSION:?}" -C compute

#########################################################################################
#
# Layer extension-tests
#
#########################################################################################

FROM pg-build AS extension-tests
ARG PG_VERSION
RUN mkdir /ext-src

COPY --from=pg-build /postgres /postgres
#COPY --from=postgis-src /ext-src/ /ext-src/
COPY --from=plv8-src /ext-src/ /ext-src/
#COPY --from=h3-pg-src /ext-src/ /ext-src/
COPY --from=postgresql-unit-src /ext-src/ /ext-src/
COPY --from=pgvector-src /ext-src/ /ext-src/
COPY --from=pgjwt-src /ext-src/ /ext-src/
#COPY --from=pgrag-src /ext-src/ /ext-src/
#COPY --from=pg_jsonschema-src /ext-src/ /ext-src/
COPY --from=pg_graphql-src /ext-src/ /ext-src/
#COPY --from=pg_tiktoken-src /ext-src/ /ext-src/
COPY --from=hypopg-src /ext-src/ /ext-src/
COPY --from=pg_hashids-src /ext-src/ /ext-src/
COPY --from=rum-src /ext-src/ /ext-src/
#COPY --from=pgtap-src /ext-src/ /ext-src/
COPY --from=ip4r-src /ext-src/ /ext-src/
COPY --from=prefix-src /ext-src/ /ext-src/
COPY --from=hll-src /ext-src/ /ext-src/
COPY --from=plpgsql_check-src /ext-src/ /ext-src/
#COPY --from=timescaledb-src /ext-src/ /ext-src/
COPY --from=pg_hint_plan-src /ext-src/ /ext-src/
COPY compute/patches/pg_hint_plan_${PG_VERSION:?}.patch /ext-src
RUN cd /ext-src/pg_hint_plan-src && patch -p1 < /ext-src/pg_hint_plan_${PG_VERSION:?}.patch
COPY --from=pg_cron-src /ext-src/ /ext-src/
#COPY --from=pgx_ulid-src /ext-src/ /ext-src/
#COPY --from=pgx_ulid-pgrx12-src /ext-src/ /ext-src/
#COPY --from=pg_session_jwt-src /ext-src/ /ext-src/
#COPY --from=rdkit-src /ext-src/ /ext-src/
COPY --from=pg_uuidv7-src /ext-src/ /ext-src/
COPY --from=pg_roaringbitmap-src /ext-src/ /ext-src/
COPY --from=pg_semver-src /ext-src/ /ext-src/
#COPY --from=pg_embedding-src /ext-src/ /ext-src/
#COPY --from=wal2json-src /ext-src/ /ext-src/
COPY --from=pg_ivm-src /ext-src/ /ext-src/
COPY --from=pg_partman-src /ext-src/ /ext-src/
#COPY --from=pg_mooncake-src /ext-src/ /ext-src/
#COPY --from=pg_repack-src /ext-src/ /ext-src/

COPY --chmod=755 docker-compose/run-tests.sh /run-tests.sh
ENV PATH=/usr/local/pgsql/bin:$PATH
ENV PGHOST=compute
ENV PGPORT=55433
ENV PGUSER=cloud_admin
ENV PGDATABASE=postgres

#########################################################################################
#
# Final layer
# Put it all together into the final image
#
#########################################################################################
FROM $BASE_IMAGE_SHA
ARG DEBIAN_VERSION

# Use strict mode for bash to catch errors early
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Install:
# libreadline8 for psql
# liblz4-1 for lz4
# libossp-uuid16 for extension ossp-uuid
# libgeos, libsfcgal1, and libprotobuf-c1 for PostGIS
# libxml2, libxslt1.1 for xml2
# libzstd1 for zstd
# libboost* for rdkit
# ca-certificates for communicating with s3 by compute_ctl
# libevent for pgbouncer
RUN echo 'Acquire::Retries "5";' > /etc/apt/apt.conf.d/80-retries && \
    echo -e "retry_connrefused = on\ntimeout=15\ntries=5\n" > /root/.wgetrc
RUN apt update && \
    case $DEBIAN_VERSION in \
      # Version-specific installs for Bullseye (PG14-PG16):
      # libicu67, locales for collations (including ICU and plpgsql_check)
      # libgdal28, libproj19 for PostGIS
      bullseye) \
        VERSION_INSTALLS="libicu67 libgdal28 libproj19"; \
      ;; \
      # Version-specific installs for Bookworm (PG17):
      # libicu72, locales for collations (including ICU and plpgsql_check)
      # libgdal32, libproj25 for PostGIS
      bookworm) \
        VERSION_INSTALLS="libicu72 libgdal32 libproj25"; \
      ;; \
      *) \
        echo "Unknown Debian version ${DEBIAN_VERSION}" && exit 1 \
      ;; \
    esac && \
    apt install --no-install-recommends -y \
        gdb \
        liblz4-1 \
        libreadline8 \
        libboost-iostreams1.74.0 \
        libboost-regex1.74.0 \
        libboost-serialization1.74.0 \
        libboost-system1.74.0 \
        libossp-uuid16 \
        libgeos-c1v5 \
        libprotobuf-c1 \
        libsfcgal1 \
        libxml2 \
        libxslt1.1 \
        libzstd1 \
        libcurl4 \
        libevent-2.1-7 \
        locales \
        procps \
        ca-certificates \
        $VERSION_INSTALLS && \
    apt clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

# Add user postgres
RUN mkdir /var/db && useradd -m -d /var/db/postgres postgres && \
    echo "postgres:test_console_pass" | chpasswd && \
    mkdir /var/db/postgres/compute && mkdir /var/db/postgres/specs && \
    mkdir /var/db/postgres/pgbouncer && \
    chown -R postgres:postgres /var/db/postgres && \
    chmod 0750 /var/db/postgres/compute && \
    chmod 0750 /var/db/postgres/pgbouncer && \
    # create folder for file cache
    mkdir -p -m 777 /neon/cache && \
    # Create remote extension download directory
    mkdir /usr/local/download_extensions && \
    chown -R postgres:postgres /usr/local/download_extensions

# aws cli is used by fast_import
COPY --from=awscli /usr/local/aws-cli /usr/local/aws-cli

# pgbouncer and its config
COPY --from=pgbouncer         /usr/local/pgbouncer/bin/pgbouncer /usr/local/bin/pgbouncer
COPY --chmod=0666 --chown=postgres compute/etc/pgbouncer.ini /etc/pgbouncer.ini

COPY --from=postgres-cleanup-layer --chown=postgres /usr/local/pgsql /usr/local
COPY --from=compute-tools --chown=postgres /home/nonroot/target-bin/compute_ctl /usr/local/bin/compute_ctl
COPY --from=compute-tools --chown=postgres /home/nonroot/target-bin/fast_import /usr/local/bin/fast_import

# local_proxy and its config
COPY --from=compute-tools --chown=postgres /home/nonroot/target-bin/local_proxy /usr/local/bin/local_proxy
RUN mkdir -p /etc/local_proxy && chown postgres:postgres /etc/local_proxy

# Metrics exporter binaries and configuration files
COPY --from=exporters ./postgres_exporter /bin/postgres_exporter
COPY --from=exporters ./pgbouncer_exporter /bin/pgbouncer_exporter
COPY --from=exporters ./sql_exporter /bin/sql_exporter

COPY --chown=postgres compute/etc/postgres_exporter.yml /etc/postgres_exporter.yml

COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/sql_exporter.yml               /etc/sql_exporter.yml
COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/neon_collector.yml             /etc/neon_collector.yml
COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/sql_exporter_autoscaling.yml   /etc/sql_exporter_autoscaling.yml
COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/neon_collector_autoscaling.yml /etc/neon_collector_autoscaling.yml

# Make the libraries we built available
RUN echo '/usr/local/lib' >> /etc/ld.so.conf && /sbin/ldconfig

ENV LANG=en_US.utf8
USER postgres
ENTRYPOINT ["/usr/local/bin/compute_ctl"]
