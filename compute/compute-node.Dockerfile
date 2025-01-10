ARG PG_VERSION
ARG REPOSITORY=neondatabase
ARG IMAGE=build-tools
ARG TAG=pinned
ARG BUILD_TAG
ARG DEBIAN_VERSION=bookworm
ARG DEBIAN_FLAVOR=${DEBIAN_VERSION}-slim

#########################################################################################
#
# Layer "build-deps"
#
#########################################################################################
FROM debian:$DEBIAN_FLAVOR AS build-deps
ARG DEBIAN_VERSION

# Use strict mode for bash to catch errors early
SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

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
    libicu-dev libxslt1-dev liblz4-dev libzstd-dev zstd \
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
COPY vendor/postgres-${PG_VERSION} postgres
RUN cd postgres && \
    export CONFIGURE_CMD="./configure CFLAGS='-O2 -g3' --enable-debug --with-openssl --with-uuid=ossp \
    --with-icu --with-libxml --with-libxslt --with-lz4" && \
    if [ "${PG_VERSION}" != "v14" ]; then \
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

#########################################################################################
#
# Layer "neon-pg-ext-build"
# compile neon extensions
#
#########################################################################################
FROM build-deps AS neon-pg-ext-build
ARG PG_VERSION

COPY --from=pg-build /usr/local/pgsql/ /usr/local/pgsql/

# Public extensions
COPY pgxn/ pgxn/

RUN make -j $(getconf _NPROCESSORS_ONLN) \
        PG_CONFIG=/usr/local/pgsql/bin/pg_config \
        -C pgxn/neon \
        -s install && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        PG_CONFIG=/usr/local/pgsql/bin/pg_config \
        -C pgxn/neon_utils \
        -s install && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        PG_CONFIG=/usr/local/pgsql/bin/pg_config \
        -C pgxn/neon_test_utils \
        -s install && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        PG_CONFIG=/usr/local/pgsql/bin/pg_config \
        -C pgxn/neon_rmgr \
        -s install && \
    case "${PG_VERSION}" in \
        "v14" | "v15") \
        ;; \
        "v16" | "v17") \
            echo "Skipping HNSW for PostgreSQL ${PG_VERSION}" && exit 0 \
        ;; \
        *) \
            echo "unexpected PostgreSQL version" && exit 1 \
        ;; \
        esac && \
    make -j $(getconf _NPROCESSORS_ONLN) \
        PG_CONFIG=/usr/local/pgsql/bin/pg_config \
        -C pgxn/hnsw \
        -s install

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
RUN mold -run cargo build --locked --profile release-line-debug-size-lto --bin compute_ctl --bin fast_import --bin local_proxy

#########################################################################################
#
# Final compute-tools image
#
#########################################################################################

FROM debian:$DEBIAN_FLAVOR AS compute-tools-image

COPY --from=compute-tools /home/nonroot/target/release-line-debug-size-lto/compute_ctl /usr/local/bin/compute_ctl
COPY --from=compute-tools /home/nonroot/target/release-line-debug-size-lto/fast_import /usr/local/bin/fast_import

#########################################################################################
#
# Layer "pgbouncer"
#
#########################################################################################

FROM debian:$DEBIAN_FLAVOR AS pgbouncer
RUN set -e \
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
    && LDFLAGS=-static ./configure --prefix=/usr/local/pgbouncer --without-openssl \
    && make -j $(nproc) dist_man_MANS= \
    && make install dist_man_MANS=

#########################################################################################
#
# Layers "postgres-exporter" and "sql-exporter"
#
#########################################################################################

FROM quay.io/prometheuscommunity/postgres-exporter:v0.12.1 AS postgres-exporter

# Keep the version the same as in build-tools.Dockerfile and
# test_runner/regress/test_compute_metrics.py.
FROM burningalchemist/sql_exporter:0.16.0 AS sql-exporter

#########################################################################################
#
# Clean up postgres folder before inclusion
#
#########################################################################################
FROM neon-pg-ext-build AS postgres-cleanup-layer
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

RUN make PG_VERSION="${PG_VERSION}" -C compute

#########################################################################################
#
# Layer neon-pg-ext-test
#
#########################################################################################

FROM neon-pg-ext-build AS neon-pg-ext-test
ARG PG_VERSION
RUN mkdir /ext-src

#COPY --from=postgis-build /postgis.tar.gz /ext-src/
#COPY --from=postgis-build /sfcgal/* /usr
COPY --from=plv8-build /plv8.tar.gz /ext-src/
#COPY --from=h3-pg-build /h3-pg.tar.gz /ext-src/
COPY --from=unit-pg-build /postgresql-unit.tar.gz /ext-src/
COPY --from=vector-pg-build /pgvector.tar.gz /ext-src/
COPY --from=vector-pg-build /pgvector.patch /ext-src/
COPY --from=pgjwt-pg-build /pgjwt.tar.gz /ext-src
#COPY --from=pgrag-pg-build /usr/local/pgsql/ /usr/local/pgsql/
#COPY --from=pg-jsonschema-pg-build /home/nonroot/pg_jsonschema.tar.gz /ext-src
#COPY --from=pg-graphql-pg-build /home/nonroot/pg_graphql.tar.gz /ext-src
#COPY --from=pg-tiktoken-pg-build /home/nonroot/pg_tiktoken.tar.gz /ext-src
COPY --from=hypopg-pg-build /hypopg.tar.gz /ext-src
COPY --from=pg-hashids-pg-build /pg_hashids.tar.gz /ext-src
COPY --from=rum-pg-build /rum.tar.gz /ext-src
COPY compute/patches/rum.patch /ext-src
#COPY --from=pgtap-pg-build /pgtap.tar.gz /ext-src
COPY --from=ip4r-pg-build /ip4r.tar.gz /ext-src
COPY --from=prefix-pg-build /prefix.tar.gz /ext-src
COPY --from=hll-pg-build /hll.tar.gz /ext-src
COPY --from=plpgsql-check-pg-build /plpgsql_check.tar.gz /ext-src
#COPY --from=timescaledb-pg-build /timescaledb.tar.gz /ext-src
COPY --from=pg-hint-plan-pg-build /pg_hint_plan.tar.gz /ext-src
COPY compute/patches/pg_hint_plan_${PG_VERSION}.patch /ext-src
COPY --from=pg-cron-pg-build /pg_cron.tar.gz /ext-src
COPY compute/patches/pg_cron.patch /ext-src
#COPY --from=pg-pgx-ulid-build /home/nonroot/pgx_ulid.tar.gz /ext-src
#COPY --from=rdkit-pg-build /rdkit.tar.gz /ext-src
COPY --from=pg-uuidv7-pg-build /pg_uuidv7.tar.gz /ext-src
COPY --from=pg-roaringbitmap-pg-build /pg_roaringbitmap.tar.gz /ext-src
COPY --from=pg-semver-pg-build /pg_semver.tar.gz /ext-src
#COPY --from=pg-embedding-pg-build /home/nonroot/pg_embedding-src/ /ext-src
#COPY --from=wal2json-pg-build /wal2json_2_5.tar.gz /ext-src
#pg_anon is not supported yet for pg v17 so, don't fail if nothing found
COPY --from=pg-anon-pg-build /pg_anon.tar.g? /ext-src
COPY compute/patches/pg_anon.patch /ext-src
COPY --from=pg-ivm-build /pg_ivm.tar.gz /ext-src
COPY --from=pg-partman-build /pg_partman.tar.gz /ext-src
RUN cd /ext-src/ && for f in *.tar.gz; \
    do echo $f; dname=$(echo $f | sed 's/\.tar.*//')-src; \
    rm -rf $dname; mkdir $dname; tar xzf $f --strip-components=1 -C $dname \
    || exit 1; rm -f $f; done
RUN cd /ext-src/rum-src && patch -p1 <../rum.patch
RUN cd /ext-src/pgvector-src && patch -p1 <../pgvector.patch
RUN cd /ext-src/pg_hint_plan-src && patch -p1 < /ext-src/pg_hint_plan_${PG_VERSION}.patch
COPY --chmod=755 docker-compose/run-tests.sh /run-tests.sh
RUN case "${PG_VERSION}" in "v17") \
    echo "postgresql_anonymizer does not yet support PG17" && exit 0;; \
    esac && patch -p1 </ext-src/pg_anon.patch
RUN patch -p1 </ext-src/pg_cron.patch
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
FROM debian:$DEBIAN_FLAVOR
ARG DEBIAN_VERSION
# Add user postgres
RUN mkdir /var/db && useradd -m -d /var/db/postgres postgres && \
    echo "postgres:test_console_pass" | chpasswd && \
    mkdir /var/db/postgres/compute && mkdir /var/db/postgres/specs && \
    mkdir /var/db/postgres/pgbouncer && \
    chown -R postgres:postgres /var/db/postgres && \
    chmod 0750 /var/db/postgres/compute && \
    chmod 0750 /var/db/postgres/pgbouncer && \
    echo '/usr/local/lib' >> /etc/ld.so.conf && /sbin/ldconfig && \
    # create folder for file cache
    mkdir -p -m 777 /neon/cache

COPY --from=postgres-cleanup-layer --chown=postgres /usr/local/pgsql /usr/local
COPY --from=compute-tools --chown=postgres /home/nonroot/target/release-line-debug-size-lto/compute_ctl /usr/local/bin/compute_ctl
COPY --from=compute-tools --chown=postgres /home/nonroot/target/release-line-debug-size-lto/fast_import /usr/local/bin/fast_import

# pgbouncer and its config
COPY --from=pgbouncer         /usr/local/pgbouncer/bin/pgbouncer /usr/local/bin/pgbouncer
COPY --chmod=0666 --chown=postgres compute/etc/pgbouncer.ini /etc/pgbouncer.ini

# local_proxy and its config
COPY --from=compute-tools --chown=postgres /home/nonroot/target/release-line-debug-size-lto/local_proxy /usr/local/bin/local_proxy
RUN mkdir -p /etc/local_proxy && chown postgres:postgres /etc/local_proxy

# Metrics exporter binaries and  configuration files
COPY --from=postgres-exporter /bin/postgres_exporter /bin/postgres_exporter
COPY --from=sql-exporter      /bin/sql_exporter      /bin/sql_exporter

COPY --chown=postgres compute/etc/postgres_exporter.yml /etc/postgres_exporter.yml

COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/sql_exporter.yml               /etc/sql_exporter.yml
COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/neon_collector.yml             /etc/neon_collector.yml
COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/sql_exporter_autoscaling.yml   /etc/sql_exporter_autoscaling.yml
COPY --from=sql_exporter_preprocessor --chmod=0644 /home/nonroot/compute/etc/neon_collector_autoscaling.yml /etc/neon_collector_autoscaling.yml

COPY debug-oom/backup.tar.gz /var/db/backups/backup.tar.gz

# Create remote extension download directory
RUN mkdir /usr/local/download_extensions && chown -R postgres:postgres /usr/local/download_extensions

# Install:
# libreadline8 for psql
# liblz4-1 for lz4
# libossp-uuid16 for extension ossp-uuid
# libgeos, libsfcgal1, and libprotobuf-c1 for PostGIS
# libxml2, libxslt1.1 for xml2
# libzstd1 for zstd
# libboost* for rdkit
# ca-certificates for communicating with s3 by compute_ctl


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
        locales \
        procps \
        ca-certificates \
        curl \
        unzip \
        $VERSION_INSTALLS && \
    apt clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8

# aws cli is used by fast_import (curl and unzip above are at this time only used for this installation step)
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
    curl -L "https://awscli.amazonaws.com/awscli-exe-linux-${TARGETARCH_ALT}-2.17.5.zip" -o /tmp/awscliv2.zip; \
    echo "${CHECKSUM}  /tmp/awscliv2.zip" | sha256sum -c -; \
    unzip /tmp/awscliv2.zip -d /tmp/awscliv2; \
    /tmp/awscliv2/aws/install; \
    rm -rf /tmp/awscliv2.zip /tmp/awscliv2; \
    true

ENV LANG=en_US.utf8
USER postgres
ENTRYPOINT ["/usr/local/bin/compute_ctl"]
