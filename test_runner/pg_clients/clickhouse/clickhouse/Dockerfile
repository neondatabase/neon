FROM clickhouse/clickhouse-server

RUN apt-get update && apt install -y curl ca-certificates lsb-release && install -d /usr/share/postgresql-common/pgdg && \
curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > \
    /etc/apt/sources.list.d/pgdg.list && \
apt-get update && apt -y install postgresql-client

COPY <<EOF /etc/clickhouse-server/users.d/math.xml
<clickhouse>
    <profiles>
        <default>
            <allow_experimental_database_materialized_postgresql>1</allow_experimental_database_materialized_postgresql>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <named_collection_control>1</named_collection_control>
        </default>
    </users>
</clickhouse>
EOF

COPY . .

CMD ["/runtest.sh"]