WITH
    dbs AS (
        SELECT
            datname
        FROM
            pg_catalog.pg_database
        WHERE
            datallowconn
            AND datconnlimit <> - 2
        LIMIT
            500
    )
SELECT
    t.extname,
    t.extversion,
    (t.extowner = 10) AS owned_by_superuser
    COUNT(DISTINCT d.datname) AS n_databases,
FROM
    dbs d,
    LATERAL (
        SELECT
            *
        FROM
            dblink (
                'dbname=' || quote_ident (d.datname) || ' user=' || quote_ident (current_user) || ' connect_timeout=5',
                'SELECT extname, extversion, extowner::integer FROM pg_catalog.pg_extension'
            ) AS t (extname name, extversion text, extowner integer)
    ) t
GROUP BY
    t.extname,
    t.extversion,
    t.extowner;
