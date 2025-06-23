SELECT datname database_name,
         mxid_age(datminmxid) min_mxid_age
FROM     pg_database
ORDER BY min_mxid_age desc limit 5;
