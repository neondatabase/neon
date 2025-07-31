SELECT datname database_name,
  pg_catalog.mxid_age(datminmxid) min_mxid_age
FROM pg_catalog.pg_database
ORDER BY min_mxid_age DESC LIMIT 10;
