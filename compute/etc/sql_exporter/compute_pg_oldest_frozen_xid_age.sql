SELECT datname database_name,
   pg_catalog.age(datfrozenxid) frozen_xid_age
FROM pg_catalog.pg_database
ORDER BY frozen_xid_age DESC LIMIT 10;
