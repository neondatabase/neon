SELECT datname database_name,
         age(datfrozenxid) frozen_xid_age
FROM     pg_database
ORDER BY frozen_xid_age desc limit 5;
