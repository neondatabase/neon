SELECT datname database_name,
         xid_age(datfrozenxid) frozen_xid_age,
         mxid_age(datminmxid) min_mxid_age,
FROM     pg_database
ORDER BY min_mxid_age desc, frozen_xid_age desc limit 5;
