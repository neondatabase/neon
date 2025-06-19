SELECT   datname database_name, mxid_age(datminmxid) oldest_mxid, mxid_age(datfrozenxid) oldest_frozen_xid
FROM     pg_database
ORDER BY oldest_mxid desc, oldest_frozen_xid desc limit 5;
