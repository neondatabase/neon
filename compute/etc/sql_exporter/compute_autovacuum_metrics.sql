SELECT datname database_name,
  mxid_age(datminmxid) min_mxid_age,
  age(datfrozenxid) frozen_xid_age
FROM pg_database
ORDER BY min_mxid_age DESC, frozen_xid_age DESC LIMIT 5;
