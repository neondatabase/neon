SELECT sum(pg_database_size(datname)) AS total
FROM pg_database
-- Ignore invalid databases, as we will likely have problems with
-- getting their size from the Pageserver.
WHERE datconnlimit != -2;
