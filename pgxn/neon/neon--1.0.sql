\echo Use "CREATE EXTENSION neon" to load this file. \quit

CREATE FUNCTION pg_cluster_size()
RETURNS bigint
AS 'MODULE_PATHNAME', 'pg_cluster_size'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION backpressure_lsns(
    OUT received_lsn pg_lsn,
    OUT disk_consistent_lsn pg_lsn,
    OUT remote_consistent_lsn pg_lsn
)
RETURNS record
AS 'MODULE_PATHNAME', 'backpressure_lsns'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION backpressure_throttling_time()
RETURNS bigint
AS 'MODULE_PATHNAME', 'backpressure_throttling_time'
LANGUAGE C STRICT
PARALLEL UNSAFE;

CREATE FUNCTION local_cache_pages()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'local_cache_pages'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION get_relation_access_statistics()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'get_relation_access_statistics'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE VIEW local_cache AS
	SELECT relname,P.* FROM local_cache_pages() AS P
	(pageoffs int8, relfilenode oid, reltablespace oid, reldatabase oid,
	 relforknumber int2, relblocknumber int8, accesscount int4) JOIN pg_class pc ON (P.relfilenode = pc.relfilenode);

CREATE VIEW relation_access_statistics AS
	SELECT relname,P.* FROM get_relation_access_statistics() AS P
	(relfilenode oid, reltablespace oid, reldatabase oid,
	 seqaccess int4, rndaccess int4, access_count int8) JOIN pg_class pc ON (P.relfilenode = pc.relfilenode);
