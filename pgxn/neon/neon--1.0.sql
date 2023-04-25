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

-- Create a view for convenient access.
CREATE VIEW local_cache AS
	SELECT P.* FROM local_cache_pages() AS P
	(pageoffs int8, relfilenode oid, reltablespace oid, reldatabase oid,
	 relforknumber int2, relblocknumber int8, accesscount int4);

create table postgres_log (
  log_time timestamp(3) with time zone,
  user_name text,
  database_name text,
  process_id integer,
  connection_from text,
  session_id text,
  session_line_num bigint,
  command_tag text,
  session_start_time timestamp with time zone,
  virtual_transaction_id text,
  transaction_id bigint,
  error_severity text,
  sql_state_code text,
  message text,
  detail text,
  hint text,
  internal_query text,
  internal_query_pos integer,
  context text,
  query text,
  query_pos integer,
  location text,
  application_name text,
  backend_type text,
  leader_pid integer,
  query_id bigint
);

CREATE FUNCTION read_postgres_log()
RETURNS setof postgres_log
AS 'MODULE_PATHNAME', 'read_postgres_log'
LANGUAGE C PARALLEL SAFE;

CREATE VIEW pg_log AS
	SELECT * from read_postgres_log();
