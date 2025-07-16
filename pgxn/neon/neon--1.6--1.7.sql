create function neon_communicator_min_inflight_request_lsn() returns pg_lsn
AS 'MODULE_PATHNAME', 'neon_communicator_min_inflight_request_lsn'
LANGUAGE C;
