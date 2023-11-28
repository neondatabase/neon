\echo Use "ALTER EXTENSION neon UPDATE TO '1.1'" to load this file. \quit

CREATE FUNCTION neon_get_lfc_stats()
RETURNS SETOF RECORD
AS 'MODULE_PATHNAME', 'neon_get_lfc_stats'
LANGUAGE C PARALLEL SAFE;

-- Create a view for convenient access.
CREATE VIEW neon_lfc_stats AS
	SELECT P.* FROM neon_get_lfc_stats() AS P (lfc_key text, lfc_value bigint);
