local MIN_SUPPORTED_VERSION = 14;
local MAX_SUPPORTED_VERSION = 17;
local SUPPORTED_VERSIONS = std.range(MIN_SUPPORTED_VERSION, MAX_SUPPORTED_VERSION);

# If we receive the pg_version with a leading "v", ditch it.
local pg_version = std.strReplace(std.extVar('pg_version'), 'v', '');
local pg_version_num = std.parseInt(pg_version);

assert std.setMember(pg_version_num, SUPPORTED_VERSIONS) :
       std.format('%s is an unsupported Postgres version: %s',
                  [pg_version, std.toString(SUPPORTED_VERSIONS)]);

{
  PG_MAJORVERSION: pg_version,
  PG_MAJORVERSION_NUM: pg_version_num,
}
