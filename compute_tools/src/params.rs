pub const DEFAULT_LOG_LEVEL: &str = "info";
// From Postgres docs:
//   To ease transition from the md5 method to the newer SCRAM method, if md5 is specified
//   as a method in pg_hba.conf but the user's password on the server is encrypted for SCRAM
//   (see below), then SCRAM-based authentication will automatically be chosen instead.
//   https://www.postgresql.org/docs/15/auth-password.html
//
// So it's safe to set md5 here, as `control-plane` anyway uses SCRAM for all roles.
pub const PG_HBA_ALL_MD5: &str = "host\tall\t\tall\t\tall\t\tmd5";
