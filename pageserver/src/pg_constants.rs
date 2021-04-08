
// From pg_tablespace_d.h
//
// FIXME: we'll probably need these elsewhere too, move to some common location
pub const DEFAULTTABLESPACE_OID:u32 = 1663;
pub const GLOBALTABLESPACE_OID:u32 = 1664;
//Special values for non-rel files' tags
//TODO maybe use enum?
pub const PG_CONTROLFILE_FORKNUM:u32 = 42;
pub const PG_FILENODEMAP_FORKNUM:u32 = 43;
pub const PG_XACT_FORKNUM:u32 = 44;
pub const PG_MXACT_OFFSETS_FORKNUM:u32 = 45;
pub const PG_MXACT_MEMBERS_FORKNUM:u32 = 46;