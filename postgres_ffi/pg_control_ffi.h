/*
 * This header file is the input to bindgen. It includes all the
 * PostgreSQL headers that we need to auto-generate Rust structs
 * from. If you need to expose a new struct to Rust code, add the
 * header here, and whitelist the struct in the build.rs file.
 */
#include "c.h"
#include "catalog/pg_control.h"

/*
 * PostgreSQL uses "offsetof(ControlFileData, crc)" in multiple places to get the
 * size of the control file up to the CRC, which is the last field, but there is
 * no constant for it. We also need it in the Rust code.
 */
const uint32 PG_CONTROLFILEDATA_OFFSETOF_CRC = offsetof(ControlFileData, crc);
