/*-------------------------------------------------------------------------
 *
 * multiregion.h
 * 
 * contrib/neon/multiregion.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MULTIREGION_H
#define MULTIREGION_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "pagestore_client.h"

extern void set_region_lsn(int region, NeonResponse *msg);
extern XLogRecPtr get_region_lsn(int region);
extern void clear_region_lsns(void);

#endif