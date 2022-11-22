/*-------------------------------------------------------------------------
 *
 * validate.h
 *
 * contrib/remotexact/validate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VALIDATE_H
#define VALIDATE_H

#include "postgres.h"

#include "utils/snapshot.h"

void validate_index_scan(RWSetRelation *rw_rel);
void validate_table_scan(Oid relid, XidCSN read_csn);

#endif							/* VALIDATE_H */
