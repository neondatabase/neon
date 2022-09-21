/*-------------------------------------------------------------------------
 *
 * apply.h
 *
 * contrib/remotexact/apply.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef APPLY_H
#define APPLY_H

#include "postgres.h"

#include "rwset.h"

void apply_writes(RWSet *rwset);

#endif							/* APPLY_H */
