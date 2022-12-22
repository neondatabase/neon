/*-------------------------------------------------------------------------
 *
 * neon.h
 *	  Functions used in the initialization of this extension.
 *
 * IDENTIFICATION
 *	 contrib/neon/neon.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NEON_H
#define NEON_H

extern void pg_init_libpagestore(void);
extern void pg_init_walproposer(void);
extern void pg_init_lwlc(void);

#endif							/* NEON_H */
