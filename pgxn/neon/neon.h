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
#include "access/xlogreader.h"

/* GUCs */
extern char *neon_auth_token;
extern char *neon_timeline;
extern char *neon_tenant;

extern void pg_init_libpagestore(void);
extern void pg_init_walproposer(void);

extern void pg_init_extension_server(void);

/*
 * Returns true if we shouldn't do REDO on that block in record indicated by
 * block_id; false otherwise.
 */
extern bool neon_redo_read_buffer_filter(XLogReaderState *record, uint8 block_id);
extern bool	(*old_redo_read_buffer_filter) (XLogReaderState *record, uint8 block_id);

#endif							/* NEON_H */
