/*-------------------------------------------------------------------------
 *
 * file_cache.h
 *	  Local File Cache definitions
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef FILE_CACHE_h
#define FILE_CACHE_h

#include "neon_pgversioncompat.h"

/* GUCs */
extern bool lfc_store_prefetch_result;

/* functions for local file cache */
extern void lfc_writev(NRelFileInfo rinfo, ForkNumber forkNum,
					   BlockNumber blkno, const void *const *buffers,
					   BlockNumber nblocks);
/* returns number of blocks read, with one bit set in *read for each  */
extern int lfc_readv_select(NRelFileInfo rinfo, ForkNumber forkNum,
							BlockNumber blkno, void **buffers,
							BlockNumber nblocks, bits8 *mask);

extern bool lfc_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum,
							   BlockNumber blkno);
extern int lfc_cache_containsv(NRelFileInfo rinfo, ForkNumber forkNum,
							   BlockNumber blkno, int nblocks, bits8 *bitmap);
extern void lfc_init(void);
extern bool lfc_prefetch(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blkno,
						 const void* buffer, XLogRecPtr lsn);


static inline bool
lfc_read(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		 void *buffer)
{
	bits8		rv = 0;
	return lfc_readv_select(rinfo, forkNum, blkno, &buffer, 1, &rv) == 1;
}

static inline void
lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		  const void *buffer)
{
	return lfc_writev(rinfo, forkNum, blkno, &buffer, 1);
}

#endif							/* FILE_CACHE_H */
