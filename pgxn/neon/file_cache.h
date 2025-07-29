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

#include "lfc_prewarm.h"
#include "neon.h"

#include "neon_pgversioncompat.h"

/* GUCs */
extern bool lfc_store_prefetch_result;
extern int	lfc_max_size;
extern int	lfc_size_limit;
extern char *lfc_path;

extern bool lfc_do_prewarm;
extern bool lfc_prewarm_cancel;

/* functions for local file cache */
extern void lfc_invalidate(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber nblocks);
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

extern FileCacheState* lfc_get_state(size_t max_entries);
extern LfcStatsEntry *lfc_get_stats(size_t *num_entries);

typedef struct
{
	uint32		pageoffs;
	Oid			relfilenode;
	Oid			reltablespace;
	Oid			reldatabase;
	ForkNumber	forknum;
	BlockNumber blocknum;
	uint16		accesscount;
} LocalCachePagesRec;
extern LocalCachePagesRec *lfc_local_cache_pages(size_t *num_entries);

extern int32 lfc_approximate_working_set_size_seconds(time_t duration, bool reset);

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
