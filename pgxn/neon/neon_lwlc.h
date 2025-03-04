#ifndef NEON_LWLC_H
#define NEON_LWLC_H

#include "neon_pgversioncompat.h"

extern int			lastWrittenLsnCacheSize;

void init_lwlc(void);

/* Hooks */
XLogRecPtr neon_get_lwlsn_hook(NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno);
void neon_get_lwlsn_v_hook(NRelFileInfo relfilenode, ForkNumber forknum, BlockNumber blkno, int nblocks, XLogRecPtr *lsns);
XLogRecPtr neon_set_lwlsn_block_range_hook(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber from, BlockNumber n_blocks);
XLogRecPtr neon_set_lwlsn_block_v_hook(const XLogRecPtr *lsns, NRelFileInfo relfilenode, ForkNumber forknum, BlockNumber blockno, int nblocks);
XLogRecPtr neon_set_lwlsn_block_hook(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno);
XLogRecPtr neon_set_lwlsn_relation_hook(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum);
XLogRecPtr neon_set_lwlsn_db_hook(XLogRecPtr lsn);
int GetLastWrittenLSNCacheSize(void);

#endif
