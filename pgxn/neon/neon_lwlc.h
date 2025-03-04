#ifndef NEON_LWLC_H
#define NEON_LWLC_H

#include "neon_pgversioncompat.h"

extern int			lastWrittenLsnCacheSize;

void init_lwlc(void);
void shmeminit(void);
void shmemrequest(void);

/* Hooks */
XLogRecPtr neon_get_lwlsn(NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno);
void neon_get_lwlsn_v(NRelFileInfo relfilenode, ForkNumber forknum, BlockNumber blkno, int nblocks, XLogRecPtr *lsns);
XLogRecPtr neon_set_lwlsn_block_range(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber from, BlockNumber n_blocks);
XLogRecPtr neon_set_lwlsn_block_v(const XLogRecPtr *lsns, NRelFileInfo relfilenode, ForkNumber forknum, BlockNumber blockno, int nblocks);
XLogRecPtr neon_set_lwlsn_block(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum, BlockNumber blkno);
XLogRecPtr neon_set_lwlsn_relation(XLogRecPtr lsn, NRelFileInfo rlocator, ForkNumber forknum);
XLogRecPtr neon_set_lwlsn_db(XLogRecPtr lsn);
int GetLastWrittenLSNCacheSize(void);

#endif
