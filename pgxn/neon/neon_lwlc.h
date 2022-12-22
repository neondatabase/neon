//
// Created by Matthias on 2022-12-12.
//

#ifndef NEON_NEON_LWLSNCACHE_H
#define NEON_NEON_LWLSNCACHE_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilenode.h"

extern int lsn_cache_size;

XLogRecPtr lwlc_get_max_evicted(void);

/* Get the actual last written LSN, and fill the effective LwLSN */
XLogRecPtr GetLastWrittenLsnForBuffer(RelFileNode node, ForkNumber fork,
									  BlockNumber blkno, XLogRecPtr *effective);
XLogRecPtr GetLastWrittenLsnForRelFileNode(RelFileNode node, ForkNumber fork,
										   XLogRecPtr *effective);
XLogRecPtr GetLastWrittenLsnForDatabase(Oid datoid, XLogRecPtr *effective);
XLogRecPtr GetLastWrittenLsnForDbCluster(XLogRecPtr *effective);

void neon_set_lwlf_block_hook(XLogRecPtr lsn, RelFileNode relfilenode, ForkNumber forknum, BlockNumber blkno);
void neon_set_lwlf_blockrange_hook(XLogRecPtr lsn, RelFileNode relfilenode, ForkNumber forknum, BlockNumber start, BlockNumber n_blocks);
void neon_set_lwlf_relation_hook(XLogRecPtr lsn, RelFileNode relfilenode, ForkNumber forknum);
void neon_set_lwlf_database_hook(XLogRecPtr lsn, Oid dboid);
void neon_set_lwlf_dbcluster_hook(XLogRecPtr lsn);

#endif //NEON_NEON_LWLSNCACHE_H
