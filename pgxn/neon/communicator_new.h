/*-------------------------------------------------------------------------
 *
 * communicator_new.h
 *	  new implementation
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMMUNICATOR_NEW_H
#define COMMUNICATOR_NEW_H

#include "neon_pgversioncompat.h"

#include "storage/buf_internals.h"

#include "pagestore_client.h"

/* initialization at postmaster startup */
extern void pg_init_communicator_new(void);
extern void communicator_new_shmem_request(void);
extern void communicator_new_shmem_startup(void);

/* initialization at backend startup */
extern void communicator_new_init(void);

/* Read requests */
extern bool communicator_new_rel_exists(NRelFileInfo rinfo, ForkNumber forkNum);
extern BlockNumber communicator_new_rel_nblocks(NRelFileInfo rinfo, ForkNumber forknum);
extern int64 communicator_new_dbsize(Oid dbNode);
extern void communicator_new_read_at_lsnv(NRelFileInfo rinfo, ForkNumber forkNum,
										  BlockNumber base_blockno,
										  void **buffers, BlockNumber nblocks);
extern void communicator_new_prefetch_register_bufferv(NRelFileInfo rinfo, ForkNumber forkNum,
													   BlockNumber blockno,
													   BlockNumber nblocks);
extern bool communicator_new_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum,
											BlockNumber blockno);
extern int	communicator_new_read_slru_segment(SlruKind kind, int64 segno,
											   void *buffer);

/* Write requests, to keep the caches up-to-date */
extern void communicator_new_write_page(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blockno,
										const void *buffer, XLogRecPtr lsn);
extern void communicator_new_rel_extend(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blockno,
										const void *buffer, XLogRecPtr lsn);
extern void communicator_new_rel_zeroextend(NRelFileInfo rinfo, ForkNumber forkNum,
											BlockNumber blockno, BlockNumber nblocks,
											XLogRecPtr lsn);
extern void communicator_new_rel_create(NRelFileInfo rinfo, ForkNumber forkNum, XLogRecPtr lsn);
extern void communicator_new_rel_truncate(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber nblocks, XLogRecPtr lsn);
extern void communicator_new_rel_unlink(NRelFileInfo rinfo, ForkNumber forkNum, XLogRecPtr lsn);
extern void communicator_new_update_cached_rel_size(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber nblocks, XLogRecPtr lsn);

/* other functions */
extern int32 communicator_new_approximate_working_set_size_seconds(time_t duration, bool reset);

#endif							/* COMMUNICATOR_NEW_H */
