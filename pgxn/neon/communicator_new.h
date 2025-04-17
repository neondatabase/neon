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

extern bool communicator_new_exists(NRelFileInfo rinfo, ForkNumber forkNum,
									neon_request_lsns *request_lsns);
extern BlockNumber communicator_new_nblocks(NRelFileInfo rinfo, ForkNumber forknum,
											neon_request_lsns *request_lsns);
extern int64 communicator_new_dbsize(Oid dbNode, neon_request_lsns *request_lsns);
extern void communicator_new_read_at_lsnv(NRelFileInfo rinfo, ForkNumber forkNum,
										  BlockNumber base_blockno, neon_request_lsns *request_lsns,
										  void **buffers, BlockNumber nblocks, const bits8 *mask);
extern int communicator_new_prefetch_lookupv(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blocknum,
											 neon_request_lsns *lsns,
											 BlockNumber nblocks, void **buffers, bits8 *mask);
extern void communicator_new_prefetch_register_bufferv(BufferTag tag, neon_request_lsns *frlsns,
													   BlockNumber nblocks, const bits8 *mask);
extern int communicator_new_read_slru_segment(SlruKind kind, int64 segno,
											  neon_request_lsns *request_lsns,
											  void *buffer);

#endif
