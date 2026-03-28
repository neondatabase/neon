/*-------------------------------------------------------------------------
 *
 * lfc_prewarm.h
 *	  Local File Cache prewarmer
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef LFC_PREWARM_H
#define LFC_PREWARM_H

#include "storage/buf_internals.h"

typedef struct FileCacheState
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	uint32		magic;
	uint32		n_chunks;
	uint32		n_pages;
	uint16		chunk_size_log;
	BufferTag	chunks[FLEXIBLE_ARRAY_MEMBER];
	/* followed by bitmap */
} FileCacheState;

#define FILE_CACHE_STATE_MAGIC 0xfcfcfcfc

#define FILE_CACHE_STATE_BITMAP(fcs)	((uint8*)&(fcs)->chunks[(fcs)->n_chunks])
#define FILE_CACHE_STATE_SIZE_FOR_CHUNKS(n_chunks, blocks_per_chunk)	(sizeof(FileCacheState) + (n_chunks)*sizeof(BufferTag) + (((n_chunks) * blocks_per_chunk)+7)/8)
#define FILE_CACHE_STATE_SIZE(fcs)		(sizeof(FileCacheState) + (fcs->n_chunks)*sizeof(BufferTag) + (((fcs->n_chunks) << fcs->chunk_size_log)+7)/8)

extern void pg_init_prewarm(void);
extern void PrewarmShmemRequest(void);
extern void PrewarmShmemInit(void);

#endif							/* LFC_PREWARM_H */


