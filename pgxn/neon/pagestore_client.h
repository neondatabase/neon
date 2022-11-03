/*-------------------------------------------------------------------------
 *
 * pagestore_client.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/neon/pagestore_client.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef pageserver_h
#define pageserver_h

#include "postgres.h"

#include "access/xlogdefs.h"
#include "storage/relfilenode.h"
#include "storage/block.h"
#include "storage/smgr.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "utils/memutils.h"

#include "pg_config.h"

typedef enum
{
	/* pagestore_client -> pagestore */
	T_NeonExistsRequest = 0,
	T_NeonNblocksRequest,
	T_NeonGetPageRequest,
	T_NeonDbSizeRequest,

	/* pagestore -> pagestore_client */
	T_NeonExistsResponse = 100,
	T_NeonNblocksResponse,
	T_NeonGetPageResponse,
	T_NeonErrorResponse,
	T_NeonDbSizeResponse,
}			NeonMessageTag;

/* base struct for c-style inheritance */
typedef struct
{
	NeonMessageTag tag;
}			NeonMessage;

#define messageTag(m) (((const NeonMessage *)(m))->tag)

/*
 * supertype of all the Neon*Request structs below
 *
 * If 'latest' is true, we are requesting the latest page version, and 'lsn'
 * is just a hint to the server that we know there are no versions of the page
 * (or relation size, for exists/nblocks requests) later than the 'lsn'.
 */
typedef struct
{
	NeonMessageTag tag;
	bool		latest;			/* if true, request latest page version */
	XLogRecPtr	lsn;			/* request page version @ this LSN */
}			NeonRequest;

typedef struct
{
	NeonRequest req;
	RelFileNode rnode;
	ForkNumber	forknum;
}			NeonExistsRequest;

typedef struct
{
	NeonRequest req;
	RelFileNode rnode;
	ForkNumber	forknum;
}			NeonNblocksRequest;

typedef struct
{
	NeonRequest req;
	Oid			dbNode;
}			NeonDbSizeRequest;

typedef struct
{
	NeonRequest req;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;
}			NeonGetPageRequest;

/* supertype of all the Neon*Response structs below */
typedef struct
{
	NeonMessageTag tag;
}			NeonResponse;

typedef struct
{
	NeonMessageTag tag;
	bool		exists;
}			NeonExistsResponse;

typedef struct
{
	NeonMessageTag tag;
	uint32		n_blocks;
}			NeonNblocksResponse;

typedef struct
{
	NeonMessageTag tag;
	char		page[FLEXIBLE_ARRAY_MEMBER];
}			NeonGetPageResponse;

#define PS_GETPAGERESPONSE_SIZE (MAXALIGN(offsetof(NeonGetPageResponse, page) + BLCKSZ))

typedef struct
{
	NeonMessageTag tag;
	int64		db_size;
}			NeonDbSizeResponse;

typedef struct
{
	NeonMessageTag tag;
	char		message[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated error
												 * message */
}			NeonErrorResponse;

extern StringInfoData nm_pack_request(NeonRequest * msg);
extern NeonResponse * nm_unpack_response(StringInfo s);
extern char *nm_to_string(NeonMessage * msg);

/*
 * API
 */

typedef struct
{
	void		(*send) (NeonRequest * request);
	NeonResponse *(*receive) (void);
	void		(*flush) (void);
}			page_server_api;

typedef void (*ps_disconnect_handle)(void);
typedef void (*ps_connect_handle)(void);

extern ps_disconnect_handle ps_disconnect_hook;
extern ps_connect_handle ps_connect_hook;

extern page_server_api * page_server;

extern char *page_server_connstring;
extern bool seqscan_prefetch_enabled;
extern int seqscan_prefetch_distance;
extern char *neon_timeline;
extern char *neon_tenant;
extern bool wal_redo;
extern int32 max_cluster_size;

extern const f_smgr *smgr_neon(BackendId backend, RelFileNode rnode);
extern void smgr_init_neon(void);

/* Neon storage manager functionality */

extern void neon_init(void);
extern void neon_open(SMgrRelation reln);
extern void neon_close(SMgrRelation reln, ForkNumber forknum);
extern void neon_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool neon_exists(SMgrRelation reln, ForkNumber forknum);
extern void neon_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void neon_extend(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool neon_prefetch(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum);
extern void neon_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					  char *buffer);

extern void neon_read_at_lsn(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
							 XLogRecPtr request_lsn, bool request_latest, char *buffer);

extern void neon_write(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, char *buffer, bool skipFsync);
extern void neon_writeback(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber neon_nblocks(SMgrRelation reln, ForkNumber forknum);
extern int64 neon_dbsize(Oid dbNode);
extern void neon_truncate(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber nblocks);
extern void neon_immedsync(SMgrRelation reln, ForkNumber forknum);

/* utils for neon relsize cache */
extern void relsize_hash_init(void);
extern bool get_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber *size);
extern void set_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber size);
extern void update_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber size);
extern void forget_cached_relsize(RelFileNode rnode, ForkNumber forknum);

#endif
