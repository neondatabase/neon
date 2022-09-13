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
	T_ZenithExistsRequest = 0,
	T_ZenithNblocksRequest,
	T_ZenithGetPageRequest,
	T_ZenithDbSizeRequest,

	/* pagestore -> pagestore_client */
	T_ZenithExistsResponse = 100,
	T_ZenithNblocksResponse,
	T_ZenithGetPageResponse,
	T_ZenithErrorResponse,
	T_ZenithDbSizeResponse,
} ZenithMessageTag;



/* base struct for c-style inheritance */
typedef struct
{
	ZenithMessageTag tag;
} ZenithMessage;

#define messageTag(m)		(((const ZenithMessage *)(m))->tag)

/*
 * supertype of all the Zenith*Request structs below
 *
 * If 'latest' is true, we are requesting the latest page version, and 'lsn'
 * is just a hint to the server that we know there are no versions of the page
 * (or relation size, for exists/nblocks requests) later than the 'lsn'.
 */
typedef struct
{
	ZenithMessageTag tag;
	bool		latest;			/* if true, request latest page version */
	XLogRecPtr	lsn;			/* request page version @ this LSN */
} ZenithRequest;

typedef struct
{
	ZenithRequest req;
	RelFileNode rnode;
	ForkNumber	forknum;
} ZenithExistsRequest;

typedef struct
{
	ZenithRequest req;
	RelFileNode rnode;
	ForkNumber	forknum;
} ZenithNblocksRequest;


typedef struct
{
	ZenithRequest req;
	Oid dbNode;
} ZenithDbSizeRequest;


typedef struct
{
	ZenithRequest req;
	RelFileNode rnode;
	ForkNumber	forknum;
	BlockNumber blkno;
} ZenithGetPageRequest;

/* supertype of all the Zenith*Response structs below */
typedef struct
{
	ZenithMessageTag tag;
} ZenithResponse;

typedef struct
{
	ZenithMessageTag tag;
	bool		exists;
} ZenithExistsResponse;

typedef struct
{
	ZenithMessageTag tag;
	uint32		n_blocks;
} ZenithNblocksResponse;

typedef struct
{
	ZenithMessageTag tag;
	char		page[FLEXIBLE_ARRAY_MEMBER];
} ZenithGetPageResponse;

typedef struct
{
	ZenithMessageTag tag;
	int64		db_size;
} ZenithDbSizeResponse;

typedef struct
{
	ZenithMessageTag tag;
	char		message[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated error message */
} ZenithErrorResponse;

extern StringInfoData zm_pack_request(ZenithRequest *msg);
extern ZenithResponse *zm_unpack_response(StringInfo s);
extern char *zm_to_string(ZenithMessage *msg);

/*
 * API
 */

typedef struct
{
	ZenithResponse *(*request) (ZenithRequest *request);
	void (*send) (ZenithRequest *request);
	ZenithResponse *(*receive) (void);
	void (*flush) (void);
}			page_server_api;

extern page_server_api *page_server;

extern char *page_server_connstring;
extern char *zenith_timeline;
extern char *zenith_tenant;
extern bool wal_redo;
extern int32 max_cluster_size;

extern const f_smgr *smgr_zenith(BackendId backend, RelFileNode rnode);
extern void smgr_init_zenith(void);

extern const f_smgr *smgr_inmem(BackendId backend, RelFileNode rnode);
extern void smgr_init_inmem(void);
extern void smgr_shutdown_inmem(void);

/* zenith storage manager functionality */

extern void zenith_init(void);
extern void zenith_open(SMgrRelation reln);
extern void zenith_close(SMgrRelation reln, ForkNumber forknum);
extern void zenith_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool zenith_exists(SMgrRelation reln, ForkNumber forknum);
extern void zenith_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void zenith_extend(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool zenith_prefetch(SMgrRelation reln, ForkNumber forknum,
							BlockNumber blocknum);
extern void zenith_reset_prefetch(SMgrRelation reln);
extern void zenith_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						char *buffer);

extern void zenith_read_at_lsn(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno,
			XLogRecPtr request_lsn, bool request_latest, char *buffer);

extern void zenith_write(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, char *buffer, bool skipFsync);
extern void zenith_writeback(SMgrRelation reln, ForkNumber forknum,
							 BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber zenith_nblocks(SMgrRelation reln, ForkNumber forknum);
extern int64 zenith_dbsize(Oid dbNode);
extern void zenith_truncate(SMgrRelation reln, ForkNumber forknum,
							BlockNumber nblocks);
extern void zenith_immedsync(SMgrRelation reln, ForkNumber forknum);

/* zenith wal-redo storage manager functionality */

extern void inmem_init(void);
extern void inmem_open(SMgrRelation reln);
extern void inmem_close(SMgrRelation reln, ForkNumber forknum);
extern void inmem_create(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool inmem_exists(SMgrRelation reln, ForkNumber forknum);
extern void inmem_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo);
extern void inmem_extend(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool inmem_prefetch(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber blocknum);
extern void inmem_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
					   char *buffer);
extern void inmem_write(SMgrRelation reln, ForkNumber forknum,
						BlockNumber blocknum, char *buffer, bool skipFsync);
extern void inmem_writeback(SMgrRelation reln, ForkNumber forknum,
							BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber inmem_nblocks(SMgrRelation reln, ForkNumber forknum);
extern void inmem_truncate(SMgrRelation reln, ForkNumber forknum,
						   BlockNumber nblocks);
extern void inmem_immedsync(SMgrRelation reln, ForkNumber forknum);


/* utils for zenith relsize cache */
extern void relsize_hash_init(void);
extern bool get_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber* size);
extern void set_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber size);
extern void update_cached_relsize(RelFileNode rnode, ForkNumber forknum, BlockNumber size);
extern void forget_cached_relsize(RelFileNode rnode, ForkNumber forknum);

#endif
