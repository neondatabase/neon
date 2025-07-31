/*-------------------------------------------------------------------------
 *
 * pagestore_client.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef PAGESTORE_CLIENT_h
#define PAGESTORE_CLIENT_h

#include "neon_pgversioncompat.h"

#include "access/slru.h"
#include "access/xlogdefs.h"
#include RELFILEINFO_HDR
#include "lib/stringinfo.h"
#include "storage/block.h"
#include "storage/buf_internals.h"

#define MAX_SHARDS 128
#define MAX_PAGESERVER_CONNSTRING_SIZE 256

typedef enum
{
	/* pagestore_client -> pagestore */
	T_NeonExistsRequest = 0,
	T_NeonNblocksRequest,
	T_NeonGetPageRequest,
	T_NeonDbSizeRequest,
	T_NeonGetSlruSegmentRequest,
	/* future tags above this line */
	T_NeonTestRequest = 99, /* only in cfg(feature = "testing") */

	/* pagestore -> pagestore_client */
	T_NeonExistsResponse = 100,
	T_NeonNblocksResponse,
	T_NeonGetPageResponse,
	T_NeonErrorResponse,
	T_NeonDbSizeResponse,
	T_NeonGetSlruSegmentResponse,
	/* future tags above this line */
	T_NeonTestResponse = 199, /* only in cfg(feature = "testing") */
} NeonMessageTag;

typedef uint64 NeonRequestId;

/* base struct for c-style inheritance */
typedef struct
{
	NeonMessageTag tag;
	NeonRequestId reqid;
	XLogRecPtr	lsn;
	XLogRecPtr	not_modified_since;
} NeonMessage;

#define messageTag(m) (((const NeonMessage *)(m))->tag)

/* SLRUs downloadable from page server */
typedef enum {
	SLRU_CLOG,
	SLRU_MULTIXACT_MEMBERS,
	SLRU_MULTIXACT_OFFSETS
} SlruKind;

/*--
 * supertype of all the Neon*Request structs below.
 *
 * All requests contain two LSNs:
 *
 * lsn:                request page (or relation size, etc) at this LSN
 * not_modified_since: Hint that the page hasn't been modified between
 *                     this LSN and the request LSN (`lsn`).
 *
 * To request the latest version of a page, you can use MAX_LSN as the request
 * LSN.
 *
 * If you don't know any better, you can always set 'not_modified_since' equal
 * to 'lsn', but providing a lower value can speed up processing the request
 * in the pageserver, as it doesn't need to wait for the WAL to arrive, and it
 * can skip traversing through recent layers which we know to not contain any
 * versions for the requested page.
 *
 * These structs describe the V2 of these requests. (The old now-defunct V1
 * protocol contained just one LSN and a boolean 'latest' flag.)
 *
 * V3 version of protocol adds request ID to all requests. This request ID is also included in response
 * as well as other fields from requests, which allows to verify that we receive response for our request.
 * We copy fields from request to response to make checking more reliable: request ID is formed from process ID
 * and local counter, so in principle there can be duplicated requests IDs if process PID is reused.
 */
typedef NeonMessage NeonRequest;

typedef struct
{
	NeonRequest hdr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} NeonExistsRequest;

typedef struct
{
	NeonRequest hdr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
} NeonNblocksRequest;

typedef struct
{
	NeonRequest hdr;
	Oid			dbNode;
} NeonDbSizeRequest;

typedef struct
{
	NeonRequest hdr;
	NRelFileInfo rinfo;
	ForkNumber	forknum;
	BlockNumber blkno;
} NeonGetPageRequest;

typedef struct
{
	NeonRequest hdr;
	SlruKind	kind;
	int			segno;
} NeonGetSlruSegmentRequest;


/* supertype of all the Neon*Response structs below */
typedef NeonMessage NeonResponse;

typedef struct
{
	NeonExistsRequest req;
	bool		exists;
} NeonExistsResponse;

typedef struct
{
	NeonNblocksRequest req;
	uint32		n_blocks;
} NeonNblocksResponse;

typedef struct
{
	NeonGetPageRequest req;
	char		page[FLEXIBLE_ARRAY_MEMBER];
} NeonGetPageResponse;

#define PS_GETPAGERESPONSE_SIZE (MAXALIGN(offsetof(NeonGetPageResponse, page) + BLCKSZ))

typedef struct
{
	NeonDbSizeRequest req;
	int64		db_size;
} NeonDbSizeResponse;

typedef struct
{
	NeonResponse req;
	char		message[FLEXIBLE_ARRAY_MEMBER]; /* null-terminated error
												 * message */
} NeonErrorResponse;

typedef struct
{
	NeonGetSlruSegmentRequest req;
	int			n_blocks;
	char		data[BLCKSZ * SLRU_PAGES_PER_SEGMENT];
} NeonGetSlruSegmentResponse;


extern StringInfoData nm_pack_request(NeonRequest *msg);
extern NeonResponse *nm_unpack_response(StringInfo s);
extern char *nm_to_string(NeonMessage *msg);

/*
 * If debug_compare_local>DEBUG_COMPARE_LOCAL_NONE, we pass through all the SMGR API
 * calls to md.c, and *also* do the calls to the Page Server. On every
 * read, compare the versions we read from local disk and Page Server,
 * and Assert that they are identical.
 */
typedef enum
{
	DEBUG_COMPARE_LOCAL_NONE,     /* normal mode - pages are storted locally only for unlogged relations */
	DEBUG_COMPARE_LOCAL_PREFETCH, /* if page is found in prefetch ring, then compare it with local and return */
	DEBUG_COMPARE_LOCAL_LFC,      /* if page is found in LFC or prefetch ring, then compare it with local and return */
	DEBUG_COMPARE_LOCAL_ALL       /* always fetch page from PS and compare it with local */
} DebugCompareLocalMode;

extern int debug_compare_local;

/*
 * API
 */

typedef uint16 shardno_t;

typedef struct
{
	/*
	 * Send this request to the PageServer associated with this shard.
	 * This function assigns request_id to the request which can be extracted by caller from request struct.
	 */
	bool		(*send) (shardno_t  shard_no, NeonRequest * request);
	/*
	 * Blocking read for the next response of this shard.
	 *
	 * When a CANCEL signal is handled, the connection state will be
	 * unmodified.
	 */
	NeonResponse *(*receive) (shardno_t shard_no);
	/*
	 * Try get the next response from the TCP buffers, if any.
	 * Returns NULL when the data is not yet available.
	 *
	 * This will raise errors only for malformed responses (we can't put them
	 * back into connection). All other error conditions are soft errors and
	 * return NULL as "no response available".
	 */
	NeonResponse *(*try_receive) (shardno_t shard_no);
	/*
	 * Make sure all requests are sent to PageServer.
	 */
	bool		(*flush) (shardno_t shard_no);
	/*
	 * Disconnect from this pageserver shard.
	 */
	void        (*disconnect) (shardno_t shard_no);
} page_server_api;

extern void prefetch_on_ps_disconnect(void);

extern page_server_api *page_server;

extern char *pageserver_connstring;
extern int	flush_every_n_requests;
extern int	readahead_buffer_size;
extern char *neon_timeline;
extern char *neon_tenant;
extern int32 max_cluster_size;
extern int  neon_protocol_version;

extern shardno_t get_shard_number(BufferTag* tag);

extern const f_smgr *smgr_neon(ProcNumber backend, NRelFileInfo rinfo);
extern void smgr_init_neon(void);
extern void readahead_buffer_resize(int newsize, void *extra);


/*
 * LSN values associated with each request to the pageserver
 */
typedef struct
{
	/*
	 * 'request_lsn' is the main value that determines which page version to
	 * fetch.
	 */
	XLogRecPtr request_lsn;

	/*
	 * A hint to the pageserver that the requested page hasn't been modified
	 * between this LSN and 'request_lsn'. That allows the pageserver to
	 * return the page faster, without waiting for 'request_lsn' to arrive in
	 * the pageserver, as long as 'not_modified_since' has arrived.
	 */
	XLogRecPtr not_modified_since;

	/*
	 * 'effective_request_lsn' is not included in the request that's sent to
	 * the pageserver, but is used to keep track of the latest LSN of when the
	 * request was made. In a standby server, this is always the same as the
	 * 'request_lsn', but in the primary we use UINT64_MAX as the
	 * 'request_lsn' to request the latest page version, so we need this
	 * separate field to remember that latest LSN was when the request was
	 * made. It's needed to manage prefetch request, to verify if the response
	 * to a prefetched request is still valid.
	 */
	XLogRecPtr effective_request_lsn;
} neon_request_lsns;

extern PGDLLEXPORT void neon_read_at_lsn(NRelFileInfo rnode, ForkNumber forkNum, BlockNumber blkno,
										 neon_request_lsns request_lsns, void *buffer);
extern int64 neon_dbsize(Oid dbNode);

extern void neon_get_request_lsns(NRelFileInfo rinfo, ForkNumber forknum,
								  BlockNumber blkno, neon_request_lsns *output,
								  BlockNumber nblocks);

/* utils for neon relsize cache */
extern void relsize_hash_init(void);
extern bool get_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber *size);
extern void set_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size);
extern void update_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber size);
extern void forget_cached_relsize(NRelFileInfo rinfo, ForkNumber forknum);

#endif							/* PAGESTORE_CLIENT_H */
