/*-------------------------------------------------------------------------
 *
 * communicator.c
 *	  Functions for communicating with remote pageservers.
 *
 * This is the so-called "legacy" communicator. It consists of functions that
 * are called from the smgr implementation, in pagestore_smgr.c. There are
 * plans to replace this with a different implementation, see RFC.
 *
 * The communicator is a collection of functions that are called in each
 * backend, when the backend needs to read a page or other information. It
 * does not spawn background threads or anything like that. To process
 * responses to prefetch requests in a timely fashion, however, it registers
 * a ProcessInterrupts hook that gets called periodically from any
 * CHECK_FOR_INTERRUPTS() point in the backend.
 *
 * By the time the functions in this file are called, the caller has already
 * established that a request to the pageserver is necessary. The functions
 * are only called for permanent relations (i.e. not temp or unlogged tables).
 * Before making a call to the communicator, the caller has already checked
 * the relation size or local file cache.
 *
 * However, when processing responses to getpage requests, the communicator
 * writes pages directly to the LFC.
 *
 * The communicator functions take request LSNs as arguments; the caller is
 * responsible for determining the correct LSNs to use. There's one exception
 * to that, in prefetch_do_request(); it sometimes calls back to
 * neon_get_request_lsns().  That's because sometimes a suitable response is
 * found in the prefetch buffer and the request LSns are not needed, and the
 * caller doesn't know whether it's needed or not.
 *
 * The main interface consists of the following "synchronous" calls:
 *
 * communicator_exists			- Returns true if a relation file exists
 * communicator_nblocks			- Returns a relation's size
 * communicator_dbsize			- Returns a databases's total size
 * communicator_read_at_lsnv	- Read contents of one relation block
 * communicator_read_slru_segment - Read contents of one SLRU segment
 *
 * In addition, there functions related to prefetching:
 * communicator_prefetch_register_bufferv - Start prefetching a page
 * communicator_prefetch_lookupv - Check if a page is already in prefetch queue
 *
 * Misc other functions:
 * - communicator_init			- Initialize the module at startup
 * - communicator_prefetch_pump_state - Called periodically to advance the state
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlog_internal.h"
#include "access/xlogutils.h"
#include "common/hashfn.h"
#include "executor/instrument.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "port/pg_iovec.h"
#include "postmaster/interrupt.h"
#include "replication/walsender.h"
#include "utils/timeout.h"

#include "bitmap.h"
#include "communicator.h"
#include "file_cache.h"
#include "neon.h"
#include "neon_perf_counters.h"
#include "pagestore_client.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

#if PG_VERSION_NUM < 160000
typedef PGAlignedBlock PGIOAlignedBlock;
#endif

#define NEON_PANIC_CONNECTION_STATE(shard_no, elvl, message, ...) \
	neon_shard_log(shard_no, elvl, "Broken connection state: " message, \
				   ##__VA_ARGS__)

page_server_api *page_server;

/*
 * Various settings related to prompt (fast) handling of PageStream responses
 * at any CHECK_FOR_INTERRUPTS point.
 */
int				readahead_getpage_pull_timeout_ms = 50;
static int		PS_TIMEOUT_ID = 0;
static bool		timeout_set = false;
static bool		timeout_signaled = false;

/*
 * We have a CHECK_FOR_INTERRUPTS in page_server->receive(), and we don't want
 * that to handle any getpage responses if we're already working on the
 * backlog of those, as we'd hit issues with determining which prefetch slot
 * we just got a response for.
 *
 * To protect against that, we have this variable that's set whenever we start
 * receiving data for prefetch slots, so that we don't get confused.
 *
 * Note that in certain error cases during readpage we may leak r_r_g=true,
 * which results in a failure to pick up further responses until we first
 * actively try to receive new getpage responses.
 */
static bool		readpage_reentrant_guard = false;

static void pagestore_timeout_handler(void);

#define START_PREFETCH_RECEIVE_WORK() \
	do { \
		readpage_reentrant_guard = true; \
	} while (false)

#define END_PREFETCH_RECEIVE_WORK() \
	do { \
		readpage_reentrant_guard = false; \
		if (unlikely(timeout_signaled && !InterruptPending)) \
			InterruptPending = true; \
	} while (false)

/*
 * Prefetch implementation:
 *
 * Prefetch is performed locally by each backend.
 *
 * There can be up to readahead_buffer_size active IO requests registered at
 * any time. Requests using smgr_prefetch are sent to the pageserver, but we
 * don't wait on the response. Requests using smgr_read are either read from
 * the buffer, or (if that's not possible) we wait on the response to arrive -
 * this also will allow us to receive other prefetched pages.
 * Each request is immediately written to the output buffer of the pageserver
 * connection, but may not be flushed if smgr_prefetch is used: pageserver
 * flushes sent requests on manual flush, or every neon.flush_output_after
 * unflushed requests; which is not necessarily always and all the time.
 *
 * Once we have received a response, this value will be stored in the response
 * buffer, indexed in a hash table. This allows us to retain our buffered
 * prefetch responses even when we have cache misses.
 *
 * Reading of prefetch responses is delayed until them are actually needed
 * (smgr_read). In case of prefetch miss or any other SMGR request other than
 * smgr_read, all prefetch responses in the pipeline will need to be read from
 * the connection; the responses are stored for later use.
 *
 * NOTE: The current implementation of the prefetch system implements a ring
 * buffer of up to readahead_buffer_size requests. If there are more _read and
 * _prefetch requests between the initial _prefetch and the _read of a buffer,
 * the prefetch request will have been dropped from this prefetch buffer, and
 * your prefetch was wasted.
 */

/*
 * State machine:
 *
 * not in hash : in hash
 *             :
 * UNUSED ------> REQUESTED --> RECEIVED
 *   ^         :      |            |
 *   |         :      v            |
 *   |         : TAG_REMAINS       |
 *   |         :      |            |
 *   +----------------+------------+
 *             :
 */
typedef enum PrefetchStatus
{
	PRFS_UNUSED = 0,			/* unused slot */
	PRFS_REQUESTED,				/* request was written to the sendbuffer to
								 * PS, but not necessarily flushed. all fields
								 * except response valid */
	PRFS_RECEIVED,				/* all fields valid */
	PRFS_TAG_REMAINS,			/* only buftag and my_ring_index are still
								 * valid */
} PrefetchStatus;

/* must fit in uint8; bits 0x1 are used */
typedef enum {
	PRFSF_NONE	= 0x0,
	PRFSF_LFC	= 0x1  /* received prefetch result is stored in LFC */
} PrefetchRequestFlags;

typedef struct PrefetchRequest
{
	BufferTag	buftag;			/* must be first entry in the struct */
	shardno_t	shard_no;
	uint8		status;		/* see PrefetchStatus for valid values */
	uint8		flags;		/* see PrefetchRequestFlags */
	neon_request_lsns request_lsns;
	NeonRequestId reqid;
	NeonResponse *response;		/* may be null */
	uint64		my_ring_index;
} PrefetchRequest;

/* prefetch buffer lookup hash table */

typedef struct PrfHashEntry
{
	PrefetchRequest *slot;
	uint32		status;
	uint32		hash;
} PrfHashEntry;

#define SH_PREFIX			prfh
#define SH_ELEMENT_TYPE		PrfHashEntry
#define SH_KEY_TYPE			PrefetchRequest *
#define SH_KEY				slot
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a)	((a)->hash)
#define SH_HASH_KEY(tb, key) hash_bytes( \
	((const unsigned char *) &(key)->buftag), \
	sizeof(BufferTag) \
)

#define SH_EQUAL(tb, a, b)	(BufferTagsEqual(&(a)->buftag, &(b)->buftag))
#define SH_SCOPE			static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

/*
 * PrefetchState maintains the state of (prefetch) getPage@LSN requests.
 * It maintains a (ring) buffer of in-flight requests and responses.
 *
 * We maintain several indexes into the ring buffer:
 * ring_unused >= ring_flush >= ring_receive >= ring_last >= 0
 *
 * ring_unused points to the first unused slot of the buffer
 * ring_receive is the next request that is to be received
 * ring_last is the oldest received entry in the buffer
 *
 * Apart from being an entry in the ring buffer of prefetch requests, each
 * PrefetchRequest that is not UNUSED is indexed in prf_hash by buftag.
 */
typedef struct PrefetchState
{
	MemoryContext bufctx;		/* context for prf_buffer[].response
								 * allocations */
	MemoryContext errctx;		/* context for prf_buffer[].response
								 * allocations */
	MemoryContext hashctx;		/* context for prf_buffer */

	/* buffer indexes */
	uint64		ring_unused;	/* first unused slot */
	uint64		ring_flush;		/* next request to flush */
	uint64		ring_receive;	/* next slot that is to receive a response */
	uint64		ring_last;		/* min slot with a response value */

	/* metrics / statistics  */
	int			n_responses_buffered;	/* count of PS responses not yet in
										 * buffers */
	int			n_requests_inflight;	/* count of PS requests considered in
										 * flight */
	int			n_unused;		/* count of buffers < unused, > last, that are
								 * also unused */

	/* the buffers */
	prfh_hash	*prf_hash;
	int			max_shard_no;
	/* Mark shards involved in prefetch */
	uint8		shard_bitmap[(MAX_SHARDS + 7)/8];
	PrefetchRequest prf_buffer[];	/* prefetch buffers */
} PrefetchState;

static PrefetchState *MyPState;

#define GetPrfSlotNoCheck(ring_index) ( \
	&MyPState->prf_buffer[((ring_index) % readahead_buffer_size)] \
)

#define GetPrfSlot(ring_index) ( \
	( \
		AssertMacro((ring_index) < MyPState->ring_unused && \
					(ring_index) >= MyPState->ring_last), \
		GetPrfSlotNoCheck(ring_index) \
	) \
)

#define ReceiveBufferNeedsCompaction() (\
	(MyPState->n_responses_buffered / 8) < ( \
		MyPState->ring_receive - \
			MyPState->ring_last - \
			MyPState->n_responses_buffered \
	) \
)

static process_interrupts_callback_t prev_interrupt_cb;

static bool compact_prefetch_buffers(void);
static void consume_prefetch_responses(void);
static uint64 prefetch_register_bufferv(BufferTag tag, neon_request_lsns *frlsns,
										BlockNumber nblocks, const bits8 *mask,
										bool is_prefetch);
static bool prefetch_read(PrefetchRequest *slot);
static void prefetch_do_request(PrefetchRequest *slot, neon_request_lsns *force_request_lsns);
static bool prefetch_wait_for(uint64 ring_index);
static void prefetch_cleanup_trailing_unused(void);
static inline void prefetch_set_unused(uint64 ring_index);

static bool neon_prefetch_response_usable(neon_request_lsns *request_lsns,
										  PrefetchRequest *slot);
static bool communicator_processinterrupts(void);

void
pg_init_communicator(void)
{
	prev_interrupt_cb = ProcessInterruptsCallback;
	ProcessInterruptsCallback = communicator_processinterrupts;
}

static bool
compact_prefetch_buffers(void)
{
	uint64		empty_ring_index = MyPState->ring_last;
	uint64		search_ring_index = MyPState->ring_receive;
	int			n_moved = 0;

	if (MyPState->ring_receive == MyPState->ring_last)
		return false;

	while (search_ring_index > MyPState->ring_last)
	{
		search_ring_index--;
		if (GetPrfSlot(search_ring_index)->status == PRFS_UNUSED)
		{
			empty_ring_index = search_ring_index;
			break;
		}
	}

	/*
	 * Here we have established: slots < search_ring_index have an unknown
	 * state (not scanned) slots >= search_ring_index and <= empty_ring_index
	 * are unused slots > empty_ring_index are in use, or outside our buffer's
	 * range. ... unless search_ring_index <= ring_last
	 *
	 * Therefore, there is a gap of at least one unused items between
	 * search_ring_index and empty_ring_index (both inclusive), which grows as
	 * we hit more unused items while moving backwards through the array.
	 */

	while (search_ring_index > MyPState->ring_last)
	{
		PrefetchRequest *source_slot;
		PrefetchRequest *target_slot;
		bool		found;

		/* update search index to an unprocessed entry */
		search_ring_index--;

		source_slot = GetPrfSlot(search_ring_index);

		if (source_slot->status == PRFS_UNUSED)
			continue;

		/* slot is used -- start moving slot */
		target_slot = GetPrfSlot(empty_ring_index);

		Assert(source_slot->status == PRFS_RECEIVED);
		Assert(target_slot->status == PRFS_UNUSED);

		target_slot->buftag = source_slot->buftag;
		target_slot->shard_no = source_slot->shard_no;
		target_slot->status = source_slot->status;
		target_slot->flags = source_slot->flags;
		target_slot->response = source_slot->response;
		target_slot->reqid = source_slot->reqid;
		target_slot->request_lsns = source_slot->request_lsns;
		target_slot->my_ring_index = empty_ring_index;

		prfh_delete(MyPState->prf_hash, source_slot);
		prfh_insert(MyPState->prf_hash, target_slot, &found);

		Assert(!found);

		/* Adjust the location of our known-empty slot */
		empty_ring_index--;

		/* empty the moved slot */
		source_slot->status = PRFS_UNUSED;
		source_slot->buftag = (BufferTag)
		{
			0
		};
		source_slot->response = NULL;
		source_slot->my_ring_index = 0;
		source_slot->request_lsns = (neon_request_lsns) {
			InvalidXLogRecPtr, InvalidXLogRecPtr, InvalidXLogRecPtr
		};

		/* update bookkeeping */
		n_moved++;
	}

	/*
	 * Only when we've moved slots we can expect trailing unused slots, so
	 * only then we clean up trailing unused slots.
	 */
	if (n_moved > 0)
	{
		prefetch_cleanup_trailing_unused();
		return true;
	}

	return false;
}

/*
 * If there might be responses still in the TCP buffer, then we should try to
 * use those, to reduce any TCP backpressure on the OS/PS side.
 *
 * This procedure handles that.
 *
 * Note that this works because we don't pipeline non-getPage requests.
 *
 * NOTE: This procedure is not allowed to throw errors that should be handled
 * by SMGR-related code, as this can be called from every CHECK_FOR_INTERRUPTS
 * point inside and outside PostgreSQL.
 *
 * This still does throw errors when it receives malformed responses from PS.
 */
void
communicator_prefetch_pump_state(void)
{
	START_PREFETCH_RECEIVE_WORK();

	while (MyPState->ring_receive != MyPState->ring_flush)
	{
		NeonResponse   *response;
		PrefetchRequest *slot;
		MemoryContext	old;

		slot = GetPrfSlot(MyPState->ring_receive);

		old = MemoryContextSwitchTo(MyPState->errctx);
		response = page_server->try_receive(slot->shard_no);
		MemoryContextSwitchTo(old);

		if (response == NULL)
			break;

		/* The slot should still be valid */
		if (slot->status != PRFS_REQUESTED ||
			slot->response != NULL ||
			slot->my_ring_index != MyPState->ring_receive)
			neon_shard_log(slot->shard_no, ERROR,
						   "Incorrect prefetch slot state after receive: status=%d response=%p my=%lu receive=%lu",
						   slot->status, slot->response,
						   (long) slot->my_ring_index, (long) MyPState->ring_receive);

		/* update prefetch state */
		MyPState->n_responses_buffered += 1;
		MyPState->n_requests_inflight -= 1;
		MyPState->ring_receive += 1;
		MyNeonCounters->getpage_prefetches_buffered =
			MyPState->n_responses_buffered;

		/* update slot state */
		slot->status = PRFS_RECEIVED;
		slot->response = response;

		if (response->tag == T_NeonGetPageResponse && !(slot->flags & PRFSF_LFC) && lfc_store_prefetch_result)
		{
			/*
			 * Store prefetched result in LFC (please read comments to lfc_prefetch
			 * explaining why it can be done without holding shared buffer lock
			 */
			if (lfc_prefetch(BufTagGetNRelFileInfo(slot->buftag), slot->buftag.forkNum, slot->buftag.blockNum, ((NeonGetPageResponse*)response)->page, slot->request_lsns.not_modified_since))
			{
				slot->flags |= PRFSF_LFC;
			}
		}
	}

	END_PREFETCH_RECEIVE_WORK();

	communicator_reconfigure_timeout_if_needed();
}

void
readahead_buffer_resize(int newsize, void *extra)
{
	uint64		end,
				nfree = newsize;
	PrefetchState *newPState;
	Size		newprfs_size = offsetof(PrefetchState, prf_buffer) +
		(sizeof(PrefetchRequest) * newsize);

	/* don't try to re-initialize if we haven't initialized yet */
	if (MyPState == NULL)
		return;

	/*
	 * Make sure that we don't lose track of active prefetch requests by
	 * ensuring we have received all but the last n requests (n = newsize).
	 */
	if (MyPState->n_requests_inflight > newsize)
	{
		prefetch_wait_for(MyPState->ring_unused - newsize - 1);
		Assert(MyPState->n_requests_inflight <= newsize);
	}

	/* construct the new PrefetchState, and copy over the memory contexts */
	newPState = MemoryContextAllocZero(TopMemoryContext, newprfs_size);

	newPState->bufctx = MyPState->bufctx;
	newPState->errctx = MyPState->errctx;
	newPState->hashctx = MyPState->hashctx;
	newPState->prf_hash = prfh_create(MyPState->hashctx, newsize, NULL);
	newPState->n_unused = newsize;
	newPState->n_requests_inflight = 0;
	newPState->n_responses_buffered = 0;
	newPState->ring_last = newsize;
	newPState->ring_unused = newsize;
	newPState->ring_receive = newsize;
	newPState->max_shard_no = MyPState->max_shard_no;
	memcpy(newPState->shard_bitmap, MyPState->shard_bitmap, sizeof(MyPState->shard_bitmap));

	/*
	 * Copy over the prefetches.
	 *
	 * We populate the prefetch array from the end; to retain the most recent
	 * prefetches, but this has the benefit of only needing to do one
	 * iteration on the dataset, and trivial compaction.
	 */
	for (end = MyPState->ring_unused - 1;
		 end >= MyPState->ring_last && end != UINT64_MAX && nfree != 0;
		 end -= 1)
	{
		PrefetchRequest *slot = GetPrfSlot(end);
		PrefetchRequest *newslot;
		bool		found;

		if (slot->status == PRFS_UNUSED)
			continue;

		nfree -= 1;

		newslot = &newPState->prf_buffer[nfree];
		*newslot = *slot;
		newslot->my_ring_index = nfree;

		prfh_insert(newPState->prf_hash, newslot, &found);

		Assert(!found);

		switch (newslot->status)
		{
			case PRFS_UNUSED:
				pg_unreachable();
			case PRFS_REQUESTED:
				newPState->n_requests_inflight += 1;
				newPState->ring_receive -= 1;
				newPState->ring_last -= 1;
				break;
			case PRFS_RECEIVED:
				newPState->n_responses_buffered += 1;
				newPState->ring_last -= 1;
				break;
			case PRFS_TAG_REMAINS:
				newPState->ring_last -= 1;
				break;
		}
		newPState->n_unused -= 1;
	}
	newPState->ring_flush = newPState->ring_receive;

	MyNeonCounters->getpage_prefetches_buffered =
		MyPState->n_responses_buffered;
	MyNeonCounters->pageserver_open_requests =
		MyPState->n_requests_inflight;

	for (; end >= MyPState->ring_last && end != UINT64_MAX; end -= 1)
	{
		PrefetchRequest *slot = GetPrfSlot(end);
		Assert(slot->status != PRFS_REQUESTED);
		if (slot->status == PRFS_RECEIVED)
		{
			pfree(slot->response);
		}
	}

	prfh_destroy(MyPState->prf_hash);
	pfree(MyPState);
	MyPState = newPState;
}



/*
 * Make sure that there are no responses still in the buffer.
 *
 * This function may indirectly update MyPState->pfs_hash; which invalidates
 * any active pointers into the hash table.
 */
static void
consume_prefetch_responses(void)
{
	if (MyPState->ring_receive < MyPState->ring_unused)
		prefetch_wait_for(MyPState->ring_unused - 1);
}

static void
prefetch_cleanup_trailing_unused(void)
{
	uint64		ring_index;
	PrefetchRequest *slot;

	while (MyPState->ring_last < MyPState->ring_receive)
	{
		ring_index = MyPState->ring_last;
		slot = GetPrfSlot(ring_index);

		if (slot->status == PRFS_UNUSED)
			MyPState->ring_last += 1;
		else
			break;
	}
}


static bool
prefetch_flush_requests(void)
{
	for (shardno_t shard_no = 0; shard_no < MyPState->max_shard_no; shard_no++)
	{
		if (BITMAP_ISSET(MyPState->shard_bitmap, shard_no))
		{
			if (!page_server->flush(shard_no))
				return false;
			BITMAP_CLR(MyPState->shard_bitmap, shard_no);
		}
	}
	MyPState->max_shard_no = 0;
	return true;
}

/*
 * Wait for slot of ring_index to have received its response.
 * The caller is responsible for making sure the request buffer is flushed.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 * NOTE: callers should make sure they can handle query cancellations in this
 * function's call path.
 */
static bool
prefetch_wait_for(uint64 ring_index)
{
	PrefetchRequest *entry;
	bool		result = true;

	if (MyPState->ring_flush <= ring_index &&
		MyPState->ring_unused > MyPState->ring_flush)
	{
		if (!prefetch_flush_requests())
			return false;
		MyPState->ring_flush = MyPState->ring_unused;
	}

	Assert(MyPState->ring_unused > ring_index);

	START_PREFETCH_RECEIVE_WORK();

	while (MyPState->ring_receive <= ring_index)
	{
		entry = GetPrfSlot(MyPState->ring_receive);

		Assert(entry->status == PRFS_REQUESTED);
		if (!prefetch_read(entry))
		{
			result = false;
			break;
		}
		CHECK_FOR_INTERRUPTS();
	}

	if (result)
	{
		/* Check that slot is actually received (srver can be disconnected in prefetch_pump_state called from CHECK_FOR_INTERRUPTS */
		PrefetchRequest *slot = GetPrfSlot(ring_index);
		result = slot->status == PRFS_RECEIVED;
	}
	END_PREFETCH_RECEIVE_WORK();

	return result;
;
}

/*
 * Read the response of a prefetch request into its slot.
 *
 * The caller is responsible for making sure that the request for this buffer
 * was flushed to the PageServer.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 *
 * NOTE: this does IO, and can get canceled out-of-line.
 */
static bool
prefetch_read(PrefetchRequest *slot)
{
	NeonResponse *response;
	MemoryContext old;
	BufferTag	buftag;
	shardno_t	shard_no;
	uint64		my_ring_index;

	Assert(slot->status == PRFS_REQUESTED);
	Assert(slot->response == NULL);
	Assert(slot->my_ring_index == MyPState->ring_receive);
	Assert(readpage_reentrant_guard);

	if (slot->status != PRFS_REQUESTED ||
		slot->response != NULL ||
		slot->my_ring_index != MyPState->ring_receive)
		neon_shard_log(slot->shard_no, ERROR,
					   "Incorrect prefetch read: status=%d response=%p my=%lu receive=%lu",
					   slot->status, slot->response,
					   (long)slot->my_ring_index, (long)MyPState->ring_receive);

	/*
	 * Copy the request info so that if an error happens and the prefetch
	 * queue is flushed during the receive call, we can print the original
	 * values in the error message
	 */
	buftag = slot->buftag;
	shard_no = slot->shard_no;
	my_ring_index = slot->my_ring_index;

	old = MemoryContextSwitchTo(MyPState->errctx);
	response = (NeonResponse *) page_server->receive(shard_no);
	MemoryContextSwitchTo(old);
	if (response)
	{
		/* The slot should still be valid */
		if (slot->status != PRFS_REQUESTED ||
			slot->response != NULL ||
			slot->my_ring_index != MyPState->ring_receive)
			neon_shard_log(shard_no, ERROR,
						   "Incorrect prefetch slot state after receive: status=%d response=%p my=%lu receive=%lu",
						   slot->status, slot->response,
						   (long) slot->my_ring_index, (long) MyPState->ring_receive);

		/* update prefetch state */
		MyPState->n_responses_buffered += 1;
		MyPState->n_requests_inflight -= 1;
		MyPState->ring_receive += 1;
		MyNeonCounters->getpage_prefetches_buffered =
			MyPState->n_responses_buffered;

		/* update slot state */
		slot->status = PRFS_RECEIVED;
		slot->response = response;

		if (response->tag == T_NeonGetPageResponse && !(slot->flags & PRFSF_LFC) && lfc_store_prefetch_result)
		{
			/*
			 * Store prefetched result in LFC (please read comments to lfc_prefetch
			 * explaining why it can be done without holding shared buffer lock
			 */
			if (lfc_prefetch(BufTagGetNRelFileInfo(buftag), buftag.forkNum, buftag.blockNum, ((NeonGetPageResponse*)response)->page, slot->request_lsns.not_modified_since))
			{
				slot->flags |= PRFSF_LFC;
			}
		}
		return true;
	}
	else
	{
		/*
		 * Note: The slot might no longer be valid, if the connection was lost
		 * and the prefetch queue was flushed during the receive call
		 */
		neon_shard_log(shard_no, LOG,
					   "No response from reading prefetch entry %lu: %u/%u/%u.%u block %u. This can be caused by a concurrent disconnect",
					   (long) my_ring_index,
					   RelFileInfoFmt(BufTagGetNRelFileInfo(buftag)),
					   buftag.forkNum, buftag.blockNum);
		return false;
	}
}


/*
 * Wait completion of previosly registered prefetch request.
 * Prefetch result should be placed in LFC by prefetch_wait_for.
 */
bool
communicator_prefetch_receive(BufferTag tag)
{
	PrfHashEntry *entry;
	PrefetchRequest hashkey;

	Assert(readpage_reentrant_guard);
	hashkey.buftag = tag;
	entry = prfh_lookup(MyPState->prf_hash, &hashkey);
	if (entry != NULL && prefetch_wait_for(entry->slot->my_ring_index))
	{
		prefetch_set_unused(entry->slot->my_ring_index);
		return true;
	}
	return false;
}

/*
 * Disconnect hook - drop prefetches when the connection drops
 *
 * If we don't remove the failed prefetches, we'd be serving incorrect
 * data to the smgr.
 */
void
prefetch_on_ps_disconnect(void)
{
	bool save_readpage_reentrant_guard = readpage_reentrant_guard;
	MyPState->ring_flush = MyPState->ring_unused;

	/* Prohibit callig of prefetch_pump_state */
	START_PREFETCH_RECEIVE_WORK();

	while (MyPState->ring_receive < MyPState->ring_unused)
	{
		PrefetchRequest *slot;
		uint64		ring_index = MyPState->ring_receive;

		slot = GetPrfSlot(ring_index);

		Assert(slot->status == PRFS_REQUESTED);
		Assert(slot->my_ring_index == ring_index);

		/*
		 * Drop connection to all shards which have prefetch requests.
		 * It is not a problem to call disconnect multiple times on the same connection
		 * because disconnect implementation in libpagestore.c will check if connection
		 * is alive and do nothing of connection was already dropped.
		 */
		page_server->disconnect(slot->shard_no);

		/* clean up the request */
		slot->status = PRFS_TAG_REMAINS;
		MyPState->n_requests_inflight -= 1;
		MyPState->ring_receive += 1;

		prefetch_set_unused(ring_index);
		pgBufferUsage.prefetch.expired += 1;
		MyNeonCounters->getpage_prefetch_discards_total += 1;
	}

	/* Restore guard */
	readpage_reentrant_guard = save_readpage_reentrant_guard;

	/*
	 * We can have gone into retry due to network error, so update stats with
	 * the latest available
	 */
	MyNeonCounters->pageserver_open_requests =
		MyPState->n_requests_inflight;
	MyNeonCounters->getpage_prefetches_buffered =
		MyPState->n_responses_buffered;
}

/*
 * prefetch_set_unused() - clear a received prefetch slot
 *
 * The slot at ring_index must be a current member of the ring buffer,
 * and may not be in the PRFS_REQUESTED state.
 *
 * NOTE: this function will update MyPState->pfs_hash; which invalidates any
 * active pointers into the hash table.
 */
static inline void
prefetch_set_unused(uint64 ring_index)
{
	PrefetchRequest *slot;

	if (ring_index < MyPState->ring_last)
		return;					/* Should already be unused */

	slot = GetPrfSlot(ring_index);
	if (slot->status == PRFS_UNUSED)
		return;

	Assert(slot->status == PRFS_RECEIVED || slot->status == PRFS_TAG_REMAINS);

	if (slot->status == PRFS_RECEIVED)
	{
		pfree(slot->response);
		slot->response = NULL;

		MyPState->n_responses_buffered -= 1;
		MyPState->n_unused += 1;

		MyNeonCounters->getpage_prefetches_buffered =
			MyPState->n_responses_buffered;
	}
	else
	{
		Assert(slot->response == NULL);
	}

	prfh_delete(MyPState->prf_hash, slot);

	/* clear all fields */
	MemSet(slot, 0, sizeof(PrefetchRequest));
	slot->status = PRFS_UNUSED;

	/* run cleanup if we're holding back ring_last */
	if (MyPState->ring_last == ring_index)
		prefetch_cleanup_trailing_unused();

	/*
	 * ... and try to store the buffered responses more compactly if > 12.5%
	 * of the buffer is gaps
	 */
	else if (ReceiveBufferNeedsCompaction())
		compact_prefetch_buffers();
}

/*
 * Send one prefetch request to the pageserver. To wait for the response, call
 * prefetch_wait_for().
 */
static void
prefetch_do_request(PrefetchRequest *slot, neon_request_lsns *force_request_lsns)
{
	bool		found;
	uint64		mySlotNo PG_USED_FOR_ASSERTS_ONLY = slot->my_ring_index;

	NeonGetPageRequest request = {
		.hdr.tag = T_NeonGetPageRequest,
		/* lsn and not_modified_since are filled in below */
		.rinfo = BufTagGetNRelFileInfo(slot->buftag),
		.forknum = slot->buftag.forkNum,
		.blkno = slot->buftag.blockNum,
	};

	Assert(mySlotNo == MyPState->ring_unused);

	if (force_request_lsns)
		slot->request_lsns = *force_request_lsns;
	else
		neon_get_request_lsns(BufTagGetNRelFileInfo(slot->buftag),
							  slot->buftag.forkNum, slot->buftag.blockNum,
							  &slot->request_lsns, 1);
	request.hdr.lsn = slot->request_lsns.request_lsn;
	request.hdr.not_modified_since = slot->request_lsns.not_modified_since;

	Assert(slot->response == NULL);
	Assert(slot->my_ring_index == MyPState->ring_unused);

	while (!page_server->send(slot->shard_no, (NeonRequest *) &request))
	{
		Assert(mySlotNo == MyPState->ring_unused);
		/* loop */
	}
	slot->reqid = request.hdr.reqid;

	/* update prefetch state */
	MyPState->n_requests_inflight += 1;
	MyPState->n_unused -= 1;
	MyPState->ring_unused += 1;
	BITMAP_SET(MyPState->shard_bitmap, slot->shard_no);
	MyPState->max_shard_no = Max(slot->shard_no+1, MyPState->max_shard_no);

	/* update slot state */
	slot->status = PRFS_REQUESTED;
	prfh_insert(MyPState->prf_hash, slot, &found);
	Assert(!found);
}

/*
 * Lookup of already received prefetch requests. Only already received responses matching required LSNs are accepted.
 * Present pages are marked in "mask" bitmap and total number of such pages is returned.
 */
int
communicator_prefetch_lookupv(NRelFileInfo rinfo, ForkNumber forknum, BlockNumber blocknum,
							  neon_request_lsns *lsns, BlockNumber nblocks,
							  void **buffers, bits8 *mask)
{
	int hits = 0;
	PrefetchRequest hashkey;

	/*
	 * Use an intermediate PrefetchRequest struct as the hash key to ensure
	 * correct alignment and that the padding bytes are cleared.
	 */
	memset(&hashkey.buftag, 0, sizeof(BufferTag));
	CopyNRelFileInfoToBufTag(hashkey.buftag, rinfo);
	hashkey.buftag.forkNum = forknum;

	for (int i = 0; i < nblocks; i++)
	{
		PrfHashEntry *entry;

		hashkey.buftag.blockNum = blocknum + i;
		entry = prfh_lookup(MyPState->prf_hash, &hashkey);

		if (entry != NULL)
		{
			PrefetchRequest *slot = entry->slot;
			uint64 ring_index = slot->my_ring_index;
			Assert(slot == GetPrfSlot(ring_index));

			Assert(slot->status != PRFS_UNUSED);
			Assert(MyPState->ring_last <= ring_index &&
				   ring_index < MyPState->ring_unused);
			Assert(BufferTagsEqual(&slot->buftag, &hashkey.buftag));

			if (slot->status != PRFS_RECEIVED)
				continue;

			/*
			 * If the caller specified a request LSN to use, only accept
			 * prefetch responses that satisfy that request.
			 */
			if (!neon_prefetch_response_usable(&lsns[i], slot))
				continue;

			/*
			 * Ignore errors
			 */
			if (slot->response->tag != T_NeonGetPageResponse)
			{
				if (slot->response->tag != T_NeonErrorResponse)
				{
					NEON_PANIC_CONNECTION_STATE(slot->shard_no, PANIC,
											"Expected GetPage (0x%02x) or Error (0x%02x) response to GetPageRequest, but got 0x%02x",
											T_NeonGetPageResponse, T_NeonErrorResponse, slot->response->tag);
				}
				continue;
			}
			memcpy(buffers[i], ((NeonGetPageResponse*)slot->response)->page, BLCKSZ);


			/*
			 * With lfc_store_prefetch_result=true prefetch result is stored in LFC in prefetch_pump_state when response is received
			 * from page server. But if lfc_store_prefetch_result=false then it is not yet stored in LFC and we have to do it here
			 * under buffer lock.
			 */
			if (!lfc_store_prefetch_result)
				lfc_write(rinfo, forknum, blocknum + i, buffers[i]);

			prefetch_set_unused(ring_index);
			BITMAP_SET(mask, i);

			hits += 1;
			inc_getpage_wait(0);
		}
	}
	pgBufferUsage.prefetch.hits += hits;
	return hits;
}

/*
 * prefetch_register_bufferv() - register and prefetch buffers
 *
 * Register that we may want the contents of BufferTag in the near future.
 * This is used when issuing a speculative prefetch request, but also when
 * performing a synchronous request and need the buffer right now.
 *
 * If force_request_lsns is not NULL, those values are sent to the
 * pageserver. If NULL, we utilize the lastWrittenLsn -infrastructure
 * to calculate the LSNs to send.
 *
 * Bits set in *mask (if present) indicate pages already read; i.e. pages we
 * can skip in this process.
 *
 * When performing a prefetch rather than a synchronous request,
 * is_prefetch==true. Currently, it only affects how the request is accounted
 * in the perf counters.
 *
 * NOTE: this function may indirectly update MyPState->pfs_hash; which
 * invalidates any active pointers into the hash table.
 */
void
communicator_prefetch_register_bufferv(BufferTag tag, neon_request_lsns *frlsns,
									   BlockNumber nblocks, const bits8 *mask)
{
	uint64		ring_index PG_USED_FOR_ASSERTS_ONLY;

	ring_index = prefetch_register_bufferv(tag, frlsns, nblocks, mask, true);

	Assert(ring_index < MyPState->ring_unused &&
		   MyPState->ring_last <= ring_index);
}

/* internal version. Returns the ring index */
static uint64
prefetch_register_bufferv(BufferTag tag, neon_request_lsns *frlsns,
						  BlockNumber nblocks, const bits8 *mask,
						  bool is_prefetch)
{
	uint64		min_ring_index;
	PrefetchRequest hashkey;
#ifdef USE_ASSERT_CHECKING
	bool		any_hits = false;
#endif
	/* We will never read further ahead than our buffer can store. */
	nblocks = Max(1, Min(nblocks, readahead_buffer_size));

	/*
	 * Use an intermediate PrefetchRequest struct as the hash key to ensure
	 * correct alignment and that the padding bytes are cleared.
	 */
	memset(&hashkey.buftag, 0, sizeof(BufferTag));
	hashkey.buftag = tag;

Retry:
	/*
	 * We can have gone into retry due to network error, so update stats with
	 * the latest available
	 */
	MyNeonCounters->pageserver_open_requests =
		MyPState->ring_unused - MyPState->ring_receive;
	MyNeonCounters->getpage_prefetches_buffered =
		MyPState->n_responses_buffered;

	min_ring_index = UINT64_MAX;
	for (int i = 0; i < nblocks; i++)
	{
		PrefetchRequest *slot = NULL;
		PrfHashEntry *entry = NULL;
		uint64		ring_index;
		neon_request_lsns *lsns;

		if (PointerIsValid(mask) && BITMAP_ISSET(mask, i))
			continue;

		if (frlsns)
			lsns = &frlsns[i];
		else
			lsns = NULL;

#ifdef USE_ASSERT_CHECKING
		any_hits = true;
#endif

		slot = NULL;
		entry = NULL;

		hashkey.buftag.blockNum = tag.blockNum + i;
		entry = prfh_lookup(MyPState->prf_hash, &hashkey);

		if (entry != NULL)
		{
			slot = entry->slot;
			ring_index = slot->my_ring_index;
			Assert(slot == GetPrfSlot(ring_index));

			Assert(slot->status != PRFS_UNUSED);
			Assert(MyPState->ring_last <= ring_index &&
				   ring_index < MyPState->ring_unused);
			Assert(BufferTagsEqual(&slot->buftag, &hashkey.buftag));

			/*
			 * If the caller specified a request LSN to use, only accept
			 * prefetch responses that satisfy that request.
			 */
			if (!is_prefetch)
			{
				if (!neon_prefetch_response_usable(lsns, slot))
				{
					/* Wait for the old request to finish and discard it */
					if (!prefetch_wait_for(ring_index))
						goto Retry;
					prefetch_set_unused(ring_index);
					entry = NULL;
					slot = NULL;
					pgBufferUsage.prefetch.expired += 1;
					MyNeonCounters->getpage_prefetch_discards_total += 1;
				}
			}

			if (entry != NULL)
			{
				/*
				 * We received a prefetch for a page that was recently read
				 * and removed from the buffers. Remove that request from the
				 * buffers.
				 */
				if (slot->status == PRFS_TAG_REMAINS)
				{
					prefetch_set_unused(ring_index);
					entry = NULL;
					slot = NULL;
				}
				else
				{
					min_ring_index = Min(min_ring_index, ring_index);
					/* The buffered request is good enough, return that index */
					if (is_prefetch)
						pgBufferUsage.prefetch.duplicates++;
					continue;
				}
			}
		}
		else if (!is_prefetch)
		{
			pgBufferUsage.prefetch.misses += 1;
			MyNeonCounters->getpage_prefetch_misses_total++;
		}
		/*
		 * We can only leave the block above by finding that there's
		 * no entry that can satisfy this request, either because there
		 * was no entry, or because the entry was invalid or didn't satisfy
		 * the LSNs provided.
		 *
		 * The code should've made sure to clear up the data.
		 */
		Assert(entry == NULL);
		Assert(slot == NULL);

		/* There should be no buffer overflow */
		Assert(MyPState->ring_last + readahead_buffer_size >= MyPState->ring_unused);

		/*
		 * If the prefetch queue is full, we need to make room by clearing the
		 * oldest slot. If the oldest slot holds a buffer that was already
		 * received, we can just throw it away; we fetched the page
		 * unnecessarily in that case. If the oldest slot holds a request that
		 * we haven't received a response for yet, we have to wait for the
		 * response to that before we can continue. We might not have even
		 * flushed the request to the pageserver yet, it might be just sitting
		 * in the output buffer. In that case, we flush it and wait for the
		 * response. (We could decide not to send it, but it's hard to abort
		 * when the request is already in the output buffer, and 'not sending'
		 * a prefetch request kind of goes against the principles of
		 * prefetching)
		 */
		if (MyPState->ring_last + readahead_buffer_size == MyPState->ring_unused)
		{
			uint64		cleanup_index = MyPState->ring_last;

			slot = GetPrfSlot(cleanup_index);

			Assert(slot->status != PRFS_UNUSED);

			/*
			 * If there is good reason to run compaction on the prefetch buffers,
			 * try to do that.
			 */
			if (ReceiveBufferNeedsCompaction() && compact_prefetch_buffers())
			{
				Assert(slot->status == PRFS_UNUSED);
			}
			else
			{
				/*
				 * We have the slot for ring_last, so that must still be in
				 * progress
				 */
				switch (slot->status)
				{
					case PRFS_REQUESTED:
						Assert(MyPState->ring_receive == cleanup_index);
						if (!prefetch_wait_for(cleanup_index))
							goto Retry;
						prefetch_set_unused(cleanup_index);
						pgBufferUsage.prefetch.expired += 1;
						MyNeonCounters->getpage_prefetch_discards_total += 1;
						break;
					case PRFS_RECEIVED:
					case PRFS_TAG_REMAINS:
						prefetch_set_unused(cleanup_index);
						pgBufferUsage.prefetch.expired += 1;
						MyNeonCounters->getpage_prefetch_discards_total += 1;
						break;
					default:
						pg_unreachable();
				}
			}
		}

		/*
		 * The next buffer pointed to by `ring_unused` is now definitely empty, so
		 * we can insert the new request to it.
		 */
		ring_index = MyPState->ring_unused;

		Assert(MyPState->ring_last <= ring_index &&
			   ring_index <= MyPState->ring_unused);

		slot = GetPrfSlotNoCheck(ring_index);

		Assert(slot->status == PRFS_UNUSED);

		/*
		 * We must update the slot data before insertion, because the hash
		 * function reads the buffer tag from the slot.
		 */
		slot->buftag = hashkey.buftag;
		slot->shard_no = get_shard_number(&tag);
		slot->my_ring_index = ring_index;
		slot->flags = 0;

		min_ring_index = Min(min_ring_index, ring_index);

		if (is_prefetch)
			MyNeonCounters->getpage_prefetch_requests_total++;
		else
			MyNeonCounters->getpage_sync_requests_total++;

		prefetch_do_request(slot, lsns);
	}

	MyNeonCounters->pageserver_open_requests =
		MyPState->ring_unused - MyPState->ring_receive;

	Assert(any_hits);

	Assert(GetPrfSlot(min_ring_index)->status == PRFS_REQUESTED ||
		   GetPrfSlot(min_ring_index)->status == PRFS_RECEIVED);
	Assert(MyPState->ring_last <= min_ring_index &&
		   min_ring_index < MyPState->ring_unused);

	if (flush_every_n_requests > 0 &&
		MyPState->ring_unused - MyPState->ring_flush >= flush_every_n_requests)
	{
		if (!prefetch_flush_requests())
		{
			/*
			 * Prefetch set is reset in case of error, so we should try to
			 * register our request once again
			 */
			goto Retry;
		}
		MyPState->ring_flush = MyPState->ring_unused;
	}

	return min_ring_index;
}

static bool
equal_requests(NeonRequest* a, NeonRequest* b)
{
	return a->reqid == b->reqid && a->lsn == b->lsn && a->not_modified_since == b->not_modified_since;
}


/*
 * Note: this function can get canceled and use a long jump to the next catch
 * context. Take care.
 */
static NeonResponse *
page_server_request(void const *req)
{
	NeonResponse *resp;
	BufferTag tag = {0};
	shardno_t shard_no;

	switch (messageTag(req))
	{
		case T_NeonExistsRequest:
			CopyNRelFileInfoToBufTag(tag, ((NeonExistsRequest *) req)->rinfo);
			break;
		case T_NeonNblocksRequest:
			CopyNRelFileInfoToBufTag(tag, ((NeonNblocksRequest *) req)->rinfo);
			break;
		case T_NeonDbSizeRequest:
			NInfoGetDbOid(BufTagGetNRelFileInfo(tag)) = ((NeonDbSizeRequest *) req)->dbNode;
			break;
		case T_NeonGetPageRequest:
			CopyNRelFileInfoToBufTag(tag, ((NeonGetPageRequest *) req)->rinfo);
			tag.blockNum = ((NeonGetPageRequest *) req)->blkno;
			break;
		default:
			neon_log(ERROR, "Unexpected request tag: %d", messageTag(req));
	}
	shard_no = get_shard_number(&tag);

	/*
	 * Current sharding model assumes that all metadata is present only at shard 0.
	 * We still need to call get_shard_no() to check if shard map is up-to-date.
	 */
	if (((NeonRequest *) req)->tag != T_NeonGetPageRequest)
	{
		shard_no = 0;
	}

	do
	{
		PG_TRY();
		{
			while (!page_server->send(shard_no, (NeonRequest *) req)
				   || !page_server->flush(shard_no))
			{
				/* do nothing */
			}
			MyNeonCounters->pageserver_open_requests++;
			consume_prefetch_responses();
			resp = page_server->receive(shard_no);
			MyNeonCounters->pageserver_open_requests--;
		}
		PG_CATCH();
		{
			/*
			 * Cancellation in this code needs to be handled better at some
			 * point, but this currently seems fine for now.
			 */
			page_server->disconnect(shard_no);
			MyNeonCounters->pageserver_open_requests = 0;

			/*
			 * We know for sure we're not working on any prefetch pages after
			 * this.
			 */
			END_PREFETCH_RECEIVE_WORK();

			PG_RE_THROW();
		}
		PG_END_TRY();

	} while (resp == NULL);

	return resp;
}


StringInfoData
nm_pack_request(NeonRequest *msg)
{
	StringInfoData s;

	initStringInfo(&s);

	pq_sendbyte(&s, msg->tag);
	if (neon_protocol_version >= 3)
	{
		pq_sendint64(&s, msg->reqid);
	}
	pq_sendint64(&s, msg->lsn);
	pq_sendint64(&s, msg->not_modified_since);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);

				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				pq_sendint32(&s, msg_req->dbNode);

				break;
			}
		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				pq_sendint32(&s, NInfoGetSpcOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetDbOid(msg_req->rinfo));
				pq_sendint32(&s, NInfoGetRelNumber(msg_req->rinfo));
				pq_sendbyte(&s, msg_req->forknum);
				pq_sendint32(&s, msg_req->blkno);

				break;
			}

		case T_NeonGetSlruSegmentRequest:
			{
				NeonGetSlruSegmentRequest *msg_req = (NeonGetSlruSegmentRequest *) msg;

				pq_sendbyte(&s, msg_req->kind);
				pq_sendint32(&s, msg_req->segno);

				break;
			}

			/* pagestore -> pagestore_client. We never need to create these. */
		case T_NeonExistsResponse:
		case T_NeonNblocksResponse:
		case T_NeonGetPageResponse:
		case T_NeonErrorResponse:
		case T_NeonDbSizeResponse:
		case T_NeonGetSlruSegmentResponse:
		default:
			neon_log(ERROR, "unexpected neon message tag 0x%02x", msg->tag);
			break;
	}
	return s;
}

NeonResponse *
nm_unpack_response(StringInfo s)
{
	NeonMessageTag tag = pq_getmsgbyte(s);
	NeonResponse resp_hdr = {0}; /* make valgrind happy */
	NeonResponse *resp = NULL;

	resp_hdr.tag = tag;
	if (neon_protocol_version >= 3)
	{
		resp_hdr.reqid = pq_getmsgint64(s);
		resp_hdr.lsn = pq_getmsgint64(s);
		resp_hdr.not_modified_since = pq_getmsgint64(s);
	}
	switch (tag)
	{
			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = palloc0(sizeof(NeonExistsResponse));

				if (neon_protocol_version >= 3)
				{
					NInfoGetSpcOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetDbOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetRelNumber(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					msg_resp->req.forknum = pq_getmsgbyte(s);
				}
				msg_resp->req.hdr = resp_hdr;
				msg_resp->exists = pq_getmsgbyte(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = palloc0(sizeof(NeonNblocksResponse));

				if (neon_protocol_version >= 3)
				{
					NInfoGetSpcOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetDbOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetRelNumber(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					msg_resp->req.forknum = pq_getmsgbyte(s);
				}
				msg_resp->req.hdr = resp_hdr;
				msg_resp->n_blocks = pq_getmsgint(s, 4);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetPageResponse:
			{
				NeonGetPageResponse *msg_resp;

				msg_resp = MemoryContextAllocZero(MyPState->bufctx, PS_GETPAGERESPONSE_SIZE);
				if (neon_protocol_version >= 3)
				{
					NInfoGetSpcOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetDbOid(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					NInfoGetRelNumber(msg_resp->req.rinfo) = pq_getmsgint(s, 4);
					msg_resp->req.forknum = pq_getmsgbyte(s);
					msg_resp->req.blkno = pq_getmsgint(s, 4);
				}
				msg_resp->req.hdr = resp_hdr;
				/* XXX:	should be varlena */
				memcpy(msg_resp->page, pq_getmsgbytes(s, BLCKSZ), BLCKSZ);
				pq_getmsgend(s);

				Assert(msg_resp->req.hdr.tag == T_NeonGetPageResponse);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = palloc0(sizeof(NeonDbSizeResponse));

				if (neon_protocol_version >= 3)
				{
					msg_resp->req.dbNode = pq_getmsgint(s, 4);
				}
				msg_resp->req.hdr = resp_hdr;
				msg_resp->db_size = pq_getmsgint64(s);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonErrorResponse:
			{
				NeonErrorResponse *msg_resp;
				size_t		msglen;
				const char *msgtext;

				msgtext = pq_getmsgrawstring(s);
				msglen = strlen(msgtext);

				msg_resp = palloc0(sizeof(NeonErrorResponse) + msglen + 1);
				msg_resp->req = resp_hdr;
				memcpy(msg_resp->message, msgtext, msglen + 1);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

		case T_NeonGetSlruSegmentResponse:
		    {
				NeonGetSlruSegmentResponse *msg_resp;
				int n_blocks;
				msg_resp = palloc0(sizeof(NeonGetSlruSegmentResponse));

				if (neon_protocol_version >= 3)
				{
					msg_resp->req.kind = pq_getmsgbyte(s);
					msg_resp->req.segno = pq_getmsgint(s, 4);
				}
				msg_resp->req.hdr = resp_hdr;

				n_blocks = pq_getmsgint(s, 4);
				msg_resp->n_blocks = n_blocks;
				memcpy(msg_resp->data, pq_getmsgbytes(s, n_blocks * BLCKSZ), n_blocks * BLCKSZ);
				pq_getmsgend(s);

				resp = (NeonResponse *) msg_resp;
				break;
			}

			/*
			 * pagestore_client -> pagestore
			 *
			 * We create these ourselves, and don't need to decode them.
			 */
		case T_NeonExistsRequest:
		case T_NeonNblocksRequest:
		case T_NeonGetPageRequest:
		case T_NeonDbSizeRequest:
		case T_NeonGetSlruSegmentRequest:
		default:
			neon_log(ERROR, "unexpected neon message tag 0x%02x", tag);
			break;
	}

	return resp;
}

/* dump to json for debugging / error reporting purposes */
char *
nm_to_string(NeonMessage *msg)
{
	StringInfoData s;

	initStringInfo(&s);

	switch (messageTag(msg))
	{
			/* pagestore_client -> pagestore */
		case T_NeonExistsRequest:
			{
				NeonExistsRequest *msg_req = (NeonExistsRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonExistsRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonNblocksRequest:
			{
				NeonNblocksRequest *msg_req = (NeonNblocksRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}

		case T_NeonGetPageRequest:
			{
				NeonGetPageRequest *msg_req = (NeonGetPageRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetPageRequest\"");
				appendStringInfo(&s, ", \"rinfo\": \"%u/%u/%u\"", RelFileInfoFmt(msg_req->rinfo));
				appendStringInfo(&s, ", \"forknum\": %d", msg_req->forknum);
				appendStringInfo(&s, ", \"blkno\": %u", msg_req->blkno);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonDbSizeRequest:
			{
				NeonDbSizeRequest *msg_req = (NeonDbSizeRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeRequest\"");
				appendStringInfo(&s, ", \"dbnode\": \"%u\"", msg_req->dbNode);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonGetSlruSegmentRequest:
			{
				NeonGetSlruSegmentRequest *msg_req = (NeonGetSlruSegmentRequest *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruSegmentRequest\"");
				appendStringInfo(&s, ", \"kind\": %u", msg_req->kind);
				appendStringInfo(&s, ", \"segno\": %u", msg_req->segno);
				appendStringInfo(&s, ", \"lsn\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.lsn));
				appendStringInfo(&s, ", \"not_modified_since\": \"%X/%X\"", LSN_FORMAT_ARGS(msg_req->hdr.not_modified_since));
				appendStringInfoChar(&s, '}');
				break;
			}
			/* pagestore -> pagestore_client */
		case T_NeonExistsResponse:
			{
				NeonExistsResponse *msg_resp = (NeonExistsResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonExistsResponse\"");
				appendStringInfo(&s, ", \"exists\": %d}",
								 msg_resp->exists);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonNblocksResponse:
			{
				NeonNblocksResponse *msg_resp = (NeonNblocksResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonNblocksResponse\"");
				appendStringInfo(&s, ", \"n_blocks\": %u}",
								 msg_resp->n_blocks);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonGetPageResponse:
			{
#if 0
				NeonGetPageResponse *msg_resp = (NeonGetPageResponse *) msg;
#endif

				appendStringInfoString(&s, "{\"type\": \"NeonGetPageResponse\"");
				appendStringInfo(&s, ", \"page\": \"XXX\"}");
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonErrorResponse:
			{
				NeonErrorResponse *msg_resp = (NeonErrorResponse *) msg;

				/* FIXME: escape double-quotes in the message */
				appendStringInfoString(&s, "{\"type\": \"NeonErrorResponse\"");
				appendStringInfo(&s, ", \"message\": \"%s\"}", msg_resp->message);
				appendStringInfoChar(&s, '}');
				break;
			}
		case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse *msg_resp = (NeonDbSizeResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonDbSizeResponse\"");
				appendStringInfo(&s, ", \"db_size\": %ld}",
								 msg_resp->db_size);
				appendStringInfoChar(&s, '}');

				break;
			}
		case T_NeonGetSlruSegmentResponse:
			{
				NeonGetSlruSegmentResponse *msg_resp = (NeonGetSlruSegmentResponse *) msg;

				appendStringInfoString(&s, "{\"type\": \"NeonGetSlruSegmentResponse\"");
				appendStringInfo(&s, ", \"n_blocks\": %u}",
								 msg_resp->n_blocks);
				appendStringInfoChar(&s, '}');

				break;
			}

		default:
			appendStringInfo(&s, "{\"type\": \"unknown 0x%02x\"", msg->tag);
	}
	return s.data;
}

/*
 *	communicator_init() -- Initialize per-backend private state
 */
void
communicator_init(void)
{
	Size		prfs_size;

	if (MyPState != NULL)
		return;

	/*
	 * Sanity check that theperf counters array is sized correctly. We got
	 * this wrong once, and the formula for max number of backends and aux
	 * processes might well change in the future, so better safe than sorry.
	 * This is a very cheap check so we do it even without assertions.  On
	 * v14, this gets called before initializing MyProc, so we cannot perform
	 * the check here. That's OK, we don't expect the logic to change in old
	 * releases.
	 */
#if PG_VERSION_NUM>=150000
	if (MyNeonCounters >= &neon_per_backend_counters_shared[NUM_NEON_PERF_COUNTER_SLOTS])
		elog(ERROR, "MyNeonCounters points past end of array");
#endif

	prfs_size = offsetof(PrefetchState, prf_buffer) +
		sizeof(PrefetchRequest) * readahead_buffer_size;

	MyPState = MemoryContextAllocZero(TopMemoryContext, prfs_size);

	MyPState->n_unused = readahead_buffer_size;

	MyPState->bufctx = SlabContextCreate(TopMemoryContext,
										 "NeonSMGR/prefetch",
										 SLAB_DEFAULT_BLOCK_SIZE * 17,
										 PS_GETPAGERESPONSE_SIZE);
	MyPState->errctx = AllocSetContextCreate(TopMemoryContext,
											 "NeonSMGR/errors",
											 ALLOCSET_DEFAULT_SIZES);
	MyPState->hashctx = AllocSetContextCreate(TopMemoryContext,
											  "NeonSMGR/prefetch",
											  ALLOCSET_DEFAULT_SIZES);

	MyPState->prf_hash = prfh_create(MyPState->hashctx,
									 readahead_buffer_size, NULL);
}

/*
 *  neon_prefetch_response_usable -- Can a new request be satisfied by old one?
 *
 * This is used to check if the response to a prefetch request can be used to
 * satisfy a page read now.
 */
static bool
neon_prefetch_response_usable(neon_request_lsns *request_lsns,
							  PrefetchRequest *slot)
{
	/* sanity check the LSN's on the old and the new request */
	Assert(request_lsns->request_lsn >= request_lsns->not_modified_since);
	Assert(request_lsns->effective_request_lsn >= request_lsns->not_modified_since);
	Assert(request_lsns->effective_request_lsn <= request_lsns->request_lsn);
	Assert(slot->request_lsns.request_lsn >= slot->request_lsns.not_modified_since);
	Assert(slot->request_lsns.effective_request_lsn >= slot->request_lsns.not_modified_since);
	Assert(slot->request_lsns.effective_request_lsn <= slot->request_lsns.request_lsn);
	Assert(slot->status != PRFS_UNUSED);

	/*
	 * The new request's LSN should never be older than the old one.  This
	 * could be an Assert, except that for testing purposes, we do provide an
	 * interface in neon_test_utils to fetch pages at arbitary LSNs, which
	 * violates this.
	 *
	 * Similarly, the not_modified_since value calculated for a page should
	 * never move backwards. This assumption is a bit fragile; if we updated
	 * the last-written cache when we read in a page, for example, then it
	 * might. But as the code stands, it should not.
	 *
	 * (If two backends issue a request at the same time, they might race and
	 * calculate LSNs "out of order" with each other, but the prefetch queue
	 * is backend-private at the moment.)
	 */
	if (request_lsns->effective_request_lsn < slot->request_lsns.effective_request_lsn ||
		request_lsns->not_modified_since < slot->request_lsns.not_modified_since)
	{
		ereport(LOG,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg(NEON_TAG "request with unexpected LSN after prefetch"),
				 errdetail("Request %X/%X not_modified_since %X/%X, prefetch %X/%X not_modified_since %X/%X)",
						   LSN_FORMAT_ARGS(request_lsns->effective_request_lsn),
						   LSN_FORMAT_ARGS(request_lsns->not_modified_since),
						   LSN_FORMAT_ARGS(slot->request_lsns.effective_request_lsn),
						   LSN_FORMAT_ARGS(slot->request_lsns.not_modified_since))));
		return false;
	}

	/*---
	 * Each request to the pageserver has three LSN values associated with it:
	 * `not_modified_since`, `request_lsn`, and 'effective_request_lsn'.
	 * `not_modified_since` and `request_lsn` are sent to the pageserver, but
	 * in the primary node, we always use UINT64_MAX as the `request_lsn`, so
	 * we remember `effective_request_lsn` separately. In a primary,
	 * `effective_request_lsn` is the same as  `not_modified_since`.
	 * See comments in neon_get_request_lsns why we can not use last flush WAL position here.
	 *
	 * To determine whether a response to a GetPage request issued earlier is
	 * still valid to satisfy a new page read, we look at the
	 * (not_modified_since, effective_request_lsn] range of the request. It is
	 * effectively a claim that the page has not been modified between those
	 * LSNs.  If the range of the old request in the queue overlaps with the
	 * new request, we know that the page hasn't been modified in the union of
	 * the ranges. We can use the response to old request to satisfy the new
	 * request in that case. For example:
	 *
	 *              100      500
	 * Old request:  +--------+
	 *
	 *                     400      800
	 * New request:         +--------+
	 *
	 * The old request claims that the page was not modified between LSNs 100
	 * and 500, and the second claims that it was not modified between 400 and
	 * 800. Together they mean that the page was not modified between 100 and
	 * 800. Therefore the response to the old request is also valid for the
	 * new request.
	 *
	 * This logic also holds at the boundary case that the old request's LSN
	 * matches the new request's not_modified_since LSN exactly:
	 *
	 *              100      500
	 * Old request:  +--------+
	 *
	 *                       500      900
	 * New request:           +--------+
	 *
	 * The response to the old request is the page as it was at LSN 500, and
	 * the page hasn't been changed in the range (500, 900], therefore the
	 * response is valid also for the new request.
	 */

	/* this follows from the checks above */
	Assert(request_lsns->effective_request_lsn >= slot->request_lsns.not_modified_since);

	return request_lsns->not_modified_since <= slot->request_lsns.effective_request_lsn;
}

/*
 *	Does the physical file exist?
 */
bool
communicator_exists(NRelFileInfo rinfo, ForkNumber forkNum, neon_request_lsns *request_lsns)
{
	bool		exists;
	NeonResponse *resp;

	{
		NeonExistsRequest request = {
			.hdr.tag = T_NeonExistsRequest,
			.hdr.lsn = request_lsns->request_lsn,
			.hdr.not_modified_since = request_lsns->not_modified_since,
			.rinfo = rinfo,
			.forknum = forkNum
		};

		resp = page_server_request(&request);

		switch (resp->tag)
		{
			case T_NeonExistsResponse:
			{
				NeonExistsResponse* exists_resp = (NeonExistsResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr) ||
						!RelFileInfoEquals(exists_resp->req.rinfo, request.rinfo) ||
						exists_resp->req.forknum != request.forknum)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u} to exits request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), RelFileInfoFmt(exists_resp->req.rinfo), exists_resp->req.forknum,
													request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), RelFileInfoFmt(request.rinfo), request.forknum);
					}
				}
				exists = exists_resp->exists;
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr))
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match exists request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[reqid %lx] could not read relation existence of rel %u/%u/%u.%u from page server at lsn %X/%08X",
								resp->reqid,
								RelFileInfoFmt(rinfo),
								forkNum,
								LSN_FORMAT_ARGS(request_lsns->effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;

			default:
				NEON_PANIC_CONNECTION_STATE(-1, PANIC,
											"Expected Exists (0x%02x) or Error (0x%02x) response to ExistsRequest, but got 0x%02x",
											T_NeonExistsResponse, T_NeonErrorResponse, resp->tag);
		}
		pfree(resp);
	}
	return exists;
}

/*
 * Read N pages at a specific LSN.
 *
 * *mask is set for pages read at a previous point in time, and which we
 * should not touch, nor overwrite.
 * New bits should be set in *mask for the pages we'successfully read.
 *
 * The offsets in request_lsns, buffers, and mask are linked.
 */
void
communicator_read_at_lsnv(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber base_blockno,
						  neon_request_lsns *request_lsns,
						  void **buffers, BlockNumber nblocks, const bits8 *mask)
{
	NeonResponse *resp;
	uint64		ring_index;
	PrfHashEntry *entry;
	PrefetchRequest *slot;
	PrefetchRequest hashkey;

	Assert(PointerIsValid(request_lsns));
	Assert(nblocks >= 1);

	/*
	 * Use an intermediate PrefetchRequest struct as the hash key to ensure
	 * correct alignment and that the padding bytes are cleared.
	 */
	memset(&hashkey.buftag, 0, sizeof(BufferTag));
	CopyNRelFileInfoToBufTag(hashkey.buftag, rinfo);
	hashkey.buftag.forkNum = forkNum;
	hashkey.buftag.blockNum = base_blockno;

	/*
	 * The redo process does not lock pages that it needs to replay but are
	 * not in the shared buffers, so a concurrent process may request the page
	 * after redo has decided it won't redo that page and updated the LwLSN
	 * for that page. If we're in hot standby we need to take care that we
	 * don't return until after REDO has finished replaying up to that LwLSN,
	 * as the page should have been locked up to that point.
	 *
	 * See also the description on neon_redo_read_buffer_filter below.
	 *
	 * NOTE: It is possible that the WAL redo process will still do IO due to
	 * concurrent failed read IOs. Those IOs should never have a request_lsn
	 * that is as large as the WAL record we're currently replaying, if it
	 * weren't for the behaviour of the LwLsn cache that uses the highest
	 * value of the LwLsn cache when the entry is not found.
	 */
	(void) prefetch_register_bufferv(hashkey.buftag, request_lsns, nblocks, mask, false);

	for (int i = 0; i < nblocks; i++)
	{
		void	   *buffer = buffers[i];
		BlockNumber blockno = base_blockno + i;
		neon_request_lsns *reqlsns = &request_lsns[i];
		TimestampTz		start_ts, end_ts;

		if (PointerIsValid(mask) && BITMAP_ISSET(mask, i))
			continue;

		start_ts = GetCurrentTimestamp();

		if (RecoveryInProgress() && MyBackendType != B_STARTUP)
			XLogWaitForReplayOf(reqlsns->request_lsn);

		/*
		 * Try to find prefetched page in the list of received pages.
		 */
Retry:
		hashkey.buftag.blockNum = blockno;
		entry = prfh_lookup(MyPState->prf_hash, &hashkey);

		if (entry != NULL)
		{
			slot = entry->slot;
			if (neon_prefetch_response_usable(reqlsns, slot))
			{
				ring_index = slot->my_ring_index;
			}
			else
			{
				/*
				 * Cannot use this prefetch, discard it
				 *
				 * We can't drop cache for not-yet-received requested items. It is
				 * unlikely this happens, but it can happen if prefetch distance
				 * is large enough and a backend didn't consume all prefetch
				 * requests.
				 */
				if (slot->status == PRFS_REQUESTED)
				{
					if (!prefetch_wait_for(slot->my_ring_index))
						goto Retry;
				}
				/* drop caches */
				prefetch_set_unused(slot->my_ring_index);
				pgBufferUsage.prefetch.expired += 1;
				MyNeonCounters->getpage_prefetch_discards_total++;
				/* make it look like a prefetch cache miss */
				entry = NULL;
			}
		}

		do
		{
			if (entry == NULL)
			{
				ring_index = prefetch_register_bufferv(hashkey.buftag, reqlsns, 1, NULL, false);
				Assert(ring_index != UINT64_MAX);
				slot = GetPrfSlot(ring_index);
			}
			else
			{
				/*
				 * Empty our reference to the prefetch buffer's hash entry. When
				 * we wait for prefetches, the entry reference is invalidated by
				 * potential updates to the hash, and when we reconnect to the
				 * pageserver the prefetch we're waiting for may be dropped, in
				 * which case we need to retry and take the branch above.
				 */
				entry = NULL;
			}

			Assert(slot->my_ring_index == ring_index);
			Assert(MyPState->ring_last <= ring_index &&
				   MyPState->ring_unused > ring_index);
			Assert(slot->status != PRFS_UNUSED);
			Assert(GetPrfSlot(ring_index) == slot);

		} while (!prefetch_wait_for(ring_index));

		Assert(slot->status == PRFS_RECEIVED);
		Assert(memcmp(&hashkey.buftag, &slot->buftag, sizeof(BufferTag)) == 0);
		Assert(hashkey.buftag.blockNum == base_blockno + i);

		resp = slot->response;

		switch (resp->tag)
		{
			case T_NeonGetPageResponse:
			{
				NeonGetPageResponse* getpage_resp = (NeonGetPageResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (resp->reqid != slot->reqid ||
						resp->lsn != slot->request_lsns.request_lsn ||
						resp->not_modified_since != slot->request_lsns.not_modified_since ||
						!RelFileInfoEquals(getpage_resp->req.rinfo, rinfo) ||
						getpage_resp->req.forknum != forkNum ||
						getpage_resp->req.blkno != base_blockno + i)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u, block=%u} to get page request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u, block=%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), RelFileInfoFmt(getpage_resp->req.rinfo), getpage_resp->req.forknum, getpage_resp->req.blkno,
													slot->reqid, LSN_FORMAT_ARGS(slot->request_lsns.request_lsn), LSN_FORMAT_ARGS(slot->request_lsns.not_modified_since), RelFileInfoFmt(rinfo), forkNum, base_blockno + i);
					}
				}
				memcpy(buffer, getpage_resp->page, BLCKSZ);

				/*
				 * With lfc_store_prefetch_result=true prefetch result is stored in LFC in prefetch_pump_state when response is received
				 * from page server. But if lfc_store_prefetch_result=false then it is not yet stored in LFC and we have to do it here
				 * under buffer lock.
				 */
				if (!lfc_store_prefetch_result)
					lfc_write(rinfo, forkNum, blockno, buffer);
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (resp->reqid != slot->reqid ||
						resp->lsn != slot->request_lsns.request_lsn ||
						resp->not_modified_since != slot->request_lsns.not_modified_since)
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get relsize request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 slot->reqid, LSN_FORMAT_ARGS(slot->request_lsns.request_lsn), LSN_FORMAT_ARGS(slot->request_lsns.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[shard %d, reqid %lx] could not read block %u in rel %u/%u/%u.%u from page server at lsn %X/%08X",
								slot->shard_no, resp->reqid, blockno, RelFileInfoFmt(rinfo),
								forkNum, LSN_FORMAT_ARGS(reqlsns->effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;
			default:
				NEON_PANIC_CONNECTION_STATE(slot->shard_no, PANIC,
											"Expected GetPage (0x%02x) or Error (0x%02x) response to GetPageRequest, but got 0x%02x",
											T_NeonGetPageResponse, T_NeonErrorResponse, resp->tag);
		}

		/* buffer was used, clean up for later reuse */
		prefetch_set_unused(ring_index);
		prefetch_cleanup_trailing_unused();

		end_ts = GetCurrentTimestamp();
		inc_getpage_wait(end_ts >= start_ts ? (end_ts - start_ts) : 0);
	}
}

/*
 *	neon_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
communicator_nblocks(NRelFileInfo rinfo, ForkNumber forknum, neon_request_lsns *request_lsns)
{
	NeonResponse *resp;
	BlockNumber n_blocks;

	{
		NeonNblocksRequest request = {
			.hdr.tag = T_NeonNblocksRequest,
			.hdr.lsn = request_lsns->request_lsn,
			.hdr.not_modified_since = request_lsns->not_modified_since,
			.rinfo = rinfo,
			.forknum = forknum,
		};

		resp = page_server_request(&request);

		switch (resp->tag)
		{
			case T_NeonNblocksResponse:
			{
				NeonNblocksResponse * relsize_resp = (NeonNblocksResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr) ||
						!RelFileInfoEquals(relsize_resp->req.rinfo, request.rinfo) ||
						relsize_resp->req.forknum != forknum)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u} to get relsize request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, rel=%u/%u/%u.%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), RelFileInfoFmt(relsize_resp->req.rinfo), relsize_resp->req.forknum,
													request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), RelFileInfoFmt(request.rinfo), forknum);
					}
				}
				n_blocks = relsize_resp->n_blocks;
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr))
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get relsize request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[reqid %lx] could not read relation size of rel %u/%u/%u.%u from page server at lsn %X/%08X",
								resp->reqid,
								RelFileInfoFmt(rinfo),
								forknum,
								LSN_FORMAT_ARGS(request_lsns->effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;

			default:
				NEON_PANIC_CONNECTION_STATE(-1, PANIC,
											"Expected Nblocks (0x%02x) or Error (0x%02x) response to NblocksRequest, but got 0x%02x",
											T_NeonNblocksResponse, T_NeonErrorResponse, resp->tag);
		}

		pfree(resp);
	}
	return n_blocks;
}

/*
 *	neon_db_size() -- Get the size of the database in bytes.
 */
int64
communicator_dbsize(Oid dbNode, neon_request_lsns *request_lsns)
{
	NeonResponse *resp;
	int64		db_size;

	{
		NeonDbSizeRequest request = {
			.hdr.tag = T_NeonDbSizeRequest,
			.hdr.lsn = request_lsns->request_lsn,
			.hdr.not_modified_since = request_lsns->not_modified_since,
			.dbNode = dbNode,
		};

		resp = page_server_request(&request);

		switch (resp->tag)
		{
			case T_NeonDbSizeResponse:
			{
				NeonDbSizeResponse* dbsize_resp = (NeonDbSizeResponse *) resp;
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr) ||
						dbsize_resp->req.dbNode != dbNode)
					{
						NEON_PANIC_CONNECTION_STATE(-1, PANIC,
													"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, dbNode=%u} to get DB size request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, dbNode=%u}",
													resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), dbsize_resp->req.dbNode,
													request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), dbNode);
					}
				}
				db_size = dbsize_resp->db_size;
				break;
			}
			case T_NeonErrorResponse:
				if (neon_protocol_version >= 3)
				{
					if (!equal_requests(resp, &request.hdr))
					{
						elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get DB size request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
							 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
							 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
					}
				}
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg(NEON_TAG "[reqid %lx] could not read db size of db %u from page server at lsn %X/%08X",
								resp->reqid,
								dbNode, LSN_FORMAT_ARGS(request_lsns->effective_request_lsn)),
						 errdetail("page server returned error: %s",
								   ((NeonErrorResponse *) resp)->message)));
				break;

			default:
				NEON_PANIC_CONNECTION_STATE(-1, PANIC,
											"Expected DbSize (0x%02x) or Error (0x%02x) response to DbSizeRequest, but got 0x%02x",
											T_NeonDbSizeResponse, T_NeonErrorResponse, resp->tag);
		}

		pfree(resp);
	}
	return db_size;
}

int
communicator_read_slru_segment(SlruKind kind, int64 segno, neon_request_lsns *request_lsns,
							   void *buffer)
{
	int			n_blocks;
	shardno_t	shard_no = 0; /* All SLRUs are at shard 0 */
	NeonResponse *resp;
	NeonGetSlruSegmentRequest request;

	request = (NeonGetSlruSegmentRequest) {
		.hdr.tag = T_NeonGetSlruSegmentRequest,
		.hdr.lsn = request_lsns->request_lsn,
		.hdr.not_modified_since = request_lsns->not_modified_since,
		.kind = kind,
		.segno = segno
	};

	do
	{
		while (!page_server->send(shard_no, &request.hdr) || !page_server->flush(shard_no));

		consume_prefetch_responses();

		resp = page_server->receive(shard_no);
	} while (resp == NULL);

	switch (resp->tag)
	{
		case T_NeonGetSlruSegmentResponse:
		{
			NeonGetSlruSegmentResponse* slru_resp = (NeonGetSlruSegmentResponse *) resp;
			if (neon_protocol_version >= 3)
			{
				if (!equal_requests(resp, &request.hdr) ||
					slru_resp->req.kind != kind ||
					slru_resp->req.segno != segno)
				{
					NEON_PANIC_CONNECTION_STATE(-1, PANIC,
												"Unexpect response {reqid=%lx,lsn=%X/%08X, since=%X/%08X, kind=%u, segno=%u} to get SLRU segment request {reqid=%lx,lsn=%X/%08X, since=%X/%08X, kind=%u, segno=%lluu}",
												resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since), slru_resp->req.kind, slru_resp->req.segno,
												request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since), kind, (unsigned long long) segno);
				}
			}
			n_blocks = slru_resp->n_blocks;
			memcpy(buffer, slru_resp->data, n_blocks*BLCKSZ);
			break;
		}
		case T_NeonErrorResponse:
			if (neon_protocol_version >= 3)
			{
				if (!equal_requests(resp, &request.hdr))
				{
					elog(WARNING, NEON_TAG "Error message {reqid=%lx,lsn=%X/%08X, since=%X/%08X} doesn't match get SLRU segment request {reqid=%lx,lsn=%X/%08X, since=%X/%08X}",
						 resp->reqid, LSN_FORMAT_ARGS(resp->lsn), LSN_FORMAT_ARGS(resp->not_modified_since),
						 request.hdr.reqid, LSN_FORMAT_ARGS(request.hdr.lsn), LSN_FORMAT_ARGS(request.hdr.not_modified_since));
				}
			}
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg(NEON_TAG "[reqid %lx] could not read SLRU %d segment %llu at lsn %X/%08X",
							resp->reqid,
							kind,
							(unsigned long long) segno,
							LSN_FORMAT_ARGS(request_lsns->request_lsn)),
					 errdetail("page server returned error: %s",
							   ((NeonErrorResponse *) resp)->message)));
			break;

		default:
			NEON_PANIC_CONNECTION_STATE(-1, PANIC,
										"Expected GetSlruSegment (0x%02x) or Error (0x%02x) response to GetSlruSegmentRequest, but got 0x%02x",
										T_NeonGetSlruSegmentResponse, T_NeonErrorResponse, resp->tag);
	}
	pfree(resp);

	communicator_reconfigure_timeout_if_needed();
	return n_blocks;
}

void
communicator_reconfigure_timeout_if_needed(void)
{
	bool	needs_set = MyPState->ring_receive != MyPState->ring_unused &&
						readahead_getpage_pull_timeout_ms > 0;

	if (needs_set != timeout_set)
	{
		/* The background writer doens't (shouldn't) read any pages */
		Assert(!AmBackgroundWriterProcess());
		/* The checkpointer doens't (shouldn't) read any pages */
		Assert(!AmCheckpointerProcess());

		if (unlikely(PS_TIMEOUT_ID == 0))
		{
			PS_TIMEOUT_ID = RegisterTimeout(USER_TIMEOUT, pagestore_timeout_handler);
		}

		if (needs_set)
		{
#if PG_MAJORVERSION_NUM <= 14
			enable_timeout_after(PS_TIMEOUT_ID, readahead_getpage_pull_timeout_ms);
#else
			enable_timeout_every(
				PS_TIMEOUT_ID,
				TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
											readahead_getpage_pull_timeout_ms),
				readahead_getpage_pull_timeout_ms
			);
#endif
			timeout_set = true;
		}
		else
		{
			Assert(timeout_set);
			disable_timeout(PS_TIMEOUT_ID, false);
			timeout_set = false;
		}
	}
}

static void
pagestore_timeout_handler(void)
{
#if PG_MAJORVERSION_NUM <= 14
	/*
	 * PG14: Setting a repeating timeout is not possible, so we signal here
	 * that the timeout has already been reset, and by telling the system
	 * that system will re-schedule it later if we need to.
	 */
	timeout_set = false;
#endif
	timeout_signaled = true;
	InterruptPending = true;
}

/*
 * Process new data received in our active PageStream sockets.
 *
 * This relies on the invariant that all pipelined yet-to-be-received requests
 * are getPage requests managed by MyPState. This is currently true, any
 * modification will probably require some stuff to make it work again.
 */
static bool
communicator_processinterrupts(void)
{
	if (timeout_signaled)
	{
		if (!readpage_reentrant_guard && readahead_getpage_pull_timeout_ms > 0)
			communicator_prefetch_pump_state();

		timeout_signaled = false;
		communicator_reconfigure_timeout_if_needed();
	}

	if (!prev_interrupt_cb)
		return false;

	return prev_interrupt_cb();
}
