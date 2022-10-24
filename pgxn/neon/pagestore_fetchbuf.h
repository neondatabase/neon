#ifndef NEON_PAGESTORE_FETCHBUF_H
#define NEON_PAGESTORE_FETCHBUF_H

#include "storage/buf_internals.h"

/*
 * Prefetch implementation:
 * 
 * Prefetch is performed locally by each backend.
 * 
 * 
 * There can be up to MAX_PREFETCH_REQUESTS registered using smgr_prefetch
 * before smgr_read. All this requests are appended to primary smgr_read request.
 * It is assumed that pages will be requested in prefetch order.
 * Reading of prefetch responses is delayed until them are actually needed (smgr_read).
 * It make it possible to parallelize processing and receiving of prefetched pages.
 * In case of prefetch miss or any other SMGR request other than smgr_read,
 * all prefetch responses has to be consumed.
 */

/* Max amount of tracked buffer reads */
#define READ_BUFFER_SIZE 128

typedef enum PrefetchStatus {
	PRFS_EMPTY = 0,
	PRFS_REQUESTED,
	PRFS_RECEIVED,
} PrefetchStatus;

typedef struct PrefetchRequest {
	BufferTag	buftag;
	XLogRecPtr	effective_request_lsn;
	NeonGetPageResponse *response; /* may be null */
	PrefetchStatus status;
	/*
	 * Relative offsets to next/prev of the relation fork in buftag.
	 * nextOfRel points forward, prevOfRel backwards.
	 */
	uint8		nextOfRel; /* relative offset to next prefetch on this relfork */
	uint8		prevOfRel; /* relative offset to previous prefetch on this relfork */
} PrefetchRequest;

typedef struct PrefetchState {
	int clock_low; /* not yet requested */
	int clock_high; /* prefetches sent to PS */
	int response_handle; /* prefetches handled (from PS), out of .requested */

	MemoryContext context; /* context for prf_buffer[].response allocations*/

	uint64	ring_unused;		/* first unused slot */
	uint64	ring_receive;		/* lowest slot that's set to receive its response */
	uint64	ring_last;			/* last slot with a response value */
	int n_responses_buffered;	/* count of PS responses not yet in buffers */
	int n_requestes_inflight;	/* count of PS requests considered in flight */
	int n_unused;				/* count of buffers < unused, > last, that are also unused */ 
	PrefetchRequest prf_buffer[READ_BUFFER_SIZE]; /* prefetch buffers */
} PrefetchState;

#endif //NEON_PAGESTORE_FETCHBUF_H
