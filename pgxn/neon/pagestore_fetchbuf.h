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
	PRFS_UNUSED = 0, /* unused slot */
	PRFS_REQUESTED, /* request is sent to PS, all fields except response valid */
	PRFS_RECEIVED, /* all fields valid, response contains data */
	PRFS_TAG_REMAINS, /* only buftag, *OfRel are still valid */
} PrefetchStatus;

typedef struct PrefetchRequest {
	BufferTag	buftag; /* must be first entry in the struct */
	XLogRecPtr	effective_request_lsn;
	NeonResponse *response; /* may be null */
	PrefetchStatus status;
	uint64		my_ring_index;
} PrefetchRequest;

/*
 * PrefetchState maintains the state of (prefetch) getPage@LSN requests.
 * It maintains a (ring) buffer of in-flight requests and responses.
 * 
 * We maintain several indexes into the ring buffer:
 * ring_unused >= ring_receive >= ring_last >= 0
 * 
 * ring_unused points to the first unused slot of the buffer
 * ring_receive is the next request that is to be received
 * ring_last is the oldest received entry in the buffer
 * 
 * Apart from being an entry in the ring buffer of prefetch requests, each
 * PrefetchRequest is linked to the next and previous PrefetchRequest of its
 * RelNodeFork through the nextOfRel and prevOfRel relative pointers into the
 * ring buffer. This provides a linked list for each relations' fork, which
 * will allow us to detect sequential scans; eventually removing (or reducing)
 * the need for core modifications in the heap AM for prefetching buffers.
 */
typedef struct PrefetchState {
	MemoryContext bufctx; /* context for prf_buffer[].response allocations */
	MemoryContext errctx; /* context for prf_buffer[].response allocations */
	MemoryContext hashctx; /* context for prf_buffer */

	/* buffer indexes */
	uint64	ring_unused;		/* first unused slot */
	uint64	ring_receive;		/* next slot that is to receive a response */
	uint64	ring_last;			/* min slot with a response value */

	/* metrics / statistics  */
	int		n_responses_buffered;	/* count of PS responses not yet in buffers */
	int		n_requestes_inflight;	/* count of PS requests considered in flight */
	int		n_unused;				/* count of buffers < unused, > last, that are also unused */

	/* the buffers */
	struct prfh_hash *prf_hash;
	PrefetchRequest prf_buffer[READ_BUFFER_SIZE]; /* prefetch buffers */
} PrefetchState;

#endif //NEON_PAGESTORE_FETCHBUF_H
