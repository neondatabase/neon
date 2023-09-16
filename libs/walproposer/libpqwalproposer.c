#include "postgres.h"
#include "neon.h"
#include "walproposer.h"
#include "rust_bindings.h"
#include "replication/message.h"
#include "access/xlog_internal.h"

// defined in walproposer.h
uint64 sim_redo_start_lsn;
XLogRecPtr sim_latest_available_lsn;

/* Header in walproposer.h -- Wrapper struct to abstract away the libpq connection */
struct WalProposerConn
{
	int64_t tcp;
};

/* Helper function */
static bool
ensure_nonblocking_status(WalProposerConn *conn, bool is_nonblocking)
{
	// walprop_log(LOG, "not implemented");
    return false;
}

/* Exported function definitions */
char *
walprop_error_message(WalProposerConn *conn)
{
	// walprop_log(LOG, "not implemented");
    return NULL;
}

WalProposerConnStatusType
walprop_status(WalProposerConn *conn)
{
	// walprop_log(LOG, "not implemented: walprop_status");
    return WP_CONNECTION_OK;
}

WalProposerConn *
walprop_connect_start(char *conninfo)
{
	WalProposerConn *conn;

	walprop_log(LOG, "walprop_connect_start: %s", conninfo);
	
	const char *connstr_prefix = "host=node port=";
	Assert(strncmp(conninfo, connstr_prefix, strlen(connstr_prefix)) == 0);

	int nodeId = atoi(conninfo + strlen(connstr_prefix));

	conn = palloc(sizeof(WalProposerConn));
	conn->tcp = sim_open_tcp(nodeId);
	return conn;
}

WalProposerConnectPollStatusType
walprop_connect_poll(WalProposerConn *conn)
{
	// walprop_log(LOG, "not implemented: walprop_connect_poll");
    return WP_CONN_POLLING_OK;
}

bool
walprop_send_query(WalProposerConn *conn, char *query)
{
	// walprop_log(LOG, "not implemented: walprop_send_query");
    return true;
}

WalProposerExecStatusType
walprop_get_query_result(WalProposerConn *conn)
{
	// walprop_log(LOG, "not implemented: walprop_get_query_result");
    return WP_EXEC_SUCCESS_COPYBOTH;
}

pgsocket
walprop_socket(WalProposerConn *conn)
{
	return (pgsocket) conn->tcp;
}

int
walprop_flush(WalProposerConn *conn)
{
	// walprop_log(LOG, "not implemented");
    return 0;
}

void
walprop_finish(WalProposerConn *conn)
{
	// walprop_log(LOG, "walprop_finish not implemented");
}

/*
 * Receive a message from the safekeeper.
 *
 * On success, the data is placed in *buf. It is valid until the next call
 * to this function.
 */
PGAsyncReadResult
walprop_async_read(WalProposerConn *conn, char **buf, int *amount)
{
	uintptr_t len;
	char *msg;
	Event event;

	event = sim_epoll_peek(0);
	if (event.tcp != conn->tcp || event.tag != Message || event.any_message != Bytes)
		return PG_ASYNC_READ_TRY_AGAIN;

	event = sim_epoll_rcv(0);

	// walprop_log(LOG, "walprop_async_read, T: %d, tcp: %d, tag: %d", (int) event.tag, (int) event.tcp, (int) event.any_message);
	Assert(event.tcp == conn->tcp);
	Assert(event.tag == Message);
	Assert(event.any_message == Bytes);
	
	msg = (char*) sim_msg_get_bytes(&len);
	*buf = msg;
	*amount = len;
	// walprop_log(LOG, "walprop_async_read: %d", (int) len);

    return PG_ASYNC_READ_SUCCESS;
}

PGAsyncWriteResult
walprop_async_write(WalProposerConn *conn, void const *buf, size_t size)
{
	// walprop_log(LOG, "walprop_async_write");
	sim_msg_set_bytes(buf, size);
	sim_tcp_send(conn->tcp);
    return PG_ASYNC_WRITE_SUCCESS;
}

/*
 * This function is very similar to walprop_async_write. For more
 * information, refer to the comments there.
 */
bool
walprop_blocking_write(WalProposerConn *conn, void const *buf, size_t size)
{
	// walprop_log(LOG, "walprop_blocking_write");
	sim_msg_set_bytes(buf, size);
	sim_tcp_send(conn->tcp);
    return true;
}

void
sim_start_replication(XLogRecPtr startptr)
{
	walprop_log(LOG, "sim_start_replication: %X/%X", LSN_FORMAT_ARGS(startptr));
	sim_latest_available_lsn = startptr;

	for (;;)
	{
		XLogRecPtr endptr = sim_latest_available_lsn;

		Assert(startptr <= endptr);
		if (endptr > startptr)
		{
			WalProposerBroadcast(startptr, endptr);
			startptr = endptr;
		}

		WalProposerPoll();
	}
}

#define UsableBytesInPage (XLOG_BLCKSZ - SizeOfXLogShortPHD)

static int UsableBytesInSegment =
		(DEFAULT_XLOG_SEG_SIZE / XLOG_BLCKSZ * UsableBytesInPage) -
		(SizeOfXLogLongPHD - SizeOfXLogShortPHD);

/*
 * Converts a "usable byte position" to XLogRecPtr. A usable byte position
 * is the position starting from the beginning of WAL, excluding all WAL
 * page headers.
 */
static XLogRecPtr
XLogBytePosToRecPtr(uint64 bytepos)
{
	uint64		fullsegs;
	uint64		fullpages;
	uint64		bytesleft;
	uint32		seg_offset;
	XLogRecPtr	result;

	fullsegs = bytepos / UsableBytesInSegment;
	bytesleft = bytepos % UsableBytesInSegment;

	if (bytesleft < XLOG_BLCKSZ - SizeOfXLogLongPHD)
	{
		/* fits on first page of segment */
		seg_offset = bytesleft + SizeOfXLogLongPHD;
	}
	else
	{
		/* account for the first page on segment with long header */
		seg_offset = XLOG_BLCKSZ;
		bytesleft -= XLOG_BLCKSZ - SizeOfXLogLongPHD;

		fullpages = bytesleft / UsableBytesInPage;
		bytesleft = bytesleft % UsableBytesInPage;

		seg_offset += fullpages * XLOG_BLCKSZ + bytesleft + SizeOfXLogShortPHD;
	}

	XLogSegNoOffsetToRecPtr(fullsegs, seg_offset, wal_segment_size, result);

	return result;
}

/*
 * Convert an XLogRecPtr to a "usable byte position".
 */
static uint64
XLogRecPtrToBytePos(XLogRecPtr ptr)
{
	uint64		fullsegs;
	uint32		fullpages;
	uint32		offset;
	uint64		result;

	XLByteToSeg(ptr, fullsegs, wal_segment_size);

	fullpages = (XLogSegmentOffset(ptr, wal_segment_size)) / XLOG_BLCKSZ;
	offset = ptr % XLOG_BLCKSZ;

	if (fullpages == 0)
	{
		result = fullsegs * UsableBytesInSegment;
		if (offset > 0)
		{
			Assert(offset >= SizeOfXLogLongPHD);
			result += offset - SizeOfXLogLongPHD;
		}
	}
	else
	{
		result = fullsegs * UsableBytesInSegment +
			(XLOG_BLCKSZ - SizeOfXLogLongPHD) + /* account for first page */
			(fullpages - 1) * UsableBytesInPage;	/* full pages */
		if (offset > 0)
		{
			Assert(offset >= SizeOfXLogShortPHD);
			result += offset - SizeOfXLogShortPHD;
		}
	}

	return result;
}

#define max_rdatas 16

void InitMyInsert();
static void MyBeginInsert();
static void MyRegisterData(char *data, int len);
static XLogRecPtr MyFinishInsert(RmgrId rmid, uint8 info, uint8 flags);
static void MyCopyXLogRecordToWAL(int write_len, XLogRecData *rdata, XLogRecPtr StartPos, XLogRecPtr EndPos);

/*
 * An array of XLogRecData structs, to hold registered data.
 */
static XLogRecData rdatas[max_rdatas];
static int	num_rdatas;			/* entries currently used */
static uint32 mainrdata_len;	/* total # of bytes in chain */
static XLogRecData hdr_rdt;
static char hdr_scratch[16000];
static XLogRecPtr CurrBytePos;
static XLogRecPtr PrevBytePos;

void InitMyInsert()
{
	CurrBytePos = sim_redo_start_lsn;
	PrevBytePos = InvalidXLogRecPtr;
}

static void MyBeginInsert()
{
	num_rdatas = 0;
	mainrdata_len = 0;
}

static void MyRegisterData(char *data, int len)
{
	XLogRecData *rdata;

	if (num_rdatas >= max_rdatas)
		walprop_log(ERROR, "too much WAL data");
	rdata = &rdatas[num_rdatas++];

	rdata->data = data;
	rdata->len = len;
	rdata->next = NULL;

	if (num_rdatas > 1) {
		rdatas[num_rdatas - 2].next = rdata;
	}

	mainrdata_len += len;
}

static XLogRecPtr
MyFinishInsert(RmgrId rmid, uint8 info, uint8 flags)
{
	XLogRecData *rdt;
	uint32		total_len = 0;
	int			block_id;
	pg_crc32c	rdata_crc;
	XLogRecord *rechdr;
	char	   *scratch = hdr_scratch;
	int         size;
	XLogRecPtr  StartPos;
	XLogRecPtr  EndPos;
	uint64		startbytepos;
	uint64		endbytepos;

	/*
	 * Note: this function can be called multiple times for the same record.
	 * All the modifications we do to the rdata chains below must handle that.
	 */

	/* The record begins with the fixed-size header */
	rechdr = (XLogRecord *) scratch;
	scratch += SizeOfXLogRecord;

	hdr_rdt.data = hdr_scratch;
	
	if (num_rdatas > 0)
	{
		hdr_rdt.next = &rdatas[0];
	}
	else
	{
		hdr_rdt.next = NULL;
	}

	/* followed by main data, if any */
	if (mainrdata_len > 0)
	{
		if (mainrdata_len > 255)
		{
			*(scratch++) = (char) XLR_BLOCK_ID_DATA_LONG;
			memcpy(scratch, &mainrdata_len, sizeof(uint32));
			scratch += sizeof(uint32);
		}
		else
		{
			*(scratch++) = (char) XLR_BLOCK_ID_DATA_SHORT;
			*(scratch++) = (uint8) mainrdata_len;
		}
		total_len += mainrdata_len;
	}

	hdr_rdt.len = (scratch - hdr_scratch);
	total_len += hdr_rdt.len;

	/*
	 * Calculate CRC of the data
	 *
	 * Note that the record header isn't added into the CRC initially since we
	 * don't know the prev-link yet.  Thus, the CRC will represent the CRC of
	 * the whole record in the order: rdata, then backup blocks, then record
	 * header.
	 */
	INIT_CRC32C(rdata_crc);
	COMP_CRC32C(rdata_crc, hdr_scratch + SizeOfXLogRecord, hdr_rdt.len - SizeOfXLogRecord);
	for (size_t i = 0; i < num_rdatas; i++)
	{
		rdt = &rdatas[i];
		COMP_CRC32C(rdata_crc, rdt->data, rdt->len);
	}

	/*
	 * Fill in the fields in the record header. Prev-link is filled in later,
	 * once we know where in the WAL the record will be inserted. The CRC does
	 * not include the record header yet.
	 */
	rechdr->xl_xid = 0;
	rechdr->xl_tot_len = total_len;
	rechdr->xl_info = info;
	rechdr->xl_rmid = rmid;
	rechdr->xl_prev = InvalidXLogRecPtr;
	rechdr->xl_crc = rdata_crc;

	size = MAXALIGN(rechdr->xl_tot_len);

	/* All (non xlog-switch) records should contain data. */
	Assert(size > SizeOfXLogRecord);

	startbytepos = XLogRecPtrToBytePos(CurrBytePos);
	endbytepos = startbytepos + size;

	// Get the position.
	StartPos = XLogBytePosToRecPtr(startbytepos);
	EndPos = XLogBytePosToRecPtr(startbytepos + size);
	rechdr->xl_prev = PrevBytePos;

	Assert(XLogRecPtrToBytePos(StartPos) == startbytepos);
	Assert(XLogRecPtrToBytePos(EndPos) == endbytepos);

	// Update global pointers.
	CurrBytePos = EndPos;
	PrevBytePos = StartPos;

	/*
	 * Now that xl_prev has been filled in, calculate CRC of the record
	 * header.
	 */
	rdata_crc = rechdr->xl_crc;
	COMP_CRC32C(rdata_crc, rechdr, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(rdata_crc);
	rechdr->xl_crc = rdata_crc;

	// Now write it to disk.
	MyCopyXLogRecordToWAL(rechdr->xl_tot_len, &hdr_rdt, StartPos, EndPos);

	return EndPos;
}

#define INSERT_FREESPACE(endptr)	\
	(((endptr) % XLOG_BLCKSZ == 0) ? 0 : (XLOG_BLCKSZ - (endptr) % XLOG_BLCKSZ))

static void
MyCopyXLogRecordToWAL(int write_len, XLogRecData *rdata, XLogRecPtr StartPos, XLogRecPtr EndPos)
{
	XLogRecPtr	CurrPos;
	int			written;
	int			freespace;

	// Write hdr_rdt and `num_rdatas` other datas.
	CurrPos = StartPos;
	freespace = INSERT_FREESPACE(CurrPos);
	written = 0;

	Assert(freespace >= sizeof(uint32));

	while (rdata != NULL)
	{
		char	   *rdata_data = rdata->data;
		int			rdata_len = rdata->len;

		while (rdata_len >= freespace)
		{
			char header_buf[SizeOfXLogLongPHD];
			XLogPageHeader NewPage = (XLogPageHeader) header_buf;
			
			Assert(CurrPos % XLOG_BLCKSZ >= SizeOfXLogShortPHD || freespace == 0);
			XLogWalPropWrite(rdata_data, freespace, CurrPos);
			rdata_data += freespace;
			rdata_len -= freespace;
			written += freespace;
			CurrPos += freespace;

			// Init new page
			MemSet(header_buf, 0, SizeOfXLogLongPHD);
			
			/*
			 * Fill the new page's header
			 */
			NewPage->xlp_magic = XLOG_PAGE_MAGIC;

			/* NewPage->xlp_info = 0; */	/* done by memset */
			NewPage->xlp_tli = 1;
			NewPage->xlp_pageaddr = CurrPos;

			/* NewPage->xlp_rem_len = 0; */	/* done by memset */
			NewPage->xlp_info |= XLP_BKP_REMOVABLE;

			/*
			 * If first page of an XLOG segment file, make it a long header.
			 */
			if ((XLogSegmentOffset(NewPage->xlp_pageaddr, wal_segment_size)) == 0)
			{
				XLogLongPageHeader NewLongPage = (XLogLongPageHeader) NewPage;

				NewLongPage->xlp_sysid = 0;
				NewLongPage->xlp_seg_size = wal_segment_size;
				NewLongPage->xlp_xlog_blcksz = XLOG_BLCKSZ;
				NewPage->xlp_info |= XLP_LONG_HEADER;
			}

			NewPage->xlp_rem_len = write_len - written;
			if (NewPage->xlp_rem_len > 0) {
				NewPage->xlp_info |= XLP_FIRST_IS_CONTRECORD;
			}

			/* skip over the page header */
			if (XLogSegmentOffset(CurrPos, wal_segment_size) == 0)
			{
				XLogWalPropWrite(header_buf, SizeOfXLogLongPHD, CurrPos);
				CurrPos += SizeOfXLogLongPHD;
			}
			else
			{
				XLogWalPropWrite(header_buf, SizeOfXLogShortPHD, CurrPos);
				CurrPos += SizeOfXLogShortPHD;
			}
			freespace = INSERT_FREESPACE(CurrPos);
		}

		Assert(CurrPos % XLOG_BLCKSZ >= SizeOfXLogShortPHD || rdata_len == 0);
		XLogWalPropWrite(rdata_data, rdata_len, CurrPos);
		CurrPos += rdata_len;
		written += rdata_len;
		freespace -= rdata_len;

		rdata = rdata->next;
	}

	Assert(written == write_len);
	CurrPos = MAXALIGN64(CurrPos);
	Assert(CurrPos == EndPos);
}

XLogRecPtr MyInsertRecord()
{
	const char *prefix = "prefix";
	const char *message = "message";
	size_t size = 7;
	bool transactional = false;

	xl_logical_message xlrec;

	xlrec.dbId = 0;
	xlrec.transactional = transactional;
	/* trailing zero is critical; see logicalmsg_desc */
	xlrec.prefix_size = strlen(prefix) + 1;
	xlrec.message_size = size;

	MyBeginInsert();
	MyRegisterData((char *) &xlrec, SizeOfLogicalMessage);
	MyRegisterData(unconstify(char *, prefix), xlrec.prefix_size);
	MyRegisterData(unconstify(char *, message), size);

	return MyFinishInsert(RM_LOGICALMSG_ID, XLOG_LOGICAL_MESSAGE, XLOG_INCLUDE_ORIGIN);
}
