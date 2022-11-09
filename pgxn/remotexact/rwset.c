/* contrib/remotexact/rwset.c */
#include "postgres.h"

#include "access/csn_snapshot.h"
#include "access/transam.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"
#include "rwset.h"
#include "utils/memutils.h"

static void decode_header(RWSet *rwset, StringInfo msg);
static RWSetRelation *decode_relation(RWSet *rwset, StringInfo msg);
static RWSetRelation *alloc_relation(RWSet *rwset);
static RWSetPage *decode_page(RWSet *rwset, StringInfo msg);
static RWSetPage *alloc_page(RWSet *rwset);
static RWSetTuple *decode_tuple(RWSet *rwset, StringInfo msg);
static RWSetTuple *alloc_tuple(RWSet *rwset);

static void append_tuple_string(StringInfo str, const LogicalRepTupleData *tuple);
static void free_tuple(LogicalRepTupleData* tuple);

RWSet *
RWSetAllocate(void)
{
	RWSet	   *rwset;
	MemoryContext new_ctx;

	new_ctx = AllocSetContextCreate(CurrentMemoryContext,
									"Read/write set",
									ALLOCSET_DEFAULT_SIZES);
	rwset = (RWSet *) MemoryContextAlloc(new_ctx, sizeof(RWSet));

	rwset->context = new_ctx;

	rwset->header.dbid = 0;

	dlist_init(&rwset->relations);

	rwset->writes = NULL;
	rwset->writes_len = 0;

	return rwset;
}

void
RWSetFree(RWSet *rwset)
{
	MemoryContextDelete(rwset->context);
}

void
RWSetDecode(RWSet *rwset, StringInfo msg)
{
	int			read_set_len;
	int			consumed = 0;
	int			prev_cursor;

	decode_header(rwset, msg);

	read_set_len = pq_getmsgint(msg, 4);

	if (read_set_len > msg->len)
		ereport(ERROR,
				errmsg("length of read set (%d) too large", read_set_len),
				errdetail("remaining message length: %d", msg->len));

	while (consumed < read_set_len)
	{
		RWSetRelation *rel;

		prev_cursor = msg->cursor;

		rel = decode_relation(rwset, msg);
		dlist_push_tail(&rwset->relations, &rel->node);

		consumed += msg->cursor - prev_cursor;
	}

	if (consumed > read_set_len)
		ereport(ERROR,
				errmsg("length of read set (%d) is corrupted", read_set_len),
				errdetail("length of decoded read set: %d", consumed));
	
	rwset->writes_len = msg->len - msg->cursor;
	rwset->writes = (char *) MemoryContextAlloc(rwset->context, sizeof(char) * rwset->writes_len);
	pq_copymsgbytes(msg, rwset->writes, rwset->writes_len);
}

void
decode_header(RWSet *rwset, StringInfo msg)
{
	rwset->header.dbid = pq_getmsgint(msg, 4);
	rwset->header.region_set = pq_getmsgint64(msg);
}

RWSetRelation *
decode_relation(RWSet *rwset, StringInfo msg)
{
	RWSetRelation *rel;
	unsigned char reltype;
	int			nitems;
	int			i;

	rel = alloc_relation(rwset);

	reltype = pq_getmsgbyte(msg);

	if (reltype == 'I')
		rel->is_index = true;
	else if (reltype == 'T')
		rel->is_index = false;
	else
		ereport(ERROR, errmsg("invalid relation type: %c", reltype));

	rel->relid = pq_getmsgint(msg, 4);
	rel->region = pq_getmsgbyte(msg);
	rel->csn = pq_getmsgint64(msg);
	rel->is_table_scan = pq_getmsgbyte(msg);
	nitems = pq_getmsgint(msg, 4);

	if (rel->is_index)
		for (i = 0; i < nitems; i++)
		{
			RWSetPage  *page = decode_page(rwset, msg);
			dlist_push_tail(&rel->pages, &page->node);
		}
	else
		for (i = 0; i < nitems; i++)
		{
			RWSetTuple *tup = decode_tuple(rwset, msg);
			dlist_push_tail(&rel->tuples, &tup->node);
		}

	return rel;
}

RWSetRelation *
alloc_relation(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetRelation *rel;

	ctx = rwset->context;
	rel = (RWSetRelation *) MemoryContextAlloc(ctx, sizeof(RWSetRelation));

	memset(rel, 0, sizeof(RWSetRelation));

	dlist_init(&rel->pages);
	dlist_init(&rel->tuples);

	return rel;
}

RWSetPage *
decode_page(RWSet *rwset, StringInfo msg)
{
	RWSetPage  *page;

	page = alloc_page(rwset);

	page->blkno = pq_getmsgint(msg, 4);

	return page;
}


RWSetPage *
alloc_page(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetPage  *page;

	ctx = rwset->context;
	page = (RWSetPage *) MemoryContextAlloc(ctx, sizeof(RWSetPage));

	memset(page, 0, sizeof(RWSetPage));

	return page;
}

RWSetTuple *
decode_tuple(RWSet *rwset, StringInfo msg)
{
	RWSetTuple *tup;
	int			blkno;
	int			offset;

	blkno = pq_getmsgint(msg, 4);
	offset = pq_getmsgint(msg, 2);

	tup = alloc_tuple(rwset);
	ItemPointerSet(&tup->tid, blkno, offset);

	return tup;
}

RWSetTuple *
alloc_tuple(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetTuple *tup;

	ctx = rwset->context;
	tup = (RWSetTuple *) MemoryContextAlloc(ctx, sizeof(RWSetTuple));

	memset(tup, 0, sizeof(RWSetTuple));

	return tup;
}

char *
RWSetToString(RWSet *rwset)
{
	StringInfoData s;
	bool	first_group = true;

	RWSetHeader *header;
	dlist_iter	rel_iter;

	StringInfoData writes_cur;

	initStringInfo(&s);

	/* Header */
	header = &rwset->header;
	appendStringInfoString(&s, "{\n\"header\": ");
	appendStringInfo(&s, "{ \"dbid\": %d, \"region_set\": %ld }", 
					 header->dbid, header->region_set);

	/* Relations */
	appendStringInfoString(&s, ",\n\"relations\": [");
	dlist_foreach(rel_iter, &rwset->relations)
	{
		RWSetRelation *rel = dlist_container(RWSetRelation, node, rel_iter.cur);
		dlist_iter	item_iter;
		bool		first_item;

		if (!first_group)
			appendStringInfoString(&s, ",");
		first_group = false;

		appendStringInfoString(&s, "\n\t{");
		appendStringInfo(&s, "\"is_index\": %d", rel->is_index);
		appendStringInfo(&s, ", \"relid\": %d", rel->relid);
		appendStringInfo(&s, ", \"region\": %d", rel->region);
		appendStringInfo(&s, ", \"csn\": %ld", rel->csn);

		/* Pages */
		if (!dlist_is_empty(&rel->pages))
		{
			appendStringInfoString(&s, ",\n\t \"pages\": [");
			first_item = true;
			dlist_foreach(item_iter, &rel->pages)
			{
				RWSetPage  *page = dlist_container(RWSetPage, node, item_iter.cur);

				if (!first_item)
					appendStringInfoString(&s, ",");
				first_item = false;

				appendStringInfoString(&s, "\n\t\t{");
				appendStringInfo(&s, "\"blkno\": %d, ", page->blkno);
				appendStringInfoString(&s, "}");
			}
			appendStringInfoString(&s, "\n\t ]");
		}

		/* Tuples */
		if (!dlist_is_empty(&rel->tuples))
		{
			appendStringInfoString(&s, ",\n\t \"tuples\": [");
			first_item = true;
			dlist_foreach(item_iter, &rel->tuples)
			{
				RWSetTuple *tup = dlist_container(RWSetTuple, node, item_iter.cur);

				if (!first_item)
					appendStringInfoString(&s, ",");
				first_item = false;

				appendStringInfoString(&s, "\n\t\t{");
				appendStringInfo(&s, "\"blkno\": %d, ", ItemPointerGetBlockNumber(&tup->tid));
				appendStringInfo(&s, "\"offset\": %d", ItemPointerGetOffsetNumber(&tup->tid));
				appendStringInfoString(&s, "}");
			}
			appendStringInfoString(&s, "\n\t ]");
		}

		appendStringInfoString(&s, "}");
	}
	appendStringInfoString(&s, "\n]");

	/* Writes */
	appendStringInfoString(&s, ",\n\"writes\": [");
	
	writes_cur.data = rwset->writes;
	writes_cur.len = rwset->writes_len;
	writes_cur.cursor = 0;
	first_group = true;
	while (writes_cur.cursor < writes_cur.len)
	{
		int region;
		LogicalRepMsgType action;
		LogicalRepRelId relid;
		bool hasoldtup;
		LogicalRepTupleData newtup;
		LogicalRepTupleData oldtup;

		if (!first_group)
			appendStringInfoString(&s, ",");
		first_group = false;
		appendStringInfoString(&s, "\n\t{");

		region = pq_getmsgbyte(&writes_cur);
		action = pq_getmsgbyte(&writes_cur);
		switch (action)
		{
			case LOGICAL_REP_MSG_INSERT:
				relid = logicalrep_read_insert(&writes_cur, &newtup);

				appendStringInfo(&s, "\"action\": \"INSERT\"");
				appendStringInfo(&s, ", \"relid\": %u", relid);
				appendStringInfo(&s, ", \"region\": %d", region);
				appendStringInfo(&s, ",\n\t \"new_tuple\": ");
				append_tuple_string(&s, &newtup);
				free_tuple(&newtup);
				break;

			case LOGICAL_REP_MSG_UPDATE:
				relid = logicalrep_read_update(&writes_cur, &hasoldtup, &oldtup, &newtup);

				appendStringInfo(&s, "\"action\": \"UPDATE\"");
				appendStringInfo(&s, ", \"relid\": %u", relid);
				appendStringInfo(&s, ", \"region\": %d", region);
				appendStringInfo(&s, ", \"has_old_tup\": %d", hasoldtup);
				appendStringInfo(&s, ",\n\t \"new_tuple\": ");
				append_tuple_string(&s, &newtup);
				free_tuple(&newtup);
				if (hasoldtup)
				{
					appendStringInfo(&s, ",\n\t \"old_tuple\": ");
					append_tuple_string(&s, &oldtup);
					free_tuple(&oldtup);
				}
				break;

			case LOGICAL_REP_MSG_DELETE:
				relid = logicalrep_read_delete(&writes_cur, &oldtup);

				appendStringInfo(&s, "\"action\": \"DELETE\"");
				appendStringInfo(&s, ", \"relid\": %u", relid);
				appendStringInfo(&s, ", \"region\": %d", region);
				appendStringInfo(&s, ",\n\t \"old_tuple\": ");
				append_tuple_string(&s, &oldtup);
				free_tuple(&oldtup);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg_internal("invalid write set message type \"%c\"", action)));
		}

		appendStringInfoString(&s, "}");
	}

	appendStringInfoString(&s, "\n]");
	appendStringInfoString(&s, "}");

	return s.data;
}

static void
append_tuple_string(StringInfo s, const LogicalRepTupleData *tuple)
{
	int	i;
	appendStringInfoString(s, "{");
	appendStringInfo(s, "\"ncols\": %d, ", tuple->ncols);
	appendStringInfo(s, "\"status\": \"");
	for (i = 0; i < tuple->ncols; i++)
		appendStringInfoChar(s, tuple->colstatus[i]);
	appendStringInfoString(s, "\", ");
	appendStringInfo(s, "\"colsizes\": [");
	for (i = 0; i < tuple->ncols; i++)
	{
		if (i > 0) appendStringInfo(s, ", ");
		appendStringInfo(s, "%d", tuple->colvalues[i].len);
	}
	appendStringInfoString(s, "]");
	appendStringInfoString(s, "}");
}

static void
free_tuple(LogicalRepTupleData *tuple)
{
	pfree(tuple->colvalues);
	pfree(tuple->colstatus);
}