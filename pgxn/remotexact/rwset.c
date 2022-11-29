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
static RWSetRelation *alloc_relations(RWSet *rwset);
static void decode_relation(RWSet *rwset, RWSetRelation *rel, StringInfo msg);
static RWSetPage *alloc_pages(RWSet *rwset, RWSetRelation *rel);
static void decode_page(RWSetPage * page, StringInfo msg);
static RWSetTuple *alloc_tuples(RWSet *rwset, RWSetRelation *rel);
static void decode_tuple(RWSetTuple *, StringInfo msg);

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

	rwset->n_relations = 0;
	rwset->relations = NULL;

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
	int			num_rels_received = 0;
	int			prev_cursor;

	decode_header(rwset, msg);

	read_set_len = pq_getmsgint(msg, 4);
	rwset->n_relations = pq_getmsgint(msg, 4);

	// Allocate relations based on number of relations expected.
	rwset->relations = alloc_relations(rwset);

	if (read_set_len > msg->len)
		ereport(ERROR,
				errmsg("length of read set (%d) too large", read_set_len),
				errdetail("remaining message length: %d", msg->len));

	while (consumed < read_set_len)
	{
		RWSetRelation *rel = &(rwset->relations[num_rels_received]);

		prev_cursor = msg->cursor;

		decode_relation(rwset, rel, msg);

		consumed += msg->cursor - prev_cursor;
		num_rels_received++;
	}

	if (consumed > read_set_len)
		ereport(ERROR,
				errmsg("length of read set (%d) is corrupted", read_set_len),
				errdetail("length of decoded read set: %d", consumed));
	if(rwset->n_relations != num_rels_received)
          ereport(ERROR,
              errmsg("number of read relations (%d) does not match relations "
                     "received", rwset->n_relations),
              errdetail("number of relations received: %d", num_rels_received));

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
alloc_relations(RWSet *rwset)
{
	MemoryContext ctx;
	RWSetRelation *rels;

	ctx = rwset->context;
	rels = (RWSetRelation *)MemoryContextAlloc(
		ctx, rwset->n_relations * sizeof(RWSetRelation));
    memset(rels, 0, rwset->n_relations * sizeof(RWSetRelation));
	Assert(rels != NULL);

	return rels;
}

void
decode_relation(RWSet *rwset, RWSetRelation *rel, StringInfo msg)
{
	unsigned char reltype;
	int			nitems;
	int			i;

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
	{
		rel->n_pages = nitems;
		rel->pages = alloc_pages(rwset, rel);
		for (i = 0; i < nitems; i++)
		{
			decode_page(&(rel->pages[i]), msg);
		}
	}
	else
	{
		rel->n_tuples = nitems;
		rel->tuples = alloc_tuples(rwset, rel);
		for (i = 0; i < nitems; i++)
		{
			decode_tuple(&(rel->tuples[i]), msg);
		}
	}
}


RWSetPage *
alloc_pages(RWSet *rwset, RWSetRelation *rel)
{
	MemoryContext ctx;
	RWSetPage  *pages;

	ctx = rwset->context;
	pages = (RWSetPage *)MemoryContextAlloc(
		ctx, rel->n_pages * sizeof(RWSetPage));

    memset(pages, 0, rel->n_pages * sizeof(RWSetPage));

	return pages;
}

void 
decode_page(RWSetPage *page, StringInfo msg)
{
	page->blkno = pq_getmsgint(msg, 4);
}

RWSetTuple *
alloc_tuples(RWSet *rwset, RWSetRelation *rel)
{
	MemoryContext ctx;
	RWSetTuple *tuples;

	ctx = rwset->context;
	tuples = (RWSetTuple *)MemoryContextAlloc(
		ctx, rel->n_tuples * sizeof(RWSetTuple));

	memset(tuples, 0, rel->n_tuples * sizeof(RWSetTuple));

	return tuples;
}

void
decode_tuple(RWSetTuple *tuple, StringInfo msg)
{
	int			blkno;
	int			offset;

	blkno = pq_getmsgint(msg, 4);
	offset = pq_getmsgint(msg, 2);
	ItemPointerSet(&tuple->tid, blkno, offset);
}

char *
RWSetToString(RWSet *rwset)
{
	StringInfoData s;
	bool	first_group = true;

	RWSetHeader *header;
	int i;

	StringInfoData writes_cur;

	initStringInfo(&s);

	/* Header */
	header = &rwset->header;
	appendStringInfoString(&s, "{\n\"header\": ");
	appendStringInfo(&s, "{ \"dbid\": %d, \"region_set\": %ld }",
					 header->dbid, header->region_set);

	/* Relations */
	appendStringInfoString(&s, ",\n\"relations\": [");
	for (i = 0; i < rwset->n_relations; i++)
	{
		RWSetRelation *rel = &(rwset->relations[i]);
		int j;
		bool first_item;

		if (!first_group)
			appendStringInfoString(&s, ",");
		first_group = false;

		appendStringInfoString(&s, "\n\t{");
		appendStringInfo(&s, "\"is_index\": %d", rel->is_index);
		appendStringInfo(&s, ", \"relid\": %d", rel->relid);
		appendStringInfo(&s, ", \"region\": %d", rel->region);
		appendStringInfo(&s, ", \"csn\": %ld", rel->csn);
		appendStringInfo(&s, ", \"is_table_scan\": %d", rel->is_table_scan);

		/* Pages */
		if (rel->n_pages != 0)
		{
			appendStringInfoString(&s, ",\n\t \"pages\": [");
			first_item = true;
			for (j = 0; j < rel->n_pages; j++)
			{
				RWSetPage *page = &(rel->pages[j]);

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
		if (rel->n_tuples != 0)
		{
			appendStringInfoString(&s, ",\n\t \"tuples\": [");
			first_item = true;
			for (j = 0; j < rel->n_tuples; j++)
			{
				RWSetTuple *tup = &(rel->tuples[j]);

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