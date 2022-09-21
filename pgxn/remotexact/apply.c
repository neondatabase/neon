/*-------------------------------------------------------------------------
 * contrib/remotexact/apply.c
 * 
 * This file contains function to decode and apply the writes in the rwset
 * of a remote transaction. Many functions in this file are ported from
 * backend/replication/logical/worker.c since we use the message format of
 * logical replication to encode the writes.
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/remotexact.h"
#include "access/xact.h"
#include "apply.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "optimizer/optimizer.h"
#include "replication/logicalproto.h"
#include "rewrite/rewriteHandler.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

typedef struct ApplyExecutionData
{
	EState	        *estate;			/* executor state, used to track resources */

	Relation        targetRel;	        /* replication target rel */
	ResultRelInfo   *targetRelInfo;
} ApplyExecutionData;

typedef struct SlotErrCallbackArg
{
	Relation    rel;
    int			attnum;
} SlotErrCallbackArg;

static void apply_handle_insert(StringInfo s, bool skip);
static void apply_handle_update(StringInfo s, bool skip);
static void apply_handle_delete(StringInfo s, bool skip);
static Relation open_relation(LogicalRepRelId relid, LOCKMODE lockmode);
static void close_relation(Relation rel, LOCKMODE lockmode);
static ApplyExecutionData *create_edata_for_relation(Relation rel);
static void finish_edata(ApplyExecutionData *edata);
static void slot_store_data(TupleTableSlot *slot,
                            Relation rel,
				            LogicalRepTupleData *tupleData);

void
apply_writes(RWSet *rwset)
{
	StringInfoData s;

	s.data = rwset->writes;
	s.len = rwset->writes_len;
	s.cursor = 0;
	while (s.cursor < s.len)
	{
		int region;
		LogicalRepMsgType action;
		bool skip;

		region = pq_getmsgbyte(&s);

		// We ignore tuples that do not belong to the current region
		// by setting the skip argument to true to the apply functions
		// below. They will still decode the tuples but will not apply
		// the changes for them.
		skip = region != current_region;

		action = pq_getmsgbyte(&s);
		switch (action)
		{
			case LOGICAL_REP_MSG_INSERT:
				apply_handle_insert(&s, skip);
				break;

			case LOGICAL_REP_MSG_UPDATE:
				apply_handle_update(&s, skip);
				break;

			case LOGICAL_REP_MSG_DELETE:
				apply_handle_delete(&s, skip);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg_internal("[remotexact] invalid write set message type \"%c\"", action)));
		}
	}
}

/*
 * Ported from apply_handle_insert in backend/replication/logical/worker.c
 */
static void
apply_handle_insert(StringInfo s, bool skip)
{
	Relation rel;
	LogicalRepTupleData newtup;
	LogicalRepRelId relid;
	ApplyExecutionData *edata;
	EState	   *estate;
	TupleTableSlot *remoteslot;
	MemoryContext oldctx;

	relid = logicalrep_read_insert(s, &newtup);

	if (skip)
		return;

	rel = open_relation(relid, RowExclusiveLock);

	/* Initialize the executor state. */
	edata = create_edata_for_relation(rel);
	estate = edata->estate;
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel),
										&TTSOpsVirtual);

	/* Process and store remote tuple in the slot */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_data(remoteslot, rel, &newtup);
	MemoryContextSwitchTo(oldctx);

	/* Unlike logical replication, we never write to a partitioned relation
	   (but the relation can be  a partition of a partitioned relation), so
	   we don't need to check whether the relation is partitioned or not. */

	/* We must open indexes here. */
	ExecOpenIndices(edata->targetRelInfo, false);

	/* Do the insert. */
	ExecSimpleRelationInsert(edata->targetRelInfo, edata->estate, remoteslot);

	/* Cleanup. */
	ExecCloseIndices(edata->targetRelInfo);

	finish_edata(edata);

	close_relation(rel, NoLock);
}

static void
apply_handle_update(StringInfo s, bool skip)
{
}

static void
apply_handle_delete(StringInfo s, bool skip)
{
}

/*
 * Open the local relation.
 */
static Relation
open_relation(LogicalRepRelId relid, LOCKMODE lockmode)
{
    if (!OidIsValid(relid))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("relation with id \"%d\" does not exist", relid)));

    return table_open(relid, lockmode);
}

/*
 * Close the previously opened logical relation.
 */
static void
close_relation(Relation rel, LOCKMODE lockmode)
{
	table_close(rel, lockmode);
}

/*
 * Ported from create_edata_for_relation in backend/replication/logical/worker.c
 */
static ApplyExecutionData *
create_edata_for_relation(Relation rel)
{
	ApplyExecutionData *edata;
	EState	   *estate;
	RangeTblEntry *rte;
	ResultRelInfo *resultRelInfo;

	edata = (ApplyExecutionData *) palloc0(sizeof(ApplyExecutionData));
	edata->targetRel = rel;

	edata->estate = estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
	ExecInitRangeTable(estate, list_make1(rte));

	edata->targetRelInfo = resultRelInfo = makeNode(ResultRelInfo);

	/*
	 * Use Relation opened by logicalrep_rel_open() instead of opening it
	 * again.
	 */
	InitResultRelInfo(resultRelInfo, rel, 1, NULL, 0);

	/*
	 * We put the ResultRelInfo in the es_opened_result_relations list, even
	 * though we don't populate the es_result_relations array.  That's a bit
	 * bogus, but it's enough to make ExecGetTriggerResultRel() find them.
	 *
	 * ExecOpenIndices() is not called here either, each execution path doing
	 * an apply operation being responsible for that.
	 */
	estate->es_opened_result_relations =
		lappend(estate->es_opened_result_relations, resultRelInfo);

	estate->es_output_cid = GetCurrentCommandId(true);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/* other fields of edata remain NULL for now */

	return edata;
}

/*
 * Ported from finish_edata in backend/replication/logical/worker.c
 */
static void
finish_edata(ApplyExecutionData *edata)
{
	EState	   *estate = edata->estate;

	/* Handle any queued AFTER triggers. */
	AfterTriggerEndQuery(estate);

	/*
	 * Cleanup.  It might seem that we should call ExecCloseResultRelations()
	 * here, but we intentionally don't.  It would close the rel we added to
	 * es_opened_result_relations above, which is wrong because we took no
	 * corresponding refcount.  We rely on ExecCleanupTupleRouting() to close
	 * any other relations opened during execution.
	 */
	ExecResetTupleTable(estate->es_tupleTable, false);
	FreeExecutorState(estate);
	pfree(edata);
}

/*
 * Ported from slot_store_error_callback in backend/replication/logical/worker.c
 */
static void
slot_store_error_callback(void *arg)
{
	SlotErrCallbackArg *errarg = (SlotErrCallbackArg *) arg;
	Relation rel;
    
    /* Nothing to do if attribute number is not set */
	if (errarg->attnum < 0)
		return;

    rel = errarg->rel;

	errcontext("applying remote data for relation %u column %d", rel->rd_id, errarg->attnum);
}

/*
 * Ported from slot_store_data in backend/replication/logical/worker.c
 */
static void
slot_store_data(TupleTableSlot *slot, Relation rel,
				LogicalRepTupleData *tupleData)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;
	SlotErrCallbackArg errarg;
	ErrorContextCallback errcallback;

	ExecClearTuple(slot);

	/* Push callback + info on the error context stack */
	errarg.rel = rel;
	errarg.attnum = -1;
	errcallback.callback = slot_store_error_callback;
	errcallback.arg = (void *) &errarg;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

    Assert(tupleData->ncols == natts);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (!att->attisdropped)
		{
			StringInfo	colvalue = &tupleData->colvalues[i];

			errarg.attnum = i;

			if (tupleData->colstatus[i] == LOGICALREP_COLUMN_TEXT)
			{
				Oid			typinput;
				Oid			typioparam;

				getTypeInputInfo(att->atttypid, &typinput, &typioparam);
				slot->tts_values[i] =
					OidInputFunctionCall(typinput, colvalue->data,
										 typioparam, att->atttypmod);
				slot->tts_isnull[i] = false;
			}
			else if (tupleData->colstatus[i] == LOGICALREP_COLUMN_BINARY)
			{
				Oid			typreceive;
				Oid			typioparam;

				/*
				 * In some code paths we may be asked to re-parse the same
				 * tuple data.  Reset the StringInfo's cursor so that works.
				 */
				colvalue->cursor = 0;

				getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);
				slot->tts_values[i] =
					OidReceiveFunctionCall(typreceive, colvalue,
										   typioparam, att->atttypmod);

				/* Trouble if it didn't eat the whole buffer */
				if (colvalue->cursor != colvalue->len)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
							 errmsg("incorrect binary data format in logical replication column %d",
									i + 1)));
				slot->tts_isnull[i] = false;
			}
			else
			{
				/*
				 * NULL value from remote.  (We don't expect to see
				 * LOGICALREP_COLUMN_UNCHANGED here, but if we do, treat it as
				 * NULL.)
				 */
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
			}

			errarg.attnum = -1;
		}
		else
		{
			/*
			 * We assign NULL to dropped attributes 
			 */
			slot->tts_values[i] = (Datum) 0;
			slot->tts_isnull[i] = true;
		}
	}

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	ExecStoreVirtualTuple(slot);
}
