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
#include "access/tableam.h"
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
#include "utils/snapmgr.h"

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
static void apply_handle_insert_internal(ApplyExecutionData *edata,
										 ResultRelInfo *relinfo,
										 TupleTableSlot *remoteslot);
static void apply_handle_update(StringInfo s, bool skip);
static void apply_handle_update_internal(ApplyExecutionData *edata,
										 ResultRelInfo *relinfo,
										 TupleTableSlot *remoteslot,
										 LogicalRepTupleData *newtup);
static void apply_handle_delete(StringInfo s, bool skip);
static void apply_handle_delete_internal(ApplyExecutionData *edata,
										 ResultRelInfo *relinfo,
									     TupleTableSlot *remoteslot);
static void begin_apply(void);
static void end_apply(void);
static bool FindReplTupleInLocalRel(EState *estate, Relation localrel,
									TupleTableSlot *remoteslot,
									TupleTableSlot **localslot);
static Relation open_relation(LogicalRepRelId relid, LOCKMODE lockmode);
static void close_relation(Relation rel, LOCKMODE lockmode);
static void check_relation_updatable(Relation rel);
static ApplyExecutionData *create_edata_for_relation(Relation rel);
static void finish_edata(ApplyExecutionData *edata);
static void slot_store_data(TupleTableSlot *slot, Relation rel,
				            LogicalRepTupleData *tupleData);
static void slot_modify_data(TupleTableSlot *slot, TupleTableSlot *srcslot,
							 Relation rel, LogicalRepTupleData *tupleData);

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

		CommandCounterIncrement();
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

	begin_apply();

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
	   (but the written relation can be a partition of a partitioned relation),
	   so we don't need to check whether the relation is partitioned or not. */

	apply_handle_insert_internal(edata, edata->targetRelInfo, remoteslot);

	finish_edata(edata);

	close_relation(rel, NoLock);
	
	end_apply();
}

/*
 * Ported from apply_handle_insert_internal in backend/replication/logical/worker.c
 */
static void
apply_handle_insert_internal(ApplyExecutionData *edata,
							 ResultRelInfo *relinfo,
							 TupleTableSlot *remoteslot)
{
	EState	   *estate = edata->estate;

	/* We must open indexes here. */
	ExecOpenIndices(relinfo, false);

	/* Do the insert. */
	ExecSimpleRelationInsert(relinfo, estate, remoteslot);

	/* Cleanup. */
	ExecCloseIndices(relinfo);
}

static void
apply_handle_update(StringInfo s, bool skip)
{
	Relation rel;
	LogicalRepRelId relid;
	ApplyExecutionData *edata;
	EState	   *estate;
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool		has_oldtup;
	TupleTableSlot *remoteslot;
	RangeTblEntry *target_rte;
	MemoryContext oldctx;

	relid = logicalrep_read_update(s, &has_oldtup, &oldtup,
								   &newtup);

	if (skip)
		return;

	begin_apply();

	rel = open_relation(relid, RowExclusiveLock);

	/* Check if we can do the update. */
	check_relation_updatable(rel);

	/* Initialize the executor state. */
	edata = create_edata_for_relation(rel);
	estate = edata->estate;
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel),
										&TTSOpsVirtual);

	/*
	 * Populate updatedCols so that per-column triggers can fire, and so
	 * executor can correctly pass down indexUnchanged hint.  This could
	 * include more columns than were actually changed on the publisher
	 * because the logical replication protocol doesn't contain that
	 * information.  But it would for example exclude columns that only exist
	 * on the subscriber, since we are not touching those.
	 */
	target_rte = list_nth(estate->es_range_table, 0);
	for (int i = 0; i < remoteslot->tts_tupleDescriptor->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(remoteslot->tts_tupleDescriptor, i);

		if (!att->attisdropped)
		{
			if (newtup.colstatus[i] != LOGICALREP_COLUMN_UNCHANGED)
				target_rte->updatedCols =
					bms_add_member(target_rte->updatedCols,
								   i + 1 - FirstLowInvalidHeapAttributeNumber);
		}
	}

	/* Also populate extraUpdatedCols, in case we have generated columns */
	fill_extraUpdatedCols(target_rte, rel);

	/* Build the search tuple. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_data(remoteslot, rel,
					has_oldtup ? &oldtup : &newtup);
	MemoryContextSwitchTo(oldctx);

	/* Unlike logical replication, we never write to a partitioned relation
	   (but the written relation can be a partition of a partitioned relation),
	   so we don't need to check whether the relation is partitioned or not. */

	apply_handle_update_internal(edata, edata->targetRelInfo, remoteslot, &newtup);

	finish_edata(edata);

	close_relation(rel, NoLock);

	end_apply();
}

/*
 * Ported from apply_handle_update_internal in backend/replication/logical/worker.c
 */
static void
apply_handle_update_internal(ApplyExecutionData *edata,
							 ResultRelInfo *relinfo,
							 TupleTableSlot *remoteslot,
							 LogicalRepTupleData *newtup)
{
	EState	   *estate = edata->estate;
	Relation	localrel = relinfo->ri_RelationDesc;
	EPQState	epqstate;
	TupleTableSlot *localslot;
	bool		found;
	MemoryContext oldctx;

	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);
	ExecOpenIndices(relinfo, false);

	found = FindReplTupleInLocalRel(estate, localrel,
									remoteslot, &localslot);
	ExecClearTuple(remoteslot);

	/*
	 * Tuple found.
	 *
	 * Note this will fail if there are other conflicting unique indexes.
	 */
	if (found)
	{
		/* Process and store remote tuple in the slot */
		oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
		slot_modify_data(remoteslot, localslot, localrel, newtup);
		MemoryContextSwitchTo(oldctx);

		EvalPlanQualSetSlot(&epqstate, remoteslot);

		/* Do the actual update. */
		ExecSimpleRelationUpdate(relinfo, estate, &epqstate, localslot,
								 remoteslot);
	}
	else
	{
		/*
		 * The tuple to be updated could not be found. Abort the surrogate
		 * transaction.
		 */
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			errmsg("surrogate transaction did not find row to be updated "
				   "in relation \"%s\"",
					RelationGetRelationName(localrel))));
	}

	/* Cleanup. */
	ExecCloseIndices(relinfo);
	EvalPlanQualEnd(&epqstate);
}

static void
apply_handle_delete(StringInfo s, bool skip)
{
	Relation rel;
	LogicalRepTupleData oldtup;
	LogicalRepRelId relid;
	ApplyExecutionData *edata;
	EState	   *estate;
	TupleTableSlot *remoteslot;
	MemoryContext oldctx;

	relid = logicalrep_read_delete(s, &oldtup);

	if (skip)
		return;

	begin_apply();

	rel = open_relation(relid, RowExclusiveLock);

	/* Check if we can do the delete. */
	check_relation_updatable(rel);

	/* Initialize the executor state. */
	edata = create_edata_for_relation(rel);
	estate = edata->estate;
	remoteslot = ExecInitExtraTupleSlot(estate,
										RelationGetDescr(rel),
										&TTSOpsVirtual);

	/* Build the search tuple. */
	oldctx = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));
	slot_store_data(remoteslot, rel, &oldtup);
	MemoryContextSwitchTo(oldctx);

	/* Unlike logical replication, we never write to a partitioned relation
	   (but the written relation can be a partition of a partitioned relation),
	   so we don't need to check whether the relation is partitioned or not. */

	apply_handle_delete_internal(edata, edata->targetRelInfo, remoteslot);

	finish_edata(edata);

	close_relation(rel, NoLock);

	end_apply();
}

/*
 * Ported from apply_handle_delete_internal in backend/replication/logical/worker.c
 */
static void
apply_handle_delete_internal(ApplyExecutionData *edata,
							 ResultRelInfo *relinfo,
							 TupleTableSlot *remoteslot)
{
	EState	   *estate = edata->estate;
	Relation	localrel = relinfo->ri_RelationDesc;
	EPQState	epqstate;
	TupleTableSlot *localslot;
	bool		found;

	EvalPlanQualInit(&epqstate, estate, NULL, NIL, -1);
	ExecOpenIndices(relinfo, false);

	found = FindReplTupleInLocalRel(estate, localrel,
									remoteslot, &localslot);

	/* If found delete it. */
	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, localslot);

		/* Do the actual delete. */
		ExecSimpleRelationDelete(relinfo, estate, &epqstate, localslot);
	}
	else
	{
		/*
		 * The tuple to be deleted could not be found. Abort the surrogate
		 * transaction.
		 */
		ereport(ERROR,
			(errcode(ERRCODE_DATA_CORRUPTED),
			errmsg("surrogate transaction did not find row to be deleted "
				   "in relation \"%s\"",
					RelationGetRelationName(localrel))));
	}

	/* Cleanup. */
	ExecCloseIndices(relinfo);
	EvalPlanQualEnd(&epqstate);
}

static void
begin_apply(void)
{
	if (!IsTransactionState())
		StartTransactionCommand();

	PushActiveSnapshot(GetTransactionSnapshot());
}

static void
end_apply(void)
{
	PopActiveSnapshot();

	CommandCounterIncrement();
}

/*
 * Ported from FindReplTupleInLocalRel in backend/replication/logical/worker.c
 */
static bool
FindReplTupleInLocalRel(EState *estate, Relation localrel,
						TupleTableSlot *remoteslot,
						TupleTableSlot **localslot)
{
	Oid			idxoid;
	bool		found;

	*localslot = table_slot_create(localrel, &estate->es_tupleTable);

	idxoid = RelationGetReplicaIndex(localrel);

	if (!OidIsValid(idxoid))
		idxoid = RelationGetPrimaryKeyIndex(localrel);

	Assert(OidIsValid(idxoid) ||
		   (localrel->rd_rel->relreplident == REPLICA_IDENTITY_FULL));

	if (OidIsValid(idxoid))
		found = RelationFindReplTupleByIndex(localrel, idxoid,
											 LockTupleExclusive,
											 remoteslot, *localslot);
	else
		found = RelationFindReplTupleSeq(localrel, LockTupleExclusive,
										 remoteslot, *localslot);

	return found;
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
 * Ported from check_relation_updatable in backend/replication/logical/worker.c
 */
static void
check_relation_updatable(Relation rel)
{
	char	relreplident = rel->rd_rel->relreplident;

	if (relreplident != REPLICA_IDENTITY_DEFAULT &&
		relreplident != REPLICA_IDENTITY_FULL &&
		relreplident != REPLICA_IDENTITY_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("target relation \"%s\" has neither REPLICA IDENTITY "
					   "index nor REPLICA IDENTITY FULL",
						RelationGetRelationName(rel))));
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

/*
 * Ported from slot_modify_data in backend/replication/logical/worker.c
 */
static void
slot_modify_data(TupleTableSlot *slot, TupleTableSlot *srcslot,
				 Relation rel,
				 LogicalRepTupleData *tupleData)
{
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;
	SlotErrCallbackArg errarg;
	ErrorContextCallback errcallback;

	/* We'll fill "slot" with a virtual tuple, so we must start with ... */
	ExecClearTuple(slot);

	/*
	 * Copy all the column data from srcslot, so that we'll have valid values
	 * for unreplaced columns.
	 */
	Assert(natts == srcslot->tts_tupleDescriptor->natts);
	slot_getallattrs(srcslot);
	memcpy(slot->tts_values, srcslot->tts_values, natts * sizeof(Datum));
	memcpy(slot->tts_isnull, srcslot->tts_isnull, natts * sizeof(bool));

	/* For error reporting, push callback + info on the error context stack */
	errarg.rel = rel;
	errarg.attnum = -1;
	errcallback.callback = slot_store_error_callback;
	errcallback.arg = (void *) &errarg;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Call the "in" function for each replaced attribute */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);

		if (tupleData->colstatus[i] != LOGICALREP_COLUMN_UNCHANGED)
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
				/* must be LOGICALREP_COLUMN_NULL */
				slot->tts_values[i] = (Datum) 0;
				slot->tts_isnull[i] = true;
			}

			errarg.attnum = -1;
		}
	}

	/* Pop the error context stack */
	error_context_stack = errcallback.previous;

	/* And finally, declare that "slot" contains a valid virtual tuple */
	ExecStoreVirtualTuple(slot);
}
