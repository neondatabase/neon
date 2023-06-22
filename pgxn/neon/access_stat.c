
/*
 * We want this statistic to rpresent current access patern mthis is why when
 * (n_seq_accesses + n_rnd_accesses) > MAX_ACCESS_COUNTER then we divide both counters by two,
 * so decreasng weight of historical data
 */
#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "common/hashfn.h"
#include "pagestore_client.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"

/* Structure used to predict sequential access */

typedef struct AccessStatEntry {
	RelFileNode relnode;
	BlockNumber blkno; /* last accessed black number */
	uint32      n_seq_accesses; /* number of sequential accesses (when block N+1 is accessed after block N) */
	uint32      n_rnd_accesses; /* number of random accesses */
	uint32      hash;
	uint32      status;
	uint64      access_count;  /* total number of relation accesses since backend start */
	dlist_node	lru_node; /* LRU list node */
}  AccessStatEntry;

#define SH_PREFIX			as
#define SH_ELEMENT_TYPE		AccessStatEntry
#define SH_KEY_TYPE			RelFileNode
#define SH_KEY				relnode
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a)	((a)->hash)
#define SH_HASH_KEY(tb, key) hash_bytes(		\
		((const unsigned char *) &(key)),		\
		sizeof(RelFileNode)					\
)

#define SH_EQUAL(tb, a, b)  RelFileNodeEquals((a), (b))
#define SH_SCOPE			static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

static as_hash *hash;
static dlist_head lru;
static int max_access_stat_size;
static int max_access_stat_count;
static double min_seq_access_ratio;
static int min_seq_access_count;

void access_stat_init(void)
{
	MemoryContext memctx = AllocSetContextCreate(TopMemoryContext,
												 "NeonSMGR/access_stat",
												 ALLOCSET_DEFAULT_SIZES);
	DefineCustomIntVariable("neon.max_access_stat_size",
							"Maximal size of Neon relation access statistic hash",
							NULL,
							&max_access_stat_size,
							1024,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomIntVariable("neon.max_access_stat_count",
							"Maximal value of relation access counter after which counters are divided by 2",
							NULL,
							&max_access_stat_count,
							1024,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomRealVariable("neon.min_seq_access_ratio",
							 "Minimal seq/(rnd+seq) ratio to determine sequential access",
							 NULL,
							 &min_seq_access_ratio,
							 0.9,
							 0,
							 INT_MAX,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomIntVariable("neon.min_seq_access_count",
							"Minimal access count to determine sequetial access",
							NULL,
							&min_seq_access_count,
							10,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
	hash = as_create(memctx, max_access_stat_size, NULL);
	dlist_init(&lru);
}


bool is_sequential_access(RelFileNode rnode, ForkNumber forkNum, BlockNumber blkno)
{
	bool is_seq_access = false;
	if (forkNum == MAIN_FORKNUM /* prefetch makes sense only for main fork */
		&& max_access_stat_size != 0)
	{
		AccessStatEntry* entry = as_lookup(hash, rnode);
		if (entry == NULL)
		{
			bool found;
			/* New item */
			while (hash->members >= max_access_stat_size)
			{
				/* Hash overflow: find candidate for replacement */
				AccessStatEntry* victim = dlist_container(AccessStatEntry, lru_node, dlist_pop_head_node(&lru));
				as_delete_item(hash, victim);
			}
			entry = as_insert(hash, rnode, &found);
			Assert(!found);
			/* Set both counter to zero because we don't know whethr first access is sequential or random */
			entry->n_seq_accesses = 0;
			entry->n_rnd_accesses = 0;
			entry->access_count = 1;
		}
		else
		{
			uint32 access_count = entry->n_seq_accesses + entry->n_rnd_accesses;
			/*
			 * We want this function to represent most recent access pattern,
			 * so when number of accesses exceed threashold value `max_access_stat_count`
			 * we divide bother coutners by two devaluing old data
			 */
			if (access_count >= max_access_stat_count)
			{
				entry->n_seq_accesses >>= 1;
				entry->n_rnd_accesses >>= 1;
			}
			if (entry->blkno+1 == blkno)
				entry->n_seq_accesses += 1;
			else
				entry->n_rnd_accesses += 1;
			entry->access_count += 1;
			access_count = entry->n_seq_accesses + entry->n_rnd_accesses;

			is_seq_access = access_count >= min_seq_access_count
				&& (double)entry->n_seq_accesses / access_count >= min_seq_access_ratio;


			/* Remove entry from LRU list tobe able to insert it to the end of this list */
			dlist_delete(&entry->lru_node);
		}
		/* Place entry to the tail of LRU list */
		dlist_push_tail(&lru, &entry->lru_node);
		entry->blkno = blkno;
	}
	return is_seq_access;
}

/*
 * Get relation access pattern
 */
PG_FUNCTION_INFO_V1(get_relation_access_statistics);


typedef struct
{
	TupleDesc	tupdesc;
	dlist_node* curr;
} AccessStatContext;

#define NUM_ACCESS_STAT_COLUMNS 6

Datum
get_relation_access_statistics(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum		result;
	MemoryContext oldcontext;
	AccessStatContext *fctx;	/* User function context. */
	TupleDesc	tupledesc;
	TupleDesc	expected_tupledesc;
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create a user function context for cross-call persistence */
		fctx = (AccessStatContext *) palloc(sizeof(AccessStatContext));

		/*
		 * To smoothly support upgrades from version 1.0 of this extension
		 * transparently handle the (non-)existence of the pinning_backends
		 * column. We unfortunately have to get the result type for that... -
		 * we can't use the result type determined by the function definition
		 * without potentially crashing when somebody uses the old (or even
		 * wrong) function definition though.
		 */
		if (get_call_result_type(fcinfo, NULL, &expected_tupledesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		if (expected_tupledesc->natts != NUM_ACCESS_STAT_COLUMNS)
			elog(ERROR, "incorrect number of output arguments");

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(expected_tupledesc->natts);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "relfilenode",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "reltablespace",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 3, "reldatabase",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4, "seqaccess",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 5, "rndaccess",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 6, "accesscnt",
						   INT8OID, -1, 0);

		fctx->tupdesc = BlessTupleDesc(tupledesc);
		fctx->curr = dlist_is_empty(&lru) ? NULL : dlist_tail_node(&lru);


		/* Set max calls and remember the user function context. */
		funcctx->max_calls = hash->members;
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;
	if (fctx->curr)
	{
		AccessStatEntry* entry = dlist_container(AccessStatEntry, lru_node, fctx->curr);
		Datum		values[NUM_ACCESS_STAT_COLUMNS];
		bool		nulls[NUM_ACCESS_STAT_COLUMNS] = {
			false, false, false, false, false, false
		};

		values[0] = ObjectIdGetDatum(entry->relnode.relNode);
		values[1] = ObjectIdGetDatum(entry->relnode.spcNode);
		values[2] = ObjectIdGetDatum(entry->relnode.dbNode);
		values[3] = Int32GetDatum(entry->n_seq_accesses);
		values[4] = Int32GetDatum(entry->n_rnd_accesses);
		values[5] = Int64GetDatum(entry->access_count);

		/* Build and return the tuple. */
		tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		fctx->curr = dlist_has_prev(&lru, fctx->curr) ? dlist_prev_node(&lru, fctx->curr) : NULL;

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}

