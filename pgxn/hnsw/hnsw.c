#include "postgres.h"

#include "access/amapi.h"
#include "access/generic_xlog.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/selfuncs.h"

#include <math.h>
#include <float.h>

#include "hnsw.h"

PG_MODULE_MAGIC;

typedef struct {
	int32 vl_len_;		/* varlena header (do not touch directly!) */
	int dims;
	int maxelements;
	int efConstruction;
	int efSearch;
	int M;
} HnswOptions;

static relopt_kind hnsw_relopt_kind;

typedef struct {
	HierarchicalNSW* hnsw;
	size_t curr;
	size_t n_results;
	ItemPointer results;
} HnswScanOpaqueData;

typedef HnswScanOpaqueData* HnswScanOpaque;

typedef struct {
	Oid relid;
	uint32 status;
	HierarchicalNSW* hnsw;
} HnswHashEntry;


#define SH_PREFIX			 hnsw_index
#define SH_ELEMENT_TYPE		 HnswHashEntry
#define SH_KEY_TYPE			 Oid
#define SH_KEY				 relid
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a)	 ((a)->relid)
#define SH_HASH_KEY(tb, key) (key)
#define SH_EQUAL(tb, a, b)	((a) == (b))
#define SH_SCOPE			static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

#define INDEX_HASH_SIZE     11

#define DEFAULT_EF_SEARCH   64

PGDLLEXPORT void _PG_init(void);

static hnsw_index_hash *hnsw_indexes;

/*
 * Initialize index options and variables
 */
void
_PG_init(void)
{
	hnsw_relopt_kind = add_reloption_kind();
	add_int_reloption(hnsw_relopt_kind, "dims", "Number of dimensions",
					  0, 0, INT_MAX, AccessExclusiveLock);
	add_int_reloption(hnsw_relopt_kind, "maxelements", "Maximal number of elements",
					  0, 0, INT_MAX, AccessExclusiveLock);
	add_int_reloption(hnsw_relopt_kind, "m", "Number of neighbors of each vertex",
					  100, 0, INT_MAX, AccessExclusiveLock);
	add_int_reloption(hnsw_relopt_kind, "efconstruction", "Number of inspected neighbors during index construction",
					  16, 1, INT_MAX, AccessExclusiveLock);
	add_int_reloption(hnsw_relopt_kind, "efsearch", "Number of inspected neighbors during index search",
					  64, 1, INT_MAX, AccessExclusiveLock);
	hnsw_indexes = hnsw_index_create(TopMemoryContext, INDEX_HASH_SIZE, NULL);
}


static void
hnsw_build_callback(Relation index, ItemPointer tid, Datum *values,
					bool *isnull, bool tupleIsAlive, void *state)
{
	HierarchicalNSW* hnsw = (HierarchicalNSW*) state;
	ArrayType* array;
	int n_items;
	label_t label = 0;

	/* Skip nulls */
	if (isnull[0])
		return;

	array = DatumGetArrayTypeP(values[0]);
	n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	if (n_items != hnsw_dimensions(hnsw))
	{
		elog(ERROR, "Wrong number of dimensions: %d instead of %d expected",
			 n_items, hnsw_dimensions(hnsw));
	}

	memcpy(&label, tid, sizeof(*tid));
	hnsw_add_point(hnsw, (coord_t*)ARR_DATA_PTR(array), label);
}

static void
hnsw_populate(HierarchicalNSW* hnsw, Relation indexRel, Relation heapRel)
{
	IndexInfo* indexInfo = BuildIndexInfo(indexRel);
	Assert(indexInfo->ii_NumIndexAttrs == 1);
	table_index_build_scan(heapRel, indexRel, indexInfo,
						   true, true, hnsw_build_callback, (void *) hnsw, NULL);
}

#ifdef __APPLE__

#include <sys/types.h>
#include <sys/sysctl.h>

static void
hnsw_check_available_memory(Size requested)
{
	size_t total;
	if (sysctlbyname("hw.memsize", NULL, &total, NULL, 0) < 0)
		elog(ERROR, "Failed to get amount of RAM: %m");

	if ((Size)NBuffers*BLCKSZ + requested >= total)
		elog(ERROR, "HNSW index requeries %ld bytes while only %ld are available",
			requested, total - (Size)NBuffers*BLCKSZ);
}

#else

#include <sys/sysinfo.h>

static void
hnsw_check_available_memory(Size requested)
{
	struct sysinfo si;
	Size total;
	if (sysinfo(&si) < 0)
		elog(ERROR, "Failed to get amount of RAM: %n");

	total = si.totalram*si.mem_unit;
	if ((Size)NBuffers*BLCKSZ + requested >= total)
		elog(ERROR, "HNSW index requeries %ld bytes while only %ld are available",
			requested, total - (Size)NBuffers*BLCKSZ);
}

#endif

static HierarchicalNSW*
hnsw_get_index(Relation indexRel, Relation heapRel)
{
	HierarchicalNSW* hnsw;
	Oid indexoid = RelationGetRelid(indexRel);
	HnswHashEntry* entry = hnsw_index_lookup(hnsw_indexes, indexoid);
	if (entry == NULL)
	{
		size_t dims, maxelements;
		size_t M;
		size_t maxM;
		size_t size_links_level0;
		size_t size_data_per_element;
		size_t data_size;
		dsm_handle handle = indexoid << 1; /* make it even */
		void* impl_private = NULL;
		void* mapped_address = NULL;
		Size  mapped_size = 0;
		Size  shmem_size;
		bool exists = true;
		bool found;
		HnswOptions *opts = (HnswOptions *) indexRel->rd_options;
		if (opts == NULL || opts->maxelements == 0 || opts->dims == 0) {
			elog(ERROR, "HNSW index requires 'maxelements' and 'dims' to be specified");
		}
		dims = opts->dims;
		maxelements = opts->maxelements;
		M = opts->M;
		maxM = M * 2;
		data_size = dims * sizeof(coord_t);
		size_links_level0 = (maxM + 1) * sizeof(idx_t);
		size_data_per_element = size_links_level0 + data_size + sizeof(label_t);
		shmem_size =  hnsw_sizeof() + maxelements * size_data_per_element;

		hnsw_check_available_memory(shmem_size);

		/* first try to attach to existed index */
		if (!dsm_impl_op(DSM_OP_ATTACH, handle, 0, &impl_private,
						 &mapped_address, &mapped_size, DEBUG1))
		{
			/* index doesn't exists: try to create it */
			if (!dsm_impl_op(DSM_OP_CREATE, handle, shmem_size, &impl_private,
							 &mapped_address, &mapped_size, DEBUG1))
			{
				/* We can do it under shared lock, so some other backend may
				 * try to initialize index. If create is failed because index already
				 * created by somebody else, then try to attach to it once again
				 */
				if (!dsm_impl_op(DSM_OP_ATTACH, handle, 0, &impl_private,
								 &mapped_address, &mapped_size, ERROR))
				{
					return NULL;
				}
			}
			else
			{
				exists = false;
			}
		}
		Assert(mapped_size == shmem_size);
		hnsw = (HierarchicalNSW*)mapped_address;

		if (!exists)
		{
			hnsw_init(hnsw, dims, maxelements, M, maxM, opts->efConstruction);
			hnsw_populate(hnsw, indexRel, heapRel);
		}
		entry = hnsw_index_insert(hnsw_indexes, indexoid, &found);
		Assert(!found);
		entry->hnsw = hnsw;
	}
	else
	{
		hnsw = entry->hnsw;
	}
	return hnsw;
}

/*
 * Start or restart an index scan
 */
static IndexScanDesc
hnsw_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(index, nkeys, norderbys);
	HnswScanOpaque so = (HnswScanOpaque) palloc(sizeof(HnswScanOpaqueData));
	Relation heap = relation_open(index->rd_index->indrelid, NoLock);
	so->hnsw = hnsw_get_index(index, heap);
	relation_close(heap, NoLock);
	so->curr = 0;
	so->n_results = 0;
	so->results = NULL;
	scan->opaque = so;
	return scan;
}

/*
 * Start or restart an index scan
 */
static void
hnsw_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	if (so->results)
	{
		pfree(so->results);
		so->results = NULL;
	}
	so->curr = 0;
	if (orderbys && scan->numberOfOrderBys > 0)
		memmove(scan->orderByData, orderbys, scan->numberOfOrderBys * sizeof(ScanKeyData));
}

/*
 * Fetch the next tuple in the given scan
 */
static bool
hnsw_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;

	/*
	 * Index can be used to scan backward, but Postgres doesn't support
	 * backward scan on operators
	 */
	Assert(ScanDirectionIsForward(dir));

	if (so->curr == 0)
	{
		Datum		value;
		ArrayType*	array;
		int         n_items;
		size_t      n_results;
		label_t*    results;
		HnswOptions *opts = (HnswOptions *) scan->indexRelation->rd_options;
		size_t      efSearch = opts ? opts->efSearch : DEFAULT_EF_SEARCH;

		/* Safety check */
		if (scan->orderByData == NULL)
			elog(ERROR, "cannot scan HNSW index without order");

		/* No items will match if null */
		if (scan->orderByData->sk_flags & SK_ISNULL)
			return false;

		value = scan->orderByData->sk_argument;
		array = DatumGetArrayTypeP(value);
		n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		if (n_items != hnsw_dimensions(so->hnsw))
		{
			elog(ERROR, "Wrong number of dimensions: %d instead of %d expected",
				 n_items, hnsw_dimensions(so->hnsw));
		}

		if (!hnsw_search(so->hnsw, (coord_t*)ARR_DATA_PTR(array), efSearch, &n_results, &results))
			elog(ERROR, "HNSW index search failed");
		so->results = (ItemPointer)palloc(n_results*sizeof(ItemPointerData));
		so->n_results = n_results;
		for (size_t i = 0; i < n_results; i++)
		{
			memcpy(&so->results[i], &results[i], sizeof(so->results[i]));
		}
		free(results);
	}
	if (so->curr >= so->n_results)
	{
		return false;
	}
	else
	{
		scan->xs_heaptid = so->results[so->curr++];
		scan->xs_recheckorderby = false;
		return true;
	}
}

/*
 * End a scan and release resources
 */
static void
hnsw_endscan(IndexScanDesc scan)
{
	HnswScanOpaque so = (HnswScanOpaque) scan->opaque;
	if (so->results)
		pfree(so->results);
	pfree(so);
	scan->opaque = NULL;
}


/*
 * Estimate the cost of an index scan
 */
static void
hnsw_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				 Cost *indexStartupCost, Cost *indexTotalCost,
				 Selectivity *indexSelectivity, double *indexCorrelation
				 ,double *indexPages
)
{
	GenericCosts costs;

	/* Never use index without order */
	if (path->indexorderbys == NULL)
	{
		*indexStartupCost = DBL_MAX;
		*indexTotalCost = DBL_MAX;
		*indexSelectivity = 0;
		*indexCorrelation = 0;
		*indexPages = 0;
		return;
	}

	MemSet(&costs, 0, sizeof(costs));

	genericcostestimate(root, path, loop_count, &costs);

	/* Startup cost and total cost are same */
	*indexStartupCost = costs.indexTotalCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

/*
 * Parse and validate the reloptions
 */
static bytea *
hnsw_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"dims", RELOPT_TYPE_INT, offsetof(HnswOptions, dims)},
		{"maxelements", RELOPT_TYPE_INT, offsetof(HnswOptions, maxelements)},
		{"efconstruction", RELOPT_TYPE_INT, offsetof(HnswOptions, efConstruction)},
		{"efsearch", RELOPT_TYPE_INT, offsetof(HnswOptions, efSearch)},
		{"m", RELOPT_TYPE_INT, offsetof(HnswOptions, M)}
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  hnsw_relopt_kind,
									  sizeof(HnswOptions),
									  tab, lengthof(tab));
}

/*
 * Validate catalog entries for the specified operator class
 */
static bool
hnsw_validate(Oid opclassoid)
{
	return true;
}

/*
 * Build the index for a logged table
 */
static IndexBuildResult *
hnsw_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	HierarchicalNSW* hnsw = hnsw_get_index(index, heap);
	IndexBuildResult* result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = result->index_tuples = hnsw_count(hnsw);

	return result;
}

/*
 * Insert a tuple into the index
 */
static bool
hnsw_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
			  Relation heap, IndexUniqueCheck checkUnique,
			  bool indexUnchanged,
			  IndexInfo *indexInfo)
{
	HierarchicalNSW* hnsw = hnsw_get_index(index, heap);
	Datum value;
	ArrayType* array;
	int n_items;
	label_t label = 0;

	/* Skip nulls */
	if (isnull[0])
		return false;

	/* Detoast value */
	value = PointerGetDatum(PG_DETOAST_DATUM(values[0]));
	array = DatumGetArrayTypeP(value);
	n_items = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
	if (n_items != hnsw_dimensions(hnsw))
	{
		elog(ERROR, "Wrong number of dimensions: %d instead of %d expected",
			 n_items, hnsw_dimensions(hnsw));
	}
	memcpy(&label, heap_tid, sizeof(*heap_tid));
	if (!hnsw_add_point(hnsw, (coord_t*)ARR_DATA_PTR(array), label))
		elog(ERROR, "HNSW index insert failed");
	return true;
}

/*
 * Build the index for an unlogged table
 */
static void
hnsw_buildempty(Relation index)
{
	/* index will be constructed on dema nd when accessed */
}

/*
 * Clean up after a VACUUM operation
 */
static IndexBulkDeleteResult *
hnsw_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Relation	rel = info->index;

	if (stats == NULL)
		return NULL;

	stats->num_pages = RelationGetNumberOfBlocks(rel);

	return stats;
}

/*
 * Bulk delete tuples from the index
 */
static IndexBulkDeleteResult *
hnsw_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				IndexBulkDeleteCallback callback, void *callback_state)
{
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));
	return stats;
}

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(hnsw_handler);
Datum
hnsw_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 0;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = false;	/* can change direction mid-scan */
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	/* Interface functions */
	amroutine->ambuild = hnsw_build;
	amroutine->ambuildempty = hnsw_buildempty;
	amroutine->aminsert = hnsw_insert;
	amroutine->ambulkdelete = hnsw_bulkdelete;
	amroutine->amvacuumcleanup = hnsw_vacuumcleanup;
	amroutine->amcanreturn = NULL;	/* tuple not included in heapsort */
	amroutine->amcostestimate = hnsw_costestimate;
	amroutine->amoptions = hnsw_options;
	amroutine->amproperty = NULL;	/* TODO AMPROP_DISTANCE_ORDERABLE */
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = hnsw_validate;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = hnsw_beginscan;
	amroutine->amrescan = hnsw_rescan;
	amroutine->amgettuple = hnsw_gettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = hnsw_endscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;

	/* Interface functions to support parallel index scans */
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * Get the L2 distance between vectors
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(l2_distance);
Datum
l2_distance(PG_FUNCTION_ARGS)
{
	ArrayType  *a = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType  *b = PG_GETARG_ARRAYTYPE_P(1);
	int         a_dim = ArrayGetNItems(ARR_NDIM(a), ARR_DIMS(a));
	int         b_dim = ArrayGetNItems(ARR_NDIM(b), ARR_DIMS(b));
	dist_t 		distance = 0.0;
	dist_t		diff;
	coord_t	   *ax = (coord_t*)ARR_DATA_PTR(a);
	coord_t	   *bx = (coord_t*)ARR_DATA_PTR(b);

	if (a_dim != b_dim)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATA_EXCEPTION),
				 errmsg("different array dimensions %d and %d", a_dim, b_dim)));
	}

	for (int i = 0; i < a_dim; i++)
	{
		diff = ax[i] - bx[i];
		distance += diff * diff;
	}

	PG_RETURN_FLOAT4((dist_t)sqrt(distance));
}
