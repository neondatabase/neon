/*
 * It is too expensive to check index size at each insert because it requires traverse of all index file segments and calling lseek for each.
 * But we do not need precise size, so it is enough to do it at each n-th insert. The lagest B-Tree key size is abut 2kb,
 * so with N=64K in the worst case error will be less than 128Mb and for 32-bit key just 1Mb.
 */
#define LSM3_CHECK_TOP_INDEX_SIZE_PERIOD (64*1024) /* should be power of two */

/*
 * Control structure for Lsm3 index located in shared memory
 */
typedef struct
{
	Oid base;   /* Oid of base index */
	Oid heap;   /* Oid of indexed relation */
	Oid top[2]; /* Oids of two top indexes */
	int access_count[2]; /* Access counter for top indexes */
	int active_index; /* Index used for insert */
	uint64 n_merges;  /* Number of performed merges since database open */
	uint64 n_inserts; /* Number of performed inserts since database open  */
	volatile bool start_merge; /* Start merging of top index with base index */
	volatile bool merge_in_progress; /* Overflow of top index intiate merge process */
	PGPROC* merger;   /* Merger background worker */
	Oid     db_id;    /* user ID (for background worker) */
	Oid     user_id;  /* database Id (for background worker) */
	Oid     am_id;    /* Lsm3 AM Oid */
	int     top_index_size; /* Size of top index */
	slock_t spinlock; /* Spinlock to synchronize access */
} Lsm3DictEntry;

/*
 * Opaque part of index scan descriptor
 */
typedef struct
{
	Lsm3DictEntry* entry;      /* Lsm3 control structure */
	Relation 	   top_index[2]; /* Opened top index relations */
	SortSupport    sortKeys;   /* Context for comparing index tuples */
	IndexScanDesc  scan[3];    /* Scan descriptors for two top indexes and base index */
	bool           eof[3];     /* Indicators that end of index was reached */
	bool           unique;     /* Whether index is "unique" and we can stop scan after locating first occurrence */
	int            curr_index; /* Index from which last tuple was selected (or -1 if none) */
} Lsm3ScanOpaque;

/* Lsm3 index options */
typedef struct
{
	BTOptions   nbt_opts;       /* Standard B-Tree options */
	int         top_index_size; /* Size of top index (overrode lsm3.top_index_size GUC */
	bool        unique;			/* Index may not contain duplicates. We prohibit unique constraint for Lsm3 index
                                 * because it can not be enforced. But presence of this index option allows to optimize
								 * index lookup: if key is found in active top index, do not search other two indexes.
                                 */
} Lsm3Options;
