/*-------------------------------------------------------------------------
 *
 * multiregion.c
 *	  Handles network communications in a multi-region setup.
 *
 * IDENTIFICATION
 *	 contrib/neon/multiregion.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "multiregion.h"

#include "access/remotexact.h"
#include "catalog/catalog.h"
#include "libpq-fe.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/varlena.h"
#include "walproposer_utils.h"

#define NEON_TAG "[NEON_SMGR] "
#define neon_log(tag, fmt, ...) ereport(tag, \
		(errmsg(NEON_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

#define NEON_TAG "[NEON_SMGR] "
#define neon_log(tag, fmt, ...) ereport(tag, \
		(errmsg(NEON_TAG fmt, ## __VA_ARGS__), \
		 errhidestmt(true), errhidecontext(true)))

/* GUCs */
char *neon_region_timelines;

static XLogRecPtr	*region_lsns = NULL;
static int	num_regions = 1;

static bool
check_neon_region_timelines(char **newval, void **extra, GucSource source)
{
	char *rawstring;
	List *timelines;
	ListCell *l;
	uint8 zid[16];

	/* Need a modifiable copy of string */
	rawstring = pstrdup(*newval);

	/* Parse string into list of timeline ids */
	if (!SplitIdentifierString(rawstring, ',', &timelines))
	{
		/* syntax error in list */
		GUC_check_errdetail("List syntax is invalid.");
		pfree(rawstring);
		list_free(timelines);
		return false;
	}

	/* Check if the given timeline ids are valid */
	foreach (l, timelines)
	{
		char *tok = (char *)lfirst(l);

		/* Taken from check_zenith_id in libpagestore.c */
		if (*tok != '\0' && !HexDecodeString(zid, tok, 16))
		{
			GUC_check_errdetail("Invalid Zenith id: \"%s\".", tok);
			pfree(rawstring);
			list_free(timelines);
			return false;
		}
	}

	*extra = malloc(sizeof(int));
	if (!*extra)
		return false;

	*((int *) *extra) = list_length(timelines);

	pfree(rawstring);
	list_free(timelines);
	return true;
}

static void assign_neon_region_timelines(const char *newval, void *extra)
{
	/* Add 1 for the global region */
	num_regions = *((int *) extra) + 1;
}


void DefineMultiRegionCustomVariables(void)
{
	DefineCustomStringVariable("neon.region_timelines",
								"List of timelineids corresponding to the partitions",
								NULL,
								&neon_region_timelines,
								"",
								PGC_POSTMASTER,
								GUC_LIST_INPUT,
								check_neon_region_timelines, assign_neon_region_timelines, NULL);
}

bool neon_multiregion_enabled(void)
{
	return neon_region_timelines && neon_region_timelines[0];
}

static void
init_region_lsns()
{
	region_lsns = (XLogRecPtr *)
			MemoryContextAllocZero(TopTransactionContext,
								   num_regions * sizeof(XLogRecPtr));
}

/*
 * Set the LSN for a given region if it wasn't previously set. The set LSN is use
 * for that region throughout the life of the transaction.
 */
void
set_region_lsn(int region, NeonResponse *msg)
{
	XLogRecPtr lsn;

	if (!IsMultiRegion() || !RegionIsRemote(region))
		return;

	AssertArg(region < num_regions);

	switch (messageTag(msg))
	{
		case T_NeonExistsResponse:
			lsn = ((NeonExistsResponse *) msg)->lsn;
			break;
		case T_NeonNblocksResponse:
			lsn = ((NeonNblocksResponse *) msg)->lsn;
			break;
		case T_NeonGetPageResponse:
			lsn = ((NeonGetPageResponse *) msg)->lsn;
			break;
		case T_NeonGetSlruPageResponse:
			lsn = ((NeonGetSlruPageResponse *) msg)->lsn;
			break;
		case T_NeonErrorResponse:
			break;
		default:
			neon_log(ERROR, "unexpected neon message tag 0x%02x", messageTag(msg));
			break;
	}

	Assert(lsn != InvalidXLogRecPtr);

	if (region_lsns == NULL)
		init_region_lsns();

	if (region_lsns[region] == InvalidXLogRecPtr)
		region_lsns[region] = lsn;
	else
		Assert(region_lsns[region] == lsn);
}

/*
 * Get the LSN of a region
 */
XLogRecPtr
get_region_lsn(int region)
{
	if (!IsMultiRegion())
		return InvalidXLogRecPtr;
	
	// LSN of the current region is already tracked by postgres
	AssertArg(region != current_region);
	AssertArg(region < num_regions);

	if (region_lsns == NULL)
		init_region_lsns();

	return region_lsns[region];
}

void
clear_region_lsns(void)
{
	/* The data is destroyed along with the transaction
		context so only need to set this to NULL */
	region_lsns = NULL;
}
