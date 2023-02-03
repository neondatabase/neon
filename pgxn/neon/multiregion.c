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

static XLogRecPtr	region_lsns[MAX_REGIONS];

/*
 * Set the LSN for a given region if it wasn't previously set. 
 * This LSN is used in Neon requests for that region throughout
 * the life of the current transaction.
 */
void
set_region_lsn(int region, NeonResponse *msg)
{
	XLogRecPtr lsn = InvalidXLogRecPtr;

	if (!IsMultiRegion() || !RegionIsRemote(region))
		return;

	AssertArg(region < MAX_REGIONS);

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
		case T_NeonDbSizeResponse:
			lsn = ((NeonDbSizeResponse *) msg)->lsn;
			break;
		case T_NeonErrorResponse:
			break;
		default:
			neon_log(ERROR, "unexpected neon message tag 0x%02x", messageTag(msg));
			break;
	}

	if (lsn == InvalidXLogRecPtr)
	{
		neon_log(WARNING, "region lsn is InvalidXLogRecPtr");
		return;
	}

	if (region_lsns[region] == InvalidXLogRecPtr)
		region_lsns[region] = lsn;

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
	AssertArg(region < MAX_REGIONS);
	// TODO(ctring): If this is 0, proactively contact neon for the latest lsn
	return region_lsns[region];
}

/*
 * Return an array of currently known LSNs of MAX_REGIONS region
 */
XLogRecPtr *
get_all_region_lsns(void)
{
	RelFileNode dummy_node = {InvalidOid, InvalidOid, InvalidOid};

	region_lsns[current_region] =
		GetLastWrittenLSN(dummy_node, MAIN_FORKNUM, REL_METADATA_PSEUDO_BLOCKNO);

	return region_lsns;
}

void
clear_region_lsns(void)
{
	int i;
	for (i = 0; i < MAX_REGIONS; i++)
		region_lsns[i] = InvalidXLogRecPtr;
}
