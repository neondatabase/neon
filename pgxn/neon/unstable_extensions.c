#include <stdlib.h>
#include <string.h>

#include "postgres.h"

#include "nodes/plannodes.h"
#include "nodes/parsenodes.h"
#include "tcop/utility.h"
#include "utils/errcodes.h"
#include "utils/guc.h"

#include "neon_pgversioncompat.h"
#include "unstable_extensions.h"

static bool					allow_unstable_extensions = false;
static char				   *unstable_extensions = NULL;

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

static bool
list_contains(char const* comma_separated_list, char const* val)
{
	char const* occ = comma_separated_list;
	size_t val_len = strlen(val);

	if (val_len == 0)
		return false;

	while ((occ = strstr(occ, val)) != NULL)
	{
		if ((occ == comma_separated_list || occ[-1] == ',')
			&& (occ[val_len] == '\0' || occ[val_len] == ','))
		{
			return true;
		}
		occ += val_len;
	}

	return false;
}


static void
CheckUnstableExtension(
	PlannedStmt *pstmt,
	const char *queryString,
	bool readOnlyTree,
	ProcessUtilityContext context,
	ParamListInfo params,
	QueryEnvironment *queryEnv,
	DestReceiver *dest,
	QueryCompletion *qc)
{
	Node	   *parseTree = pstmt->utilityStmt;

	if (allow_unstable_extensions || unstable_extensions == NULL)
		goto process;

	switch (nodeTag(parseTree))
	{
		case T_CreateExtensionStmt:
		{
			CreateExtensionStmt *stmt = castNode(CreateExtensionStmt, parseTree);
			if (list_contains(unstable_extensions, stmt->extname))
			{
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("%s extension is in beta and may be unstable or introduce backward-incompatible changes.\nWe recommend testing it in a separate, dedicated Neon project.", stmt->extname),
						 errhint("to proceed with installation, run SET neon.allow_unstable_extensions='true'")));
			}
			break;
		}
		default:
			goto process;
	}

process:
	if (PreviousProcessUtilityHook)
	{
		PreviousProcessUtilityHook(
			pstmt,
			queryString,
			readOnlyTree,
			context,
			params,
			queryEnv,
			dest,
			qc);
	}
	else
	{
		standard_ProcessUtility(
			pstmt,
			queryString,
			readOnlyTree,
			context,
			params,
			queryEnv,
			dest,
			qc);
	}
}

void
InitUnstableExtensionsSupport(void)
{
	DefineCustomBoolVariable(
		"neon.allow_unstable_extensions",
		"Allow unstable extensions to be installed and used",
		NULL,
		&allow_unstable_extensions,
		false,
		PGC_USERSET,
		0,
		NULL, NULL, NULL);

	DefineCustomStringVariable(
		"neon.unstable_extensions",
		"List of unstable extensions",
		NULL,
		&unstable_extensions,
		NULL,
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = CheckUnstableExtension;
}
