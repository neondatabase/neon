#include <assert.h>
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

typedef struct UnstableExtensions
{
	size_t	extc;
	char   *extv[FLEXIBLE_ARRAY_MEMBER];
} UnstableExtensions;

static bool					allow_unstable_extensions = false;
static char				   *unstable_extensions_str = NULL;
static UnstableExtensions  *unstable_extensions = NULL;

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

static bool
check_unstable_extensions(char **newval, void **extra, GucSource source)
{
	char			   *n;
	UnstableExtensions *exts;
	char			   *curr_ext;
	size_t				max_extc;

	*extra = NULL;

	if (*newval == NULL || (*newval)[0] == '\0')
		return true;

	/* At this point, we know we have at least 1 extension like: ext */
	max_extc = 1;
	for (size_t i = 0; i < strlen(*newval); ++i)
	{
		if ((*newval)[i] == ',')
			++max_extc;
	}

	exts = guc_malloc(ERROR, sizeof(*exts) + max_extc * sizeof(char *));
	exts->extc = 0;

	n = guc_strdup(ERROR, *newval);
	while ((curr_ext = strsep(&n, ",")))
	{
		/* In the event, we receive a config like: ",,ext" */
		if (curr_ext[0] == '\0')
			continue;

		exts->extv[exts->extc++] = guc_strdup(ERROR, curr_ext);
	}

	guc_free(n);

	*extra = exts;

	return true;
}

static void
assign_unstable_extensions(const char *newval, void *extra)
{
	unstable_extensions = extra;
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

			for (size_t i = 0; i < unstable_extensions->extc; ++i)
			{
				if (strcmp(unstable_extensions->extv[i], stmt->extname) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							 errmsg("using an unstable extension (%s) is currently prohibited", stmt->extname),
							 errhint("Set neon.allow_unstable_extensions to true")));
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
		"Allow unstable extensions to be installed and used",
		NULL,
		&unstable_extensions_str,
		NULL,
		PGC_SUSET,
		0,
		check_unstable_extensions, assign_unstable_extensions, NULL);

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = CheckUnstableExtension;
}
