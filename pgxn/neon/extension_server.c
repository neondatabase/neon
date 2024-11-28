/*-------------------------------------------------------------------------
 *
 * extension_server.c
 *	  Request compute_ctl to download extension files.
 *
 * IDENTIFICATION
 *	 contrib/neon/extension_server.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <curl/curl.h>

#include "access/xact.h"
#include "utils/guc.h"
#include "tcop/utility.h"

#include "extension_server.h" 
#include "neon_utils.h"

static int	extension_server_port = 0;

static download_extension_file_hook_type prev_download_extension_file_hook = NULL;

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

static bool extension_ddl_occured = false;

/*
  * to download all SQL (and data) files for an extension:
  * curl -X POST http://localhost:8080/extension_server/postgis
  * it covers two possible extension files layouts:
  * 1. extension_name--version--platform.sql
  * 2. extension_name/extension_name--version.sql
  *    extension_name/extra_files.csv
  * to download specific library file:
  * curl -X POST http://localhost:8080/extension_server/postgis-3.so?is_library=true
  */
static bool
neon_download_extension_file_http(const char *filename, bool is_library)
{
	static CURL	   *handle = NULL;

	CURLcode	res;
	char	   *compute_ctl_url;
	bool		ret = false;

	if (handle == NULL)
	{
		handle = alloc_curl_handle();

		curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "POST");
		curl_easy_setopt(handle, CURLOPT_TIMEOUT, 3L /* seconds */ );
	}

	compute_ctl_url = psprintf("http://localhost:%d/extension_server/%s%s",
							   extension_server_port, filename, is_library ? "?is_library=true" : "");

	elog(LOG, "Sending request to compute_ctl: %s", compute_ctl_url);

	curl_easy_setopt(handle, CURLOPT_URL, compute_ctl_url);

	/* Perform the request, res will get the return code */
	res = curl_easy_perform(handle);
	/* Check for errors */
	if (res == CURLE_OK)
	{
		ret = true;
	}
	else
	{
		/*
		 * Don't error here because postgres will try to find the file and will
		 * fail with some proper error message if it's not found.
		 */
		elog(WARNING, "neon_download_extension_file_http failed: %s\n", curl_easy_strerror(res));
	}

	return ret;
}


// Handle extension DDL: we need this for monitoring of installed extensions.
// General solution is hard, because extensions can be installed in many ways,
// i.e. sometimes as a cascade operations.
//
// Also, we don't have enough information in the statement itself,
// i.e. extension version is not always present and retrieved from the control file
// at a later stage.
//
// So, just remember the fact of the extension DDL and send it to compute_ctl
// on commit.
static void
NeonExtensionProcessUtility(
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

	switch (nodeTag(parseTree))
	{
		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
			extension_ddl_occured = true;
			break;
		case T_DropStmt:
			{
				switch (((DropStmt *) parseTree)->removeType)
				{
					case OBJECT_EXTENSION:
						extension_ddl_occured = true;
						break;
					default:
						break;
				}
			}
			break;
		default:
			break;
	}

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



static bool
neon_send_extension_ddl_event_http()
{
	static CURL	   *handle = NULL;

	CURLcode	res;
	char	   *compute_ctl_url;
	bool		ret = false;

	if (handle == NULL)
	{
		handle = alloc_curl_handle();
	}

	compute_ctl_url = psprintf("http://localhost:%d/installed_extensions",
							   extension_server_port);

	elog(LOG, "Sending request to compute_ctl: %s", compute_ctl_url);

	curl_easy_setopt(handle, CURLOPT_URL, compute_ctl_url);

	// Use HEAD request without payload, because this is just a notification.
	//
	// This is probably not the best API design, but I didn't want to introduce
	// new endpoint for this. Suggestions are welcome.
	curl_easy_setopt(handle, CURLOPT_NOBODY, 1L); 

	/* Perform the request, res will get the return code */
	res = curl_easy_perform(handle);

	/* Check for errors */
	if (res != CURLE_OK)
	{
		/*
		 * Don't error here because this is just a monitoring feature.
		 */
		elog(WARNING, "neon_send_extension_ddl_event_http failed: %s\n", curl_easy_strerror(res));
	}

	return ret;
}

static void
NeonExtensionXactCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		if (extension_ddl_occured)
			neon_send_extension_ddl_event_http();
	}

	extension_ddl_occured = false;
}

void
pg_init_extension_server()
{

	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = NeonExtensionProcessUtility;
	RegisterXactCallback(NeonExtensionXactCallback, NULL);

	/* Port to connect to compute_ctl on localhost */
	/* to request extension files. */
	DefineCustomIntVariable("neon.extension_server_port",
							"connection string to the compute_ctl",
							NULL,
							&extension_server_port,
							0, 0, INT_MAX,
							PGC_POSTMASTER,
							0,	/* no flags required */
							NULL, NULL, NULL);

	/* set download_extension_file_hook */
	prev_download_extension_file_hook = download_extension_file_hook;
	download_extension_file_hook = neon_download_extension_file_http;
}
