/*-------------------------------------------------------------------------
 *
 * extension_server.c
 *	  Request compute_ctl to download extension files.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <curl/curl.h>

#include "utils/guc.h"

#include "extension_server.h"
#include "neon_utils.h"

int	hadron_extension_server_port = 0;
static int	extension_server_request_timeout = 60;
static int	extension_server_connect_timeout = 60;

static download_extension_file_hook_type prev_download_extension_file_hook = NULL;

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
	CURLcode	res;
	bool		ret = false;
	CURL	   *handle = NULL;
	char	   *compute_ctl_url;

	handle = alloc_curl_handle();

	curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "POST");
	if (extension_server_request_timeout > 0)
		curl_easy_setopt(handle, CURLOPT_TIMEOUT, (long)extension_server_request_timeout /* seconds */ );
	if (extension_server_connect_timeout > 0)
		curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT, (long)extension_server_connect_timeout /* seconds */ );

	compute_ctl_url = psprintf("http://localhost:%d/extension_server/%s%s",
							   hadron_extension_server_port, filename, is_library ? "?is_library=true" : "");

	elog(LOG, "Sending request to compute_ctl: %s", compute_ctl_url);

	curl_easy_setopt(handle, CURLOPT_URL, compute_ctl_url);

	/* Perform the request, res will get the return code */
	res = curl_easy_perform(handle);
	curl_easy_cleanup(handle);

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

void
pg_init_extension_server()
{
	/* Port to connect to compute_ctl on localhost */
	/* to request extension files. */
	DefineCustomIntVariable("neon.extension_server_port",
							"connection string to the compute_ctl",
							NULL,
							&hadron_extension_server_port,
							0, 0, INT_MAX,
							PGC_POSTMASTER,
							0,	/* no flags required */
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.extension_server_request_timeout",
							"timeout for fetching extensions in seconds",
							NULL,
							&extension_server_request_timeout,
							60, 0, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("neon.extension_server_connect_timeout",
							"timeout for connecting to the extension server in seconds",
							NULL,
							&extension_server_connect_timeout,
							60, 0, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	/* set download_extension_file_hook */
	prev_download_extension_file_hook = download_extension_file_hook;
	download_extension_file_hook = neon_download_extension_file_http;
}
