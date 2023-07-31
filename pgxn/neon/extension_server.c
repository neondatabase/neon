
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
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "commands/defrem.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "fmgr.h"
#include "utils/guc.h"
#include "port.h"
#include "fmgr.h"

#include <curl/curl.h>

static int extension_server_port = 0;

static download_extension_file_hook_type prev_download_extension_file_hook = NULL;

// to download all SQL (and data) files for an extension:
// curl -X POST http://localhost:8080/extension_server/postgis
// it covers two possible extension files layouts:
// 1. extension_name--version--platform.sql
// 2. extension_name/extension_name--version.sql
//    extension_name/extra_files.csv
//
// to download specific library file:
// curl -X POST http://localhost:8080/extension_server/postgis-3.so?is_library=true
static bool
neon_download_extension_file_http(const char *filename, bool is_library)
{
    CURL *curl;
    CURLcode res;
    char *compute_ctl_url;
    char *postdata;
    bool ret = false;

    if ((curl = curl_easy_init()) == NULL)
    {
        elog(ERROR, "Failed to initialize curl handle");
    }

    compute_ctl_url = psprintf("http://localhost:%d/extension_server/%s%s",
                               extension_server_port, filename, is_library ? "?is_library=true" : "");

    elog(LOG, "Sending request to compute_ctl: %s", compute_ctl_url);

    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_URL, compute_ctl_url);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3L /* seconds */);

    if (curl)
    {
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(curl);
        /* Check for errors */
        if (res == CURLE_OK)
        {
            ret = true;
        }
        else
        {
            // Don't error here because postgres will try to find the file
            // and will fail with some proper error message if it's not found.
            elog(WARNING, "neon_download_extension_file_http failed: %s\n", curl_easy_strerror(res));
        }

        /* always cleanup */
        curl_easy_cleanup(curl);
    }

    return ret;
}

void pg_init_extension_server()
{
    // Port to connect to compute_ctl on localhost
    // to request extension files.
    DefineCustomIntVariable("neon.extension_server_port",
                            "connection string to the compute_ctl",
                            NULL,
                            &extension_server_port,
                            0, 0, INT_MAX,
                            PGC_POSTMASTER,
                            0, /* no flags required */
                            NULL, NULL, NULL);

    // set download_extension_file_hook
    prev_download_extension_file_hook = download_extension_file_hook;
    download_extension_file_hook = neon_download_extension_file_http;
}
