
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

// curl -X POST http://localhost:8080/extension_server/postgis-3.so
static bool
neon_download_extension_file_http(const char *filename)
{
    CURL *curl;
    CURLcode res;
    char * compute_ctl_url;
    char *postdata;
    bool ret = false;

    if ((curl = curl_easy_init()) == NULL)
	{
		elog(ERROR, "Failed to initialize curl handle");
	}

    compute_ctl_url = psprintf("http://localhost:%d/extension_server/%s", extension_server_port, filename);

    elog(LOG, "curl_easy_perform() url: %s", compute_ctl_url);

	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "POST");
    curl_easy_setopt(curl, CURLOPT_URL, compute_ctl_url);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3L /* seconds */ );

    if(curl)
    {
        /* Perform the request, res will get the return code */
        res = curl_easy_perform(curl);
        /* Check for errors */
        if(res == CURLE_OK)
        {
            elog(LOG, "curl_easy_perform() succeeded");
            ret = true;
        }
        else
        {
            elog(WARNING, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
        }

        /* always cleanup */
        curl_easy_cleanup(curl);
    }

    return ret;
}

void
pg_init_extension_server()
{
	DefineCustomIntVariable("neon.extension_server_port",
							   "connection string to the compute_ctl",
							   NULL,
							   &extension_server_port,
							   0,0,INT_MAX,
							   PGC_POSTMASTER,
							   0,	/* no flags required */
							   NULL, NULL, NULL);

    //set download_extension_file_hook
    prev_download_extension_file_hook = download_extension_file_hook;
    download_extension_file_hook = neon_download_extension_file_http;
}
