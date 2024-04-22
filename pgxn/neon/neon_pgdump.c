#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/guc.h"

#include <stdio.h>
#include <stdlib.h>

PG_FUNCTION_INFO_V1(neon_pgdump_schema);

char *COMPUTEHOST = "127.0.0.1";
char *COMPUTEROLE = "cloud_admin";

Datum neon_pgdump_schema(PG_FUNCTION_ARGS)
{
    text *database_name = PG_GETARG_TEXT_PP(0);
    char *dbname = text_to_cstring(database_name);
    char command[1024];
    FILE *pipe;
    char result[1024];
    StringInfoData buf;

    const char *port = GetConfigOptionByName("port", NULL, false);

    int command_res = snprintf(command, sizeof(command), "pg_dump --schema-only -h %s -p %s -U %s -d %s", COMPUTEHOST, port, COMPUTEROLE, dbname);

    if (command_res == -1)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                 errmsg("Failed to build command")));
    }

    pipe = popen(command, "r");
    if (!pipe)
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                 errmsg("Failed to run command")));

    initStringInfo(&buf);

    while (fgets(result, sizeof(result), pipe) != NULL)
    {
        appendStringInfoString(&buf, result);
    }

    int status = pclose(pipe);
    if (status == -1)
    {
        ereport(ERROR,
                (errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
                 errmsg("Failed to close command pipe")));
    }

    text *ret = cstring_to_text(buf.data);
    pfree(buf.data);
    PG_RETURN_TEXT_P(ret);
}