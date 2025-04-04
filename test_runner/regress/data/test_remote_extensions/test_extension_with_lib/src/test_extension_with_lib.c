#include <postgres.h>

#include <fmgr.h>

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(motd);
PG_FUNCTION_INFO_V1(fun_fact);

/* Old versions of Postgres didn't pre-declare this in fmgr.h */
#if PG_MAJORVERSION_NUM <= 15
void _PG_init(void);
#endif

void
_PG_init(void)
{
}

Datum
motd(PG_FUNCTION_ARGS)
{
    elog(NOTICE, "Have a great day");

    PG_RETURN_VOID();
}

Datum
fun_fact(PG_FUNCTION_ARGS)
{
    elog(NOTICE, "Neon has a melting point of -246.08 C");

    PG_RETURN_VOID();
}
