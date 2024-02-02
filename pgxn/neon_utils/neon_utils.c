/*-------------------------------------------------------------------------
 *
 * neon_utils.c
 *	  neon_utils - small useful functions
 *
 * IDENTIFICATION
 *	 contrib/neon_utils/neon_utils.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(num_cpus);

Datum
num_cpus(PG_FUNCTION_ARGS)
{
#ifdef _WIN32
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	uint32 num_cpus = (uint32) sysinfo.dwNumberOfProcessors;
#else
	uint32 num_cpus = (uint32) sysconf(_SC_NPROCESSORS_ONLN);
#endif
	PG_RETURN_UINT32(num_cpus);
}
