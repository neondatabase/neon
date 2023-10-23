#ifndef __NEON_WALREADER_H__
#define __NEON_WALREADER_H__

#include "access/xlogdefs.h"

/* forward declare so we don't have to expose the struct to the public */
struct NeonWALReader;
typedef struct NeonWALReader NeonWALReader;

extern NeonWALReader *NeonWALReaderAllocate(int wal_segment_size, XLogRecPtr available_lsn);
extern void NeonWALReaderFree(NeonWALReader *state);
extern bool NeonWALRead(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli);
extern char *NeonWALReaderErrMsg(NeonWALReader *state);
