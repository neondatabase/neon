#ifndef __NEON_WALREADER_H__
#define __NEON_WALREADER_H__

#include "access/xlogdefs.h"

/* forward declare so we don't have to expose the struct to the public */
struct NeonWALReader;
typedef struct NeonWALReader NeonWALReader;

/* avoid including walproposer.h as it includes us */
struct WalProposer;
typedef struct WalProposer WalProposer;

/* NeonWALRead return value */
typedef enum
{
	NEON_WALREAD_SUCCESS,
	NEON_WALREAD_WOULDBLOCK,
	NEON_WALREAD_ERROR,
} NeonWALReadResult;

extern NeonWALReader *NeonWALReaderAllocate(int wal_segment_size, XLogRecPtr available_lsn, WalProposer *wp, char *log_prefix);
extern void NeonWALReaderFree(NeonWALReader *state);
extern NeonWALReadResult NeonWALRead(NeonWALReader *state, char *buf, XLogRecPtr startptr, Size count, TimeLineID tli);
extern pgsocket NeonWALReaderSocket(NeonWALReader *state);
extern uint32 NeonWALReaderEvents(NeonWALReader *state);
extern char *NeonWALReaderErrMsg(NeonWALReader *state);

#endif							/* __NEON_WALREADER_H__ */
