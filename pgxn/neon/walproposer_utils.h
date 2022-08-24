#ifndef __NEON_WALPROPOSER_UTILS_H__
#define __NEON_WALPROPOSER_UTILS_H__

#include "walproposer.h"

int        CompareLsn(const void *a, const void *b);
char*      FormatSafekeeperState(SafekeeperState state);
void       AssertEventsOkForState(uint32 events, Safekeeper* sk);
uint32     SafekeeperStateDesiredEvents(SafekeeperState state);
char*      FormatEvents(uint32 events);
bool       HexDecodeString(uint8 *result, char *input, int nbytes);
uint32     pq_getmsgint32_le(StringInfo msg);
uint64     pq_getmsgint64_le(StringInfo msg);
void       pq_sendint32_le(StringInfo buf, uint32 i);
void       pq_sendint64_le(StringInfo buf, uint64 i);
void       XLogWalPropWrite(char *buf, Size nbytes, XLogRecPtr recptr);
void       XLogWalPropClose(XLogRecPtr recptr);

#endif /* __NEON_WALPROPOSER_UTILS_H__ */
