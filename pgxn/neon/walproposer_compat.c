#include <stdio.h>
#include "walproposer.h"

unsigned int pq_getmsgint(StringInfo msg, int b) {}
int64 pq_getmsgint64(StringInfo msg) {}

int	pq_getmsgbyte(StringInfo msg) {}
const char *pq_getmsgbytes(StringInfo msg, int datalen) {}
void pq_copymsgbytes(StringInfo msg, char *buf, int datalen) {}
const char *pq_getmsgstring(StringInfo msg) {}
void pq_getmsgend(StringInfo msg) {}

const char *
timestamptz_to_str(TimestampTz dt) {}

bool TimestampDifferenceExceeds(TimestampTz start_time,
									   TimestampTz stop_time,
									   int msec) {}

void ExceptionalCondition(const char *conditionName,
								 const char *fileName, int lineNumber) {
    exit(1);
}
