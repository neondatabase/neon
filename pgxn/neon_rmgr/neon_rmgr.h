#ifndef NEON_RMGR_H
#define NEON_RMGR_H
#if PG_MAJORVERSION_NUM >= 16
#include "access/xlog_internal.h"
#include "replication/decode.h"
#include "replication/logical.h"

/*
 * The Neon RMGR adds command ID logging to heap records.
 * This means data is (mostly) safe from other processes' crashes.
 */
extern PGDLLEXPORT void register_neon_rmgr(void);

extern void neon_rm_desc(StringInfo buf, XLogReaderState *record);
extern void neon_rm_decode(LogicalDecodingContext *ctx, XLogRecordBuffer *buf);
extern const char *neon_rm_identify(uint8 info);

#endif
#endif //NEON_RMGR_H
