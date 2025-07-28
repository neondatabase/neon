/*-------------------------------------------------------------------------
 *
 * neon.h
 *	  Functions used in the initialization of this extension.
 *
 *-------------------------------------------------------------------------
 */

#ifndef NEON_H
#define NEON_H

#include "access/xlogdefs.h"
#include "utils/wait_event.h"

/* GUCs */
extern char *neon_auth_token;
extern char *neon_timeline;
extern char *neon_tenant;
extern char *wal_acceptors_list;
extern int	wal_acceptor_reconnect_timeout;
extern int	wal_acceptor_connection_timeout;
extern int	readahead_getpage_pull_timeout_ms;
extern bool	disable_wal_prev_lsn_checks;
extern bool	lakebase_mode;

extern bool AmPrewarmWorker;

#if PG_MAJORVERSION_NUM >= 17
extern uint32		WAIT_EVENT_NEON_LFC_MAINTENANCE;
extern uint32		WAIT_EVENT_NEON_LFC_READ;
extern uint32		WAIT_EVENT_NEON_LFC_TRUNCATE;
extern uint32		WAIT_EVENT_NEON_LFC_WRITE;
extern uint32		WAIT_EVENT_NEON_LFC_CV_WAIT;
extern uint32		WAIT_EVENT_NEON_PS_STARTING;
extern uint32		WAIT_EVENT_NEON_PS_CONFIGURING;
extern uint32		WAIT_EVENT_NEON_PS_SEND;
extern uint32		WAIT_EVENT_NEON_PS_READ;
extern uint32		WAIT_EVENT_NEON_WAL_DL;
#else
#define WAIT_EVENT_NEON_LFC_MAINTENANCE	PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_LFC_READ		WAIT_EVENT_BUFFILE_READ
#define WAIT_EVENT_NEON_LFC_TRUNCATE	WAIT_EVENT_BUFFILE_TRUNCATE
#define WAIT_EVENT_NEON_LFC_WRITE		WAIT_EVENT_BUFFILE_WRITE
#define WAIT_EVENT_NEON_LFC_CV_WAIT 	WAIT_EVENT_BUFFILE_READ
#define WAIT_EVENT_NEON_PS_STARTING		PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_PS_CONFIGURING	PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_PS_SEND			PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_PS_READ			PG_WAIT_EXTENSION
#define WAIT_EVENT_NEON_WAL_DL			WAIT_EVENT_WAL_READ
#endif


#define NEON_TAG "[NEON_SMGR] "
#define neon_log(tag, fmt, ...) ereport(tag,                                  \
										(errmsg(NEON_TAG fmt, ##__VA_ARGS__), \
										 errhidestmt(true), errhidecontext(true), errposition(0), internalerrposition(0)))
#define neon_shard_log(shard_no, tag, fmt, ...) ereport(tag,	\
														(errmsg(NEON_TAG "[shard %d] " fmt, shard_no, ##__VA_ARGS__), \
														 errhidestmt(true), errhidecontext(true), errposition(0), internalerrposition(0)))


extern void pg_init_libpagestore(void);
extern void pg_init_walproposer(void);

extern uint64 BackpressureThrottlingTime(void);
extern void SetNeonCurrentClusterSize(uint64 size);
extern uint64 GetNeonCurrentClusterSize(void);
extern void replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn);

extern PGDLLEXPORT void WalProposerSync(int argc, char *argv[]);
extern PGDLLEXPORT void WalProposerMain(Datum main_arg);
extern PGDLLEXPORT void LogicalSlotsMonitorMain(Datum main_arg);

extern void LfcShmemRequest(void);
extern void PagestoreShmemRequest(void);
extern void RelsizeCacheShmemRequest(void);
extern void WalproposerShmemRequest(void);
extern void LwLsnCacheShmemRequest(void);
extern void NeonPerfCountersShmemRequest(void);

extern void LfcShmemInit(void);
extern void PagestoreShmemInit(void);
extern void RelsizeCacheShmemInit(void);
extern void WalproposerShmemInit(void);
extern void LwLsnCacheShmemInit(void);
extern void NeonPerfCountersShmemInit(void);


#endif							/* NEON_H */
