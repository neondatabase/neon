/*-------------------------------------------------------------------------
 *
 * neon.h
 *	  Functions used in the initialization of this extension.
 *
 * IDENTIFICATION
 *	 contrib/neon/neon.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NEON_H
#define NEON_H
#include "access/xlogreader.h"

/* GUCs */
extern char *neon_auth_token;
extern char *neon_timeline;
extern char *neon_tenant;

extern char *wal_acceptors_list;
extern int	wal_acceptor_reconnect_timeout;
extern int	wal_acceptor_connection_timeout;

extern void pg_init_libpagestore(void);
extern void pg_init_walproposer(void);

extern uint64 BackpressureThrottlingTime(void);
extern void SetNeonCurrentClusterSize(uint64 size);
extern uint64 GetNeonCurrentClusterSize(void);
extern void replication_feedback_get_lsns(XLogRecPtr *writeLsn, XLogRecPtr *flushLsn, XLogRecPtr *applyLsn);

extern void PGDLLEXPORT WalProposerSync(int argc, char *argv[]);
extern void PGDLLEXPORT WalProposerMain(Datum main_arg);
PGDLLEXPORT void LogicalSlotsMonitorMain(Datum main_arg);

#endif							/* NEON_H */
