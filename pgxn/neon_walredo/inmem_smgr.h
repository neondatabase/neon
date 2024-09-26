/*-------------------------------------------------------------------------
 *
 * inmem_smgr.h
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef INMEM_SMGR_H
#define INMEM_SMGR_H

extern const f_smgr *smgr_inmem(ProcNumber backend, NRelFileInfo rinfo);
extern void smgr_init_inmem(void);

#endif /* INMEM_SMGR_H */
