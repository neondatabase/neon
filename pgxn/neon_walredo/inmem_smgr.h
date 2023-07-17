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

#if PG_VERSION_NUM >= 160000
#include "storage/relfilelocator.h"
#else
#include "storage/relfilenode.h"
#endif

// This is a hack to avoid too many ifdefs in the function definitions.
#if PG_VERSION_NUM >= 160000
typedef RelFileLocator RelFileNode;
typedef RelFileLocatorBackend RelFileNodeBackend;
#define RelFileNodeBackendIsTemp RelFileLocatorBackendIsTemp
#endif

#if PG_VERSION_NUM >= 160000
#define RelnGetRnode(reln) (reln->smgr_rlocator.locator)
#define RnodeGetSpcOid(rnode) (rnode.spcOid)
#define RnodeGetDbOid(rnode) (rnode.dbOid)
#define RnodeGetRelNumber(rnode) (rnode.relNumber)

#define BufTagGetRnode(tag) (BufTagGetRelFileLocator(&tag))
#else
#define RelnGetRnode(reln) (reln->smgr_rnode.node)
#define RnodeGetSpcOid(rnode) (rnode.spcNode)
#define RnodeGetDbOid(rnode) (rnode.dbNode)
#define RnodeGetRelNumber(rnode) (rnode.relNode)

#define BufTagGetRnode(tag) (tag.rnode)

#endif

#define RelnGetSpcOid(reln) (RnodeGetRelNumber(RelnGetRnode(reln)))
#define RelnGetDbOid(reln) (RnodeGetDbOid(RelnGetRnode(reln)))
#define RelnGetRelNumber(reln) (RnodeGetRelNumber(RelnGetRnode(reln)))

extern const f_smgr *smgr_inmem(BackendId backend, RelFileNode rnode);
extern void smgr_init_inmem(void);

#endif /* INMEM_SMGR_H */
