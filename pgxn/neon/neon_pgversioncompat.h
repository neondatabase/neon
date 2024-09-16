/*
 * Compatibility macros to cover up differences between supported PostgreSQL versions,
 * to help with compiling the same sources for all of them.
 */

#ifndef NEON_PGVERSIONCOMPAT_H
#define NEON_PGVERSIONCOMPAT_H

#if PG_MAJORVERSION_NUM < 17
#define NRelFileInfoBackendIsTemp(rinfo) (rinfo.backend != InvalidBackendId)
#else
#define NRelFileInfoBackendIsTemp(rinfo) (rinfo.backend != INVALID_PROC_NUMBER)
#endif

#define RelFileInfoEquals(a, b) ( \
	NInfoGetSpcOid(a) == NInfoGetSpcOid(b) && \
	NInfoGetDbOid(a) == NInfoGetDbOid(b) && \
	NInfoGetRelNumber(a) == NInfoGetRelNumber(b) \
)

/* buftag population & RelFileNode/RelFileLocator rework */
#if PG_MAJORVERSION_NUM < 16

#define InitBufferTag(tag, rfn, fn, bn) INIT_BUFFERTAG(*tag, *rfn, fn, bn)

#define USE_RELFILENODE

#define RELFILEINFO_HDR "storage/relfilenode.h"

#define NRelFileInfo RelFileNode
#define NRelFileInfoBackend RelFileNodeBackend
#define NRelFileNumber Oid

#define InfoFromRelation(rel) (rel)->rd_node
#define InfoFromSMgrRel(srel) (srel)->smgr_rnode.node
#define InfoBFromSMgrRel(srel) (srel)->smgr_rnode
#define InfoFromNInfoB(ninfob) ninfob.node

#define RelFileInfoFmt(rinfo) \
	(rinfo).spcNode, \
	(rinfo).dbNode, \
	(rinfo).relNode

#define RelFileInfoBackendFmt(ninfob) \
	(ninfob).backend, \
	(ninfob).node.spcNode, \
	(ninfob).node.dbNode, \
	(ninfob).node.relNode

#define NInfoGetSpcOid(ninfo)		(ninfo).spcNode
#define NInfoGetDbOid(ninfo)		(ninfo).dbNode
#define NInfoGetRelNumber(ninfo)	(ninfo).relNode

#define CopyNRelFileInfoToBufTag(tag, rinfo) \
	do { \
		(tag).rnode = (rinfo); \
	} while (false)

#define BufTagGetNRelFileInfo(tag) tag.rnode

#define BufTagGetRelNumber(tagp) ((tagp)->rnode.relNode)

#define InvalidRelFileNumber InvalidOid

#define SMgrRelGetRelInfo(reln) \
	(reln->smgr_rnode.node)

#define DropRelationAllLocalBuffers DropRelFileNodeAllLocalBuffers

#else							/* major version >= 16 */

#define USE_RELFILELOCATOR

#define BUFFERTAGS_EQUAL(a, b) BufferTagsEqual(&(a), &(b))

#define RELFILEINFO_HDR "storage/relfilelocator.h"

#define NRelFileInfo RelFileLocator
#define NRelFileInfoBackend RelFileLocatorBackend

#define InfoFromRelation(rel) (rel)->rd_locator
#define InfoFromSMgrRel(srel) (srel)->smgr_rlocator.locator
#define InfoBFromSMgrRel(srel) (srel)->smgr_rlocator
#define InfoFromNInfoB(ninfob) (ninfob).locator

#define RelFileInfoFmt(rinfo) \
	(rinfo).spcOid, \
	(rinfo).dbOid, \
	(rinfo).relNumber
#define RelFileInfoBackendFmt(ninfob) \
	(ninfob).backend, \
	(ninfob).locator.spcOid, \
	(ninfob).locator.dbOid, \
	(ninfob).locator.relNumber

#define NInfoGetSpcOid(ninfo)		(ninfo).spcOid
#define NInfoGetDbOid(ninfo)		(ninfo).dbOid
#define NInfoGetRelNumber(ninfo)	(ninfo).relNumber

#define CopyNRelFileInfoToBufTag(tag, rinfo) \
	do { \
		(tag).spcOid = (rinfo).spcOid; \
		(tag).dbOid = (rinfo).dbOid; \
		(tag).relNumber = (rinfo).relNumber; \
	} while (false)

#define BufTagGetNRelFileInfo(tag) \
	((RelFileLocator) { \
		.spcOid = (tag).spcOid, \
		.dbOid = (tag).dbOid, \
		.relNumber = (tag).relNumber, \
	})

#define SMgrRelGetRelInfo(reln) \
	((reln)->smgr_rlocator)

#define DropRelationAllLocalBuffers DropRelationAllLocalBuffers
#endif

#if PG_MAJORVERSION_NUM < 17
#define ProcNumber BackendId
#define INVALID_PROC_NUMBER InvalidBackendId
#define AmAutoVacuumWorkerProcess() (IsAutoVacuumWorkerProcess())
#endif

#endif							/* NEON_PGVERSIONCOMPAT_H */
