/*-------------------------------------------------------------------------
 *
 * inmem_smgr.c
 *
 * This is an implementation of the SMGR interface, used in the WAL redo
 * process (see src/backend/tcop/zenith_wal_redo.c). It has no persistent
 * storage, the pages that are written out are kept in a small number of
 * in-memory buffers.
 *
 * Normally, replaying a WAL record only needs to access a handful of
 * buffers, which fit in the normal buffer cache, so this is just for
 * "overflow" storage when the buffer cache is not large enough.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  contrib/neon/inmem_smgr.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "pagestore_client.h"
#include "storage/block.h"
#include "storage/buf_internals.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"

/* Size of the in-memory smgr */
#define MAX_PAGES 64

/* If more than WARN_PAGES are used, print a warning in the log */
#define WARN_PAGES 32

static BufferTag page_tag[MAX_PAGES];
static char page_body[MAX_PAGES][BLCKSZ];
static int	used_pages;

static int
locate_page(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno)
{
	/* We only hold a small number of pages, so linear search */
	for (int i = 0; i < used_pages; i++)
	{
		if (RelFileNodeEquals(reln->smgr_rnode.node, page_tag[i].rnode)
			&& forknum == page_tag[i].forkNum
			&& blkno == page_tag[i].blockNum)
		{
			return i;
		}
	}
	return -1;
}

/*
 *	inmem_init() -- Initialize private state
 */
void
inmem_init(void)
{
	used_pages = 0;
}

/*
 *	inmem_exists() -- Does the physical file exist?
 */
bool
inmem_exists(SMgrRelation reln, ForkNumber forknum)
{
	for (int i = 0; i < used_pages; i++)
	{
		if (RelFileNodeEquals(reln->smgr_rnode.node, page_tag[i].rnode)
			&& forknum == page_tag[i].forkNum)
		{
			return true;
		}
	}
	return false;
}

/*
 *	inmem_create() -- Create a new relation on zenithd storage
 *
 * If isRedo is true, it's okay for the relation to exist already.
 */
void
inmem_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
}

/*
 *	inmem_unlink() -- Unlink a relation.
 */
void
inmem_unlink(RelFileNodeBackend rnode, ForkNumber forknum, bool isRedo)
{
}

/*
 *	inmem_extend() -- Add a block to the specified relation.
 *
 *		The semantics are nearly the same as mdwrite(): write at the
 *		specified position.  However, this is to be used for the case of
 *		extending a relation (i.e., blocknum is at or beyond the current
 *		EOF).  Note that we assume writing a block beyond current EOF
 *		causes intervening file space to become filled with zeroes.
 */
void
inmem_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
			 char *buffer, bool skipFsync)
{
	/* same as smgwrite() for us */
	inmem_write(reln, forknum, blkno, buffer, skipFsync);
}

/*
 *  inmem_open() -- Initialize newly-opened relation.
 */
void
inmem_open(SMgrRelation reln)
{
}

/*
 *	inmem_close() -- Close the specified relation, if it isn't closed already.
 */
void
inmem_close(SMgrRelation reln, ForkNumber forknum)
{
}

/*
 *	inmem_prefetch() -- Initiate asynchronous read of the specified block of a relation
 */
bool
inmem_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	return true;
}

/*
 * inmem_writeback() -- Tell the kernel to write pages back to storage.
 */
void
inmem_writeback(SMgrRelation reln, ForkNumber forknum,
				BlockNumber blocknum, BlockNumber nblocks)
{
}

/*
 *	inmem_read() -- Read the specified block from a relation.
 */
void
inmem_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno,
		   char *buffer)
{
	int			pg;

	pg = locate_page(reln, forknum, blkno);
	if (pg < 0)
		memset(buffer, 0, BLCKSZ);
	else
		memcpy(buffer, page_body[pg], BLCKSZ);
}

/*
 *	inmem_write() -- Write the supplied block at the appropriate location.
 *
 *		This is to be used only for updating already-existing blocks of a
 *		relation (ie, those before the current EOF).  To extend a relation,
 *		use mdextend().
 */
void
inmem_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
			char *buffer, bool skipFsync)
{
	int			pg;

	pg = locate_page(reln, forknum, blocknum);
	if (pg < 0)
	{
		/*
		 * We assume the buffer cache is large enough to hold all the buffers
		 * needed for most operations. Overflowing to this "in-mem smgr" in rare
		 * cases is OK. But if we find that we're using more than WARN_PAGES,
		 * print a warning so that we get alerted and get to investigate why
		 * we're accessing so many buffers.
		 */
		elog(used_pages >= WARN_PAGES ? WARNING : DEBUG1,
			 "inmem_write() called for %u/%u/%u.%u blk %u: used_pages %u",
			 reln->smgr_rnode.node.spcNode,
			 reln->smgr_rnode.node.dbNode,
			 reln->smgr_rnode.node.relNode,
			 forknum,
			 blocknum,
			 used_pages);
		if (used_pages == MAX_PAGES)
			elog(ERROR, "Inmem storage overflow");

		pg = used_pages;
		used_pages++;
		INIT_BUFFERTAG(page_tag[pg], reln->smgr_rnode.node, forknum, blocknum);
	}  else {
		elog(DEBUG1, "inmem_write() called for %u/%u/%u.%u blk %u: found at %u",
			 reln->smgr_rnode.node.spcNode,
			 reln->smgr_rnode.node.dbNode,
			 reln->smgr_rnode.node.relNode,
			 forknum,
			 blocknum,
			 used_pages);
	}
	memcpy(page_body[pg], buffer, BLCKSZ);
}

/*
 *	inmem_nblocks() -- Get the number of blocks stored in a relation.
 */
BlockNumber
inmem_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	/*
	 * It's not clear why a WAL redo function would call smgrnblocks().
	 * During recovery, at least before reaching consistency, the size of a
	 * relation could be arbitrarily small, if it was truncated after the
	 * record being replayed, or arbitrarily large if it was extended
	 * afterwards. But one place where it's called is in
	 * XLogReadBufferExtended(): it extends the relation, if it's smaller than
	 * the requested page. That's a waste of time in the WAL redo
	 * process. Pretend that all relations are maximally sized to avoid it.
	 */
	return MaxBlockNumber;
}

/*
 *	inmem_truncate() -- Truncate relation to specified number of blocks.
 */
void
inmem_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks)
{
}

/*
 *	inmem_immedsync() -- Immediately sync a relation to stable storage.
 */
void
inmem_immedsync(SMgrRelation reln, ForkNumber forknum)
{
}

static const struct f_smgr inmem_smgr =
{
	.smgr_init = inmem_init,
	.smgr_shutdown = NULL,
	.smgr_open = inmem_open,
	.smgr_close = inmem_close,
	.smgr_create = inmem_create,
	.smgr_exists = inmem_exists,
	.smgr_unlink = inmem_unlink,
	.smgr_extend = inmem_extend,
	.smgr_prefetch = inmem_prefetch,
	.smgr_read = inmem_read,
	.smgr_write = inmem_write,
	.smgr_writeback = inmem_writeback,
	.smgr_nblocks = inmem_nblocks,
	.smgr_truncate = inmem_truncate,
	.smgr_immedsync = inmem_immedsync,
};

const f_smgr *
smgr_inmem(BackendId backend, RelFileNode rnode)
{
	Assert(InRecovery);
	if (backend != InvalidBackendId)
		return smgr_standard(backend, rnode);
	else
		return &inmem_smgr;
}

void
smgr_init_inmem()
{
	inmem_init();
}
