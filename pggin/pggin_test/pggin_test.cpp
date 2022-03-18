// pggin_test.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include "../libpggin/pggin.h"

extern "C"
{
	#include "postgres.h"

	#include "access/gin_private.h"
	#include "access/ginblock.h"
	#include "access/htup_details.h"
	#include "fmgr.h"

	#include "lib/rbtree.h"
	//#include "utils/memutils.h"
}

/*
 * Encodes a pair of TIDs, and decodes it back. The first TID is always
 * (0, 1), the second one is formed from the blk/off arguments. The 'maxsize'
 * argument is passed to ginCompressPostingList(); it can be used to test the
 * overflow checks.
 *
 * The reason that we test a pair, instead of just a single TID, is that
 * the GinPostingList stores the first TID as is, and the varbyte-encoding
 * is only used for the deltas between TIDs. So testing a single TID would
 * not exercise the varbyte encoding at all.
 *
 * This function prints NOTICEs to describe what is tested, and how large the
 * resulting GinPostingList is. Any incorrect results, e.g. if the encode +
 * decode round trip doesn't return the original input, are reported as
 * ERRORs.
 */
static void
test_itemptr_pair(BlockNumber blk, OffsetNumber off, int maxsize)
{
	ItemPointerData orig_itemptrs[2];
	ItemPointer decoded_itemptrs;
	GinPostingList* pl;
	int			nwritten;
	int			ndecoded;

	elog(NOTICE, "testing with (%u, %d), (%u, %d), max %d bytes",
		0, 1, blk, off, maxsize);
	ItemPointerSet(&orig_itemptrs[0], 0, 1);
	ItemPointerSet(&orig_itemptrs[1], blk, off);

	/* Encode, and decode it back */
	pl = ginCompressPostingList(orig_itemptrs, 2, maxsize, &nwritten);
	elog(NOTICE, "encoded %d item pointers to %zu bytes",
		nwritten, SizeOfGinPostingList(pl));

	if (SizeOfGinPostingList(pl) > maxsize)
		elog(ERROR, "overflow: result was %zu bytes, max %d",
			SizeOfGinPostingList(pl), maxsize);

	decoded_itemptrs = ginPostingListDecode(pl, &ndecoded);
	if (nwritten != ndecoded)
		elog(NOTICE, "encoded %d itemptrs, %d came back", nwritten, ndecoded);

	/* Check the result */
	if (!ItemPointerEquals(&orig_itemptrs[0], &decoded_itemptrs[0]))
		elog(ERROR, "mismatch on first itemptr: (%u, %d) vs (%u, %d)",
			0, 1,
			ItemPointerGetBlockNumber(&decoded_itemptrs[0]),
			ItemPointerGetOffsetNumber(&decoded_itemptrs[0]));

	if (ndecoded == 2 &&
		!ItemPointerEquals(&orig_itemptrs[0], &decoded_itemptrs[0]))
	{
		elog(ERROR, "mismatch on second itemptr: (%u, %d) vs (%u, %d)",
			0, 1,
			ItemPointerGetBlockNumber(&decoded_itemptrs[0]),
			ItemPointerGetOffsetNumber(&decoded_itemptrs[0]));
	}
}

/*
 * SQL-callable entry point to perform all tests.
 */
Datum
test_ginpostinglist()
{
	test_itemptr_pair(0, 2, 14);
	test_itemptr_pair(0, MaxHeapTuplesPerPage, 14);
	test_itemptr_pair(MaxBlockNumber, MaxHeapTuplesPerPage, 14);
	test_itemptr_pair(MaxBlockNumber, MaxHeapTuplesPerPage, 16);

	PG_RETURN_VOID();
}

void
tbm_add_tuples(TIDBitmap* tbm, const ItemPointer tids, int ntids,
	bool recheck)
{

}

bool
ItemPointerEquals(ItemPointer pointer1, ItemPointer pointer2)
{
	if (ItemPointerGetBlockNumber(pointer1) ==
		ItemPointerGetBlockNumber(pointer2) &&
		ItemPointerGetOffsetNumber(pointer1) ==
		ItemPointerGetOffsetNumber(pointer2))
		return true;
	else
		return false;
}

bool errstart(int elevel, const char* domain)
{
	return true;
}
void errfinish(const char* filename, int lineno, const char* funcname)
{

}
extern int	errmsg_internal(const char* fmt, ...)
{
	return 0;
}

static void pgtest_printf(char* str) {
	std::cout << std::string(str);
}

static inline void*
pg_malloc_internal(size_t size, int flags)
{
	void* tmp;

	/* Avoid unportable behavior of malloc(0) */
	if (size == 0)
		size = 1;
	tmp = malloc(size);
	if (tmp == NULL)
	{
		if ((flags & MCXT_ALLOC_NO_OOM) == 0)
		{
			pgtest_printf("out of memory\n");
			exit(EXIT_FAILURE);
		}
		return NULL;
	}

	if ((flags & MCXT_ALLOC_ZERO) != 0)
		MemSet(tmp, 0, size);
	return tmp;
}

void*
pg_malloc(size_t size)
{
	return pg_malloc_internal(size, 0);
}

void*
pg_malloc0(size_t size)
{
	return pg_malloc_internal(size, MCXT_ALLOC_ZERO);
}

void*
pg_malloc_extended(size_t size, int flags)
{
	return pg_malloc_internal(size, flags);
}

void*
pg_realloc(void* ptr, size_t size)
{
	void* tmp;

	/* Avoid unportable behavior of realloc(NULL, 0) */
	if (ptr == NULL && size == 0)
		size = 1;
	tmp = realloc(ptr, size);
	if (!tmp)
	{
		pgtest_printf("out of memory\n");
		exit(EXIT_FAILURE);
	}
	return tmp;
}

/*
 * "Safe" wrapper around strdup().
 */
char*
pg_strdup(const char* in)
{
	char* tmp;

	if (!in)
	{
		pgtest_printf("cannot duplicate null pointer (internal error)\n");
		exit(EXIT_FAILURE);
	}
	tmp = _strdup(in);
	if (!tmp)
	{
		pgtest_printf("out of memory\n");
		exit(EXIT_FAILURE);
	}
	return tmp;
}

void
pg_free(void* ptr)
{
	if (ptr != NULL)
		free(ptr);
}

/*
 * Frontend emulation of backend memory management functions.  Useful for
 * programs that compile backend files.
 */
void*
palloc(Size size)
{
	return pg_malloc_internal(size, 0);
}

void*
palloc0(Size size)
{
	return pg_malloc_internal(size, MCXT_ALLOC_ZERO);
}

void*
palloc_extended(Size size, int flags)
{
	return pg_malloc_internal(size, flags);
}

void
pfree(void* pointer)
{
	pg_free(pointer);
}

char*
pstrdup(const char* in)
{
	return pg_strdup(in);
}

char*
pnstrdup(const char* in, Size size)
{
	char* tmp;
	int			len;

	if (!in)
	{
		pgtest_printf("cannot duplicate null pointer (internal error)\n");
		exit(EXIT_FAILURE);
	}

	len = strnlen(in, size);
	tmp = (char*) malloc(len + 1);
	if (tmp == NULL)
	{
		pgtest_printf("out of memory\n");
		exit(EXIT_FAILURE);
	}

	memcpy(tmp, in, len);
	tmp[len] = '\0';

	return tmp;
}

void*
repalloc(void* pointer, Size size)
{
	return pg_realloc(pointer, size);
}
//------------------------------------------------------------------------------------

/*
 * Our test trees store an integer key, and nothing else.
 */
typedef struct IntRBTreeNode
{
	RBTNode		rbtnode;
	int			key;
} IntRBTreeNode;


/*
 * Node comparator.  We don't worry about overflow in the subtraction,
 * since none of our test keys are negative.
 */
static int
irbt_cmp(const RBTNode* a, const RBTNode* b, void* arg)
{
	const IntRBTreeNode* ea = (const IntRBTreeNode*)a;
	const IntRBTreeNode* eb = (const IntRBTreeNode*)b;

	return ea->key - eb->key;
}

/*
 * Node combiner.  For testing purposes, just check that library doesn't
 * try to combine unequal keys.
 */
static void
irbt_combine(RBTNode* existing, const RBTNode* newdata, void* arg)
{
	const IntRBTreeNode* eexist = (const IntRBTreeNode*)existing;
	const IntRBTreeNode* enew = (const IntRBTreeNode*)newdata;

	if (eexist->key != enew->key)
		elog(ERROR, "red-black tree combines %d into %d",
			enew->key, eexist->key);
}

/* Node allocator */
static RBTNode*
irbt_alloc(void* arg)
{
	return (RBTNode*)palloc(sizeof(IntRBTreeNode));
}

/* Node freer */
static void
irbt_free(RBTNode* node, void* arg)
{
	pfree(node);
}

/*
 * Create a red-black tree using our support functions
 */
static RBTree*
create_int_rbtree(void)
{
	return rbt_create(sizeof(IntRBTreeNode),
		irbt_cmp,
		irbt_combine,
		irbt_alloc,
		irbt_free,
		NULL);
}

/*
 * Generate a random permutation of the integers 0..size-1
 */
static int* GetPermutation(int size)
{
	int* permutation;
	int			i;

	permutation = (int*)palloc(size * sizeof(int));

	permutation[0] = 0;

	/*
	 * This is the "inside-out" variant of the Fisher-Yates shuffle algorithm.
	 * Notionally, we append each new value to the array and then swap it with
	 * a randomly-chosen array element (possibly including itself, else we
	 * fail to generate permutations with the last integer last).  The swap
	 * step can be optimized by combining it with the insertion.
	 */
	for (i = 1; i < size; i++)
	{
		int			j = rand() % (i + 1);

		if (j < i)				/* avoid fetching undefined data if j=i */
			permutation[i] = permutation[j];
		permutation[j] = i;
	}

	return permutation;
}

/*
 * Populate an empty RBTree with "size" integers having the values
 * 0, step, 2*step, 3*step, ..., inserting them in random order
 */
static void
rbt_populate(RBTree* tree, int size, int step)
{
	int* permutation = GetPermutation(size);
	IntRBTreeNode node;
	bool		isNew;
	int			i;

	/* Insert values.  We don't expect any collisions. */
	for (i = 0; i < size; i++)
	{
		node.key = step * permutation[i];
		rbt_insert(tree, (RBTNode*)&node, &isNew);
		if (!isNew)
			elog(ERROR, "unexpected !isNew result from rbt_insert");
	}

	/*
	 * Re-insert the first value to make sure collisions work right.  It's
	 * probably not useful to test that case over again for all the values.
	 */
	if (size > 0)
	{
		node.key = step * permutation[0];
		rbt_insert(tree, (RBTNode*)&node, &isNew);
		if (isNew)
			elog(ERROR, "unexpected isNew result from rbt_insert");
	}

	pfree(permutation);
}

/*
 * Check the correctness of left-right traversal.
 * Left-right traversal is correct if all elements are
 * visited in increasing order.
 */
static void
testleftright(int size)
{
	RBTree* tree = create_int_rbtree();
	IntRBTreeNode* node;
	RBTreeIterator iter;
	int			lastKey = -1;
	int			count = 0;

	/* check iteration over empty tree */
	rbt_begin_iterate(tree, LeftRightWalk, &iter);
	if (rbt_iterate(&iter) != NULL)
		elog(ERROR, "left-right walk over empty tree produced an element");

	/* fill tree with consecutive natural numbers */
	rbt_populate(tree, size, 1);

	/* iterate over the tree */
	rbt_begin_iterate(tree, LeftRightWalk, &iter);

	while ((node = (IntRBTreeNode*)rbt_iterate(&iter)) != NULL)
	{
		/* check that order is increasing */
		if (node->key <= lastKey)
			elog(ERROR, "left-right walk gives elements not in sorted order");
		lastKey = node->key;
		count++;
	}

	if (lastKey != size - 1)
		elog(ERROR, "left-right walk did not reach end");
	if (count != size)
		elog(ERROR, "left-right walk missed some elements");
}

/*
 * Check the correctness of right-left traversal.
 * Right-left traversal is correct if all elements are
 * visited in decreasing order.
 */
static void
testrightleft(int size)
{
	RBTree* tree = create_int_rbtree();
	IntRBTreeNode* node;
	RBTreeIterator iter;
	int			lastKey = size;
	int			count = 0;

	/* check iteration over empty tree */
	rbt_begin_iterate(tree, RightLeftWalk, &iter);
	if (rbt_iterate(&iter) != NULL)
		elog(ERROR, "right-left walk over empty tree produced an element");

	/* fill tree with consecutive natural numbers */
	rbt_populate(tree, size, 1);

	/* iterate over the tree */
	rbt_begin_iterate(tree, RightLeftWalk, &iter);

	while ((node = (IntRBTreeNode*)rbt_iterate(&iter)) != NULL)
	{
		/* check that order is decreasing */
		if (node->key >= lastKey)
			elog(ERROR, "right-left walk gives elements not in sorted order");
		lastKey = node->key;
		count++;
	}

	if (lastKey != 0)
		elog(ERROR, "right-left walk did not reach end");
	if (count != size)
		elog(ERROR, "right-left walk missed some elements");
}

/*
 * Check the correctness of the rbt_find operation by searching for
 * both elements we inserted and elements we didn't.
 */
static void
testfind(int size)
{
	RBTree* tree = create_int_rbtree();
	int			i;

	/* Insert even integers from 0 to 2 * (size-1) */
	rbt_populate(tree, size, 2);

	/* Check that all inserted elements can be found */
	for (i = 0; i < size; i++)
	{
		IntRBTreeNode node;
		IntRBTreeNode* resultNode;

		node.key = 2 * i;
		resultNode = (IntRBTreeNode*)rbt_find(tree, (RBTNode*)&node);
		if (resultNode == NULL)
			elog(ERROR, "inserted element was not found");
		if (node.key != resultNode->key)
			elog(ERROR, "find operation in rbtree gave wrong result");
	}

	/*
	 * Check that not-inserted elements can not be found, being sure to try
	 * values before the first and after the last element.
	 */
	for (i = -1; i <= 2 * size; i += 2)
	{
		IntRBTreeNode node;
		IntRBTreeNode* resultNode;

		node.key = i;
		resultNode = (IntRBTreeNode*)rbt_find(tree, (RBTNode*)&node);
		if (resultNode != NULL)
			elog(ERROR, "not-inserted element was found");
	}
}

/*
 * Check the correctness of the rbt_leftmost operation.
 * This operation should always return the smallest element of the tree.
 */
static void
testleftmost(int size)
{
	RBTree* tree = create_int_rbtree();
	IntRBTreeNode* result;

	/* Check that empty tree has no leftmost element */
	if (rbt_leftmost(tree) != NULL)
		elog(ERROR, "leftmost node of empty tree is not NULL");

	/* fill tree with consecutive natural numbers */
	rbt_populate(tree, size, 1);

	/* Check that leftmost element is the smallest one */
	result = (IntRBTreeNode*)rbt_leftmost(tree);
	if (result == NULL || result->key != 0)
		elog(ERROR, "rbt_leftmost gave wrong result");
}

/*
 * Check the correctness of the rbt_delete operation.
 */
static void
testdelete(int size, int delsize)
{
	RBTree* tree = create_int_rbtree();
	int* deleteIds;
	bool* chosen;
	int			i;

	/* fill tree with consecutive natural numbers */
	rbt_populate(tree, size, 1);

	/* Choose unique ids to delete */
	deleteIds = (int*)palloc(delsize * sizeof(int));
	chosen = (bool*)palloc0(size * sizeof(bool));

	for (i = 0; i < delsize; i++)
	{
		int			k = rand() % size;

		while (chosen[k])
			k = (k + 1) % size;
		deleteIds[i] = k;
		chosen[k] = true;
	}

	/* Delete elements */
	for (i = 0; i < delsize; i++)
	{
		IntRBTreeNode find;
		IntRBTreeNode* node;

		find.key = deleteIds[i];
		/* Locate the node to be deleted */
		node = (IntRBTreeNode*)rbt_find(tree, (RBTNode*)&find);
		if (node == NULL || node->key != deleteIds[i])
			elog(ERROR, "expected element was not found during deleting");
		/* Delete it */
		rbt_delete(tree, (RBTNode*)node);
	}

	/* Check that deleted elements are deleted */
	for (i = 0; i < size; i++)
	{
		IntRBTreeNode node;
		IntRBTreeNode* result;

		node.key = i;
		result = (IntRBTreeNode*)rbt_find(tree, (RBTNode*)&node);
		if (chosen[i])
		{
			/* Deleted element should be absent */
			if (result != NULL)
				elog(ERROR, "deleted element still present in the rbtree");
		}
		else
		{
			/* Else it should be present */
			if (result == NULL || result->key != i)
				elog(ERROR, "delete operation removed wrong rbtree value");
		}
	}

	/* Delete remaining elements, so as to exercise reducing tree to empty */
	for (i = 0; i < size; i++)
	{
		IntRBTreeNode find;
		IntRBTreeNode* node;

		if (chosen[i])
			continue;
		find.key = i;
		/* Locate the node to be deleted */
		node = (IntRBTreeNode*)rbt_find(tree, (RBTNode*)&find);
		if (node == NULL || node->key != i)
			elog(ERROR, "expected element was not found during deleting");
		/* Delete it */
		rbt_delete(tree, (RBTNode*)node);
	}

	/* Tree should now be empty */
	if (rbt_leftmost(tree) != NULL)
		elog(ERROR, "deleting all elements failed");

	pfree(deleteIds);
	pfree(chosen);
}

/*
 * SQL-callable entry point to perform all tests
 *
 * Argument is the number of entries to put in the trees
 */
PG_FUNCTION_INFO_V1(test_rb_tree);

Datum
test_rb_tree()
{
	int			size =100;

	if (size <= 0)
		elog(ERROR, "invalid size for test_rb_tree: %d", size);
	testleftright(size);
	testrightleft(size);
	testfind(size);
	testleftmost(size);
	testdelete(size, Max(size / 10, 1));
	PG_RETURN_VOID();
}

//------------------------------------------------------------
extern "C" {
	void XLogBeginInsert(void)
	{
	}
	void XLogSetRecordFlags(uint8 flags)
	{
	}
	XLogRecPtr XLogInsert(RmgrId rmid, uint8 info)
	{
		return NULL;
	}
	void XLogEnsureRecordSpace(int max_block_id, int ndatas)
	{
	}

	void XLogRegisterData(char* data, int len)
	{
	}

	void XLogRegisterBuffer(uint8 block_id, Buffer buffer, uint8 flags)
	{
	}

	void XLogRegisterBlock(uint8 block_id, RelFileNode* rnode,
		ForkNumber forknum, BlockNumber blknum, char* page,
		uint8 flags)
	{
	}

	void XLogRegisterBufData(uint8 block_id, char* data, int len)
	{
	}

	void XLogResetInsertion(void)
	{
	}

	bool XLogCheckBufferNeedsBackup(Buffer buffer)
	{
		return false;
	}

	XLogRecPtr log_newpage(RelFileNode* rnode, ForkNumber forkNum,
		BlockNumber blk, char* page, bool page_std)
	{
		return NULL;
	}

	void log_newpages(RelFileNode* rnode, ForkNumber forkNum, int num_pages,
		BlockNumber* blknos, char** pages, bool page_std)
	{

	}
	XLogRecPtr log_newpage_buffer(Buffer buffer, bool page_std)
	{
		return NULL;
	}
	void log_newpage_range(Relation rel, ForkNumber forkNum,
		BlockNumber startblk, BlockNumber endblk, bool page_std)
	{

	}
	XLogRecPtr XLogSaveBufferForHint(Buffer buffer, bool buffer_std)
	{
		return NULL;
	}

	void InitXLogInsert(void)
	{
	}
}
//------------------------------------------------------------------------------------

int main()
{
	ginbuild(NULL, NULL, NULL);
    fnlibpggin();
	test_ginpostinglist();
	test_rb_tree();
}
