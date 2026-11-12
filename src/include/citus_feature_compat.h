/*-------------------------------------------------------------------------
 *
 * citus_feature_compat.h
 *	  Compatibility helpers for downstream PostgreSQL API drift
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_FEATURE_COMPAT_H
#define CITUS_FEATURE_COMPAT_H

#include "postgres.h"

#include "access/htup_details.h"
#include "nodes/nodeFuncs.h"

#ifdef PACKAGE_BUGREPORT
#undef PACKAGE_BUGREPORT
#endif
#ifdef PACKAGE_NAME
#undef PACKAGE_NAME
#endif
#ifdef PACKAGE_STRING
#undef PACKAGE_STRING
#endif
#ifdef PACKAGE_TARNAME
#undef PACKAGE_TARNAME
#endif
#ifdef PACKAGE_URL
#undef PACKAGE_URL
#endif
#ifdef PACKAGE_VERSION
#undef PACKAGE_VERSION
#endif

#include "citus_config.h"


#ifdef HAVE_EXPRESSION_TREE_MUTATOR_TRAVERSAL_FLAGS
#define expression_tree_mutator_compat(node, mutator, context) \
	expression_tree_mutator((node), (mutator), (context), 0)
#else
#define expression_tree_mutator_compat(node, mutator, context) \
	expression_tree_mutator((node), (mutator), (context))
#endif


#ifdef HAVE_PARSE_ANALYZE_VARPARAMS_PARAM_NAMES
#define parse_analyze_varparams_compat(parseTree, sourceText, paramTypes, \
									   numParams, queryEnv) \
	parse_analyze_varparams((parseTree), (sourceText), (paramTypes), \
							(numParams), (queryEnv), NULL)
#else
#define parse_analyze_varparams_compat(parseTree, sourceText, paramTypes, \
									   numParams, queryEnv) \
	parse_analyze_varparams((parseTree), (sourceText), (paramTypes), \
							(numParams), (queryEnv))
#endif


#ifdef HAVE_LOCAL_BACKEND_STATUS_POINTER
#define LocalBackendStatusGetEntry(localEntry) ((localEntry)->backendStatus)
#else
#define LocalBackendStatusGetEntry(localEntry) (&(localEntry)->backendStatus)
#endif


/* compressor 0 opens the file without compression, as PathNameOpenFile does */
#ifdef HAVE_PATH_NAME_OPEN_FILE_PERM_COMPRESSOR
#define PathNameOpenFilePermCompat(fileName, fileFlags, fileMode) \
	PathNameOpenFilePerm((fileName), (fileFlags), (fileMode), 0)
#else
#define PathNameOpenFilePermCompat(fileName, fileFlags, fileMode) \
	PathNameOpenFilePerm((fileName), (fileFlags), (fileMode))
#endif


/* RelationTruncate without drop_buffers always drops the local buffers */
#ifdef HAVE_RELATION_TRUNCATE_DROP_BUFFERS
#define RelationTruncateCompat(rel, nblocks, dropBuffers) \
	RelationTruncate((rel), (nblocks), (dropBuffers))
#else
#define RelationTruncateCompat(rel, nblocks, dropBuffers) \
	((void) (dropBuffers), RelationTruncate((rel), (nblocks)))
#endif


static inline TransactionId
HeapTupleGetXminCompat(HeapTuple tuple)
{
#ifdef HAVE_HEAPTUPLE_GETXMIN
	return HeapTupleGetXmin(tuple);
#else
	return HeapTupleHeaderGetXmin(tuple->t_data);
#endif
}


static inline void
HeapTupleSetXminCompat(HeapTuple tuple, TransactionId xid)
{
#ifdef HAVE_HEAPTUPLE_GETXMIN
	HeapTupleSetXmin(tuple, xid);
#else
	HeapTupleHeaderSetXmin(tuple->t_data, xid);
#endif
}


#endif /* CITUS_FEATURE_COMPAT_H */
