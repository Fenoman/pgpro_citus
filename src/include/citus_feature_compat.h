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


static inline TransactionId
HeapTupleGetXminCompat(HeapTuple tuple)
{
#ifdef HAVE_HEAPTUPLE_GETXMIN
	return HeapTupleGetXmin(tuple);
#else
	return HeapTupleHeaderGetXmin(tuple->t_data);
#endif
}


#endif /* CITUS_FEATURE_COMPAT_H */
