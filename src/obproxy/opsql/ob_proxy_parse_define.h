/**
 * Copyright (c) 2021 OceanBase
 * OceanBase Database Proxy(ODP) is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBPROXY_PARSER_PARSE_DEFINE_H
#define OBPROXY_PARSER_PARSE_DEFINE_H

#include <stdint.h>
static const int OB_SUCCESS = 0;
static const int OB_INVALID_ARGUMENT = -4002;
static const int OB_PARSER_ERR_PARSE_SQL = -5001;

#ifndef UNUSED
#define UNUSED(v) ((void)(v))
#endif

#ifndef OB_LIKELY
#define OB_LIKELY(x)       __builtin_expect(!!(x),1)
#endif

#ifndef OB_UNLIKELY
#define OB_UNLIKELY(x)     __builtin_expect(!!(x),0)
#endif

#ifndef OB_SUCC
#define OB_SUCC(statement) (OB_LIKELY(OB_SUCCESS == (ret = (statement))))
#endif

#ifndef OB_FAIL
#define OB_FAIL(statement) (OB_UNLIKELY(OB_SUCCESS != (ret = (statement))))
#endif

#ifndef OB_ISNULL
#ifdef OB_PERF_MODE
#define OB_ISNULL(statement) ({UNUSED(statement); false; })
#else
#define OB_ISNULL(statement) (OB_UNLIKELY(NULL == (statement)))
#endif /* OB_PERF_MODE */
#endif /* OB_ISNULL */

#ifndef OB_NOTNULL
#ifdef OB_PERF_MODE
#define OB_NOTNULL(statement) ({UNUSED(statement); true; })
#else
#define OB_NOTNULL(statement) (OB_LIKELY(NULL != (statement)))
#endif /* OB_PERF_MODE */
#endif /* OB_ISNULL */

#ifdef NDEBUG
#define YYDEBUG 0
#else
#define YYDEBUG 1
#endif

#endif /* end of OBPROXY_PARSER_PARSE_DEFINE_H */
