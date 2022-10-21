/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifdef FLT_DEF_SPAN

#ifdef __HIGH_LEVEL_SPAN
FLT_DEF_SPAN(ob_proxy, "obproxy")
FLT_DEF_SPAN(ob_proxy_server_process_req, "server process req")
FLT_DEF_SPAN(ob_proxy_partition_location_lookup, "proxy partition location lookup")
#endif // __HIGH_LEVEL_SPAN

#ifdef __MIDDLE_LEVEL_SPAN
FLT_DEF_SPAN(ob_proxy_server_response_read, "read resp from server")
FLT_DEF_SPAN(ob_proxy_cluster_resource_create, "proxy create cluster resource")
#endif // __MIDDLE_LEVEL_SPAN

#ifdef __LOW_LEVEL_SPAN
FLT_DEF_SPAN(ob_proxy_do_observer_open, "proxy connect with server")
FLT_DEF_SPAN(ob_proxy_client_response_write, "proxy write resp to client")
FLT_DEF_SPAN(ob_proxy_server_request_write, "proxy write req to server")
#endif // __LOW_LEVEL_SPAN

#endif // DEF_SPAN


#ifdef FLT_DEF_TAG

#ifdef __HIGH_LEVEL_TAG
FLT_DEF_TAG(query_start_ts, "server query start ts")
FLT_DEF_TAG(query_end_ts, "server query end ts")
FLT_DEF_TAG(client_host, "client host addr")

// debug
FLT_DEF_TAG(span_back_trace, "full link tracing debug")
#endif // __HIGH_LEVEL_TAG

#ifdef __MIDDLE_LEVEL_TAG
#endif // __MIDDLE_LEVEL_TAG

#ifdef __LOW_LEVEL_TAG
#endif // __LOW_LEVEL_TAG

#endif // DEF_TAG


#ifndef _OB_TRACE_DEF_H
#define _OB_TRACE_DEF_H

#include <cstdint>
#include "lib/utility/ob_macro_utils.h"

enum ObTagType
{
#define FLT_DEF_TAG(name, comment) name,
#define __HIGH_LEVEL_TAG
#define __MIDDLE_LEVEL_TAG
#define __LOW_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_TAG
#undef __MIDDLE_LEVEL_TAG
#undef __HIGH_LEVEL_TAG
#undef FLT_DEF_TAG
};

namespace oceanbase
{
namespace trace
{
#define GET_SPANLEVEL(ID) (::oceanbase::trace::__SpanIdMapper<ID>::level)
#define GET_TAGLEVEL(ID) (::oceanbase::trace::__tag_level_mapper[ID])

enum ObSpanType
{
#define __HIGH_LEVEL_SPAN
#define __MIDDLE_LEVEL_SPAN
#define __LOW_LEVEL_SPAN
#define FLT_DEF_SPAN(name, comment) name,
#include "lib/trace/ob_trace_def.h"
#undef FLT_DEF_SPAN
#undef __LOW_LEVEL_SPAN
#undef __MIDDLE_LEVEL_SPAN
#undef __HIGH_LEVEL_SPAN
};

static const uint8_t __tag_level_mapper[] = {
#define FLT_DEF_TAG(ID, comment) 1,
#define __HIGH_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __HIGH_LEVEL_TAG
#undef FLT_DEF_TAG

#define FLT_DEF_TAG(ID, comment) 2,
#define __MIDDLE_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __MIDDLE_LEVEL_TAG
#undef FLT_DEF_TAG

#define FLT_DEF_TAG(ID, comment) 3,
#define __LOW_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_TAG
#undef FLT_DEF_TAG
};

static const char* __tag_name_mapper[] = {
#define FLT_DEF_TAG(name, comment) #name,
#define __HIGH_LEVEL_TAG
#define __MIDDLE_LEVEL_TAG
#define __LOW_LEVEL_TAG
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_TAG
#undef __MIDDLE_LEVEL_TAG
#undef __HIGH_LEVEL_TAG
#undef FLT_DEF_TAG
};


template <ObSpanType ID>
class __SpanIdMapper {};

#define FLT_DEF_SPAN(ID, comment)     \
template <>                           \
class __SpanIdMapper<ObSpanType::ID>  \
{                                     \
public:                               \
  static constexpr uint8_t level = 1; \
};
#define __HIGH_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __HIGH_LEVEL_SPAN
#undef FLT_DEF_SPAN

#define FLT_DEF_SPAN(ID, comment)     \
template <>                           \
class __SpanIdMapper<ObSpanType::ID>  \
{                                     \
public:                               \
  static constexpr uint8_t level = 2; \
};
#define __MIDDLE_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __MIDDLE_LEVEL_SPAN
#undef FLT_DEF_SPAN

#define FLT_DEF_SPAN(ID, comment)     \
template <>                           \
class __SpanIdMapper<ObSpanType::ID>  \
{                                     \
public:                               \
  static constexpr uint8_t level = 3; \
};
#define __LOW_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_SPAN
#undef FLT_DEF_SPAN

} // trace
} // oceanbase

#endif /* _OB_TRACE_DEF_H */
