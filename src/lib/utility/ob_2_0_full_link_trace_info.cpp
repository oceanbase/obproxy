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
#define USING_LOG_PREFIX PROXY

#include "lib/utility/ob_2_0_full_link_trace_info.h"
#include "lib/oblog/ob_log.h"
#include "obproxy/ob_proxy_main.h"


namespace oceanbase
{

namespace common
{

// pos=0, len->sub_len, buf->sub_buf
int FLTExtraInfo::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t buf_end = pos + len;

  while (OB_SUCC(ret) && pos < len) {  
    int32_t v_len = 0;
    int16_t id = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::resolve_type_and_len(buf, buf_end, pos, id, v_len))) {
      LOG_WARN("fail to resolve type and len", K(ret));
    } else if (OB_FAIL(deserialize_field(static_cast<FullLinkTraceExtraInfoId>(id), v_len, buf, buf_end, pos))) {
      LOG_WARN("fail to deserialize field", K(ret), K(id));
    } else {
      //nothing
    }
  }

  return ret;
}

int FLTControlInfo::serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  int ret = OB_SUCCESS;

  int64_t orig_pos = pos;
  if (pos + FLT_TYPE_AND_LEN > len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size overflow", K(len), K(pos));
  } else {
    pos += FLT_TYPE_AND_LEN;

    if (OB_FAIL(ret)) {
      // nothing
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int1(buf, len, pos, level_, FLT_LEVEL))) {
      LOG_WARN("fail to store int1", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_double(buf, len, pos, sample_percentage_,
                                                                FLT_SAMPLE_PERCENTAGE))) {
      LOG_WARN("fail to store sample pct", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int1(buf, len, pos, record_policy_, FLT_RECORD_POLICY))) {
      LOG_WARN("fail to store int1", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_double(buf, len, pos, print_sample_percentage_,
                                                                FLT_PRINT_SAMPLE_PCT))) {
      LOG_WARN("fail to store print sample pct", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int8(buf, len, pos, slow_query_threshold_,
                                                              FLT_SLOW_QUERY_THRES))) {
      LOG_WARN("fail to store slow query threshold", K(ret));
    } else if (ctx.flt_ext_enable_) {
      if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int1(buf, len, pos, show_trace_enable_, FLT_SHOW_TRACE_ENABLE))) {
        LOG_WARN("fail to serialize show trace", K(ret));
      }
    } else {
      // nothing
    }

    if (OB_SUCC(ret)) {
      int32_t total_len = static_cast<int32_t>(pos - orig_pos - FLT_TYPE_AND_LEN);
      if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(buf, len, orig_pos, type_, total_len))) {
        LOG_WARN("fail to store type and len", K(ret));
      } else {
        LOG_DEBUG("succ to serialize control info", K(pos), K(len), K(total_len), KPC(this));
      }
    }
  }

  return ret;
}

int FLTControlInfo::deserialize_field(FullLinkTraceExtraInfoId id,
                                      const int64_t v_len,
                                      const char *buf,
                                      const int64_t len,
                                      int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (id == FLT_LEVEL) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int1(buf, len, pos, v_len, level_))) {
      LOG_WARN("fail to resolve level", K(ret));
    }
  } else if (id == FLT_SAMPLE_PERCENTAGE) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_double(buf, len, pos, v_len, sample_percentage_))) {
      LOG_WARN("fail to resolve sample percentage", K(ret));
    }
  } else if (id == FLT_RECORD_POLICY) {
    int8_t rc = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int1(buf, len, pos, v_len, rc))) {
      LOG_WARN("fail to resolve record policy", K(ret));
    } else {
      record_policy_ = static_cast<FullLinkTraceRecordPolicy>(rc);
    }
  } else if (id == FLT_PRINT_SAMPLE_PCT) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_double(buf, len, pos, v_len, print_sample_percentage_))) {
      LOG_WARN("fail to resolve print sample percentage", K(ret));
    }
  } else if (id == FLT_SLOW_QUERY_THRES) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int8(buf, len, pos, v_len, slow_query_threshold_))) {
      LOG_WARN("fail to resolve slow query threshold", K(ret));
    }
  } else if (id == FLT_SHOW_TRACE_ENABLE) {
    int8_t v = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int1(buf, len, pos, v_len, v))) {
      LOG_WARN("fail to resolve trace enable", K(ret));
    } else {
      show_trace_enable_ = static_cast<bool>(v);
    }    
  } else {
    pos += v_len;
    LOG_DEBUG("unexpected control info id, ignore", K(id), K(v_len));
  }

  if (OB_SUCC(ret)) {
    set_need_send(true);
  }

  return ret;
}

// total: 62
// with show trace, 69
int64_t FLTControlInfo::get_serialize_size(FLTCtx &ctx)
{
  UNUSED(ctx);
  return FLT_TYPE_AND_LEN
         + FLT_TYPE_AND_LEN + sizeof(level_)
         + FLT_TYPE_AND_LEN + sizeof(sample_percentage_)
         + FLT_TYPE_AND_LEN + sizeof(int8_t)
         + FLT_TYPE_AND_LEN + sizeof(print_sample_percentage_)
         + FLT_TYPE_AND_LEN + sizeof(slow_query_threshold_)
         + FLT_TYPE_AND_LEN + sizeof(show_trace_enable_);
}

void FLTSpanInfo::reset() {
  trace_enable_ = false;
  force_print_ = false;
  trace_id_.reset();
  span_id_.reset();
}

int FLTSpanInfo::serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  int64_t orig_pos = pos;
  if (pos + FLT_TYPE_AND_LEN > len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size overflow", K(len), K(pos));
  } else {
    pos += FLT_TYPE_AND_LEN;

    if (OB_FAIL(ret)) {
      //nothing
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int1(buf, len, pos, trace_enable_, FLT_TRACE_ENABLE))) {
      LOG_WARN("fail to store int1", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int1(buf, len, pos, force_print_, FLT_FORCE_PRINT))) {
      LOG_WARN("fail to store int1", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int1(buf, len, pos, ref_type_, FLT_REF_TYPE))) {
      LOG_WARN("fail to store int1", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_uuid(buf, len, pos, trace_id_, FLT_TRACE_ID))) {
      LOG_WARN("fail to store trace id", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_uuid(buf, len, pos, span_id_, FLT_SPAN_ID))) {
      LOG_WARN("fail to store span id", K(ret));
    } else {
      // fill type and len in the head
      int32_t total_len = static_cast<int32_t>(pos - orig_pos - FLT_TYPE_AND_LEN);
      if (OB_SUCC(ret)
          && OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(buf, len, orig_pos, type_, total_len))) {
        LOG_WARN("fail to store type and len in head", K(ret));
      } else {
        LOG_DEBUG("succ to seri span info", K(pos), K(len), K(type_), K(total_len));
      }
    }
  }
  
  return ret;
}

int FLTSpanInfo::serialize_as_json_format(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  int ret = OB_SUCCESS;
  
  int64_t orig_pos = pos;
  if (pos + FLT_TYPE_AND_LEN > len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size overflow", K(len), K(pos));
  } else {
    LOG_DEBUG("before serialize as json format", K(pos), K(len));
    pos += FLT_TYPE_AND_LEN;

    // fill as json format 
    // ref to TRACE_PATTERN / UUID_PATTERN
    trace::UUID root_span;
    int64_t start_ts = ctx.span_start_ts_;
    int64_t end_ts = ctx.span_end_ts_;
    pos += snprintf(buf + pos, len - pos, "[" TRACE_PATTERN "}]",
                    UUID_TOSTRING(trace_id_),
                    "obclient",
                    UUID_TOSTRING(span_id_),
                    start_ts, end_ts,
                    UUID_TOSTRING(root_span),
                    "false");
    if (pos > len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos and len", K(ret), K(pos), K(len));
    } else {
      int32_t total_len = static_cast<int32_t>(pos - orig_pos - FLT_TYPE_AND_LEN);
      if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(buf, len, orig_pos,
                                                                 FLT_DRV_SHOW_TRACE_SPAN, total_len))) {
        LOG_WARN("fail to store type and len in head", K(ret));
      } else {
        LOG_DEBUG("succ to seri span info as json format", K(pos), K(len), K(total_len));
      }
    }
  }

  return ret;
}

int FLTSpanInfo::deserialize_field(FullLinkTraceExtraInfoId id,
                                   const int64_t v_len,
                                   const char *buf,
                                   const int64_t len,
                                   int64_t &pos)
{
  int ret = OB_SUCCESS;
  
  if (id == FLT_TRACE_ENABLE) {
    int8_t v = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int1(buf, len, pos, v_len, v))) {
      LOG_WARN("fail to resolve trace enable", K(ret));
    } else {
      trace_enable_ = static_cast<bool>(v);
    }
  } else if (id == FLT_FORCE_PRINT) {
    int8_t v = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int1(buf, len, pos, v_len, v))) {
      LOG_WARN("fail to resolve force print", K(ret));
    } else {
      force_print_ = static_cast<bool>(v);
    }
  } else if (id == FLT_TRACE_ID) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_uuid(buf, len, pos, v_len, trace_id_))) {
      LOG_WARN("fail to resolve trace id", K(ret));
    }
  } else if (id == FLT_REF_TYPE) {
    int8_t v = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int1(buf, len, pos, v_len, v))) {
      LOG_WARN("fail to get ref type", K(ret));
    } else {
      ref_type_ = static_cast<FLTSpanRefType>(v);
    }
  } else if (id == FLT_SPAN_ID) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_uuid(buf, len, pos, v_len, span_id_))) {
      LOG_WARN("fail to get span id", K(ret));
    }
  } else {
    pos += v_len;
    LOG_DEBUG("unexpected span info id, ignore", K(id), K(v_len));
  }

  return ret;
}

// total: 71
int64_t FLTSpanInfo::get_serialize_size(FLTCtx &ctx)
{
  UNUSED(ctx);
  return FLT_TYPE_AND_LEN
         + FLT_TYPE_AND_LEN + sizeof(trace_enable_)
         + FLT_TYPE_AND_LEN + sizeof(force_print_)
         + FLT_TYPE_AND_LEN + trace_id_.get_serialize_size()
         + FLT_TYPE_AND_LEN + sizeof(int8_t)
         + FLT_TYPE_AND_LEN + trace_id_.get_serialize_size();   // single span_id UUID
}

// show trace json size
// ref to TRACE_PATTERN / UUID_PATTERN
// sum: 246 -> 250
int64_t FLTSpanInfo::get_show_trace_serialize_size()
{
  return 2                // []
         + 2              //  { }
         + 13 + 36        // "trace_id":"uuid"
         + 1 + 9 + 8      // ,"name":"obclient"
         + 1 + 7 + 36     // ,"id":"uuid"
         + 1 + 11 + 20    // ,"start_ts":%ld   int64t_max -> 20
         + 1 + 9 + 20     // ,"end_ts":%ld
         + 1 + 14 + 36    // ,"parent_id":"uuid"
         + 1 + 12 + 5     // ,"is_follow":false/true
         + 4;             // pad to 250
}

int FLTQueryInfo::serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  int64_t orig_pos = pos;
  if (pos + FLT_TYPE_AND_LEN > len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf size overflow", K(len), K(pos));
  } else {
    pos += FLT_TYPE_AND_LEN;

    if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int8(buf, len, pos, query_start_ts_, FLT_QUERY_START_TIMESTAMP))) {
      LOG_WARN("fail to store int8", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int8(buf, len, pos, query_end_ts_, FLT_QUERY_END_TIMESTAMP))) {
      LOG_WARN("fail to store int8", K(ret));
    } else {
      int32_t total_len = static_cast<int32_t>(pos - orig_pos - FLT_TYPE_AND_LEN);
      if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(buf, len, orig_pos, type_, total_len))) {
        LOG_WARN("fail to store type and len", K(ret));
      } else {
        LOG_DEBUG("succ to serialize query info", K(pos), K(len), K(total_len));
      }
    }
  }

  return ret;
}

int FLTQueryInfo::deserialize_field(FullLinkTraceExtraInfoId id,
                                    const int64_t v_len,
                                    const char *buf,
                                    const int64_t len,
                                    int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (id == FLT_QUERY_START_TIMESTAMP) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int8(buf, len, pos, v_len, query_start_ts_))) {
      LOG_WARN("fail to resolve query start timestamp", K(ret));
    }
  } else if (id == FLT_QUERY_END_TIMESTAMP) {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_int8(buf, len, pos, v_len, query_end_ts_))) {
      LOG_WARN("fail to resolve query end timestamp", K(ret));
    }
  } else {
    pos += v_len;
    LOG_WARN("unexpected query info id, ignore", K(id), K(v_len));
  }

  return ret;
}

// 34
int64_t FLTQueryInfo::get_serialize_size(FLTCtx &ctx)
{
  UNUSED(ctx);
  return FLT_TYPE_AND_LEN
         + FLT_TYPE_AND_LEN + sizeof(query_start_ts_)
         + FLT_TYPE_AND_LEN + sizeof(query_end_ts_);
}

int FLTAppInfo::serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;

  if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_str(buf, len, pos, flt_app_info_buf_,
                                                    static_cast<int32_t>(flt_app_info_buf_len_), FLT_APP_INFO))) {
    LOG_WARN("fail to store str as app info", K(ret));
  } else {
    LOG_DEBUG("succ to serialize FLTAppInfo");
  }

  return ret;
}

// only save the buffer, transfer to server, no need to decode content
int FLTAppInfo::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  MEMCPY(flt_app_info_buf_, buf, len);
  flt_app_info_buf_len_ = len;
  pos += len;

  LOG_DEBUG("succ to deserialize app info", K(pos), K(len));

  return ret;
}

int FLTAppInfo::deserialize_field(FullLinkTraceExtraInfoId id, const int64_t v_len,
                                  const char *buf, const int64_t len, int64_t &pos)
{
  UNUSED(v_len);
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  
  LOG_DEBUG("flt app info need deserialize directly", K(id));
  
  return OB_SUCCESS;
}

// 256 + 6 = 262 at max
int64_t FLTAppInfo::get_serialize_size(FLTCtx &ctx)
{
  UNUSED(ctx);
  return FLT_TYPE_AND_LEN + flt_app_info_buf_len_;
}

FLTAppInfo &FLTAppInfo::operator=(const FLTAppInfo &other)
{
  if (this != &other) {
    if (other.flt_app_info_buf_len_ != 0) {
      MEMCPY(flt_app_info_buf_, other.flt_app_info_buf_, other.flt_app_info_buf_len_);
      flt_app_info_buf_len_ = other.flt_app_info_buf_len_;
    }
  }
  
  return *this;
}

int FLTDriverSpanInfo::serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  UNUSED(ctx);
  
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("unexpected serialize type", K(type_));
  
  return ret;
}

// after this info resolved, use TRACE interface to print immediatly, other wise the buf will be freed,
// proxy will not persist the mem of buf
int FLTDriverSpanInfo::deserialize_field(FullLinkTraceExtraInfoId id,
                                         const int64_t v_len,
                                         const char *buf,
                                         const int64_t len,
                                         int64_t &pos)
{  
  int ret = OB_SUCCESS;

  if (id == FLT_DRV_SPAN) {
    char *ptr = NULL;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::get_str(buf, len, pos, v_len, ptr))) {
      LOG_WARN("fail to get str for driver span info", K(ret));
    } else {
      curr_driver_span_.assign_ptr(ptr, static_cast<int32_t>(v_len));
    }
  } else {
    pos += v_len;
    LOG_WARN("unexpected driver span info id, ignore", K(id), K(v_len));
  }
  
  return ret;
}

int64_t FLTDriverSpanInfo::get_serialize_size(FLTCtx &ctx)
{
  UNUSED(ctx);
  return 0;
}

void FLTDrvShowTraceSpanByProxy::reset()
{
  reset_curr();
  reset_last();
}

void FLTDrvShowTraceSpanByProxy::reset_curr()
{
  curr_drv_span_info_.reset();
  curr_drv_span_start_ts_ = 0;
  curr_drv_span_end_ts_ = 0;
}

void FLTDrvShowTraceSpanByProxy::reset_last()
{
  last_drv_span_info_.reset();
  last_drv_span_start_ts_ = 0;
  last_drv_span_end_ts_ = 0;
}

void FLTDrvShowTraceSpanByProxy::move_curr_to_last()
{
  last_drv_span_info_ = curr_drv_span_info_;
  last_drv_span_start_ts_ = curr_drv_span_start_ts_;
  last_drv_span_end_ts_ = curr_drv_span_end_ts_;
}

int FLTShowTraceJsonSpanInfo::deep_copy_drv_show_trace_span(FLTShowTraceJsonSpanInfo &info)
{
  int ret = OB_SUCCESS;

  if (this != &info) {
    char *src_buf = info.flt_drv_show_trace_span_.ptr();
    int32_t src_len = info.flt_drv_show_trace_span_.length();
    char *dst_buf = NULL;
    if (OB_ISNULL(dst_buf = (char *)ob_malloc(src_len, common::ObModIds::OB_PROXY_SHOW_TRACE_JSON))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K(src_len));
    } else {
      char *orig_buf = flt_drv_show_trace_span_.ptr();
      if (orig_buf != NULL && span_is_alloc_) {
        ob_free(orig_buf);
        orig_buf = NULL;
        flt_drv_show_trace_span_.reset();
        span_is_alloc_ = false;
      }
      MEMCPY(dst_buf, src_buf, src_len);
      flt_drv_show_trace_span_.assign_ptr(dst_buf, src_len);
      span_is_alloc_ = true;
    }
  }

  return ret;
}

// 2005
// -> 2050 FLT_DRV_SHOW_TRACE_SPAN only
int FLTShowTraceJsonSpanInfo::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t buf_end = pos + len;
  while (OB_SUCC(ret) && pos < len) {  
    int32_t v_len = 0;
    int16_t id = 0;
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::resolve_type_and_len(buf, buf_end, pos, id, v_len))) {
      LOG_WARN("fail to resolve type and len", K(ret));
    } else {
      FullLinkTraceExtraInfoId info_id = static_cast<FullLinkTraceExtraInfoId>(id);
      if (info_id != FLT_DRV_SHOW_TRACE_SPAN) {
        LOG_DEBUG("resolved unknown id from client, ignore", K(info_id));
      } else {
        // record ptr and len is enough, we will memcpy to our buffer later while handling both request and response
        flt_drv_show_trace_span_.assign_ptr(buf + pos, v_len);
        span_is_alloc_ = false;
        LOG_DEBUG("succ to record show trace span info", K(v_len), K(pos));
      }
      pos += v_len;
    }
  }
  
  return ret;
}

int FLTShowTraceJsonSpanInfo::serialize(char *buf, const int64_t len, int64_t &pos, FLTCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (last_sql_json_span_array_.count() == 0) {
    // nothing, sample do not hint, passed
    LOG_DEBUG("there is no json span array in last sql, ignore");
  } else if (ctx.is_client_support_show_trace_
             && flt_drv_show_trace_span_.empty()) {
    LOG_DEBUG("client support show trace, but send no show trace json to proxy, ignore");
  } else {
    int64_t orig_pos = pos;
    if (pos + FLT_TYPE_AND_LEN > len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("buf size overflow", K(len), K(pos));
    } else {
      pos += FLT_TYPE_AND_LEN;    // for obj head info, type2 + len4, fill at last
    }

    if (OB_SUCC(ret)) {
      if (ctx.is_client_support_show_trace_) {
        // get driver span from client, fill driver show trace json span array
        if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_str(buf, len, pos, flt_drv_show_trace_span_.ptr(),
                                                          flt_drv_show_trace_span_.length(),
                                                          FLT_DRV_SHOW_TRACE_SPAN))) {
          LOG_WARN("fail to store drv show trace str", K(ret), K(flt_drv_show_trace_span_));
        } else {
          LOG_DEBUG("save driver show trace json span", K(flt_drv_show_trace_span_), K(pos), K(len));
        }
      } else {
        // generate driver span from client, serialize drv span to buffer
        if (drv_show_by_proxy_.last_drv_span_info_.is_valid()) {
          ctx.span_start_ts_ = drv_show_by_proxy_.last_drv_span_start_ts_;
          ctx.span_end_ts_ = drv_show_by_proxy_.last_drv_span_end_ts_;
          if (OB_FAIL(drv_show_by_proxy_.last_drv_span_info_.serialize_as_json_format(buf, len, pos, ctx))) {
            LOG_WARN("fail to serialize span info as json format", K(ret));
          } else {
            LOG_DEBUG("serialize drv span as json by proxy", K(pos), K(len), K(ctx));
          }
        }
      }
    }
    
    // add proxy last sql show trace json span
    // make sure the pos += len do not size over flow
    int sub_ret = 0;
    if (OB_SUCC(ret)) {
      int64_t sub_orig_pos = pos;
      if (pos + FLT_TYPE_AND_LEN > len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("buf size overflow", K(ret), K(len), K(pos));
      } else {
        pos += FLT_TYPE_AND_LEN;
        
        // fill each json to array format
        // [
        sub_ret = snprintf(buf + pos, len - pos, "[");
        if (sub_ret <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to snprintf, please check buffer and pos", K(ret), K(sub_ret), K(pos), K(len));
        } else if (len - pos < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to check pos and len", K(ret), K(len), K(pos));
        } else {
          pos += sub_ret;
        }
        
        // a,b,c,d...
        for (int k = 0; OB_SUCC(ret) && k < last_sql_json_span_array_.count(); ++k) {
          ObString &each_json = last_sql_json_span_array_.at(k);
          LOG_DEBUG("fill each json", K(each_json));
          sub_ret = snprintf(buf + pos, len - pos, "%s", each_json.ptr());
          if (sub_ret <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to snprintf, please check buffer and pos", K(ret), K(sub_ret), K(pos), K(len));
          } else if (len - pos < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to check pos and len", K(ret), K(len), K(pos));
          } else {
            pos += sub_ret;
          }

          if (OB_SUCC(ret)
              && k < last_sql_json_span_array_.count() - 1) {
            sub_ret = snprintf(buf + pos, len - pos, ",");
            if (sub_ret <= 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to snprintf, please check buffer and pos", K(ret), K(sub_ret), K(pos), K(len));
            } else if (len - pos < 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to check pos and len", K(ret), K(len), K(pos));
            } else {
              pos += sub_ret;
            }
          }
        }
        
        // ]
        sub_ret = snprintf(buf + pos, len - pos, "]");
        if (sub_ret <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to snprintf, please check buffer and pos", K(ret), K(sub_ret), K(pos), K(len));
        } else if (len - pos < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to check pos and len", K(ret), K(len), K(pos));
        } else {
          pos += sub_ret;
        }

        // length check
        if (pos > len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("after fill total show trace span info, pos larger than buf len", K(ret), K(pos), K(len));
        } else {
          buf[pos] = '\0';
          // fill sub type and sub len in head
          int32_t sub_total_len = static_cast<int32_t>(pos - sub_orig_pos - FLT_TYPE_AND_LEN);
          if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(buf, len, sub_orig_pos,
                                                                     FLT_PROXY_SHOW_TRACE_SPAN, sub_total_len))) {
            LOG_WARN("fail to store sub type and len", K(ret), K(sub_orig_pos), K(pos), K(len), K(sub_total_len));
          }
        }
      }
    }

    // add show trace span head info, fill type and len in the head at last
    if (OB_SUCC(ret)) {
      int32_t total_len = static_cast<int32_t>(pos - orig_pos - FLT_TYPE_AND_LEN);
      if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_type_and_len(buf, len, orig_pos, type_, total_len))) {
        LOG_WARN("fail to store type and len in head", K(ret), K(orig_pos), K(len), K(total_len));
      } else {
        LOG_DEBUG("succ to seri show trace span info", K(pos), K(len), K(type_), K(total_len));
      }
    }
  }

  return ret;
}

int FLTShowTraceJsonSpanInfo::deserialize_field(FullLinkTraceExtraInfoId extra_id, const int64_t v_len,
                                            const char *buf, const int64_t len, int64_t &pos)
{
  UNUSED(extra_id);
  UNUSED(v_len);
  UNUSED(buf);
  UNUSED(len);
  UNUSED(pos);
  int ret = OB_SUCCESS;
  return ret;
}

// total json span array size
int64_t FLTShowTraceJsonSpanInfo::get_serialize_size(FLTCtx &ctx)
{
  int64_t ret = 0;

  if (ctx.is_client_support_show_trace_) {
    if (!flt_drv_show_trace_span_.empty()) {
      ret += FLT_TYPE_AND_LEN + flt_drv_show_trace_span_.length();   // key-value, 2,4,value
      ret += FLT_TYPE_AND_LEN;                                       // 2005, 2 + 4 + drv_json + proxy_json
    } else {
      /* client support show trace then give no show trace json to proxy, proxy do not send show trace too */
    }
  } else {
    if (drv_show_by_proxy_.last_drv_span_info_.is_valid()) {
      ret += FLT_TYPE_AND_LEN + drv_show_by_proxy_.last_drv_span_info_.get_show_trace_serialize_size();
    }
    ret += FLT_TYPE_AND_LEN;                                         // 2005, 2 + 4 + drv_json + proxy_json
  }

  if (ret > 0) {
    if (last_sql_json_span_array_.count() > 0) {
      for (int i = 0; i < last_sql_json_span_array_.count(); ++i) {
        ret += last_sql_json_span_array_.at(i).length();
      }
      ret += (last_sql_json_span_array_.count() - 1) + 2;     // comma(,) number, and '[' , ']'
      ret += FLT_TYPE_AND_LEN;                                // key-value 2,4,value
    } 
  }

  LOG_DEBUG("show trace json info serialize size", K(ret));
  
  return ret;
}

void FLTShowTraceJsonSpanInfo::clear()
{
  // reset current driver send show trace info
  reset_flt_drv_show_trace_span();

  // reset proxy record drv show trace span
  drv_show_by_proxy_.reset();
  
  // free last sql json buf
  reset_flt_show_trace_json_array(last_sql_json_span_array_);
  reset_flt_show_trace_json_array(curr_sql_json_span_array_);
  
  // reset basic type
  type_ = FLT_TYPE_SHOW_TRACE_SPAN;
}

void FLTShowTraceJsonSpanInfo::reset()
{
  clear();
}

void FLTShowTraceJsonSpanInfo::destroy()
{
  clear();
}

// after sql complete, invoke here, free thread mem
void FLTShowTraceJsonSpanInfo::move_curr_to_last_span_array()
{
  // reset current driver send show trace info
  reset_flt_drv_show_trace_span();

  // move proxy record drv show trace span
  drv_show_by_proxy_.curr_drv_span_end_ts_ = ObTimeUtility::current_time();
  drv_show_by_proxy_.move_curr_to_last();
  drv_show_by_proxy_.reset_curr();

  // free last sql json buf
  reset_flt_show_trace_json_array(last_sql_json_span_array_);
  
  // push to last
  if (curr_sql_json_span_array_.count() > 0) {
    for (int i = 0; i < curr_sql_json_span_array_.count(); ++i) {
      ObString &str = curr_sql_json_span_array_.at(i);
      IGNORE_RETURN last_sql_json_span_array_.push_back(str);
      LOG_DEBUG("push each curr json span to last", K(str));
    }
    curr_sql_json_span_array_.reset();
    LOG_DEBUG("last sql json span array cnt", "count", last_sql_json_span_array_.count());
  }
}

void FLTShowTraceJsonSpanInfo::reset_flt_drv_show_trace_span()
{
  if (!flt_drv_show_trace_span_.empty()) {
    if (span_is_alloc_) {
      char *drv_ptr = flt_drv_show_trace_span_.ptr();
      ob_free(drv_ptr);
      drv_ptr = NULL;
      span_is_alloc_ = false;
    }
    flt_drv_show_trace_span_.reset();
  }
}

void FLTShowTraceJsonSpanInfo::reset_flt_show_trace_json_array(FltShowTraceJsonArrayType &json_array)
{
  if (!json_array.empty()) {
    for (int i = 0; i < json_array.count(); ++i) {
      ObString &str = json_array.at(i);
      if (!str.empty()) {
        char *str_ptr = str.ptr();
        ob_free(str_ptr);
        str_ptr = NULL;
      }
    }
  }
  json_array.reset();
}

/*
 * deserialize from full_trc
 * the format described in ob_2_0_full_link_trace_util.h
 * check while ret back, if (pos != len) err
 */
int FLTObjManage::deserialize(const char *buf, const int64_t len, int64_t &pos) // FLTCtx
{
  int ret = OB_SUCCESS;
  
  while (OB_SUCC(ret) && pos < len) {
    int16_t first_type = 0;
    int32_t first_len = 0;
    FLTExtraInfo *extra = NULL;
    
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::resolve_type_and_len(buf,
                                                                 len,
                                                                 pos,
                                                                 first_type,
                                                                 first_len))) {
      LOG_WARN("fail to resolve first type and len", K(ret), K(buf), K(len), K(pos));
      break;
    } else if (OB_FAIL(get_extra_info_ref_by_type(static_cast<FullLinkTraceExtraInfoType>(first_type), extra))) {
      LOG_WARN("fail to get extra info ref by type", K(ret));
      break;
    } else if (OB_ISNULL(extra)) {
      pos += first_len;
      LOG_DEBUG("unexpected extra info type, ignore it.", K(first_type), K(first_len));
    } else {
      int64_t second_pos = 0;
      if (OB_FAIL(extra->deserialize(buf + pos, first_len, second_pos))) {
        LOG_WARN("fail to deserialize extra", K(ret));
      } else if (second_pos != first_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, please check the packet content or the deserialize logic",
                 K(ret), K(first_len), K(second_pos));
      } else {
        pos += second_pos;
      }
    }
  }

  return ret;
}

int FLTObjManage::get_extra_info_ref_by_type(FullLinkTraceExtraInfoType type, FLTExtraInfo* &extra)
{
  int ret = OB_SUCCESS;

  if (type <= FLT_EXTRA_TYPE_BEGIN || type >= FLT_EXTRA_TYPE_END) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected flt extra info type", K(ret), K(type));
  } else {
    if (type == FLT_SPAN_INFO) {
      span_info_.reset();
      extra = &span_info_;
    } else if (type == FLT_CONTROL_INFO) {
      control_info_.reset();
      extra = &control_info_;
    } else if (type == FLT_QUERY_INFO) {
      query_info_.reset();
      extra = &query_info_;
    } else if (type == FLT_APP_INFO) {
      app_info_.reset();
      extra = &app_info_;
    } else if (type == FLT_DRIVER_SPAN_INFO) {
      driver_span_info_.reset();
      extra = &driver_span_info_;
    } else if (type == FLT_TYPE_SHOW_TRACE_SPAN) {
      show_trace_json_info_.flt_drv_show_trace_span_.reset();
      extra = &show_trace_json_info_;
    } else {
      LOG_DEBUG("unexpected extra info type, ignore", K(type));
    }
  }

  return ret;
}

void FLTObjManage::reset()
{
  saved_control_info_.reset();
  control_info_.reset();
  span_info_.reset();
  query_info_.reset();
  app_info_.reset();            // memset!
  driver_span_info_.reset();   // memset!
  trace_log_info_.reset();
  show_trace_json_info_.reset();    // reset ptr
}

int64_t FLTTraceLogInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("is_init", is_inited_,
       "proxy_root_span_begin_time", proxy_root_span_begin_time_);
  if (proxy_root_span_ctx_ != NULL) {
    J_COMMA();
    J_KV("proxy_root_span_ctx_uuid", proxy_root_span_ctx_->span_id_,
         "proxy_root_span_ctx_start_ts", proxy_root_span_ctx_->start_ts_,
         "proxy_root_span_ctx_end_ts", proxy_root_span_ctx_->end_ts_);
  }
  if (server_process_req_ctx_ != NULL) {
    J_COMMA();
    J_KV("server_process_req_ctx_uuid", server_process_req_ctx_->span_id_,
         "server_process_req_ctx_start_ts", server_process_req_ctx_->start_ts_,
         "server_process_req_ctx_end_ts", server_process_req_ctx_->end_ts_);
  }
  if (server_response_read_ctx_ != NULL) {
    J_COMMA();
    J_KV("server_response_read_ctx_uuid", server_response_read_ctx_->span_id_,
         "server_response_read_ctx_start_ts", server_response_read_ctx_->start_ts_,
         "server_response_read_ctx_end_ts", server_response_read_ctx_->end_ts_);
  }
  if (cluster_resource_create_ctx_ != NULL) {
    J_COMMA();
    J_KV("cluster_resource_create_ctx_uuid", cluster_resource_create_ctx_->span_id_,
         "cluster_resource_create_ctx_start_ts", cluster_resource_create_ctx_->start_ts_,
         "cluster_resource_create_ctx_end_ts", cluster_resource_create_ctx_->end_ts_);
  }
  if (partition_location_lookup_ctx_ != NULL) {
    J_COMMA();
    J_KV("partition_location_lookup_ctx_uuid", partition_location_lookup_ctx_->span_id_,
         "partition_location_lookup_ctx_start_ts", partition_location_lookup_ctx_->start_ts_,
         "partition_location_lookup_ctx_end_ts", partition_location_lookup_ctx_->end_ts_);
  }
  if (do_observer_open_ctx_ != NULL) {
    J_COMMA();
    J_KV("do_observer_open_ctx_uuid", do_observer_open_ctx_->span_id_,
         "do_observer_open_ctx_start_ts", do_observer_open_ctx_->start_ts_,
         "do_observer_open_ctx_end_ts", do_observer_open_ctx_->end_ts_);
  }
  if (client_response_write_ctx_ != NULL) {
    J_COMMA();
    J_KV("client_response_write_ctx_uuid", client_response_write_ctx_->span_id_,
         "client_response_write_ctx_start_ts", client_response_write_ctx_->start_ts_,
         "client_response_write_ctx_end_ts", client_response_write_ctx_->end_ts_);
  }
  if (server_request_write_ctx_ != NULL) {
    J_COMMA();
    J_KV("server_request_write_ctx_uuid", server_request_write_ctx_->span_id_,
         "server_request_write_ctx_start_ts", server_request_write_ctx_->start_ts_,
         "server_request_write_ctx_end_ts", server_request_write_ctx_->end_ts_);
  }
  J_OBJ_END();
  return pos;
}


} // common

} // oceanbase

