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

#include "ob_trace.h"

#include <cstdlib>
#include <ctime>
#include <random>

#include "lib/trace/ob_trace_def.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"

using namespace oceanbase::trace;
using namespace oceanbase::common;

namespace oceanbase
{

namespace sql
{
ObFLTSpanMgr* __attribute__((weak)) get_flt_span_manager()
{
  return NULL;
}
int __attribute__((weak)) handle_span_record(char* buf, const int64_t buf_len, ObFLTSpanMgr* flt_span_manager)
{
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(flt_span_manager);
  return 0;
}
}

namespace trace
{

static const char* __span_type_mapper[] = {
#define FLT_DEF_SPAN(name, comment) #name,
#define __HIGH_LEVEL_SPAN
#define __MIDDLE_LEVEL_SPAN
#define __LOW_LEVEL_SPAN
#include "lib/trace/ob_trace_def.h"
#undef __LOW_LEVEL_SPAN
#undef __MIDDLE_LEVEL_SPAN
#undef __HIGH_LEVEL_SPAN
#undef FLT_DEF_SPAN
};

thread_local ObTrace* ObTrace::save_buffer = NULL;
thread_local void *ObTrace::json_span_array_ = NULL;    // for show trace record json span


void flush_trace()
{
  auto& trace = *OBTRACE;
  auto& current_span = trace.current_span_;
  if (trace.is_inited() && !current_span.is_empty()) {
    auto* span = current_span.get_first();
    if (!trace.is_auto_flush()) {
      _LIB_LOG(INFO, "FLT flush trace, trace_id="UUID_PATTERN", root_span_id="UUID_PATTERN", buffer_size=%ld, offset=%ld, current_span size=%d, freed_span size=%d, policy=0x%x",
                 UUID_TOSTRING(trace.get_trace_id()),
                 UUID_TOSTRING(trace.get_root_span_id()),
                 trace.buffer_size_,
                 trace.offset_,
                 trace.current_span_.get_size(),
                 trace.freed_span_.get_size(),
                 trace.get_policy());
    }
    //SET_OB_LOG_TRACE_MODE();
    //PRINT_OB_LOG_TRACE_BUF(COMMON, INFO);
    while (current_span.get_header() != span) {
      auto* next = span->get_next();
      if (NULL != span->tags_ || 0 != span->end_ts_) {
        int64_t pos = 0;
        thread_local char buf[MAX_TRACE_LOG_SIZE];
        int ret = common::OB_SUCCESS;
        auto* tag = span->tags_;
        bool first = true;
        INIT_SPAN(span);
        while (OB_SUCC(ret) && OB_NOT_NULL(tag)) {
          if (pos + 10 >= MAX_TRACE_LOG_SIZE) {
            ret = common::OB_BUF_NOT_ENOUGH;
          } else {
            buf[pos++] = ',';
            if (first) {
              strcpy(buf + pos, "\"tags\":[");
              pos += 8;
              first = false;
            }
            ret = tag->tostring(buf, MAX_TRACE_LOG_SIZE, pos);
            tag = tag->next_;
          }
        }
        if (0 != pos) {
          if (pos + 1 < MAX_TRACE_LOG_SIZE) {
            buf[pos++] = ']';
            buf[pos++] = 0;
          } else {
            buf[MAX_TRACE_LOG_SIZE - 2] = ']';
            buf[MAX_TRACE_LOG_SIZE - 1] = 0;
          }
        }
        INIT_SPAN(span->source_span_);
        _OBPROXY_TRACE_LOG(INFO,
                           TRACE_PATTERN "%s}",
                           UUID_TOSTRING(trace.get_trace_id()),
                           __span_type_mapper[span->span_type_],
                           UUID_TOSTRING(span->span_id_),
                           span->start_ts_,
                           span->end_ts_,
                           UUID_TOSTRING(OB_ISNULL(span->source_span_) ?
                           OBTRACE->get_root_span_id() : span->source_span_->span_id_),
                           span->is_follow_ ? "true" : "false",
                           buf);
        buf[0] = '\0';
        IGNORE_RETURN sql::handle_span_record(buf, MAX_TRACE_LOG_SIZE, sql::get_flt_span_manager());
        IGNORE_RETURN trace.record_each_span_buf_for_show_trace(buf, pos, &trace, span);

        if (0 != span->end_ts_) {
          current_span.remove(span);
          trace.freed_span_.add_first(span);
        }
        span->tags_ = NULL;
      }
      span = next;
    }
    // reset ptr
    trace.reset_show_trace_info();
    
    //PRINT_OB_LOG_TRACE_BUF(OBTRACE, INFO);
    //CANCLE_OB_LOG_TRACE_MODE();
    trace.offset_ = trace.buffer_size_ / 2;
  }
}

double get_random_percentage()
{
  uint64_t random_u64 = UUID::gen_rand();
  return static_cast<double>(random_u64) / static_cast<double>(UINT64_MAX);
}

uint64_t UUID::gen_rand()
{
  static thread_local std::random_device dev;
  static thread_local std::mt19937 rng(dev());
  static thread_local std::uniform_int_distribution<uint64_t> dist;
  return dist(rng);
}

UUID UUID::gen()
{
  UUID ret;
  ret.high_ = ObTimeUtility::current_time();
  ret.low_ = gen_rand();
  return ret;
}

UUID::UUID(const char* uuid)
{
  high_ = low_ = 0;
  for (auto i = 0; i < 18; ++i) {
    if (8 == i || 13 == i) {
      continue;
    } else if (uuid[i] >= '0' && uuid[i] <= '9') {
      high_ = (high_ << 4) + (uuid[i] - '0');
    } else if (uuid[i] >= 'a' && uuid[i] <= 'f') {
      high_ = (high_ << 4) + (uuid[i] - 'a') + 10;
    } else if (uuid[i] >= 'A' && uuid[i] <= 'F') {
      high_ = (high_ << 4) + (uuid[i] - 'A') + 10;
    } else {
      return;
    }
  }
  for (auto i = 19; i < 36; ++i) {
    if (23 == i) {
      continue;
    } else if (uuid[i] >= '0' && uuid[i] <= '9') {
      low_ = (low_ << 4) + (uuid[i] - '0');
    } else if (uuid[i] >= 'a' && uuid[i] <= 'f') {
      low_ = (low_ << 4) + (uuid[i] - 'a') + 10;
    } else if (uuid[i] >= 'A' && uuid[i] <= 'F') {
      low_ = (low_ << 4) + (uuid[i] - 'A') + 10;
    } else {
      return;
    }
  }
}

int UUID::tostring(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = common::OB_SUCCESS;
  const char* transfer = "0123456789abcdef";
  if (pos + 37 > buf_len) {
    ret = common::OB_BUF_NOT_ENOUGH;
  } else {
    buf[pos++] = transfer[high_ >> 60 & 0xf];
    buf[pos++] = transfer[high_ >> 56 & 0xf];
    buf[pos++] = transfer[high_ >> 52 & 0xf];
    buf[pos++] = transfer[high_ >> 48 & 0xf];
    buf[pos++] = transfer[high_ >> 44 & 0xf];
    buf[pos++] = transfer[high_ >> 40 & 0xf];
    buf[pos++] = transfer[high_ >> 36 & 0xf];
    buf[pos++] = transfer[high_ >> 32 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[high_ >> 28 & 0xf];
    buf[pos++] = transfer[high_ >> 24 & 0xf];
    buf[pos++] = transfer[high_ >> 20 & 0xf];
    buf[pos++] = transfer[high_ >> 16 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[high_ >> 12 & 0xf];
    buf[pos++] = transfer[high_ >>  8 & 0xf];
    buf[pos++] = transfer[high_ >>  4 & 0xf];
    buf[pos++] = transfer[high_ >>  0 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[ low_ >> 60 & 0xf];
    buf[pos++] = transfer[ low_ >> 56 & 0xf];
    buf[pos++] = transfer[ low_ >> 52 & 0xf];
    buf[pos++] = transfer[ low_ >> 48 & 0xf];
    buf[pos++] = '-';
    buf[pos++] = transfer[ low_ >> 44 & 0xf];
    buf[pos++] = transfer[ low_ >> 40 & 0xf];
    buf[pos++] = transfer[ low_ >> 36 & 0xf];
    buf[pos++] = transfer[ low_ >> 32 & 0xf];
    buf[pos++] = transfer[ low_ >> 28 & 0xf];
    buf[pos++] = transfer[ low_ >> 24 & 0xf];
    buf[pos++] = transfer[ low_ >> 20 & 0xf];
    buf[pos++] = transfer[ low_ >> 16 & 0xf];
    buf[pos++] = transfer[ low_ >> 12 & 0xf];
    buf[pos++] = transfer[ low_ >>  8 & 0xf];
    buf[pos++] = transfer[ low_ >>  4 & 0xf];
    buf[pos++] = transfer[ low_ >>  0 & 0xf];
    buf[pos  ] = '\0';
  }
  return ret;
}

int UUID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(common::serialization::encode_i64(buf, buf_len, pos, high_))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::encode_i64(buf, buf_len, pos, low_))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  }
  return ret;
}

int UUID::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(common::serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&high_)))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::decode_i64(buf, buf_len, pos, reinterpret_cast<int64_t*>(&low_)))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  }
  return ret;
}

int to_string_and_strip(const char* str, const int64_t length, char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;
  char from[] = "\"\n\r\\\t";
  const char* to[] = { "\\\"", "\\n", "\\r", "\\\\", " "};
  buf[pos++] = '\"';
  for (auto j = 0; j < length && str[j]; ++j) {
    bool conv = false;
    for (auto i = 0; i < sizeof(from) - 1; ++i) {
      if (from[i] == str[j]) {
        for (const char* toc = to[i]; *toc; ++toc) {
          if (pos < buf_len) {
            buf[pos++] = *toc;
          } else {
            ret = common::OB_BUF_NOT_ENOUGH;
          }
        }
        conv = true;
        break;
      }
    }
    if (!conv) {
      if (pos < buf_len) {
        buf[pos++] = str[j];
      } else {
        ret = common::OB_BUF_NOT_ENOUGH;
      }
    }
  }
  if (OB_FAIL(ret) || pos + 2 >= buf_len) {
    buf[buf_len - 1] = 0;
  } else {
    buf[pos++] = '\"';
    buf[pos++] = '}';
    buf[pos] = 0;
  }
  return ret;
}

template<>
int ObTagCtx<ObString>::tostring(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObTagCtxBase::tostring(buf, buf_len, pos))) {
    // do nothing
  } else {
    ret = to_string_and_strip(data_.ptr(), data_.length(), buf, buf_len, pos);
  }
  return ret;
}

template<>
int ObTagCtx<char*>::tostring(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObTagCtxBase::tostring(buf, buf_len, pos))) {
    // do nothing
  } else {
    ret = to_string_and_strip(data_, INT64_MAX, buf, buf_len, pos);
  }
  return ret;
}

template<>
int ObTagCtx<void*>::tostring(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(ObTagCtxBase::tostring(buf, buf_len, pos))) {
    // do nothing
  } else {
    ret = to_string_and_strip((char*)&data_, INT64_MAX, buf, buf_len, pos);
  }
  return ret;
}

ObSpanCtx::ObSpanCtx()
  : span_id_(),
    source_span_(NULL),
    start_ts_(0),
    end_ts_(0),
    tags_(NULL),
    span_type_(0),
    is_follow_(false)
{
}

// use local buffer for obproxy, not tsi buffer as observer
ObTrace* ObTrace::get_instance()
{
  thread_local char default_buffer[MIN_BUFFER_SIZE];
  struct Guard {
    Guard(char* buffer, int64_t size) {
      IGNORE_RETURN new(buffer) ObTrace(size);
    }
  };
  thread_local Guard guard(default_buffer, MIN_BUFFER_SIZE);
  if (OB_ISNULL(save_buffer)) {
    save_buffer = (ObTrace*)default_buffer;
  }
  return save_buffer; 
}

// special set api for obproxy, inorder to manage the thread local buffer by obproxy itself.
void ObTrace::set_trace_buffer(void* buffer, int64_t buffer_size)
{
  if (OB_ISNULL(buffer) || buffer_size < MIN_BUFFER_SIZE) {
    save_buffer = NULL;
  } else {
    if (!((ObTrace*)buffer)->check_magic()) {
      IGNORE_RETURN new(buffer) ObTrace(buffer_size);
      _LIB_LOG(DEBUG, "set trace buffer, set buffer size:%ld", buffer_size);
    }
    save_buffer = (ObTrace*)buffer;
  }
}

/*
 * special set api for obproxy, set thread json span array before flush,
 * to record every span while flush trace
 */
void ObTrace::set_show_trace_info(void *json_span_array)
{
  json_span_array_ = json_span_array;
}

/*
 * special reset api for obproxy, reset thread json array ptr after each flush,
 * avoid to record unexpected
 */
void ObTrace::reset_show_trace_info()
{
  json_span_array_ = NULL;
}

int ObTrace::record_each_span_buf_for_show_trace(const char *buf,
                                                 const int64_t len,
                                                 ObTrace *trace,
                                                 oceanbase::trace::ObSpanCtx *span)
{
  int ret = common::OB_SUCCESS;

  if (json_span_array_ == NULL
      || buf == NULL
      || len < 0
      || trace == NULL
      || span == NULL) {
    // do nothing
    _LIB_LOG(DEBUG, "ignore each span in record show trace:%s,%s,%ld,%s,%s",
             json_span_array_ == NULL ? "empty" : "no",
             buf == NULL ? "empty" : "no",
             len,
             trace == NULL ? "empty" : "no",
             span == NULL ? "empty" : "no");
  } else {
    // calculate total buf len
    int64_t dst_buf_len = calc_total_span_buf_len(len, span);
    if (OB_UNLIKELY(dst_buf_len <= 0)) {
      ret = common::OB_INVALID_ARGUMENT;
      _LIB_LOG(WDIAG, "invalid total buf len:%ld", dst_buf_len);
    } else {
      common::ObSEArray<common::ObString, 10> *array = (common::ObSEArray<common::ObString, 10> *)json_span_array_;
      char *dst_buf = NULL;
    
      // alloc buf
      if (OB_ISNULL(dst_buf = (char *)ob_malloc(dst_buf_len, common::ObModIds::OB_PROXY_SHOW_TRACE_JSON))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        _LIB_LOG(EDIAG, "alloc mem failed: %ld", dst_buf_len);
      } else {
        // fill buf
        IGNORE_RETURN snprintf(dst_buf, dst_buf_len, TRACE_PATTERN "%s}",
                               UUID_TOSTRING(trace->get_trace_id()),
                               __span_type_mapper[span->span_type_],
                               UUID_TOSTRING(span->span_id_),
                               span->start_ts_,
                               span->end_ts_,
                               UUID_TOSTRING(OB_ISNULL(span->source_span_) ?
                                 OBTRACE->get_root_span_id() : span->source_span_->span_id_),
                               span->is_follow_ ? "true" : "false",
                               buf);
        // record
        ObString span_json(dst_buf_len, dst_buf);
        if (OB_FAIL(array->push_back(span_json))) {
          _LIB_LOG(EDIAG, "fail to push back json span array:%d", ret);
        } else {
          // debug log
          _LIB_LOG(DEBUG, "push each span to json span array:[%s], dst buf len:%ld", dst_buf, dst_buf_len);
        }
      }
    }
  }

  return ret;
}

int64_t ObTrace::calc_total_span_buf_len(const int64_t tag_len,
                                         oceanbase::trace::ObSpanCtx *span)
{
  int64_t ret = 0;

  // {
  ret += 1;

  // "trace_id":"..."
  ret += 11 + 36 + 2;  // "trace_id": + " + uuid + "

  // "name":"..."
  int64_t name_len = strlen(trace::__span_type_mapper[span->span_type_]);
  ret += 1 + 7 + 2 + name_len;  // , + "name": + " + name_len + "

  // "id":"..."
  ret += 1 + 5 + 36 + 2;  // , + "id": + uuid + " + " 

  // start_ts:
  ret += 1 + 11 + 16;   // , + "start_ts": + int64_t

  // end_ts:
  ret += 1 + 9 + 16;    // , + "end_ts": + int64_t

  // "parent_id":"..."
  ret += 1 + 12 + 36 + 2;   // , + "parent_id": + " + uuid + "

  // "is_follow": "true"/"false"
  ret += 1 + 12 + 7;    // , + "is_follow": + (" " + "true")/("false")

  // tag
  if (tag_len > 0) {  // tag buf + tag len
    ret += 1 + 7 + tag_len;    // , + "tags": + tag_len
  }
  
  // }
  ret += 1;

  return ret;
}
                                                 

ObTrace::ObTrace(int64_t buffer_size)
  : magic_code_(MAGIC_CODE),
    buffer_size_(buffer_size - sizeof(ObTrace)),
    offset_(buffer_size_ / 2),
    current_span_(),
    freed_span_(),
    last_active_span_(NULL),
    trace_id_(),
    root_span_id_(),
    policy_(0),
    seq_(0)
{
  for (auto i = 0; i < (offset_ / sizeof(ObSpanCtx)); ++i) {
    IGNORE_RETURN freed_span_.add_last(new(data_ + i * sizeof(ObSpanCtx)) ObSpanCtx);
  }
}

void ObTrace::init(UUID trace_id, UUID root_span_id, uint8_t policy)
{
  #ifndef NDEBUG
  if (check_magic()) {
    check_leak_span();
  }
  #endif
  reset();
  trace_id_ = trace_id;
  root_span_id_ = root_span_id;
  policy_ = policy;
}

UUID ObTrace::begin()
{
  trace_id_ = UUID::gen();
  return trace_id_;
}

void ObTrace::end()
{
  #ifndef NDEBUG
  check_leak_span();
  #endif
  reset();
}

ObSpanCtx* ObTrace::begin_span(uint32_t span_type, uint8_t level, bool is_follow)
{
  ObSpanCtx* new_span = NULL;
  if (!trace_id_.is_inited() || level > level_) {
    // do nothing
  } else {
    if (freed_span_.is_empty()) {
      FLUSH_TRACE();
    }
    if (freed_span_.is_empty()) {
      check_leak_span();
    } else {
      new_span = freed_span_.remove_last();
      current_span_.add_first(new_span);
      new_span->span_type_ = static_cast<uint16_t>(span_type);
      new_span->span_id_.high_ = 0;
      new_span->span_id_.low_ = ++seq_;
      new_span->source_span_ = last_active_span_;
      new_span->is_follow_ = is_follow;
      new_span->start_ts_ = ObTimeUtility::current_time();
      new_span->end_ts_ = 0;
      new_span->tags_ = NULL;
      last_active_span_ = new_span;
    }
  }
  return new_span;
}

void ObTrace::end_span(ObSpanCtx* span)
{
  if (!trace_id_.is_inited() || OB_ISNULL(span) || !span->span_id_.is_inited()) {
    // do nothing
  } else {
    span->end_ts_ = ObTimeUtility::current_time();
    last_active_span_ = span->source_span_;
  }
}

// clear all tags and span which is ended before
void ObTrace::reset_span()
{
  #ifndef NDEBUG
  if (!check_magic()) {
    LIB_LOG(EDIAG, "trace buffer was not inited");
  }
  #endif
  // remove all end span
  if (is_inited() && !current_span_.is_empty()) {
    auto* span = current_span_.get_first();
    while (current_span_.get_header() != span) {
      auto* next = span->get_next();
      if (0 != span->end_ts_) {
        current_span_.remove(span);
        freed_span_.add_first(span);
      }
      span->tags_ = nullptr;
      span = next;
    }
  }
  offset_ = buffer_size_ / 2;
}

int ObTrace::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = common::OB_SUCCESS;
  auto* span = const_cast<ObTrace*>(this)->last_active_span_;
  INIT_SPAN(span);
  auto& span_id = span == NULL ? root_span_id_ : span->span_id_;
  if (OB_FAIL(trace_id_.serialize(buf, buf_len, pos))) {
    // LOG_WDIAG("serialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(span_id.serialize(buf, buf_len, pos))) {
    // LOG_WDIAG("serialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::encode_i8(buf, buf_len, pos, policy_))) {
    // LOG_WDIAG("serialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else {
    // do nothing
  }
  return ret;
}

int ObTrace::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(trace_id_.deserialize(buf, buf_len, pos))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(root_span_id_.deserialize(buf, buf_len, pos))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(common::serialization::decode_i8(buf, buf_len, pos, reinterpret_cast<int8_t*>(&policy_)))) {
    // LOG_WDIAG("deserialize failed", K(ret), K(buf), K(buf_len), K(pos));
  } else {
    // do nothing
  }
  return ret;
}

void ObTrace::check_leak_span()
{
  ObSpanCtx* span = current_span_.get_first();
  while (current_span_.get_header() != span) {
    if (0 == span->end_ts_) {
      LIB_LOG(EDIAG, "there were leak span");
      dump_span();
      break;
    }
    span = span->get_next();
  }
}

void ObTrace::reset()
{
  #ifndef NDEBUG
  if (!check_magic()) {
    LIB_LOG(EDIAG, "trace buffer was not inited");
  }
  #endif
  offset_ = buffer_size_ / 2;
  freed_span_.push_range(current_span_);
  last_active_span_ = NULL;
  trace_id_ = UUID();
  root_span_id_ = UUID();
  policy_ = 0;
  seq_ = 0;
}

void ObTrace::dump_span()
{
  constexpr int64_t buf_len = 1 << 10;
  char buf[buf_len];
  int64_t pos = 0;
  int ret = common::OB_SUCCESS;
  auto* span = current_span_.get_first();
  IGNORE_RETURN databuff_printf(buf, buf_len, pos, "active_span: ");
  while (OB_SUCC(ret) && current_span_.get_header() != span) {
    if (0 == span->end_ts_) {
      ret = databuff_printf(buf, buf_len, pos, "%s, ", __span_type_mapper[span->span_type_]);
    }
    span = span->get_next();
  }
  _LIB_LOG(EDIAG, "%s", buf);
}

}
}
