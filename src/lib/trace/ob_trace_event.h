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

#ifndef _OB_TRACE_EVENT_H
#define _OB_TRACE_EVENT_H 1
#include "lib/json/ob_yson.h"
#include "lib/ob_name_id_def.h"
#include "lib/utility/ob_template_utils.h"  // for STATIC_ASSERT
#include "lib/lock/ob_mutex.h"
namespace oceanbase
{
namespace common
{
// event id name
typedef uint16_t ObEventID;
// <event_id, timestamp>
struct ObTimestampEvent
{
  ObTimestampEvent()
  {
    id_ = 0;
    timestamp_ = 0;
  }
  void set_id_time(ObEventID id) {id_ = id; timestamp_ = ObTimeUtility::current_time(); }
  ObEventID id_;
  int64_t timestamp_;
};
// <event_id, timestamp, (yson elment KV)*>
// @note sizeof(ObTraceEvent)=16
struct ObTraceEvent
{
  ObTraceEvent()
  {
    timestamp_ = 0;
    id_ = 0;
    yson_beg_pos_ = 0;
    yson_end_pos_ = 0;
  }
  void set_id_time(ObEventID id) {id_ = id; timestamp_ = ObTimeUtility::current_time(); yson_beg_pos_ = yson_end_pos_ = 0; }
  int64_t timestamp_;
  ObEventID id_;
  int16_t yson_beg_pos_;
  int16_t yson_end_pos_;
};

template <typename EventType>
struct ObSeqEventRecorder
{
  ObSeqEventRecorder(bool need_lock, uint32_t latch_id)
      :need_lock_(need_lock),
       lock_(latch_id),
       next_idx_(0),
       buffer_pos_(0),
       dropped_events_(0)
  {
    STATIC_ASSERT(MAX_INFO_BUFFER_SIZE<INT16_MAX, "MAX_INFO_BUFFER_SIZE too large");
    STATIC_ASSERT(sizeof(ObTraceEvent)==16, "sizeof(ObTraceEvent)=16");
    STATIC_ASSERT(sizeof(*this)<=4096, "sizeof(*this)<=4096");
  }
  int assign(const ObSeqEventRecorder &other)
  {
    // don't copy need_lock_ and lock_
    memcpy(this->events_, other.events_, sizeof(EventType)*other.next_idx_);
    memcpy(this->buffer_, other.buffer_, other.buffer_pos_);
    next_idx_ = other.next_idx_;
    buffer_pos_ = other.buffer_pos_;
    dropped_events_ = other.dropped_events_;
    return common::OB_SUCCESS;
  }

  EventType &add_event() { return next_idx_ < MAX_EVENT_COUNT ? events_[next_idx_++] : (dropped_events_++, events_[MAX_EVENT_COUNT-1]);}
  const EventType &get_event(int64_t idx) const { return events_[idx]; }
  int64_t count() { return next_idx_; }
  void reset() { next_idx_ = 0; buffer_pos_ = 0; dropped_events_ = 0; }
  char *get_buffer() { return buffer_;}
  int64_t &get_buffer_pos() { return buffer_pos_; }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    int64_t prev_ts = 0;
    int64_t total_time = 0;
    const char* event_name = NULL;
    for (int64_t i = 0; i < next_idx_; ++i) {
      const EventType &ev = events_[i];
      event_name = NAME(ev.id_);
      if (prev_ts == 0) {
        (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "begin_ts=%ld ", ev.timestamp_);
        (void)common::ObTimeUtility::usec_to_str(ev.timestamp_, buf, buf_len, pos);
        prev_ts = ev.timestamp_;
      }
      if (NULL == event_name) {
        (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%hu] u=%ld ",
                                                   ev.id_, ev.timestamp_ - prev_ts);
      } else {
        (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|[%s] u=%ld ",
                                                   event_name, ev.timestamp_ - prev_ts);
      }
      if (ev.yson_end_pos_ > ev.yson_beg_pos_ && ev.yson_beg_pos_ >= 0) {
        (void)::oceanbase::yson::databuff_print_elements(buf, buf_len, pos,
                                                         buffer_ + ev.yson_beg_pos_, ev.yson_end_pos_-ev.yson_beg_pos_);
      }
      total_time += ev.timestamp_ - prev_ts;
      prev_ts = ev.timestamp_;
    }
    (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, "|total_timeu=%ld", total_time);
    if (dropped_events_ > 0) {
      (void)::oceanbase::common::databuff_printf(buf, buf_len, pos, " DROPPED_EVENTS=%ld", dropped_events_);
    }
    return pos;
  }
  void check_lock()
  {
    if (need_lock_) {
      (void)lock_.lock();
    }
  }
  void check_unlock()
  {
    if (need_lock_) {
      (void)lock_.unlock();
    }
  }
public:
  static const int64_t MAX_INFO_BUFFER_SIZE = 1024;
private:
  static const int64_t MAX_EVENT_COUNT = 189;
  bool need_lock_;
  lib::ObMutex lock_;
  EventType events_[MAX_EVENT_COUNT];
  int64_t next_idx_;
  char buffer_[MAX_INFO_BUFFER_SIZE];
  int64_t buffer_pos_;
  int64_t dropped_events_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSeqEventRecorder);
};

typedef ObSeqEventRecorder<ObTraceEvent> ObTraceEventRecorder;

ObTraceEventRecorder &get_trace_recorder();
void set_trace_recorder(ObTraceEventRecorder *r);
ObTraceEventRecorder *&get_trace_recorder_ptr();

} // end namespace common
} // end namespace oceanbase

// record into the specified recorder, e.g.
//   REC_TRACE_EXT(m, start_trans, ID(arg1), a, ID(arg2), b, N(c), N_(d));
#define REC_TRACE(recorder, trace_event) (recorder).add_event().set_id_time(ID(trace_event))

#define REC_TRACE_EXT(recorder, trace_event, pairs...) do {             \
    recorder.check_lock();                                              \
    ObTraceEvent &trace_ev = (recorder).add_event();                    \
    trace_ev.set_id_time(ID(trace_event));                              \
    trace_ev.yson_beg_pos_ = static_cast<int16_t>((recorder).get_buffer_pos()); \
    int rec__err_ = oceanbase::yson::databuff_encode_elements((recorder).get_buffer(), ObTraceEventRecorder::MAX_INFO_BUFFER_SIZE, (recorder).get_buffer_pos(), ##pairs); \
    if (OB_LIKELY(oceanbase::common::OB_SUCCESS == rec__err_)) {          \
      trace_ev.yson_end_pos_ = static_cast<int16_t>((recorder).get_buffer_pos()); \
    } else {                                                            \
      (recorder).get_buffer_pos() = trace_ev.yson_beg_pos_;             \
      trace_ev.yson_beg_pos_ = trace_ev.yson_end_pos_ = 0;              \
    }                                                                   \
    recorder.check_unlock();                                            \
  } while (0)

// record trace events into THE one recorder
#define THE_TRACE ::oceanbase::common::get_trace_recorder()
#define NG_TRACE(...) REC_TRACE(THE_TRACE, __VA_ARGS__)
#define NG_TRACE_EXT(...) REC_TRACE_EXT(THE_TRACE, __VA_ARGS__)

#endif /* _OB_TRACE_EVENT_H */
