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

#ifndef OBPROXY_SHOW_WARNING_HANDLER_H
#define OBPROXY_SHOW_WARNING_HANDLER_H

#include "cmd/ob_internal_cmd_handler.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "iocore/eventsystem/ob_event_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObWarningRecord
{
  ObWarningRecord()
    : log_id_(0),
      thread_id_(0),
      time_(0),
      log_buf_(NULL),
      log_len_(0)
  {
  }

  ~ObWarningRecord() { }

  int64_t log_id_;
  int64_t thread_id_;
  int64_t time_;

  char *log_buf_;
  int64_t log_len_;
};

class ObWarningProcessor
{
public:
  ObWarningProcessor()
  {
    memset(all_event_threads_warn_log_index_, 0, sizeof(all_event_threads_warn_log_index_));
    memset(all_dedicate_threads_warn_log_index_, 0, sizeof(all_dedicate_threads_warn_log_index_));
    warn_record_array_len_ = OB_PROXY_WARN_LOG_BUF_LENGTH / OB_PROXY_WARN_LOG_AVG_LENGTH;
    log_id_ = 0;
  }

  ~ObWarningProcessor() { }


  int get_warn_log_index(const event::ObThreadType type, const int64_t ethread_id,
                         int64_t &start_index, int64_t &end_index) const;

  int64_t get_warn_record_array_len() const { return warn_record_array_len_; }

  // attention !!! can't print log in this func
  void record_log(const int32_t level, const int64_t time,
                  const char *head_buf, const int64_t head_len,
                  const char *data, const int64_t data_len);

private:
  void get_warn_log_index_internal(const event::ObThreadType type, const int64_t ethread_id,
                                   int64_t &start_index, int64_t &end_index) const;
  void set_warn_log_index_internal(const event::ObThreadType type, const int64_t ethread_id,
                                   const int64_t start_index, const int64_t end_index);

private:
  int64_t all_event_threads_warn_log_index_[event::MAX_EVENT_THREADS * 2];
  int64_t all_dedicate_threads_warn_log_index_[event::MAX_EVENT_THREADS * 2];
  int64_t warn_record_array_len_;
  int64_t log_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObWarningProcessor);
};

inline void ObWarningProcessor::get_warn_log_index_internal(const event::ObThreadType type, const int64_t ethread_id,
                                                            int64_t &start_index, int64_t &end_index) const
{
  int64_t i = 2 * ethread_id;
  if (event::REGULAR == type) {
    start_index = all_event_threads_warn_log_index_[i];
    end_index = all_event_threads_warn_log_index_[i + 1];
  } else {
    start_index = all_dedicate_threads_warn_log_index_[i];
    end_index = all_dedicate_threads_warn_log_index_[i + 1];
  }
}

inline void ObWarningProcessor::set_warn_log_index_internal(const event::ObThreadType type, const int64_t ethread_id,
                                                            const int64_t start_index, const int64_t end_index)
{
  int64_t i = 2 * ethread_id;
  if (event::REGULAR == type) {
    all_event_threads_warn_log_index_[i] = start_index;
    all_event_threads_warn_log_index_[i + 1] = end_index;
  } else {
    all_dedicate_threads_warn_log_index_[i] = start_index;
    all_dedicate_threads_warn_log_index_[i + 1] = end_index;
  }
}

struct ObShowWaringParam
{

  ObShowWaringParam() : log_id_(-1), thread_id_(-1), time_(-1) {}
  ObShowWaringParam(const int64_t log_id, const int64_t thread_id)
    : log_id_(log_id), thread_id_(thread_id), time_(-1) {}
  ~ObShowWaringParam() { }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K(log_id_), K(thread_id_), K(time_));
    J_OBJ_END();
    return pos;
  }

  int64_t log_id_;
  int64_t thread_id_;
  int64_t time_;
};

class ObShowWarningHandler : public ObInternalCmdHandler
{
public:
  ObShowWarningHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf, const ObInternalCmdInfo &info);
  virtual ~ObShowWarningHandler() { }

  int handle_show_warning(int event, event::ObEvent *e);

private:
  int resolve_param();
  int dump_warning_record_from_ethreads(event::ObEThread **ethreads, const int64_t count);
  int dump_header();
  int dump_warning_record(const ObWarningRecord &record);

private:
  static const int64_t RESERVED_PERCENTAGE = 10;

  ObShowWaringParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObShowWarningHandler);
};

extern ObWarningProcessor g_warning_processor;
inline ObWarningProcessor &get_global_warning_processor()
{
  return g_warning_processor;
}

int show_warning_cmd_init();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_SHOW_WARNING_HANDLER_H
