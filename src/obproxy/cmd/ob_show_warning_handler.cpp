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

#include "cmd/ob_show_warning_handler.h"
#include "lib/time/ob_time_utility.h"
#include "iocore/eventsystem/ob_task.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
ObWarningProcessor g_warning_processor;

int ObWarningProcessor::get_warn_log_index(const ObThreadType type, const int64_t ethread_id,
                                           int64_t &start_index, int64_t &end_index) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(MONITOR == type) || OB_UNLIKELY(ethread_id < 0) || OB_UNLIKELY(ethread_id > MAX_EVENT_THREADS)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument", K(type), K(ethread_id), K(ret));
  } else {
    get_warn_log_index_internal(type, ethread_id, start_index, end_index);
  }
  return ret;
}

void ObWarningProcessor::record_log(const int32_t level, const int64_t time,
                                    const char *head_buf, const int64_t head_len,
                                    const char *data, const int64_t data_len)
{
  ObEThread *ethread = this_ethread();
  int64_t total_len = head_len + data_len;

  if (OB_ISNULL(ethread)
      || OB_UNLIKELY(OB_LOG_LEVEL_ERROR != level && OB_LOG_LEVEL_WARN != level)
      || OB_ISNULL(head_buf) || head_len <= 0
      || OB_ISNULL(ethread->warn_log_buf_)
      || OB_UNLIKELY(total_len > ObLogger::MAX_LOG_SIZE)) {
  } else {
    bool first_record_log = false;
    if (NULL == ethread->warn_log_buf_start_) {
      first_record_log = true;
      ethread->warn_log_buf_start_ = ethread->warn_log_buf_ + (warn_record_array_len_ * sizeof(ObWarningRecord));
    }
    char *warn_log_buf_end = ethread->warn_log_buf_ + OB_PROXY_WARN_LOG_BUF_LENGTH;

    char *write_start = ethread->warn_log_buf_start_;
    if (write_start >= warn_log_buf_end) {
      write_start = ethread->warn_log_buf_ + (warn_record_array_len_ * sizeof(ObWarningRecord));
    }
    char *writr_end = write_start + total_len;
    if (writr_end >= warn_log_buf_end) {
      writr_end = warn_log_buf_end;
      total_len = warn_log_buf_end - write_start;
    }

    int64_t start_index = -1;
    int64_t end_index = -1;
    get_warn_log_index_internal(ethread->tt_, ethread->id_, start_index, end_index);

    ObWarningRecord *warning_record_array = reinterpret_cast<ObWarningRecord *>(ethread->warn_log_buf_);
    ObWarningRecord *record = NULL;
    char *p = NULL;

    if (OB_LIKELY(!first_record_log)) {

      if (end_index == start_index) {
        record = &(warning_record_array[start_index]);
        p = record->log_buf_ + record->log_len_;
        record->log_buf_ = NULL;
        record->log_len_ = 0;
        start_index = (start_index + 1) % warn_record_array_len_;
      }

      // if p < writr_end means that buf for write log is not enough,
      // we need search the warning_record_array for enough buf to write the all log.
      // if the buf left is not enough for this log, we only use the last buf (this buf isn't round).
      if (p < writr_end) {
        int64_t delta = end_index - start_index;
        int64_t array_len = ((delta > 0) ? delta : (delta + warn_record_array_len_));
        int64_t j = start_index;
        for (int64_t i = 0; i < array_len; ++i) {
          record = &(warning_record_array[j]);
          p = record->log_buf_ + record->log_len_;
          if (p > write_start) {
            record->log_buf_ = NULL;
            record->log_len_ = 0;
            if (p >= writr_end) {
              start_index = j;
              break;
            }
          }
          j = (j + 1) % warn_record_array_len_;
        }
      }
    }

    warning_record_array[end_index].log_id_ = ATOMIC_FAA(&log_id_, 1);
    warning_record_array[end_index].thread_id_ = GETTID();
    warning_record_array[end_index].time_ = time;
    warning_record_array[end_index].log_buf_ = write_start;
    warning_record_array[end_index].log_len_ = total_len;

    int64_t pos = -1;
    pos = snprintf(warning_record_array[end_index].log_buf_,
                   warning_record_array[end_index].log_len_, "%s", head_buf);
    if (NULL != data && pos > 0 && pos < total_len) {
      snprintf(warning_record_array[end_index].log_buf_ + pos,
               warning_record_array[end_index].log_len_ - pos, "%s", data);
    }

    end_index = (end_index + 1) % warn_record_array_len_;
    ethread->warn_log_buf_start_ = writr_end;
    set_warn_log_index_internal(ethread->tt_, ethread->id_, start_index, end_index);
  }
}

enum
{
  OB_SW_LOG_ID = 0,
  OB_SW_THREAD_ID,
  OB_SW_TIME,
  OB_SW_LOG,

  OB_SW_MAX_COLUMN_ID
};

const ObProxyColumnSchema SW_COLUMN_ARRAY[OB_SW_MAX_COLUMN_ID] = {
  ObProxyColumnSchema::make_schema(OB_SW_LOG_ID,      "log_id", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_SW_THREAD_ID,   "thread_id", obmysql::OB_MYSQL_TYPE_LONGLONG),
  ObProxyColumnSchema::make_schema(OB_SW_TIME,        "time", obmysql::OB_MYSQL_TYPE_TIMESTAMP),
  ObProxyColumnSchema::make_schema(OB_SW_LOG,         "log", obmysql::OB_MYSQL_TYPE_VARCHAR),
};

ObShowWarningHandler::ObShowWarningHandler(event::ObContinuation *cont, event::ObMIOBuffer *buf,
                                           const ObInternalCmdInfo &info)
    : ObInternalCmdHandler(cont, buf, info), param_(info.get_log_id(), info.get_thread_id())
{
}

int ObShowWarningHandler::handle_show_warning(int event, event::ObEvent *e)
{
  int event_ret = EVENT_NONE;
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_argument_valid(event, e))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument, it should not happen", K(event), K(e), K(ret));
  } else if (OB_FAIL(resolve_param())) {
    WARN_ICMD("fail to resolve_param", K(ret));
  } else if (OB_FAIL(dump_header())) {
    WARN_ICMD("fail to dump_header", K(ret));
  } else if (OB_FAIL(dump_warning_record_from_ethreads(g_event_processor.all_event_threads_,
                                                       g_event_processor.event_thread_count_))) {
    INFO_ICMD("fail to dump warning_record form event_threads", K(ret));
  } else if (OB_FAIL(dump_warning_record_from_ethreads(g_event_processor.all_dedicate_threads_,
                                                       g_event_processor.dedicate_thread_count_))) {
    INFO_ICMD("fail to dump warning_record form dedicate_threads", K(ret));
  } else if (OB_FAIL(encode_eof_packet())) {
    WARN_ICMD("fail to encode eof packet", K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    DEBUG_ICMD("succ to dump warning log");
    event_ret = handle_callback(INTERNAL_CMD_EVENTS_SUCCESS, NULL);
  } else {
    event_ret = internal_error_callback(ret);
  }

  return event_ret;
}

int resolve_value(ObString &str, int64_t &value)
{
  int ret = OB_SUCCESS;
  char *endptr = NULL;
  if (NULL != str.find(':')) {
    if (OB_FAIL(ObTimeUtility::str_to_usec(str, value))) {
      WARN_ICMD("fail to str_to_usec", K(str), K(ret));
    }
  } else {
    value = std::strtol(str.ptr(), &endptr, 10);
    if ((ERANGE == errno && (LONG_MAX == value || LONG_MIN == value))
        || (0 != errno && 0 == value)) {
      ret = OB_ERR_SYS;
      WARN_ICMD("fail to strtol", K(str), K(value), K(ret));
    } else if (str.ptr() == endptr) {
      ret = OB_INVALID_ARGUMENT;
      WARN_ICMD("no digits were found", K(str), K(ret));
    } else if ('\0' != *endptr) {
      str.split_on(endptr);
      ObString remain = str.trim();
      if(!remain.empty()) {
        ret = OB_INVALID_ARGUMENT;
        WARN_ICMD("invalid digits", K(str), K(remain), K(endptr), K(ret));
      }
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObShowWarningHandler::resolve_param()
{
  int ret = OB_SUCCESS;
  ObString str(like_name_);
  if (!str.empty()) {
    if (OB_FAIL(resolve_value(str, param_.time_))) {
      WARN_ICMD("fail to resolve time", K(ret));
      ret = OB_ERR_OPERATOR_UNKNOWN;
    }
  }

  INFO_ICMD("show warning param", K_(like_name), K_(param));
  return ret;
}

int ObShowWarningHandler::dump_warning_record_from_ethreads(ObEThread **ethreads, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ethreads) || OB_UNLIKELY(count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("invalid argument", KP(ethreads), K(count));
  } else {
    int64_t j = 0;
    int64_t k = 0;

    ObWarningProcessor &g_warning_processor = get_global_warning_processor();
    int64_t array_len = g_warning_processor.get_warn_record_array_len();
    ObWarningRecord *warning_record_array = NULL;
    ObWarningRecord *record = NULL;

    ObEThread *ethread = NULL;
    char *warn_log_buf_end = NULL;

    int64_t delta = 0;
    int64_t total_len = 0;
    int64_t reserved_len = 0;

    int64_t start_index = 0;
    int64_t end_index = 0;

    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      ethread = ethreads[i];
      if (OB_ISNULL(ethread)) {
        ret = OB_ERR_UNEXPECTED;
        WARN_ICMD("can't find ethread in all_event_threads_", K(i), K(ret));
      } else {
        if (OB_ISNULL(warning_record_array = reinterpret_cast<ObWarningRecord *>(ethread->warn_log_buf_))) {
          ret = OB_ERR_UNEXPECTED;
          WARN_ICMD("ethread warn_log_buf_ is NULL", K(ethread->id_), K(ethread->tt_), K(ret));
        } else if (OB_FAIL(g_warning_processor.get_warn_log_index(ethread->tt_, ethread->id_, start_index, end_index))) {
          WARN_ICMD("fail to get_warn_seq_id", K(ethread->id_), K(ethread->tt_), K(ret));
        }
      }
      warn_log_buf_end = ethread->warn_log_buf_ + OB_PROXY_WARN_LOG_BUF_LENGTH;

      if (OB_SUCC(ret) && OB_LIKELY(NULL != ethread->warn_log_buf_start_)) {
        delta = end_index - start_index;
        total_len = ((delta > 0) ? delta : (delta + array_len));
        reserved_len = (total_len * RESERVED_PERCENTAGE) / 100;

        total_len -= reserved_len;
        k = (start_index + reserved_len) % array_len;

        for (j = 0; j < total_len && OB_SUCC(ret); ++j) {
          record = &(warning_record_array[k]);
          if ((-1 == param_.log_id_ || record->log_id_ >= param_.log_id_)
              && (-1 == param_.thread_id_ || record->thread_id_ == param_.thread_id_)
              && (-1 == param_.time_ || record->time_ >= param_.time_)) {

            // In most cases, the last log will be truncated,
            // we will not dump this log in show warnlog cmd.
            if (warn_log_buf_end != (record->log_buf_ + record->log_len_)) {
              if (OB_FAIL(dump_warning_record(*record))) {
                INFO_ICMD("fail to dump_warning_record", K(ret));
              }
            }
          }
          k = (k + 1) % array_len;
        }
      }
    }
  }
  return ret;
}


int ObShowWarningHandler::dump_header()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(encode_header(SW_COLUMN_ARRAY, OB_SW_MAX_COLUMN_ID))) {
    WARN_ICMD("fail to encode header", K(ret));
  }
  return ret;
}

int ObShowWarningHandler::dump_warning_record(const ObWarningRecord &record)
{
  int ret = OB_SUCCESS;
  ObNewRow row;
  ObObj cells[OB_SW_MAX_COLUMN_ID];

  cells[OB_SW_LOG_ID].set_int(record.log_id_);
  cells[OB_SW_THREAD_ID].set_int(record.thread_id_);
  cells[OB_SW_TIME].set_timestamp(hrtime_to_usec(HRTIME_HOURS(8)) + record.time_);
  cells[OB_SW_LOG].set_varchar(record.log_buf_, static_cast<ObString::obstr_size_t>(record.log_len_));

  row.cells_ = cells;
  row.count_ = OB_SW_MAX_COLUMN_ID;

  if (OB_FAIL(encode_row_packet(row))) {
    INFO_ICMD("fail to encode row packet", K(row), K(ret));
  } else {
    DEBUG_ICMD("succ to encode row packet", K(row));
  }
  return ret;
}

static int show_warning_callback(ObContinuation *cont, ObInternalCmdInfo &info,
                                 ObMIOBuffer *buf, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;

  ObShowWarningHandler *handler = NULL;

  if (OB_UNLIKELY(!ObInternalCmdHandler::is_constructor_argument_valid(cont, buf))) {
    ret = OB_INVALID_ARGUMENT;
    WARN_ICMD("constructor argument is invalid", K(cont), K(buf), K(ret));
  } else if (OB_ISNULL(handler = new (std::nothrow) ObShowWarningHandler(cont, buf, info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    WARN_ICMD("fail to new ObShowWarningHandler", K(ret));
  } else if (OB_FAIL(handler->init())) {
    WARN_ICMD("fail to init for ObShowWarningHandler", K(ret));
  } else {
    SET_CONTINUATION_HANDLER(handler, &ObShowWarningHandler::handle_show_warning);
    action = &handler->get_action();
    if (OB_ISNULL(g_event_processor.schedule_imm(handler, ET_TASK))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      WARN_ICMD("fail to schedule ObShowWarningHandler", K(ret));
      action = NULL;
    } else {
      DEBUG_ICMD("succ to schedule ObShowWarningHandler");
    }
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != handler)) {
    delete handler;
    handler = NULL;
  }
  return ret;
}

int show_warning_cmd_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_global_internal_cmd_processor().register_cmd(OBPROXY_T_ICMD_SHOW_WARNLOG,
                                                               &show_warning_callback))) {
    WARN_ICMD("fail to register OBPROXY_T_ICMD_SHOW_WARNLOG", K(ret));
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
