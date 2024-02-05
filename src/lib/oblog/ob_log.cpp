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

#define USING_LOG_PREFIX LIB
#include "lib/oblog/ob_log.h"

#include <string.h>
#include <sys/uio.h>
#include <dirent.h>
#include <libgen.h>
#include <sys/statfs.h>
#include <sys/prctl.h>
#include <linux/prctl.h>
#include "lib/oblog/ob_warning_buffer.h"
#include "lib/ob_errno.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/ob_define.h"
#include "lib/list/ob_list.h"
#include "lib/utility/utility.h"
#include "lib/container/ob_vector.h"
#include "lib/oblog/ob_async_log_buffer.h"
#include "lib/queue/ob_lighty_queue.h"

namespace oceanbase
{
namespace common
{

__thread uint64_t ObLogger::curr_logging_seq_ = 0;
__thread uint64_t ObLogger::last_logging_seq_ = 0;
__thread int64_t ObLogger::last_logging_cost_time_us_ = 0;
__thread bool ObLogger::disable_logging_ = false;
__thread time_t ObLogger::last_unix_sec_ = 0;
__thread struct tm ObLogger::last_localtime_;

const int64_t ObLogger::MAX_LOG_ITEM_COUNT[MAX_LOG_ITEM_TYPE] = {
  32 * 1024, //tiny log item use 32k(count) * 1k(size) = 32M fix size
  4 * 1024,
  128,
  4 * 1024,
};

static int64_t last_check_file_ts = 0;//last file sample timestamps
static int64_t last_check_disk_ts = 0;//last disk sample timestamps


int logdata_printf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  va_list args;
  va_start(args, fmt);
  ret = logdata_vprintf(buf, buf_len, pos, fmt, args);
  va_end(args);
  return ret;
}

int logdata_vprintf(char *buf, const int64_t buf_len, int64_t &pos, const char *fmt, va_list args)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != buf) && OB_LIKELY(0 <= pos && pos < buf_len)) {
    int len = vsnprintf(buf + pos, buf_len - pos, fmt, args);
    if (OB_UNLIKELY(len < 0)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_LIKELY(len < buf_len - pos)) {
      pos += len;
    } else {
      pos = buf_len - 1;  //skip '\0' written by vsnprintf
      ret = OB_SIZE_OVERFLOW;
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

void ObLogIdLevelMap::set_level(const int8_t level)
{
  non_mod_level_ = level;
  for (uint64_t par_mod_index = 0; par_mod_index < MAX_PAR_MOD_SIZE; ++par_mod_index) {
    for (uint64_t sub_mod_index = 0; sub_mod_index < MAX_SUB_MOD_SIZE + 1; ++sub_mod_index) {
      log_level_[par_mod_index][sub_mod_index] = level;
    }
  }
}

int ObLogIdLevelMap::set_level(const uint64_t par_mod_id, const int8_t level)
{
  int ret = OB_SUCCESS;
  if (par_mod_id < MAX_PAR_MOD_SIZE) {
    for (uint64_t sub_mod_index = 0; sub_mod_index < MAX_SUB_MOD_SIZE + 1; ++sub_mod_index) {
      log_level_[par_mod_id][sub_mod_index] = level;
    }
  } else {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WDIAG("Invalid mod id", K(ret), K(par_mod_id));
  }
  return ret;
}


int ObLogIdLevelMap::set_level(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                               const int8_t level)
{
  int ret = OB_SUCCESS;
  if (par_mod_id < MAX_PAR_MOD_SIZE && sub_mod_id < MAX_SUB_MOD_SIZE) {
    log_level_[par_mod_id][sub_mod_id + 1] = level;
  } else {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WDIAG("Invalid mod id", K(ret), K(par_mod_id), K(sub_mod_id));
  }
  return ret;
}

ObLogNameIdMap::ObLogNameIdMap()
{
  for (uint64_t par_index = 0; par_index < MAX_PAR_MOD_SIZE; ++par_index) {
    for (uint64_t sub_index = 0; sub_index < MAX_SUB_MOD_SIZE; ++sub_index) {
      name_id_map_[par_index][sub_index] = NULL;
    }
  }
}

int ObLogNameIdMap::register_mod(const uint64_t mod_id, const char *mod_name)
{
  int ret = OB_SUCCESS;
  if (mod_id >= MAX_PAR_MOD_SIZE || NULL == mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid mod id and name", K(ret), K(mod_id), K(mod_name));
  } else {
    name_id_map_[mod_id][0] = mod_name;
  }
  return ret;
}

int ObLogNameIdMap::register_mod(const uint64_t mod_id,
                                 const uint64_t sub_mod_id,
                                 const char *sub_mod_name)
{
  int ret = OB_SUCCESS;
  if (mod_id >= MAX_PAR_MOD_SIZE || sub_mod_id >= MAX_SUB_MOD_SIZE || NULL == sub_mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid mod id or name", K(ret), K(mod_id), K(sub_mod_id), K(sub_mod_name));
  } else {
    name_id_map_[mod_id][sub_mod_id + 1] = sub_mod_name;
  }
  return ret;
}

int ObLogNameIdMap::get_mod_id(const char *mod_name,
                               const char *sub_mod_name,
                               uint64_t &par_mod_id,
                               uint64_t &sub_mod_id) const
{
  int ret = OB_SUCCESS;
  par_mod_id = 0;
  sub_mod_id = 0;

  if (NULL == mod_name || NULL == sub_mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid argument", K(ret), K(mod_name), K(sub_mod_name));
  } else if (OB_FAIL(get_mod_id(mod_name, par_mod_id))) {
    LOG_WDIAG("Failed to get mod id", K(ret), K(mod_name));
  } else if (OB_UNLIKELY(par_mod_id >= MAX_PAR_MOD_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("Get invalid par mod id", K(ret));
  } else {
    //find sub_mod_id
    bool find_mod = false;
    for (uint64_t idx = 0; OB_SUCC(ret) && !find_mod && idx < MAX_SUB_MOD_SIZE; ++idx) {
      if (NULL == name_id_map_[par_mod_id][idx + 1]) {
        //do nothing
      } else if (0 == STRCASECMP(sub_mod_name, name_id_map_[par_mod_id][idx + 1])) {
        sub_mod_id = idx;
        find_mod = true;
      } else {
        //do nothing
      }
    }//end of for

    if (!find_mod) {
      ret = OB_LOG_MODULE_UNKNOWN;
      LOG_WDIAG("Failed to find sub_mod", K(ret), K(mod_name), K(sub_mod_name));
    }
  }
  return ret;
}

int ObLogNameIdMap::get_mod_id(const char *mod_name, uint64_t &par_mod_id) const
{
  int ret = OB_SUCCESS;
  par_mod_id = 0;

  if (NULL == mod_name) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid argument", K(ret), K(mod_name));
  } else {
    bool find_mod = false;
    for (uint64_t idx = 0; OB_SUCC(ret) && !find_mod && idx < MAX_PAR_MOD_SIZE; ++idx) {
      if (NULL == name_id_map_[idx][0]) {
        //do nothing
      } else if (0 == STRCASECMP(mod_name, name_id_map_[idx][0])) {
        par_mod_id = idx;
        find_mod = true;
      } else {
        //do nothing
      }
    }
    if (!find_mod) {
      ret = OB_LOG_MODULE_UNKNOWN;
      LOG_WDIAG("Failed to find sub_mod", K(ret), K(mod_name));
    }
  }
  return ret;
}

int ObLogNameIdMap::get_par_mod_name(const uint64_t par_mod_id, const char *&mod_name) const
{
  int ret = OB_SUCCESS;
  mod_name = NULL;
  if (par_mod_id >= MAX_PAR_MOD_SIZE) {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WDIAG("Invalid par mod id", K(ret), K(par_mod_id));
  } else {
    mod_name = name_id_map_[par_mod_id][0];
  }
  return ret;
}

int ObLogNameIdMap::get_sub_mod_name(const uint64_t par_mod_id, const uint64_t sub_mod_id,
    const char *&mod_name) const
{
  int ret = OB_SUCCESS;
  mod_name = NULL;
  if (par_mod_id >= MAX_PAR_MOD_SIZE || sub_mod_id >= MAX_SUB_MOD_SIZE) {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WDIAG("Invalid mod id", K(ret), K(par_mod_id), K(sub_mod_id));
  } else {
    mod_name = name_id_map_[par_mod_id][sub_mod_id + 1];
  }
  return ret;
}

int64_t ObLogger::FileName::to_string(char * buff, const int64_t len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buff) || OB_UNLIKELY(len <= 0)) {
  } else {
    pos = snprintf(buff, len, "%s", file_name_);
    if (OB_UNLIKELY(pos < 0)) {
      pos = 0;
    } else if (OB_UNLIKELY(pos >= len)) {
      pos = len - 1;
    } else { }//do nothing
  }
  return pos;
}

void ObLogger::LogBuffer::reset()
{
  pos_ = 0;
  trace_mode_ = false;
}

void ObLogger::LogBuffer::set_trace_mode(bool trace_mode)
{
  pos_ = 0;
  trace_mode_ = trace_mode;
}

void ObLogger::LogBufferMgr::reset()
{
  idx_ = 0;
  trace_mode_ = false;
  for (uint64_t i = 0; i < MAX_THREAD_LOG_NUM; ++i) {
    buffers_[i].reset();
  }
}

void ObLogger::LogBufferMgr::set_trace_mode(bool trace_mode)
{
  trace_mode_ = trace_mode;
  idx_ = 0;
  for (uint64_t i = 0; i < MAX_THREAD_LOG_NUM; ++i) {
    buffers_[i].reset();
  }
}

ObLogger::LogBuffer &ObLogger::LogBufferMgr::get_buffer()
{
  uint64_t buf_id = idx_ % MAX_THREAD_LOG_NUM;
  if (trace_mode_) {//trace mode
    if (buffers_[buf_id].trace_mode_) { //in use
      if ((MAX_LOG_SIZE - buffers_[buf_id].pos_) < REMAIN_TRACE_LOG_SIZE) {//no enough space
        buf_id = (++idx_) % MAX_THREAD_LOG_NUM; //use new LogBuffer
        if (buffers_[buf_id].trace_mode_) { //new LogBuffer already used
          OB_LOGGER.log_data("", OB_LOG_LEVEL_INFO, LogLocation("", 0, ""), buffers_[buf_id]);//log the LogBuffer used
          buffers_[buf_id].reset();//reset, than reuse the LogBuffer
        }
        buffers_[buf_id].set_trace_mode(true);//set new LogBuffer
      }
    } else {//not is use
      buffers_[buf_id].set_trace_mode(true);
    }
  } else {//normal mode
    buffers_[buf_id].set_trace_mode(false);
    ++idx_;//Next time,Get next buffer
  }
  return buffers_[buf_id];
}

void ObLogger::LogBufferMgr::print_trace_buffer(
    int32_t level,
    const char *file,
    int32_t line,
    const char *function)
{
  for (uint64_t i = 0; i < MAX_THREAD_LOG_NUM; ++i) {
    if (buffers_[i].trace_mode_) {
      OB_LOGGER.log_data("", level, LogLocation(file, line, function), buffers_[i]);
      buffers_[i].set_trace_mode(false);
    }
  }
}

ObLogger::LogBufferMgr *ObLogger::get_buffer_mgr()
{
  static __thread LogBufferMgr *log_buf_mgr = NULL;
  if (OB_UNLIKELY(NULL == log_buf_mgr)) {
    log_buf_mgr = new(std::nothrow) LogBufferMgr();
  }
  return log_buf_mgr;
}

const char *const ObLogger::errstr_[] = {"ERROR", "USER_ERR", "WARN", "INFO", "EDIAG", "WDIAG", "TRACE", "DEBUG"};
ObLogger::ObLogger()
: log_file_(), max_file_size_(0), name_id_map_(), id_level_map_(),
  syslog_level_(OB_LOG_LEVEL_WDIAG), monitor_level_(OB_LOG_LEVEL_WARN), xflush_level_(OB_LOG_LEVEL_WARN),
  wf_level_(OB_LOG_LEVEL_WARN), level_version_(0), disable_thread_log_level_(true), force_check_(false), redirect_flag_(false),
  can_print_(true), stop_flush_(false), enable_async_log_(true), stop_append_log_(false),
  async_log_queue_(NULL), last_async_flush_count_per_sec_(0), async_tid_(0),
  callback_handler_(NULL), syslog_io_bandwidth_limit_(INT64_MAX), left_syslog_io_bandwidth_(INT64_MAX),
  start_bandwidth_time_(0), syslog_start_out_of_limit_(0), syslog_out_of_limit_cnt_(0)
{
  id_level_map_.set_level(OB_LOG_LEVEL_ERROR);

  (void)pthread_mutex_init(&file_size_mutex_, NULL);

  for (ObLogItemType i = LOG_ITEM_TINY; i < MAX_LOG_ITEM_TYPE; i = static_cast<ObLogItemType>(static_cast<int32_t>(i) + 1)) {
    free_item_queue_[i] = NULL;
  }
  memset(dropped_log_count_, 0, sizeof(dropped_log_count_));
}

ObLogger::~ObLogger()
{
  stop_append_log_ = true;
  destroy_free_litem_queue();

  if (NULL != async_log_queue_) {
    async_log_queue_->destroy();
    ob_free(async_log_queue_);
    async_log_queue_ = NULL;
  }

  (void)pthread_mutex_destroy(&file_size_mutex_);
}

void ObLogger::destroy_async_log_thread()
{
  set_enable_async_log(false);
  set_stop_flush();
  if (0 != async_tid_) {
    LOG_STDOUT("Stop thread, %ld.\n", async_tid_);

    pthread_join(async_tid_, NULL);
    async_tid_ = 0;
  }
}

void ObLogger::destroy_free_litem_queue()
{
  int ret = OB_SUCCESS;
  for (ObLogItemType i = LOG_ITEM_TINY; i < MAX_LOG_ITEM_TYPE;  i = static_cast<ObLogItemType>(static_cast<int32_t>(i) + 1)) {
    if (NULL != free_item_queue_[i]) {
      if (free_item_queue_[i]->is_inited()) {
        void *item = NULL;
        while (OB_SUCC(free_item_queue_[i]->pop(item))) {
          ObLogItemFactory::release((ObLogItem *)(item));
          item = NULL;
        }
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_STDERR("fail to pop from free_item_queue. ret = %d\n", ret);
        }
      }
      free_item_queue_[i]->destroy();
      ob_free(free_item_queue_[i]);
      free_item_queue_[i] = NULL;
    }
  }
}

void ObLogger::set_trace_mode(bool trace_mode)
{
  LogBufferMgr *buf_mgr = get_buffer_mgr();
  if (NULL != buf_mgr) {
    buf_mgr->set_trace_mode(trace_mode);
  }
}

bool ObLogger::get_trace_mode()
{
  bool bret = false;
  LogBufferMgr *buf_mgr = get_buffer_mgr();
  if (NULL != buf_mgr) {
    bret = buf_mgr->trace_mode_;
  }
  return bret;
}

void ObLogger::print_trace_buffer(
    int32_t level,
    const char *file,
    int32_t line,
    const char *function)
{
  if (!is_async_log_used()) {
    LogBufferMgr *buf_mgr = get_buffer_mgr();
    if (NULL != buf_mgr) {
      buf_mgr->print_trace_buffer(level, file, line, function);
    }
  }
}

void ObLogger::set_log_level(const char *level, const char *wf_level, int64_t version)
{
  int ret = OB_SUCCESS;
  if (check_and_set_level_version(version)) {
    if (NULL != level) {
      int8_t level_int = OB_LOG_LEVEL_INFO;
      if (OB_SUCC(level_str2int(level, level_int))) {
        set_log_level(level_int);
      }
    }

    if (NULL != wf_level) {
      int8_t level_int = OB_LOG_LEVEL_INFO;
      if (OB_SUCC(level_str2int(wf_level, level_int))) {
        wf_level_ = level_int;
      }
    }
  }
}

void ObLogger::set_log_level(const int8_t level, int64_t version)
{
  if (check_and_set_level_version(version)) {
    if (level >= 0 && level < static_cast<int8_t>(sizeof(errstr_) / sizeof(char *))) {
      id_level_map_.set_level(level);
    }
  }
}

void ObLogger::set_file_name(const ObLogFDType type,
                             const char *filename,
                             const bool redirect_flag,
                             const bool open_wf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(type >= MAX_FD_FILE) || OB_ISNULL(filename)) {
    LOG_STDERR("invalid argument type = %d, log_file = %p\n", type, filename);
  } else {
    if (FD_DEFAULT_FILE == type) {
      redirect_flag_ = redirect_flag;
      ret = log_file_[type].open(filename, open_wf, redirect_flag);
    } else {
      ret = log_file_[type].open(filename, false, false);
    }

    if (OB_FAIL(ret)) {
      LOG_STDERR("fail to open log_file = %p, ret=%d\n", filename, ret);
    }
  }
}

void ObLogger::set_enable_wf(const ObLogFDType type,
                             const bool open_wf,
                             const bool enable_wf)
{
  if (OB_UNLIKELY(type >= MAX_FD_FILE)) {
    LOG_STDERR("invalid argument type =%d", type);
  } else {
    log_file_[type].open_wf_flag_ = open_wf;
    log_file_[type].enable_wf_flag_ = enable_wf;
  }
}

int ObLogger::reopen_monitor_log()
{
  int ret = OB_SUCCESS;

  for (ObLogFDType type = FD_XFLUSH_FILE; OB_SUCC(ret) && type < MAX_FD_FILE; type = (ObLogFDType)(type + 1)) {
    if (OB_FAIL(log_file_[type].reopen(false))) {
      LOG_STDERR("fail to reopen log_file = %p, ret=%d\n", log_file_[type].filename_, ret);
    }
  }
  return ret;
}

ObLogger::LogBuffer *ObLogger::get_thread_buffer()
{
  LogBuffer *log_buffer = NULL;
  if (OB_ISNULL(get_buffer_mgr())) {
  } else {
    log_buffer = &(get_buffer_mgr()->get_buffer());
  }
  return log_buffer;
}

char NEWLINE[1] = {'\n'};

void ObLogger::log_message(const char *mod_name,
                           int32_t level,
                           const char *file,
                           int32_t line,
                           const char *function,
                           const char *fmt,
                           ...)
{
  if (OB_LIKELY(is_enable_logging())) {
    const ObLogFDType type = (NULL == mod_name ? FD_XFLUSH_FILE : FD_DEFAULT_FILE);
    va_list args;
    va_start(args, fmt);
    log_message(type, mod_name, level, file, line, function, fmt, args);
    va_end(args);
  }
}

void ObLogger::log_message(const ObLogFDType type,
                           const char *mod_name,
                           int32_t level,
                           const char *file,
                           int32_t line,
                           const char *function,
                           const char *fmt,
                           ...)
{
  if (OB_LIKELY(is_enable_logging())) {
    va_list args;
    va_start(args, fmt);
    log_message(type, mod_name, level, file, line, function, fmt, args);
    va_end(args);
  }
}

void ObLogger::log_message(const ObLogFDType type,
                           const char *mod_name,
                           int32_t level,
                           const char *file,
                           int32_t line,
                           const char *function,
                           const char *fmt,
                           va_list args)
{
  if (OB_LIKELY(is_enable_logging())) {
    set_disable_logging(true);
    LogBuffer *log_buffer = get_thread_buffer();
    if (OB_LIKELY(NULL != log_buffer)
        && OB_LIKELY(log_buffer->pos_ >= 0)
        && OB_LIKELY(log_buffer->pos_ < MAX_LOG_SIZE)) {
      if (log_buffer->trace_mode_) {
        log_head_info(type, mod_name, level, LogLocation(file, line, function), *log_buffer);
      }
      (void)logdata_vprintf(log_buffer->buffer_, MAX_LOG_SIZE, log_buffer->pos_, fmt, args);
      log_buffer->trace_mode_ ? log_tail(level, *log_buffer)
                              : log_data(type, mod_name, level, LogLocation(file, line, function), *log_buffer);
    }
    set_disable_logging(false);
  }
}

void ObLogger::log_user_message(
    const UserMsgLevel user_msg_level,
    const int errcode,
    const char *fmt,
    ...)
{
  char buf[ObWarningBuffer::WarningItem::STR_LEN] = {};
  va_list args;
  va_start(args, fmt);
  int64_t len = vsnprintf(buf, ObWarningBuffer::WarningItem::STR_LEN, fmt, args);
  va_end(args);
  insert_warning_buffer(user_msg_level, errcode, buf, len);
}

bool ObLogger::check_and_set_level_version(int64_t version)
{
  bool refresh_level = true;
  if (version <= 0) {
    //do nothing
  } else if (version > level_version_) {
    level_version_ = version;
  } else {
    refresh_level = false;
  }
  return refresh_level;
}

void ObLogger::log_tail(const int32_t level, LogBuffer &log_buffer)
{
  if (OB_LIKELY(log_buffer.pos_ >= 0)) {
    if (OB_UNLIKELY(OB_LOG_LEVEL_EDIAG == level)) {
      const char *bt = oceanbase::common::lbt();
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_SIZE, log_buffer.pos_,
                           " BACKTRACE:%s", bt);
    }
    if (log_buffer.pos_ >= MAX_LOG_SIZE) {
      log_buffer.pos_ = MAX_LOG_SIZE - 1;
    }
    while (log_buffer.pos_ > 0 && '\n' == log_buffer.buffer_[log_buffer.pos_ - 1]) {
      --log_buffer.pos_;
    }
    log_buffer.buffer_[log_buffer.pos_] = '\n';
    ++log_buffer.pos_;
  }
}

void ObLogger::log_head_info(const ObLogFDType type,
                             const char *mod_name,
                             int32_t level,
                             LogLocation location,
                             LogBuffer &log_buffer)
{
  if (level >= 0 && level < static_cast<int>(sizeof(errstr_) / sizeof(char *))
      && NULL != location.file_ && NULL != location.function_) {
    //only print base filename.
    const char *base_file_name = strrchr(location.file_, '/');
    base_file_name = (NULL != base_file_name) ? base_file_name + 1 : location.file_;

    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    struct tm tm;
    ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(tv.tv_sec), &tm);

    const uint64_t *trace_id = ObCurTraceId::get();
    uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
    uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];
    if (FD_DIAGNOSIS_FILE == type) {
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_SIZE, log_buffer.pos_,
                           "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                           "[%ld][" TRACE_ID_FORMAT "] ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec, GETTID(), trace_id_0, trace_id_1);
    } else if (FD_XFLUSH_FILE == type) {
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_HEAD_SIZE, log_buffer.pos_,
                           "%04d-%02d-%02d %02d:%02d:%02d.%06ld [%s] ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec, errstr_[level]);
    } else if (FD_CONFIG_FILE == type) { // header for config file
      /** Format for config_log is: '###${content}'
       *    '###' is consultation flag for Inspection Module
       *    ${content} is value of config in json format. */
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_HEAD_SIZE, log_buffer.pos_, "###");
    } else if (is_monitor_file(type)) {
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_HEAD_SIZE, log_buffer.pos_,
                           "%04d-%02d-%02d %02d:%02d:%02d.%06ld,",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec);
    } else if (level < OB_LOG_LEVEL_INFO) {
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_SIZE, log_buffer.pos_,
                           "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                           "%-5s %s%s (%s:%d) [%ld][" TRACE_ID_FORMAT "] ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec, errstr_[level], mod_name, location.function_,
                           base_file_name, location.line_, GETTID(), trace_id_0, trace_id_1);
    } else {
      (void)logdata_printf(log_buffer.buffer_, MAX_LOG_SIZE, log_buffer.pos_,
                           "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                           "%-5s %s%s:%d [%ld][" TRACE_ID_FORMAT "] ",
                           tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                           tm.tm_sec, tv.tv_usec, errstr_[level], mod_name, base_file_name,
                           location.line_, GETTID(), trace_id_0, trace_id_1);
    }
  }
}

void ObLogger::log_data(const char *mod_name,
                        int32_t level,
                        LogLocation location,
                        LogBuffer &log_buffer)
{
  const ObLogFDType type = (NULL == mod_name ? FD_XFLUSH_FILE : FD_DEFAULT_FILE);
  log_data(type, mod_name, level, location, log_buffer);
}

void ObLogger::log_data(const ObLogFDType type,
                        const char *mod_name,
                        int32_t level,
                        LogLocation location,
                        LogBuffer &log_buffer)
{
  char *data = log_buffer.buffer_;
  const int64_t data_size = MAX_LOG_SIZE;
  int64_t data_len = log_buffer.pos_;
  if (level >= 0 && level < static_cast<int>(sizeof(errstr_) / sizeof(char *))
      && NULL != location.file_ && NULL != location.function_ && NULL != data
      && data_size > 0 && data_len > 0
      && !stop_append_log_) {
    static __thread int64_t last_msg_time = 0;
    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    int64_t b_ts = static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000)
                    + static_cast<int64_t>(tv.tv_usec);
    //check disk has enough space
    if (b_ts > (last_check_disk_ts + DISK_SAMPLE_TIME)) {
      last_check_disk_ts = b_ts;
      struct statfs disk_info;
      if (0 == statfs(log_file_[FD_DEFAULT_FILE].filename_, &disk_info)) {
        can_print_ = ((disk_info.f_bfree * disk_info.f_bsize) > CAN_PRINT_DISK_SIZE);
      }
    }
    if (can_print_) {
      const int64_t lcf_ts = last_check_file_ts;
      if (force_check_ || b_ts > (lcf_ts + FILE_SAMPLE_TIME)) {
        if (ATOMIC_BCAS(&last_check_file_ts, lcf_ts, b_ts)) {
          check_file();
        }
      }

      //only print base filename.
      const char *base_file_name = strrchr(location.file_, '/');
      base_file_name = (NULL != base_file_name) ? base_file_name + 1 : location.file_;

      if (data_len >= data_size) {
        data_len = data_size - 1;
      }
      while (data_len > 0 && data[data_len - 1] == '\n') {
        data_len--;
      }
      data[data_len] = '\0';

      struct tm tm;
      ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(tv.tv_sec), &tm);

      const int32_t MAX_LOG_HEAD_SIZE = 256;
      char head[MAX_LOG_HEAD_SIZE];
      int32_t head_size = 0;
      const uint64_t *trace_id = ObCurTraceId::get();
      uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
      uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];
      if (FD_DIAGNOSIS_FILE == type) {
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE,
                             "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                             "[%ld][" TRACE_ID_FORMAT "] ",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                             tm.tm_sec, tv.tv_usec, GETTID(), trace_id_0, trace_id_1);
      } else if (FD_XFLUSH_FILE == type) {
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE, "%04d-%02d-%02d %02d:%02d:%02d.%06ld [%s] ",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                             tm.tm_sec, tv.tv_usec, errstr_[level]);
      } else if (FD_CONFIG_FILE == type) {
        /** Format for config_log is: '###${content}'
         *    '###' is consultation flag for Inspection Module
         *    ${content} is value of config in json format. */
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE, "###"); //just print '###'
      } else if (FD_TRACE_FILE == type) {
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE,
                             "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                             "%s (%s:%d) [%ld][" TRACE_ID_FORMAT "] ",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec,
                             location.function_, base_file_name, location.line_, GETTID(), trace_id_0, trace_id_1);
      } else if (is_monitor_file(type)) {
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE, "%04d-%02d-%02d %02d:%02d:%02d.%06ld,",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                             tm.tm_sec, tv.tv_usec);
      } else if (level < OB_LOG_LEVEL_INFO) {
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE,
                             "[%04d-%02d-%02d "
                             "%02d:%02d:%02d.%06ld] "
                             "%-5s %s%s "
                             "(%s:%d) "
                             "[%ld][" TRACE_ID_FORMAT "] [lt=%ld] ",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                             tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec,
                             errstr_[level], mod_name, location.function_,
                             base_file_name, location.line_,
                             GETTID(), trace_id_0, trace_id_1, last_msg_time);
        if (NULL != callback_handler_) {
          callback_handler_(level, (tv.tv_sec * 1000000 + tv.tv_usec), head, head_size, data, data_len);
        }

      } else {
        head_size = snprintf(head, MAX_LOG_HEAD_SIZE,
                             "[%04d-%02d-%02d "
                             "%02d:%02d:%02d.%06ld] "
                             "%-5s %s%s:%d "
                             "[%ld][" TRACE_ID_FORMAT "] [lt=%ld] ",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                             tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec,
                             errstr_[level], mod_name, base_file_name, location.line_,
                             GETTID(), trace_id_0, trace_id_1, last_msg_time);
      }

      if (OB_UNLIKELY(head_size < 0)) {
        head_size = 0;
      } else if (OB_UNLIKELY(head_size >= MAX_LOG_HEAD_SIZE)) {
        head_size = MAX_LOG_HEAD_SIZE - 1;
      } else { } //do nothing

      struct iovec vec[5];
      int iovcnt = 3;
      vec[0].iov_base = head;
      vec[0].iov_len = head_size;
      vec[1].iov_base = data;
      vec[1].iov_len = data_len;
      vec[2].iov_base = NEWLINE;
      vec[2].iov_len = sizeof(NEWLINE);

      if (OB_LIKELY(data_len > 0)) {
        if (OB_UNLIKELY(OB_LOG_LEVEL_EDIAG == level) && FD_DEFAULT_FILE == type) {
          // print backtrace for error message
          iovcnt = 5;
          vec[3].iov_base = oceanbase::common::lbt();
          vec[3].iov_len = strlen(static_cast<char*>(vec[3].iov_base));
          static const char* const BACKTRACE_END = "\nBACKTRACE END\n";
          vec[4].iov_base = (void*)BACKTRACE_END;
          vec[4].iov_len = strlen(BACKTRACE_END);
        }
        int logfd = log_file_[type].fd_;
        int wf_logfd = log_file_[type].wf_fd_;

        ssize_t size = 0;
        size = ::writev(logfd, vec, iovcnt);
        size = size > 0 ? size : 0;
        if (log_file_[type].enable_wf_flag_ && log_file_[type].open_wf_flag_ && level <= wf_level_) {
          ssize_t wf_size = 0;
          wf_size = ::writev(wf_logfd, vec, iovcnt);
          wf_size = wf_size > 0 ? wf_size : 0;
        }
        (void)ATOMIC_AAF(&log_file_[type].write_size_, size);
        (void)ATOMIC_AAF(&log_file_[type].file_size_, size);
        (void)ATOMIC_AAF(&log_file_[type].write_count_, 1);
        if (OB_LIKELY(need_auto_rotate_log_by_size(type))) {
          const bool redirect_flag = (FD_DEFAULT_FILE == type ? redirect_flag_ : false);
          rotate_log(size, redirect_flag, log_file_[type]);
        }
      }
      int64_t e_ts = ::oceanbase::common::ObTimeUtility::current_time();
      last_msg_time = e_ts - b_ts;
    }
  }
}

void ObLogger::rotate_log(const int64_t size, const bool redirect_flag, ObLogFileStruct &log_struct)
{
  if (OB_LIKELY(size > 0) && max_file_size_ > 0 && log_struct.file_size_ >= max_file_size_) {
    if (OB_LIKELY(0 == pthread_mutex_trylock(&file_size_mutex_))) {
      if (log_struct.file_size_ >= max_file_size_) {
        rotate_log(log_struct.filename_, NULL, redirect_flag,
                   log_struct.open_wf_flag_, log_struct.enable_wf_flag_,
                   log_struct.fd_, log_struct.wf_fd_);
        (void)ATOMIC_SET(&log_struct.file_size_, 0);
      }
      (void)pthread_mutex_unlock(&file_size_mutex_);
    }
  }
}

void ObLogger::force_rotate_log(const ObLogFDType &type, const char *version)
{
  if (!need_auto_rotate_log_by_size(type)) {
    ObLogFileStruct &log_struct = log_file_[type];
    if (OB_LIKELY(OB_NOT_NULL(version))) {
      if (OB_LIKELY(0 == pthread_mutex_trylock(&file_size_mutex_))) {
          rotate_log(log_struct.filename_, version, false,
                     log_struct.open_wf_flag_, log_struct.enable_wf_flag_,
                     log_struct.fd_, log_struct.wf_fd_);
          (void)ATOMIC_SET(&log_struct.file_size_, 0);
      }
      (void)pthread_mutex_unlock(&file_size_mutex_);
    }
  }
}

void ObLogger::rotate_log(const char *filename,
                          const char *fmt,
                          const bool redirect_flag,
                          const bool open_wf_flag,
                          const bool enable_wf_flag,
                          int32_t &fd,
                          int32_t &wf_fd)
{
  int ret = OB_SUCCESS;
  if (NULL != filename) {
    char wf_filename[ObLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
    memset(wf_filename, 0, sizeof(wf_filename));
    //Need to think how to deal failure.
    snprintf(wf_filename, sizeof(wf_filename), "%s.wf", filename);
    if (access(filename, R_OK) == 0) {
      char old_log_file[ObLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
      char old_wf_log_file[ObLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
      memset(old_log_file, 0, sizeof(old_log_file));
      memset(old_wf_log_file, 0, sizeof(old_wf_log_file));
      if (fmt != NULL) {
        snprintf(old_log_file, sizeof(old_log_file), "%s.%s", filename, fmt);
        snprintf(old_wf_log_file, sizeof(old_wf_log_file), "%s.%s", wf_filename, fmt);
      } else {
        time_t t = 0;
        time(&t);
        struct tm tm;
        ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(t), &tm);

        snprintf(old_log_file, sizeof(old_log_file), "%s.%04d%02d%02d%02d%02d%02d",
                filename, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                tm.tm_hour, tm.tm_min, tm.tm_sec);
        snprintf(old_wf_log_file, sizeof(old_wf_log_file), "%s.%04d%02d%02d%02d%02d%02d",
                 wf_filename, tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec);
      }

      ret = rename(filename, old_log_file); //If failed, TODO
      int tmp_fd = open(filename, O_WRONLY | O_CREAT | O_APPEND, ObLogFileStruct::LOG_FILE_MODE);
      if (redirect_flag) {
        dup2(tmp_fd, STDERR_FILENO);
        dup2(tmp_fd, STDOUT_FILENO);
        if (fd > STDERR_FILENO) {
          dup2(tmp_fd, fd);
          close(tmp_fd);
        } else {
          fd = tmp_fd;
        }
      } else {
        if (fd > STDERR_FILENO) {
          dup2(tmp_fd, fd);
          close(tmp_fd);
        } else {
          fd = tmp_fd;
        }
      }

      if (open_wf_flag && enable_wf_flag) {
        ret = rename(wf_filename, old_wf_log_file); //If failed, TODO
        tmp_fd = open(wf_filename, O_WRONLY | O_CREAT | O_APPEND, ObLogFileStruct::LOG_FILE_MODE);
        if (wf_fd > STDERR_FILENO) {
          dup2(tmp_fd, wf_fd);
          close(tmp_fd);
        } else {
          wf_fd = tmp_fd;
        }
      }
    }
  }
  UNUSED(ret);
}

void ObLogger::check_file()
{
  for (ObLogFDType type = FD_DEFAULT_FILE; type < MAX_FD_FILE; type = (ObLogFDType)(type + 1)) {
    if (FD_DEFAULT_FILE == type) {
      check_file(log_file_[type], redirect_flag_);
    } else {
      check_file(log_file_[type], false);
    }
  }
}

void ObLogger::check_file(ObLogFileStruct &log_struct, const bool redirect_flag)
{
  if (log_struct.is_opened()) {
    struct stat st_file;
    int err = stat(log_struct.filename_, &st_file);
    if ((err == -1 && errno == ENOENT)
        || (err == 0 && (st_file.st_dev != log_struct.stat_.st_dev || st_file.st_ino != log_struct.stat_.st_ino))) {
      log_struct.reopen(redirect_flag);
    }

    if (log_struct.open_wf_flag_) {
      char wf_file_name[ObLogFileStruct::MAX_LOG_FILE_NAME_SIZE + 3];
      memset(wf_file_name, 0, sizeof(wf_file_name));
      snprintf(wf_file_name, sizeof(wf_file_name), "%s.wf", log_struct.filename_);
      err = stat(wf_file_name, &st_file);
      if ((err == -1 && errno == ENOENT)
          || (err == 0 && (st_file.st_dev != log_struct.wf_stat_.st_dev || st_file.st_ino != log_struct.wf_stat_.st_ino))) {
        log_struct.reopen_wf();
      }
    }
  }
}

int ObLogger::register_mod(const uint64_t par_mod_id, const char *par_mod_name)
{
  return name_id_map_.register_mod(par_mod_id, par_mod_name);
}

int ObLogger::register_mod(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                           const char *sub_mod_name)
{
  return name_id_map_.register_mod(par_mod_id, sub_mod_id, sub_mod_name);
}

void ObLogger::set_max_file_size(int64_t max_file_size)
{
  //max file size 1GB
  if (max_file_size < 0x0 || max_file_size > 0x40000000) {
    max_file_size = 0x40000000;//1GB
  }
  max_file_size_ = max_file_size;
}

//@brief string copy with dst's length and src's length checking and src trim.
int64_t str_copy_trim(char *dst,
                      const int64_t dst_length,
                      const char *src,
                      const int64_t src_length)
{
  int64_t length = 0;
  if (NULL != dst && NULL != src && dst_length > 0) {
    length = src_length;
    //left trim
    while (length != 0 && isspace(*src)) {
      length--;
      src++;
    }
    //right trim
    while (length != 0 && isspace(*(src + length - 1))) {
      length--;
    }
    length = (dst_length - 1) > length ? length : (dst_length - 1);
    MEMCPY(dst, src, length);
    dst[length] = '\0';
  }
  return length;
}

//@brief get sub-string from p_start to the location of delimiter
int get_delim_str(const char *&p_start,
                  const char *const p_end,
                  char delim,
                  char *dst_str,
                  const int32_t dst_str_size)
{
  int ret = 0;
  const char *p_delim = NULL;
  if ((p_start >= p_end) || (NULL == (p_delim = strchr(p_start, delim)))) {
    ret = -1;
  } else {
    str_copy_trim(dst_str, dst_str_size, p_start, p_delim - p_start);
    p_start = p_delim + 1;
  }
  return ret;
}

int ObLogger::parse_check(const char *str, const int32_t str_length)
{
  int32_t valid_length = 0;
  return parse_check(str, str_length, valid_length, NULL);
}

int ObLogger::parse_check(const char *str, const int32_t str_length, int32_t &valid_length)
{
  return parse_check(str, str_length, valid_length, NULL);
}

int ObLogger::parse_check(const char *str,
                          const int32_t str_length,
                          int32_t &valid_length,
                          void *mod_setting_list)
{
  int ret = OB_SUCCESS;
  char buffer[OB_MAX_CONFIG_VALUE_LEN];
  valid_length = 0;
  const int32_t MAX_MOD_NAME_LENGTH = 20;
  const int32_t MAX_LEVEL_NAME_LENGTH = 10;
  ObList<ModSetting> *list = NULL;
  if (NULL != mod_setting_list) {
    list = static_cast<ObList<ModSetting> *>(mod_setting_list);
  }
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    str_copy_trim(buffer, OB_MAX_CONFIG_VALUE_LEN, str, str_length);
    //check log_level = "level", to set all modules.
    if (strchr(buffer, ':') == NULL) {
      int8_t level_int = 0;
      if (OB_FAIL(level_str2int(buffer, level_int))) {
        OB_LOG(WDIAG, "failed to get level_int", K(buffer), K(str), K(str_length), K(ret));
      } else {
        if (NULL != list) {
          ModSetting mod_set(ModSetting::NON_SUBMOD_ID, ModSetting::NON_SUBMOD_ID, level_int);
          list->push_back(mod_set);
        }
        valid_length = str_length;
      }
    } else { //check log_level like "ALL.*:INFO, COMMON.*:ERROR", to set some modules
      char par_mod[MAX_MOD_NAME_LENGTH];
      char sub_mod[MAX_MOD_NAME_LENGTH];
      char level[MAX_LEVEL_NAME_LENGTH];
      const char *p_start = buffer;
      const char *const p_end = buffer + strlen(buffer);

      while (OB_SUCC(ret) && p_start < p_end) {
        //get par-module name
        if (0 != get_delim_str(p_start, p_end, '.', par_mod, MAX_MOD_NAME_LENGTH)) {
          ret = OB_LOG_PARSER_SYNTAX_ERR;
        } else if (0 != get_delim_str(p_start, p_end, ':', sub_mod, MAX_MOD_NAME_LENGTH)) {
          ret = OB_LOG_PARSER_SYNTAX_ERR;
        } else if (0 != get_delim_str(p_start, p_end, ',', level, MAX_LEVEL_NAME_LENGTH)
                   && 0 != get_delim_str(p_start, p_end, ';', level, MAX_LEVEL_NAME_LENGTH)) {
          if (p_start >= p_end) {
            ret = OB_LOG_PARSER_SYNTAX_ERR;
          } else {
            str_copy_trim(level, MAX_LEVEL_NAME_LENGTH, p_start, p_end - p_start);
            p_start = p_end;
          }
        } else {
          //do nothing
        }

        if (OB_SUCC(ret)) {
          ModSetting mod_set;
          if (OB_FAIL(get_mod_set(par_mod, sub_mod, level, mod_set))) {
            LOG_WDIAG("Get mod set error", K(ret));
            if (NULL != list) {
              list->reset();
            }
          } else {
            if (NULL != list) {
              if (OB_FAIL(list->push_back(mod_set))) {
                LOG_WDIAG("Failed to add mod set to list", K(ret));
              }
            }
            int64_t valid_length_tmp = p_start - buffer;
            if (valid_length_tmp > 0 && valid_length_tmp <= static_cast<int64_t>(str_length)) {
              valid_length = static_cast<int32_t>(valid_length_tmp);
            }
          }
        }
      }//end of while
    }
  }
  if (OB_LOG_PARSER_SYNTAX_ERR == ret
      || OB_LOG_MODULE_UNKNOWN == ret
      || OB_LOG_LEVEL_INVALID == ret) {
    LOG_USER_ERROR(ret, str_length - valid_length, str + valid_length);
  } else if (OB_INVALID_ARGUMENT == ret) {
    LOG_USER_ERROR(ret, "log_level");
  } else {
    //do nothing
  }
  return ret;
}

int ObLogger::parse_set(const char *str, const int32_t str_length, int64_t version)
{
  int ret = OB_SUCCESS;
  int valid_length = 0;
  if (check_and_set_level_version(version)) {
    ret = parse_set(str, str_length, valid_length, id_level_map_);
  }
  return ret;
}

int ObLogger::parse_set_with_valid_ret(const char *str, const int32_t str_length, int32_t &valid_length, int64_t version)
{
  int ret = OB_SUCCESS;
  if (check_and_set_level_version(version)) {
    ret = parse_set(str, str_length, valid_length, id_level_map_);
  }
  return ret;
}

int ObLogger::parse_set(const char *str,
                        const int32_t str_length,
                        int32_t &valid_length,
                        ObLogIdLevelMap &id_level_map)
{
  int ret = OB_SUCCESS;
  ObList<ModSetting> mod_setting_list;
  if (OB_FAIL(parse_check(str, str_length, valid_length, &mod_setting_list))) {
    LOG_WDIAG("Failed to parse check log level", K(ret));
  } else if (OB_FAIL(setting_list_processing(id_level_map, &mod_setting_list))) {
    LOG_WDIAG("Failed to process setting list", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

int ObLogger::setting_list_processing(ObLogIdLevelMap &id_level_map, void *mod_setting_list)
{
  int ret = OB_SUCCESS;
  ModSetting mod_set;
  if (OB_ISNULL(mod_setting_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Mod setting list should not be NULL", K(ret));
  } else {
    ObList<ModSetting> *list = static_cast<ObList<ModSetting> *>(mod_setting_list);
    for (; OB_SUCC(ret) && list->size() > 0;) {
      if (OB_FAIL(list->pop_front(mod_set))) {
        LOG_WDIAG("Failed to pop mod set", K(ret));
      } else {
        if (ModSetting::NON_SUBMOD_ID == mod_set.par_mod_id_) {
          id_level_map.set_level(mod_set.level_);
        } else if (ModSetting::NON_SUBMOD_ID == mod_set.sub_mod_id_) {
          if (OB_FAIL(id_level_map.set_level(mod_set.par_mod_id_, mod_set.level_))) {
            LOG_WDIAG("Failed to set log level", K(ret));
          }
        } else {
          if (OB_FAIL(id_level_map.set_level(mod_set.par_mod_id_,
                                             mod_set.sub_mod_id_,
                                             mod_set.level_))) {
            LOG_WDIAG("Failed to set log level", K(ret));
          }
        }
      }
    }//end of for
  }
  return ret;
}

int ObLogger::get_mod_set(const char *par_mod, const char *sub_mod, const char *level,
                          ModSetting &mod_set)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(level_str2int(level, mod_set.level_))) {
    LOG_WDIAG("Failed to convert level", K(ret));
  } else {
    if (0 == STRCASECMP(par_mod, "ALL") &&  0 == STRCASECMP(sub_mod, "*")) {
      mod_set.par_mod_id_ = ModSetting::NON_SUBMOD_ID;
    } else if (0 == STRCASECMP(sub_mod, "*")) {
      if (OB_FAIL(name_id_map_.get_mod_id(par_mod, mod_set.par_mod_id_))) {
        LOG_WDIAG("Failed to get mod id", K(ret), K(par_mod));
      } else {
        mod_set.sub_mod_id_ = ModSetting::NON_SUBMOD_ID;
      }
    } else {
      if (OB_FAIL(name_id_map_.get_mod_id(par_mod, sub_mod, mod_set.par_mod_id_,
                                          mod_set.sub_mod_id_))) {
        LOG_WDIAG("Failed to get mod id", K(ret), K(par_mod), K(sub_mod));
      }
    }
  }
  return ret;
}

int ObLogger::get_level_str(const int8_t level_id, const char *&level_str) const
{
  int ret = OB_SUCCESS;;
  level_str = NULL;
  if (level_id < 0 || level_id >= static_cast<int8_t>(sizeof(errstr_) / sizeof(char *))) {
    ret = OB_LOG_INVALID_MOD_ID;
    LOG_WDIAG("Invalid level", K(ret), K(level_id));
  } else {
    level_str = errstr_[level_id];
  }
  return ret;
}

int ObLogger::level_str2int(const char *level_name, int8_t &level_int)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(level_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("Invalid argument", K(ret), K(level_name));
  } else {
    bool find_level = false;
    int8_t level_num = static_cast<int8_t>(sizeof(errstr_) / sizeof(char *));
    for (int8_t level_index = 0;!find_level && level_index < level_num; level_index++) {
      if (0 == STRCASECMP(level_name, errstr_[level_index])) {
        level_int = level_index;
        find_level = true;
      }
    }//end of for
    if (!find_level) {
      ret = OB_LOG_LEVEL_INVALID;
      LOG_WDIAG("Invalid log level", K(ret));
    }
  }
  return ret;
}

void ObLogger::insert_warning_buffer(const UserMsgLevel user_msg_level, const int errcode,
    const char *data, const int64_t data_len)
{
  if (ObWarningBuffer::is_warn_log_on() && data_len > 0) {
    ObWarningBuffer *wb = ob_get_tsi_warning_buffer();
    if (NULL != wb) {
      if (user_msg_level == USER_ERROR) {
        wb->set_error(data, errcode);
      } else if (user_msg_level == USER_WARN) {
        wb->append_warning(data, errcode);
      } else if (user_msg_level == USER_NOTE) {
        wb->append_note(data, errcode);
      }
    } else {
      // OB_LOG(WDIAG, "wb is NULL", K(errcode));
      // BACKTRACE(EDIAG, 1, "wb");
    }
  }
}

int ObLogger::set_syslog_level(const char *level_str)
{
  int ret = OB_SUCCESS;
  int8_t level_int = 0;
  if (OB_FAIL(get_log_level_from_str(level_str, level_int))) {
  } else {
    syslog_level_ = level_int;
  }

  return ret;
}

int ObLogger::set_monitor_log_level(const char *level_str)
{
  int ret = OB_SUCCESS;
  int8_t level_int = 0;
  if (OB_FAIL(get_log_level_from_str(level_str, level_int))) {
  } else {
    monitor_level_ = level_int;
  }
  return ret;
}

int ObLogger::set_xflush_log_level(const char *level_str)
{
  int ret = OB_SUCCESS;
  int8_t level_int = 0;
  if (OB_FAIL(get_log_level_from_str(level_str, level_int))) {
  } else {
    xflush_level_ = level_int;
  }
  return ret;
}

int ObLogger::get_log_level_from_str(const char *level_str, int8_t &level_int)
{
  int ret = OB_SUCCESS;
  level_int = 0;
  if (OB_ISNULL(level_str)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    char buffer[OB_MAX_CONFIG_VALUE_LEN];
    const int64_t str_length = static_cast<int64_t>(strlen(level_str));
    str_copy_trim(buffer, OB_MAX_CONFIG_VALUE_LEN, level_str, str_length);
    if (OB_FAIL(level_str2int(buffer, level_int))) {
      OB_LOG(WDIAG, "failed to get level_int", K(buffer), K(level_str), K(str_length), K(ret));
    }
  }
  return ret;
}

int64_t ObLogger::get_active_log_item_count(const ObLogItemType type) const
{
  if (OB_LIKELY(type < MAX_LOG_ITEM_TYPE) && OB_LIKELY(type >= 0)) {
    return (NULL != free_item_queue_[type]
            ? (free_item_queue_[type]->max_size() - free_item_queue_[type]->curr_size())
            : 0);
  } else {
    return 0;
  }
}

int ObLogger::init_async_log_thread(const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObLogItem *item = NULL;
  int64_t all_item_count = 0;

  for (ObLogItemType i = LOG_ITEM_TINY;
       OB_SUCC(ret) && i < MAX_LOG_ITEM_TYPE;
       i = static_cast<ObLogItemType>(static_cast<int32_t>(i) + 1)) {
    ptr = NULL;
    if (NULL == (ptr = ob_malloc(sizeof(LightyQueue), ObModIds::OB_ASYNC_LOG_BUFFER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_STDERR("ob malloc LightyQueue error. ptr = %p\n", ptr);
    } else {
      free_item_queue_[i] = new(ptr)LightyQueue();
      if (OB_FAIL(free_item_queue_[i]->init(ObLogger::MAX_LOG_ITEM_COUNT[i]))) {
        LOG_STDERR("init normal free_item_queue_ error. ret=%d\n", ret);
      } else {
        all_item_count += ObLogger::MAX_LOG_ITEM_COUNT[i];
        for (int64_t j = 0; OB_SUCC(ret) && j < ObLogger::MAX_LOG_ITEM_COUNT[i]; ++j) {
          if (OB_ISNULL(item = ObLogItemFactory::alloc(i))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_STDERR("op alloc normal ObLogItem error\n");
          } else if (OB_FAIL(free_item_queue_[i]->push(item))) {
            LOG_STDERR("push normalfree_item_queue_ error. j=%ld, total=%ld\n", j, ObLogger::MAX_LOG_ITEM_COUNT[i]);
          } else {
            item = NULL;
          }
        }
        if (NULL != item) {
          ObLogItemFactory::release(item);
          item = NULL;
        }
      }//end of init
    }//end of else obmallc
  }//end of for

  if (OB_SUCC(ret)) {
    if (NULL == (ptr = ob_malloc(sizeof(LightyQueue), ObModIds::OB_ASYNC_LOG_BUFFER))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_STDERR("ob malloc LightyQueue error. ptr = %p\n", ptr);
    } else {
      async_log_queue_ = new(ptr)LightyQueue();
      if (OB_FAIL(async_log_queue_->init(all_item_count))) {
        LOG_STDERR("init async_log_queue_ error. ret=%d\n", ret);
      } else {
        //init
        struct timeval tv;
        (void)gettimeofday(&tv, NULL);
        struct tm tm_result;
        localtime_r(&tv.tv_sec, &tm_result);
        last_unix_sec_ = tv.tv_sec;
        last_localtime_ = tm_result;
      }
    }
  }

  if (OB_SUCC(ret)) {
    pthread_attr_t attr;
    int err_code = 0;
    if (OB_UNLIKELY(0 != (err_code = pthread_attr_init(&attr)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_STDERR("failed to pthread_attr_init, err_code=%d", err_code);
      //PTHREAD_SCOPE_SYSTEM: The thread competes for resources with all other threads in
      //                      all processes on the system that are in the same scheduling
      //                      allocation domain (a group of one or more processors).
      //PTHREAD_SCOPE_PROCESS: The thread competes for resources with all other threads in
      //                       the same process that were also created with the
      //                       PTHREAD_SCOPE_PROCESS contention scope
    } else if (OB_UNLIKELY(0 != (err_code = pthread_attr_setscope(&attr, PTHREAD_SCOPE_SYSTEM)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_STDERR("failed to pthread_attr_setscope, err_code=%d", err_code);
    } else if (stacksize > 0 && OB_UNLIKELY(0 != (err_code = pthread_attr_setstacksize(&attr, stacksize)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_STDERR("failed to pthread_attr_setstacksize, err_code=%d", err_code);
    } else if (OB_UNLIKELY(0 != pthread_create(&async_tid_, NULL, ObLogger::async_flush_log_handler, this))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_STDERR("ObLogger pthread create error, err_code=%d", err_code);
    }
  }

  if (OB_FAIL(ret)) {
    destroy_free_litem_queue();

    if (NULL != async_log_queue_) {
      async_log_queue_->destroy();
      ob_free(async_log_queue_);
      async_log_queue_ = NULL;
    }
  }
  return ret;
}

void cleanup_async_log(void *arg)
{
  if (OB_ISNULL(arg)) {
    LOG_STDERR("invalid argument, arg = %p\n", arg);
  } else {
    ObLogger *log = reinterpret_cast<ObLogger *>(arg);
    log->set_enable_async_log(false);
    log->set_stop_flush();
    LOG_STDERR("async log thread exited, rollback to sync log");
  }
}

void *ObLogger::async_flush_log_handler(void *arg)
{
  int ret = 0;
  if (OB_ISNULL(arg)) {
    LOG_STDERR("invalid argument, arg = %p\n", arg);
  } else if (OB_UNLIKELY(0 != (ret = prctl(PR_SET_NAME, "[ASYNC_LOG]", 0, 0, 0)))) {
    LOG_STDERR("failed to prctl PR_SET_NAME, ret=%d", ret);
  } else {
    // current thread maybe pthread_cancel in other place, we need register its cleanup func
    pthread_cleanup_push(cleanup_async_log, arg);
    ObLogger *t = reinterpret_cast<ObLogger *>(arg);
    t->do_async_flush_log();
    pthread_cleanup_pop(1);
  }
  return NULL;
}

void ObLogger::do_async_flush_log()
{
  int ret = OB_SUCCESS;
  void *item = NULL;
  static int64_t last_async_flush_ts = 0;
  static int64_t async_flush_log_count = 0;
  const int64_t pop_timeout_us = 500*1000;

  int64_t item_cnt = 0;
  int64_t process_item_cnt = 0;
  ObLogItem *process_items[GROUP_COMMIT_MAX_ITEM_COUNT];
  memset((void*) process_items, 0, sizeof(process_items));

  while (!stop_flush_ && NULL != async_log_queue_) {
    int64_t curr_ts = ObTimeUtility::current_time();
    // 限流是以 1s 为单位
    if (0 == start_bandwidth_time_ || (curr_ts - start_bandwidth_time_ >= 1 * 1000 * 1000)) {
      start_bandwidth_time_ = curr_ts;
      if (left_syslog_io_bandwidth_ <= 0) {
        syslog_out_of_limit_cnt_++;
      }
      left_syslog_io_bandwidth_ = syslog_io_bandwidth_limit_;
    }

    // 5s 内，如果触发了 1 次限流就打印 WARN 日志
    if (0 == syslog_start_out_of_limit_ || (curr_ts - syslog_start_out_of_limit_ >= 5 * 1000 * 1000)) {
      if (syslog_out_of_limit_cnt_ >= 1) {
        MPRINT("WARN !!! REACH SYSLOG RATE LIMIT");
      }
      syslog_start_out_of_limit_ = curr_ts;
      syslog_out_of_limit_cnt_ = 0;
    }

    // 对于 WARN 和 ERROR 日志不进行限流
    if (OB_SUCC(async_log_queue_->pop(item, pop_timeout_us)) && OB_NOT_NULL(item)) {
      process_items[process_item_cnt++] = reinterpret_cast<ObLogItem *>(item);
      if (OB_NOT_NULL(item)) {
        ObLogItem *log_item = reinterpret_cast<ObLogItem *>(item);
        if (LOG_ITEM_FOR_WARN_ERROR == log_item->get_item_type()) {
          left_syslog_io_bandwidth_ -= log_item->get_data_len();
        } else if (left_syslog_io_bandwidth_ <= 0) {
          process_item_cnt--;
        }
      }
      item = NULL;
      item_cnt++;

      if ((item_cnt += async_log_queue_->size()) > GROUP_COMMIT_MAX_ITEM_COUNT) {
        item_cnt = GROUP_COMMIT_MAX_ITEM_COUNT;
      }

      for (int i = 1; OB_SUCC(ret) && left_syslog_io_bandwidth_ > 0 && i < item_cnt; i++) {
        if (OB_SUCC(async_log_queue_->pop(item)) && OB_NOT_NULL(item)) {
          process_items[process_item_cnt++] = reinterpret_cast<ObLogItem *>(item);
          if (OB_NOT_NULL(item)) {
            ObLogItem *log_item = reinterpret_cast<ObLogItem *>(item);
            if (LOG_ITEM_FOR_WARN_ERROR == log_item->get_item_type()) {
              left_syslog_io_bandwidth_ -= log_item->get_data_len();
            } else if (left_syslog_io_bandwidth_ <= 0) {
              process_item_cnt--;
            }
          }
          item = NULL;
        }
      }

      do_async_flush_to_file(process_items, process_item_cnt);

      async_flush_log_count += process_item_cnt;
      if (process_items[process_item_cnt - 1]->get_timestamp() > (last_async_flush_ts + FLUSH_SAMPLE_TIME)) {
        if (curr_ts != last_async_flush_ts) {
          last_async_flush_count_per_sec_ = static_cast<int64_t>((double)(async_flush_log_count * 1000000) / (double)(curr_ts - last_async_flush_ts));
          last_async_flush_ts = curr_ts;
          async_flush_log_count = 0;
        }
      }

      for (int64_t i = 0; i < process_item_cnt; ++i) {
        push_to_free_queue(process_items[i]);
        process_items[i] = NULL;
      }

      item_cnt = 0;
      process_item_cnt = 0;
    }
  }
}

void ObLogger::do_async_flush_to_file(ObLogItem **log_item, const int64_t count)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(log_item)
      && OB_LIKELY(count > 0)
      && OB_LIKELY(count <= GROUP_COMMIT_MAX_ITEM_COUNT)
      && OB_NOT_NULL(log_item[0])) {
    if (log_item[0]->get_timestamp() > (last_check_disk_ts + DISK_SAMPLE_TIME)) {
      last_check_disk_ts = log_item[0]->get_timestamp();
      struct statfs disk_info;
      if (0 == statfs(log_file_[FD_DEFAULT_FILE].filename_, &disk_info)) {
        can_print_ = ((disk_info.f_bfree * disk_info.f_bsize) > CAN_PRINT_DISK_SIZE);
      }
    }

    if (can_print_) {
      const int64_t lcf_ts = last_check_file_ts;
      if (force_check_ || log_item[0]->get_timestamp() > (lcf_ts + FILE_SAMPLE_TIME)) {
        if (ATOMIC_BCAS(&last_check_file_ts, lcf_ts, log_item[0]->get_timestamp())) {
          check_file();
        }
      }

      struct iovec vec[MAX_FD_FILE][GROUP_COMMIT_MAX_ITEM_COUNT * 2];
      int iovcnt[MAX_FD_FILE] = {0};
      int large_iovcnt[MAX_FD_FILE] = {0};
      struct iovec wf_vec[MAX_FD_FILE][GROUP_COMMIT_MAX_ITEM_COUNT * 2];
      int wf_iovcnt[MAX_FD_FILE] = {0};

      memset(vec, 0, sizeof(vec));
      memset(wf_vec, 0, sizeof(wf_vec));

      ObLogFDType fd_type = MAX_FD_FILE;
      for (int64_t i = 0; i < count; ++i) {
        if (OB_ISNULL(log_item[i])) {
          LOG_STDERR("log_item is null, it should not happened, i=%ld, count=%ld\n", i, count);
        } else if (OB_LIKELY(log_item[i]->get_data_len() > 0) && OB_LIKELY(MAX_FD_FILE != log_item[i]->get_fd_type())) {
          const ObLogItem::ObLogItemHeader &header = log_item[i]->get_log_header();
          timeval tv;
          tv.tv_sec = header.timestamp_ / 1000000;
          tv.tv_usec = header.timestamp_ % 1000000;
          if (OB_FAIL(async_log_data_header(header.fd_type_, *log_item[i], tv,
                                            header.mod_name_, header.log_level_, header.file_name_,
                                            header.line_, header.function_name_, header.trace_id_0_,
                                            header.trace_id_1_, header.dropped_log_count_, header.tid_))) {
            break;
          }

          fd_type = log_item[i]->get_fd_type();
          vec[fd_type][iovcnt[fd_type]].iov_base = log_item[i]->get_buf();
          vec[fd_type][iovcnt[fd_type]].iov_len = static_cast<size_t>(log_item[i]->get_header_len());
          iovcnt[fd_type] += 1;
          vec[fd_type][iovcnt[fd_type]].iov_base = log_item[i]->get_buf() + MAX_LOG_HEAD_SIZE;
          vec[fd_type][iovcnt[fd_type]].iov_len = static_cast<size_t>(log_item[i]->get_data_len() - MAX_LOG_HEAD_SIZE);
          iovcnt[fd_type] += 1;

          if ((log_file_[fd_type].enable_wf_flag_ && log_file_[fd_type].open_wf_flag_ && log_item[i]->get_log_level() <= wf_level_)) {
            wf_vec[fd_type][wf_iovcnt[fd_type]].iov_base = log_item[i]->get_buf();
            wf_vec[fd_type][wf_iovcnt[fd_type]].iov_len = static_cast<size_t>(log_item[i]->get_header_len());
            wf_iovcnt[fd_type] += 1;
            wf_vec[fd_type][wf_iovcnt[fd_type]].iov_base = log_item[i]->get_buf() + MAX_LOG_HEAD_SIZE;
            wf_vec[fd_type][wf_iovcnt[fd_type]].iov_len = static_cast<size_t>(log_item[i]->get_data_len() - MAX_LOG_HEAD_SIZE);
            wf_iovcnt[fd_type] += 1;
          }

          if (log_item[i]->is_large_log_item()) {
            large_iovcnt[fd_type] +=1;
          }
        }
      }

      ssize_t size = 0;
      ssize_t writen[MAX_FD_FILE] = {0};
      for (int32_t i = 0; i < static_cast<int32_t>(MAX_FD_FILE); i++) {
        size = 0;
        if (iovcnt[i] > 0 && log_file_[i].fd_ > 0) {
          size = ::writev(log_file_[i].fd_, vec[i], iovcnt[i]) * 2;
        }

        if (size > 0) {
          writen[i] = size;
          (void)ATOMIC_AAF(&log_file_[i].write_size_, size);
          (void)ATOMIC_AAF(&log_file_[i].file_size_, size);
          (void)ATOMIC_AAF(&log_file_[i].write_count_, iovcnt[i]);

          if (large_iovcnt[i] > 0) {
            (void)ATOMIC_AAF(&large_write_count_[i], large_iovcnt[i]);
          }
        }

        if (wf_iovcnt[i] > 0 && log_file_[i].wf_fd_ > 0) {
          (void)::writev(log_file_[i].wf_fd_, wf_vec[i], wf_iovcnt[i]);
        }
      }

      if (max_file_size_ > 0) {
        for (int32_t i = 0; i < static_cast<int32_t>(MAX_FD_FILE); i++) {
          if (OB_LIKELY(need_auto_rotate_log_by_size(static_cast<ObLogFDType>(i)))) {
            const bool redirect_flag = (static_cast<int32_t>(FD_DEFAULT_FILE) == i ? redirect_flag_ : false);
            rotate_log(writen[i], redirect_flag, log_file_[i]);
          }
        }
      }
    }//can print
  }
}

void ObLogger::async_set_log_header(const ObLogFDType type,
                                    ObLogItem &item,
                                    const timeval &tv,
                                    const char *mod_name,
                                    const int32_t level,
                                    const char *file,
                                    const int32_t line,
                                    const char* function,
                                    const uint64_t dropped_log_count)
{
  const uint64_t *trace_id = ObCurTraceId::get();
  uint64_t trace_id_0 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[0];
  uint64_t trace_id_1 = (OB_ISNULL(trace_id)) ? OB_INVALID_ID : trace_id[1];
  item.set_header(tv, level, type, trace_id_0, trace_id_1, mod_name, file, line, function, dropped_log_count, GETTID());
  item.set_data_len(MAX_LOG_HEAD_SIZE);
  item.set_header_len(MAX_LOG_HEAD_SIZE);
}

int ObLogger::async_log_data_header(const ObLogFDType type,
                                    ObLogItem &item,
                                    const timeval &tv,
                                    const char *mod_name,
                                    const int32_t level,
                                    const char *file,
                                    const int32_t line,
                                    const char *function,
                                    const uint64_t trace_id_0,
                                    const uint64_t trace_id_1,
                                    const uint64_t dropped_log_count,
                                    const int64_t tid)
{
  int ret = OB_SUCCESS;
  struct tm tm;
  ob_fast_localtime(last_unix_sec_, last_localtime_, static_cast<time_t>(tv.tv_sec), &tm);

  char *data_buf = item.get_buf();
  int64_t pos = 0;
  if (FD_DIAGNOSIS_FILE == type) {
    ret = logdata_printf(data_buf, MAX_LOG_HEAD_SIZE, pos,
                         "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                         "[%ld][" TRACE_ID_FORMAT "] ",
                         tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                         tm.tm_sec, tv.tv_usec, tid, trace_id_0, trace_id_1);
  } else if (FD_XFLUSH_FILE == item.get_fd_type()) {
    ret = logdata_printf(data_buf, MAX_LOG_HEAD_SIZE, pos, "%04d-%02d-%02d %02d:%02d:%02d.%06ld [%s] ",
                         tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                         tm.tm_sec, tv.tv_usec, errstr_[level]);
  } else if (FD_CONFIG_FILE == type) { // header for config file
    (void)logdata_printf(data_buf, MAX_LOG_HEAD_SIZE, pos, "###"); //just print '###'
  } else if (is_monitor_file(type)) {
    ret = logdata_printf(data_buf, MAX_LOG_HEAD_SIZE, pos, "%04d-%02d-%02d %02d:%02d:%02d.%06ld,",
                         tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                         tm.tm_sec, tv.tv_usec);
  } else if (item.is_trace_file()) {
    const char *base_file_name = (NULL != file ? strrchr(file, '/') : NULL);
    ret = logdata_printf(data_buf, MAX_LOG_HEAD_SIZE, pos,
                         "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                         "%s (%s:%d) [%ld][" TRACE_ID_FORMAT "] ",
                         tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec,
                         function, base_file_name, line, tid, trace_id_0, trace_id_1);
  } else {
    //only print base filename.
    const char *base_file_name = (NULL != file ? strrchr(file, '/') : NULL);
    base_file_name = (NULL != base_file_name) ? base_file_name + 1 : file;
    //[lt=%ld] last log cost time us
    //[dc=%lu] async dropped log count
    ret = logdata_printf(data_buf, MAX_LOG_HEAD_SIZE, pos,
                          "[%04d-%02d-%02d %02d:%02d:%02d.%06ld] "
                          "%-5s %s%s (%s:%d) [%ld][" TRACE_ID_FORMAT "] [lt=%ld] [dc=%lu] ",
                          tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                          tm.tm_sec, tv.tv_usec, errstr_[level], mod_name, function,
                          base_file_name, line, tid, trace_id_0, trace_id_1,
                          +last_logging_cost_time_us_,
                          dropped_log_count);
  }
  if (OB_SUCC(ret) || OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
    ret = OB_SUCCESS;
    item.set_header_len(pos);
  }
  return ret;
}

int ObLogger::try_upgrade_log_item(ObLogItem *&log_item, bool &upgrade_result)
{
  int ret = OB_SUCCESS;
  upgrade_result = false;
  if (OB_LIKELY(NULL != log_item)
      && OB_LIKELY(LOG_ITEM_LARGE != log_item->get_item_type())) {
    const ObLogItemType type = static_cast<ObLogItemType>(log_item->get_item_type() + 1);
    ObLogItem *new_log_item = NULL;
    if (OB_FAIL(pop_from_free_queue(log_item->get_log_level(), new_log_item, type))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        LOG_STDERR("pop_from_free_queue error, continue use origin log item, ret=%d\n", ret);
        ret = OB_SUCCESS;
      } else {
        LOG_STDERR("pop_from_free_queue erro, ret=%d, type=%d\n", ret, type);
      }
    } else {
      new_log_item->deep_copy_header_only(*log_item);
      push_to_free_queue(log_item);
      log_item = new_log_item;
      new_log_item = NULL;
      upgrade_result = true;
    }
  }
  return ret;
}

void ObLogger::get_pop_limit(const int32_t level, int64_t &timeout_us)
{
  switch (level) {
    // ERROR/WARN 是关键信息，等待保证尽量能够输出
    case OB_LOG_LEVEL_ERROR:
    case OB_LOG_LEVEL_WARN: {

      timeout_us = 10;//10us
      break;
    }
    default: {
      timeout_us = 0;//0us
      break;
      //do nothiong
    }
  }
}

int ObLogger::async_log_data_body(ObLogItem &log_item, const char *info_string, const int64_t string_len)
{
  int ret = OB_SUCCESS;
  char *data = log_item.get_buf();
  int64_t pos = log_item.get_data_len();
  ret = logdata_printf(data, log_item.get_buf_size(), pos, "%.*s", static_cast<int32_t>(string_len), info_string);
  if (OB_SUCC(ret) || OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
    ret = OB_SUCCESS;
    check_log_end(log_item, pos);
  } else {
    LOG_STDERR("logdata_printf error ret = %d\n", ret);
  }
  return ret;
}

int ObLogger::check_error_log(ObLogItem &log_item)
{
  static const char* const BACKTRACE_END = " BACKTRACE:";
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_LOG_LEVEL_EDIAG == log_item.get_log_level()) && !log_item.is_trace_file()) {
    int64_t pos = (log_item.get_data_len() > 0 ? log_item.get_data_len() - 1 : 0);
    char *buf = log_item.get_buf();
    const int64_t buf_size = log_item.get_buf_size();
    if (OB_FAIL(logdata_print_info(buf, buf_size, pos, BACKTRACE_END))) {
      //do nothing
    } else if (OB_FAIL(logdata_print_info(buf, buf_size, pos, lbt()))) {
      //do nothing
    } else {
      //do nothing
    }
    check_log_end(log_item, pos);
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) {
      //treat it succ
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogger::check_callback(ObLogItem &item)
{
  int ret = common::OB_SUCCESS;
  if (FD_DEFAULT_FILE == item.get_fd_type()
      && NULL != callback_handler_
      && item.get_log_level() < OB_LOG_LEVEL_INFO) {
    callback_handler_(item.get_log_level(), item.get_timestamp(), item.get_buf(), item.get_data_len(), NULL, 0);
  }
  return ret;
}

int ObLogger::async_log_message_kv(const ObLogFDType type,
                                   const char *mod_name,
                                   const int32_t level,
                                   const LogLocation &location,
                                   const char *info_string,
                                   const int64_t string_len)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_async_log_used())
      && OB_LIKELY(is_enable_logging())
      && OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG)
      && OB_LIKELY(level >= OB_LOG_LEVEL_ERROR)
      && OB_NOT_NULL(location.file_) && OB_NOT_NULL(location.function_)
      && OB_NOT_NULL(info_string) && OB_LIKELY(string_len >= 0)) {
    set_disable_logging(true);
    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    ++curr_logging_seq_;
    const int64_t logging_time_us_begin = static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec);
    ObLogItem *log_item = NULL;
    uint64_t dropped_log_count = curr_logging_seq_ - last_logging_seq_ - 1;
    //1. fill log buffer
    if (OB_FAIL(pop_from_free_queue(level, log_item, string_len))) {
      LOG_STDERR("pop_from_free_queue error, ret=%d\n", ret);
    } else if (FALSE_IT(async_set_log_header(type, *log_item, tv, mod_name, level, location.file_, location.line_, location.function_, dropped_log_count))) {
      LOG_STDERR("async_set_log_header error ret = %d\n", ret);
    } else if (OB_FAIL(async_log_data_body(*log_item, info_string, string_len))) {
      LOG_STDERR("async_log_data_body error ret = %d\n", ret);
    } else if (OB_FAIL(check_error_log(*log_item))) {
      LOG_STDERR("check_error_log error ret = %d\n", ret);
    } else if (OB_FAIL(check_callback(*log_item))) {
      LOG_STDERR("check_callback error ret = %d\n", ret);
    } else if (OB_FAIL(async_log_queue_->push(log_item))) {
      LOG_STDERR("push log item to buffer error ret = %d\n", ret);
    } else {
      last_logging_seq_ = curr_logging_seq_;
      last_logging_cost_time_us_ = ObTimeUtility::current_time() - logging_time_us_begin;
    }

    //3. stat
    if (OB_FAIL(ret)) {
      push_to_free_queue(log_item);
      log_item = NULL;
    }
    set_disable_logging(false);
  }
  return ret;
}

ObLogItemType ObLogger::calc_log_item_type(const int32_t level, const int64_t data_len /*0*/) const
{
  ObLogItemType ret_type = LOG_ITEM_TINY;
  if (OB_LOG_LEVEL_WARN == level || OB_LOG_LEVEL_ERROR == level) {
    ret_type = LOG_ITEM_FOR_WARN_ERROR;
  } else if (data_len <= LOG_ITEM_SIZE[LOG_ITEM_TINY]) {
    //default
  } else if (data_len <= LOG_ITEM_SIZE[LOG_ITEM_NORMAL]) {
    ret_type = LOG_ITEM_NORMAL;
  } else {
    ret_type = LOG_ITEM_LARGE;
  }

  return ret_type;
}

int ObLogger::pop_from_free_queue(const int32_t level, ObLogItem *&log_item, const int64_t data_len)
{
  const ObLogItemType type = calc_log_item_type(level, data_len + MAX_LOG_HEAD_SIZE);
  return pop_from_free_queue(level, log_item, type);
}

int ObLogger::pop_from_free_queue(const int32_t level, ObLogItem *&log_item, const ObLogItemType type)
{
  int ret = OB_SUCCESS;
  log_item = NULL;
  if (!stop_append_log_ && OB_LIKELY(NULL != free_item_queue_[type])) {
    void *item = NULL;
    int64_t timeout_us = 0;
    get_pop_limit(level, timeout_us);
    if (OB_FAIL(free_item_queue_[type]->pop(item, timeout_us))) {
      LOG_STDERR("pop free_item_queue error, ret=%d\n", ret);
    } else {
      log_item = reinterpret_cast<ObLogItem *>(item);
      log_item->reuse();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_STDERR("can not pop_from_free_queue, ret=%d, stop_append_log=%d\n", ret, stop_append_log_);
  }
  return ret;
}


void ObLogger::push_to_free_queue(ObLogItem *&log_item)
{
  if (NULL != log_item) {
    const ObLogItemType type = log_item->get_item_type();
    if (OB_LIKELY(MAX_LOG_ITEM_TYPE != type) && OB_LIKELY(NULL != free_item_queue_[type])) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(free_item_queue_[type]->push(log_item))) {
        ObLogItemFactory::release(log_item);
        LOG_STDERR("push item to free_item_queue_ error, destory it ret=%d\n", ret);
      }
    } else {
      ObLogItemFactory::release(log_item);
      LOG_STDERR("free_item_queue_ is null, destory it\n");
    }
    log_item = NULL;
  }
}

void ObLogger::inc_dropped_log_count(const int32_t level)
{
  if (OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG)
      && OB_LIKELY(level >= OB_LOG_LEVEL_ERROR)) {
    //recode dropped count
    ATOMIC_AAF(dropped_log_count_ + level, 1);
  }
}

int ObLogger::push_to_async_queue(ObLogItem &log_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(async_log_queue_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_STDERR("async_log_queue_ is null\n");
  } else if (OB_FAIL(async_log_queue_->push(&log_item))) {
    LOG_STDERR("push item to async_log_queue_ error, ret=%d\n", ret);
  }
  return ret;
}

#define ASYNC_LOG_DATA_BODY(log_item) \
  int64_t pos = log_item.get_data_len(); \
  char *data = log_item.get_buf(); \
  va_list args_tmp;                \
  va_copy(args_tmp, args);         \
  ret = logdata_vprintf(data, log_item.get_buf_size(), pos, fmt, args_tmp); \
  va_end(args_tmp);                \
  if (OB_SUCC(ret) || OB_UNLIKELY(OB_SIZE_OVERFLOW == ret)) { \
    ret = OB_SUCCESS; \
    check_log_end(log_item, pos); \
  } \
  if (OB_FAIL(ret)) { \
    LOG_STDERR("ASYNC_LOG_DATA_BODY error ret = %d\n", ret); \
  } else if (OB_FAIL(check_error_log(log_item))) { \
    LOG_STDERR("check_error_log error ret = %d\n", ret); \
  }

void ObLogger::async_log_message(const char *mod_name,
                                 int32_t level,
                                 const char *file,
                                 int32_t line,
                                 const char *function,
                                 const char *fmt,
                                 ...)
{
  const ObLogFDType type = (NULL == mod_name ? FD_XFLUSH_FILE : FD_DEFAULT_FILE);
  va_list args;
  va_start(args, fmt);
  async_log_message(type, mod_name, level, file, line, function, fmt, args);
  va_end(args);
}

void ObLogger::async_log_message(const ObLogFDType type,
                                 const char *mod_name,
                                 int32_t level,
                                 const char *file,
                                 int32_t line,
                                 const char *function,
                                 const char *fmt,
                                 ...)
{
  va_list args;
  va_start(args, fmt);
  async_log_message(type, mod_name, level, file, line, function, fmt, args);
  va_end(args);
}

void ObLogger::async_log_message(const ObLogFDType type,
                                 const char *mod_name,
                                 int32_t level,
                                 const char *file,
                                 int32_t line,
                                 const char *function,
                                 const char *fmt,
                                 va_list args)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_async_log_used())
      && OB_LIKELY(is_enable_logging())
      && OB_LIKELY(level <= OB_LOG_LEVEL_DEBUG)
      && OB_LIKELY(level >= OB_LOG_LEVEL_ERROR)
      && OB_NOT_NULL(file)
      && OB_NOT_NULL(function) && OB_NOT_NULL(fmt)) {
    set_disable_logging(true);
    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    const int64_t logging_time_us_begin = static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec);
    ++curr_logging_seq_;
    ObLogItem *log_item = NULL;
    uint64_t dropped_log_count = curr_logging_seq_ - last_logging_seq_ - 1;
    //1. fill log buffer
    if (OB_FAIL(pop_from_free_queue(level, log_item))) {
      LOG_STDERR("pop_from_free_queue error, ret=%d\n", ret);
    } else if (FALSE_IT(async_set_log_header(type, *log_item, tv, mod_name, level, file, line, function, dropped_log_count))) {
      LOG_STDERR("async_set_log_header error ret = %d\n", ret);
    } else {
      ASYNC_LOG_DATA_BODY((*log_item));
    }

    //2. check size overflow
    if (OB_SUCC(ret)
        && OB_NOT_NULL(log_item)
        && log_item->is_size_overflow()) {
      bool upgrade_result = false;
      if (OB_FAIL(try_upgrade_log_item(log_item, upgrade_result))) {
          LOG_STDERR("try_upgrade_log_item error, ret=%d\n", ret);
      } else if (upgrade_result) {
        ASYNC_LOG_DATA_BODY((*log_item));
      }
    }

    //3. check size overflow
    if (OB_SUCC(ret)
        && OB_NOT_NULL(log_item)
        && log_item->is_size_overflow()) {
      bool upgrade_result = false;
      if (OB_FAIL(try_upgrade_log_item(log_item, upgrade_result))) {
          LOG_STDERR("try_upgrade_log_item error, ret=%d\n", ret);
      } else if (upgrade_result) {
        ASYNC_LOG_DATA_BODY((*log_item));
      }
    }

    //4. check log limiter
    if (OB_SUCC(ret) && OB_NOT_NULL(log_item)) {
      if (OB_FAIL(check_callback(*log_item))) {
        LOG_STDERR("check_callback error ret = %d\n", ret);
      } else if (OB_FAIL(push_to_async_queue(*log_item))) {
        LOG_STDERR("push log item to buffer error ret = %d\n", ret);
      } else {
        last_logging_seq_ = curr_logging_seq_;
        last_logging_cost_time_us_ = ObTimeUtility::current_time() - logging_time_us_begin;
      }
    }

    //5. stat
    if (OB_FAIL(ret)) {
      push_to_free_queue(log_item);
      log_item = NULL;
    }
    set_disable_logging(false);
  }
}


void ObLogger::log_message_kv(const char *mod_name,
                                     const int32_t level,
                                     const char *file,
                                     const int32_t line,
                                     const char *function,
                                     const char *info_string)
{
  const ObLogFDType type = (NULL == mod_name ? FD_XFLUSH_FILE : FD_DEFAULT_FILE);
  log_message_kv(type, mod_name, level, file, line, function, info_string);
}

void ObLogger::log_message_kv(const ObLogFDType type,
                                     const char *mod_name,
                                     const int32_t level,
                                     const char *file,
                                     const int32_t line,
                                     const char *function,
                                     const char *info_string)
{
  int ret = common::OB_SUCCESS;
  LogBuffer *log_buffer = NULL;
  if (OB_NOT_NULL(info_string)) {
    if (get_trace_mode()) {
      if (OB_LIKELY(is_enable_logging())
          && OB_NOT_NULL(log_buffer = get_thread_buffer())
          && OB_LIKELY(!log_buffer->is_oversize())) {
        set_disable_logging(true);
        log_head_info(type, mod_name, level, LogLocation(file, line, function), *log_buffer);
        int64_t &pos = log_buffer->pos_;
        char *data = log_buffer->buffer_;
        LOG_PRINT_INFO(info_string);
        log_tail(level, *log_buffer);
        set_disable_logging(false);
      }
    } else if (is_async_log_used()) {
      ret = async_log_message_kv(type, mod_name, level, LogLocation(file, line, function), info_string,
          static_cast<int64_t>(strlen(info_string)));
    } else if (OB_LIKELY(is_enable_logging())) {//sync away
      set_disable_logging(true);
      if (OB_NOT_NULL(log_buffer = get_thread_buffer())
          && OB_LIKELY(!log_buffer->is_oversize())) {
        int64_t &pos = log_buffer->pos_;
        char *data = log_buffer->buffer_;
        LOG_PRINT_INFO(info_string);
        log_data(type, mod_name, level, LogLocation(file, line, function), *log_buffer);
        set_disable_logging(false);
      }
    }
  }
}

}
}

#include "ob_log_module.ipp"
