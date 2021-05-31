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

#ifndef OCEANBASE_COMMON_OB_ASYNC_LOG_STRUCT_
#define OCEANBASE_COMMON_OB_ASYNC_LOG_STRUCT_

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <algorithm>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace common
{
enum ObLogFDType {
  FD_DEFAULT_FILE = 0,
  FD_XFLUSH_FILE,
  FD_DIGEST_FILE,
  FD_ERROR_FILE,
  FD_SLOW_FILE,
  FD_STAT_FILE,
  FD_LIMIT_FILE,
  FD_CONFIG_FILE, //obproxy_config.file
  FD_POOL_FILE,
  FD_POOL_STAT_FILE,
  MAX_FD_FILE,
};

enum ObLogItemType {
  LOG_ITEM_TINY = 0,
  LOG_ITEM_NORMAL,
  LOG_ITEM_LARGE,
  LOG_ITEM_TINY_FOR_WARN_ERR,
  LOG_ITEM_NORMAL_FOR_WARN_ERR,
  LOG_ITEM_LARGE_FOR_WARN_ERR,
  MAX_LOG_ITEM_TYPE,
};

static int64_t LOG_ITEM_SIZE[MAX_LOG_ITEM_TYPE] = {
  1 * 1024,
  4 * 1024,
  64 * 1024,
  1 * 1024,
  4 * 1024,
  64 * 1024,
};

class ObLogItem
{
public:
  static const int64_t MAX_LOG_SIZE = 63 * 1024;

  ObLogItem();
  virtual ~ObLogItem() { destroy();}
  void reuse();
  void destroy() { reuse(); }
public:
  char *get_buf() { return buf_; }
  const char *get_buf() const { return buf_; }
  int64_t get_data_len() const { return pos_; }
  int64_t get_header_len() const { return header_pos_; }
  void set_data_len(const int64_t len) { pos_ = len;}
  void set_header_len(const int64_t len) { header_pos_ = len;}
  int64_t get_buf_size() const
  {
    return MAX_LOG_ITEM_TYPE == item_type_ ? 0 : (LOG_ITEM_SIZE[item_type_] - sizeof(ObLogItem));
  }
  int get_log_level() const { return log_level_; }
  void set_log_level(const int log_level) { log_level_ = log_level; }
  int64_t get_timestamp() const { return timestamp_; }
  void set_timestamp(const int64_t timestamp) { timestamp_ = timestamp;}
  void set_timestamp(const timeval &tv)
  {
    timestamp_ = static_cast<int64_t>(tv.tv_sec) * static_cast<int64_t>(1000000) + static_cast<int64_t>(tv.tv_usec);
  }
  bool is_large_log_item() const { return LOG_ITEM_LARGE == item_type_; }
  bool is_size_overflow() const { return get_data_len() >= (get_buf_size() - 1); }
  ObLogItemType get_item_type() const { return item_type_; }
  void set_item_type(const ObLogItemType type) { item_type_ = type; }
  ObLogFDType get_fd_type() const { return fd_type_; }
  void set_fd_type(const ObLogFDType fd_type) { fd_type_ = fd_type;}
  bool is_xflush_file() const { return FD_XFLUSH_FILE == fd_type_; }
  bool is_default_file() const { return FD_DEFAULT_FILE == fd_type_; }
  void deep_copy_header_only(const ObLogItem &other);

private:
  ObLogItemType item_type_;
  ObLogFDType fd_type_;
  int log_level_;
  int64_t timestamp_;
  int64_t header_pos_;
  int64_t pos_;
  char *buf_;

  DISALLOW_COPY_AND_ASSIGN(ObLogItem);
};

class ObLogItemFactory
{
public:
  static ObLogItem *alloc(const int32_t type);
  static void release(ObLogItem *item);
  static int64_t alloc_count_[MAX_LOG_ITEM_TYPE];
  static int64_t release_count_[MAX_LOG_ITEM_TYPE];
};

class ObLogFileStruct
{
public:
  ObLogFileStruct();
  virtual ~ObLogFileStruct() { close_all(); }
  int open(const char *log_file, const bool open_wf_flag, const bool redirect_flag);
  int reopen(const bool redirect_flag);
  int reopen_wf();
  int close_all();
  bool is_opened() { return fd_ > STDERR_FILENO; }
  int64_t get_write_size() const { return write_size_; }
  bool is_enable_wf_file() const { return enable_wf_flag_ && open_wf_flag_; }
public:
  static const int32_t MAX_LOG_FILE_NAME_SIZE = 256;
  static const mode_t LOG_FILE_MODE = 0644;

  char filename_[MAX_LOG_FILE_NAME_SIZE];
  int32_t fd_;//descriptor of log-file
  int32_t wf_fd_;//descriptor of warning log-file
  uint32_t write_count_;
  int64_t write_size_;
  int64_t file_size_;
  struct stat stat_;
  struct stat wf_stat_;
  bool open_wf_flag_;//whether open warning log-file.
  bool enable_wf_flag_; //whether write waring log to wf log-file.
};

} // common
} // oceanbase
#endif //OCEANBASE_COMMON_OB_ASYNC_LOG_STRUCT_
