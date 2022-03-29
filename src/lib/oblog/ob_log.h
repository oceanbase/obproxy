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
 *
 * *************************************************************
 *
 * Surpport OB_LOG, ModName_LOG.
 * To use ModName_LOG, you have to:
 * (1)Add statement of the module in 'ob_log_module.h'.
 * For par-module, add DEFINE_LOG_MOD(par-modName) bettween 'LOG_MOD_BEGIN(ROOT)' and 'LOG_MOD_END(ROOT)'
 * For sub-module of some par-module, add DEFINE_LOG_MOD(sub-modName) bettween 'LOG_MOD_BEGIN(par-modName)' and 'LOG_MOD_END(par-modName)'
 * (2)Regist the module to ObLogger in 'ob_log_module.ipp'.
 * For par-module, add REG_LOG_PAR_MOD(par-modName).
 * For sub-module, add REG_LOG_SUB_MOD(par-modName, sub-modName) after registing par-module.
 * (3)Define ModName_LOG in 'ob_log_module.h'
 * For par-module, define parModule_LOG(level, fmt, args...) OB_MOD_LOG(parModule, level, fmt, ##args).
 * For sub-module, define parModule_subModule_LOG(level, fmt, arg...) OB_SUB_MOD_LOG(parModule, subModule, level, fmt, ##args)
 */

#ifndef OCEANBASE_LIB_OBLOG_OB_LOG_
#define OCEANBASE_LIB_OBLOG_OB_LOG_

#include <stdarg.h>
#include <time.h>
#include <stdio.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <deque>
#include <string>
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>
#include <cstring>
#include <sys/uio.h>

#include "lib/oblog/ob_log_print_kv.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_async_log_struct.h"

#define OB_LOG_MAX_PAR_MOD_SIZE 16
#define OB_LOG_MAX_SUB_MOD_SIZE 16

namespace oceanbase
{
namespace common
{
template<typename Type, int size> class ObLogRingBuffer;
class ObLogItem;
class LightyQueue;

//@class ObLogIdLevelMap
//@brief stroe the level of each par-module and sub-module. The key is module ID.
//       To be used for SQL hint, this class should be POD-type.
struct ObLogIdLevelMap
{
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  //@brief Set to default value OB_LOG_LEVEL_INFO.
  inline void reset_level() {set_level(OB_LOG_LEVEL_INFO);}
  //@brief Set all modules' levels.
  void set_level(const int8_t level);
  //@brief Set the par-module's level.
  int set_level(const uint64_t par_mod_id, const int8_t level);
  //@brief Set the sub-module's level.
  int set_level(const uint64_t par_mod_id, const uint64_t sub_mod_id, const int8_t level);

  //@brief Get the non-module's level.
  inline int8_t get_level() const {return non_mod_level_;}
  //@brief Get par-module's level.
  inline int8_t get_level(uint64_t par_mod_id) const;
  //@brief Get sub-module's level
  inline int8_t get_level(uint64_t par_mod_id, uint64_t sub_mod_id) const;

  /// convert log level map to a bitset.
  //int to_bitset(ObBitSet<768> &bitset) const;
  //int from_bitset(const ObBitSet<768> &bitset);

  //log_level_[i][0] representing level of the par-module.
  int8_t log_level_[MAX_PAR_MOD_SIZE][MAX_SUB_MOD_SIZE + 1];
  int8_t non_mod_level_;
};



//@class ObLogIdLevelMap
//@brief stroe the ID of each par-module and sub-module. The key is module's name.
class ObLogNameIdMap
{
public:
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  ObLogNameIdMap();

  //@brief Regist parent module.
  int register_mod(const uint64_t mod_id, const char *mod_name);
  //@brief Regist sub-module.
  int register_mod(const uint64_t mod_id,
                   const uint64_t sub_mod_id,
                   const char *sub_mod_name);
  //@brief Get par-module's ID and sub-module's ID by module's name.
  int get_mod_id(const char *mod_name,
                 const char *sub_mod_name,
                 uint64_t &par_mod_id,
                 uint64_t &sub_mod_id) const;
  //@brief Get par-module's ID by module's name.
  int get_mod_id(const char *mod_name, uint64_t &mod_id) const;

  //@brief Get par-module's name by module's ID.
  int get_par_mod_name(const uint64_t par_mod_id, const char *&mod_name) const;

  //@brief Get sub-module's name by module's ID.
  int get_sub_mod_name(const uint64_t par_mod_id, const uint64_t sub_mod_id, const char *&mod_name) const;

private:
  //name_id_map_[i][0] par-module's name, name_id_map_[i][i](i>1) sub-module's name
  const char *name_id_map_[MAX_PAR_MOD_SIZE][MAX_SUB_MOD_SIZE + 1];
};


//@class ObThreadLogLevel
//@brief Deliver the id_level_map of the session which set the session variable
//'log_level'
class ObThreadLogLevel
{
public:
  ObThreadLogLevel(): id_level_map_(NULL), level_(OB_LOG_LEVEL_NONE) { }

  //@brief Set pointer to default value NULL.
  static void init();
  //@brief Set the pointer to the session's id_level_map.
  static inline void init(const ObLogIdLevelMap *id_level_map);

  static inline void init(const int8_t level);

  //@brief Set pointer to NULL.
  static inline void clear();

  //@brief Get the pointer of the session's id_level_map.
  static inline const ObLogIdLevelMap *get();

  static inline int8_t get_level();
  //@brief Get the thread-only ObThreadLogLevel.
  static inline ObThreadLogLevel *get_thread_log_level();

private:
  const ObLogIdLevelMap *id_level_map_;
  int8_t level_; //Used for transmit log_level in packet.
};

//@class ObLogger
//@brief main class of logging facilities. Provide base function, for example log_message(),
//parse_set().
//
//This class is changed from the class 'CLogger' in tblog.h which was written by the God
//named DuoLong.

typedef void (*LoggerCallbackHandler) (const int32_t level,
                                       const int64_t time,
                                       const char *head_buf,
                                       const int64_t head_len,
                                       const char *data,
                                       const int64_t data_len);

class ObLogger
{
public:
  static const int64_t DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024; // default max log file size
  //check whether disk storing log-file has no space every 2s
  //Only max_file_size_ > 0, this is effective.
  static const int64_t DISK_SAMPLE_TIME = 2 * 1000 * 1000;
  //check whether log file exist every 5s.
  static const int64_t FILE_SAMPLE_TIME = 5 * 1000 * 1000;
  //stat flush speed every 1s.
  static const int64_t FLUSH_SAMPLE_TIME = 1 * 1000 * 1000;
  static const uint64_t MAX_THREAD_LOG_NUM = 1;
  static const uint64_t CAN_PRINT_DISK_SIZE = 32 * 1024 * 1024; //32MB
  static const uint64_t MAX_PAR_MOD_SIZE = OB_LOG_MAX_PAR_MOD_SIZE;
  static const uint64_t MAX_SUB_MOD_SIZE = OB_LOG_MAX_SUB_MOD_SIZE;

  static const int64_t MAX_LOG_HEAD_SIZE = 256;
  static const int64_t MAX_LOG_SIZE = 64 * 1024; //64kb
  static const int64_t REMAIN_TRACE_LOG_SIZE = 4 * 1024; //4kb

  static const int64_t MAX_LOG_ITEM_COUNT[MAX_LOG_ITEM_TYPE];

  static const int64_t GROUP_COMMIT_MAX_ITEM_COUNT = 4;

  //mainly for ob_localtime
  static __thread time_t last_unix_sec_;
  static __thread struct tm last_localtime_;

  enum UserMsgLevel
  {
    USER_WARN,
    USER_ERROR,
    USER_NOTE
  };

  enum LogLevel
  {
    LOG_ERROR = 0,
    LOG_USER_ERROR,
    LOG_WARN,
    LOG_INFO,
    LOG_TRACE,
    LOG_DEBUG,
    LOG_MAX_LEVEL,
  };

  struct FileName
  {
    //can not add other member and virtual function in this class.
    FileName() { memset(file_name_, 0, sizeof(file_name_)); }
    int64_t to_string(char *buf, const int64_t buf_len) const;
    char file_name_[ObLogFileStruct::MAX_LOG_FILE_NAME_SIZE];
  };

private:
  struct LogBuffer
  {
    LogBuffer() : pos_(0), trace_mode_(false)
    { buffer_[0] = '\0'; }
    ~LogBuffer()
    { }
    void reset();
    void set_trace_mode(bool trace_mode);
    bool is_oversize() const { return (pos_ < 0 || pos_ >= MAX_LOG_SIZE); }
    int64_t pos_;
    bool trace_mode_;
    char buffer_[MAX_LOG_SIZE];
  };

  struct LogBufferMgr
  {
    LogBufferMgr()
    { reset(); }
    ~LogBufferMgr()
    { }
    void reset();
    void set_trace_mode(bool trace_mode);
    //@param no_trace true:get the buffer not controlled by trace mode
    LogBuffer &get_buffer();
    void print_trace_buffer(int32_t level, const char *file, int32_t line, const char *function);

    uint64_t idx_;
    bool trace_mode_;
    LogBuffer buffers_[MAX_THREAD_LOG_NUM];
  };

  struct LogLocation
  {
    LogLocation(const char *file, const int32_t line, const char *function) :
    file_(file), line_(line), function_(function) { }
    const char *file_;
    const int32_t line_;
    const char *function_;
  };

  struct ModSetting
  {
    static const uint64_t NON_SUBMOD_ID = 1024;
    ModSetting() : par_mod_id_(NON_SUBMOD_ID), sub_mod_id_(NON_SUBMOD_ID), level_(OB_LOG_LEVEL_INFO) { }
    ModSetting(uint64_t par_mod_id, uint64_t sub_mod_id, int8_t level) :
        par_mod_id_(par_mod_id), sub_mod_id_(sub_mod_id), level_(level) { }
    uint64_t par_mod_id_;//ALL:NON_SUBMOD_ID
    uint64_t sub_mod_id_;//sub_mod_id_:NON_SUBMOD_ID present only setting par_mod.
    int8_t level_;
  };

public:
  ~ObLogger();
  void destory_async_log_thread();

  int init_async_log_thread(const int64_t stacksize = 0);
  void destroy_free_litem_queue();

  inline pthread_t get_async_tid() const { return async_tid_; }
  inline bool is_async_log_used() const { return (enable_async_log() && NULL != async_log_queue_) && !has_stop_flush(); }
  inline bool is_in_async_logging() const { return is_async_log_used() && !is_enable_logging(); }
  inline bool need_auto_rotate_log_by_size(const ObLogFDType type) const {return FD_CONFIG_FILE != type;}

  int64_t get_active_log_item_count(const ObLogItemType type) const;
  static int64_t get_alloced_log_item_count(const ObLogItemType type);
  static int64_t get_released_log_item_count(const ObLogItemType type);

  int64_t get_dropped_error_log_count() const { return dropped_log_count_[LOG_ERROR]; }
  int64_t get_dropped_warn_log_count() const { return dropped_log_count_[LOG_WARN]; }
  int64_t get_dropped_info_log_count() const { return dropped_log_count_[LOG_INFO]; }
  int64_t get_dropped_trace_log_count() const { return dropped_log_count_[LOG_TRACE]; }
  int64_t get_dropped_debug_log_count() const { return dropped_log_count_[LOG_DEBUG]; }
  int64_t get_async_flush_log_speed() const { return last_async_flush_count_per_sec_; }
  bool enable_async_log() const { return enable_async_log_; }
  void set_enable_async_log(const bool flag) { enable_async_log_ = flag; }
  void set_stop_append_log() { stop_append_log_ = true; }
  void disable() { stop_append_log_ = true; }
  void set_disable_logging(const bool flag) { disable_logging_ = flag; }
  bool is_enable_logging() const { return !disable_logging_; }
  static void get_pop_limit(const int32_t level, int64_t &timeout_us);
  bool has_stop_flush() const { return stop_flush_; }
  void set_stop_flush() { stop_flush_ = true; }

  //@brief thread buffer for printing log
  //@param no_trace true:get the buffer not controlled by trace mode
  LogBuffer *get_thread_buffer();

  //@brief set thread trace mode
  void set_trace_mode(bool trace_mode);
  bool get_trace_mode();

  //print thread trace buffer
  void print_trace_buffer(int32_t level, const char*file, int32_t line, const char *function);

  int64_t get_write_size() const { return log_file_[FD_DEFAULT_FILE].write_size_; }
  int64_t get_write_count() const { return log_file_[FD_DEFAULT_FILE].write_count_; }
  int64_t get_large_write_count() const { return large_write_count_[FD_DEFAULT_FILE]; }
  int64_t get_xflush_write_size() const { return log_file_[FD_XFLUSH_FILE].write_size_; }
  int64_t get_xflush_write_count() const { return log_file_[FD_XFLUSH_FILE].write_count_; }
  int64_t get_xflush_large_write_count() const { return large_write_count_[FD_XFLUSH_FILE]; }
  const ObLogFileStruct& get_log_file(const ObLogFDType type) const { return log_file_[type]; }

  //@brief Log the message without level checking.
  void log_message(const char *mod_name,
                   const int32_t level,
                   const char *file,
                   const int32_t line,
                   const char *function,
                   const char *fmt, ...) __attribute__((format(printf, 7, 8)));

  void log_message(const ObLogFDType type,
                   const char *mod_name,
                   const int32_t level,
                   const char *file,
                   const int32_t line,
                   const char *function,
                   const char *fmt, ...) __attribute__((format(printf, 8, 9)));

  void log_message(const ObLogFDType type,
                   const char *mod_name,
                   const int32_t level,
                   const char *file,
                   const int32_t line,
                   const char *function,
                   const char *fmt,
                   va_list args);

  void log_user_message(const UserMsgLevel user_msg_level,
                        const int errcode,
                        const char *fmt, ...) __attribute__((format(printf, 4, 5)));

  void log_message_kv(const char *mod_name,
                             const int32_t level,
                             const char *file,
                             const int32_t line,
                             const char *function,
                             const char *info_string);

  void log_message_kv(const ObLogFDType type,
                             const char *mod_name,
                             const int32_t level,
                             const char *file,
                             const int32_t line,
                             const char *function,
                             const char *info_string);

  inline void log_user_message_info(const char *mod_name,
                             UserMsgLevel user_msg_level,
                             const int32_t level,
                             const char *file,
                             const int32_t line,
                             const char *function,
                             int errcode,
                             const char *info_string);

  int async_log_message_kv(const ObLogFDType type,
                           const char *mod_name,
                           const int32_t level,
                           const LogLocation &location,
                           const char *info_string,
                           const int64_t string_len);

  //@brief Log the message without level checking.
  void async_log_message(const char *mod_name,
                         const int32_t level,
                         const char *file,
                         const int32_t line,
                         const char *function,
                         const char *fmt, ...) __attribute__((format(printf, 7, 8)));

  void async_log_message(const ObLogFDType type,
                         const char *mod_name,
                         const int32_t level,
                         const char *file,
                         const int32_t line,
                         const char *function,
                         const char *fmt, ...) __attribute__((format(printf, 8, 9)));

  void async_log_message(const ObLogFDType type,
                         const char *mod_name,
                         const int32_t level,
                         const char *file,
                         const int32_t line,
                         const char *function,
                         const char *fmt,
                         va_list args);
  //@brief Rename the log to a filename with version info. And open a new file with the old
  void force_rotate_log(const ObLogFDType &type, const char *version);

  DEFINE_LOG_PRINT_KV(1);
  DEFINE_LOG_PRINT_KV(2);
  DEFINE_LOG_PRINT_KV(3);
  DEFINE_LOG_PRINT_KV(4);
  DEFINE_LOG_PRINT_KV(5);
  DEFINE_LOG_PRINT_KV(6);
  DEFINE_LOG_PRINT_KV(7);
  DEFINE_LOG_PRINT_KV(8);
  DEFINE_LOG_PRINT_KV(9);
  DEFINE_LOG_PRINT_KV(10);
  DEFINE_LOG_PRINT_KV(11);
  DEFINE_LOG_PRINT_KV(12);
  DEFINE_LOG_PRINT_KV(13);
  DEFINE_LOG_PRINT_KV(14);
  DEFINE_LOG_PRINT_KV(15);
  DEFINE_LOG_PRINT_KV(16);
  DEFINE_LOG_PRINT_KV(17);
  DEFINE_LOG_PRINT_KV(18);
  DEFINE_LOG_PRINT_KV(19);
  DEFINE_LOG_PRINT_KV(20);

  //@brief Check whether the level to print.
  inline bool need_to_print(const int32_t level) {return (level <= get_log_level());}
  inline bool need_to_print_monitor(const int32_t level) {return (level <= get_monitor_log_level());}
  //@brief Check whether the xflush level to print.
  inline bool need_to_print_xflush(const int32_t level) {return (level <= get_xflush_log_level());}
  //@brief Check whether the level of the par-module to print.
  bool need_to_print(const uint64_t par_mod_id, const int32_t level);
  //@brief Check whether the level of the sub-module to print.
  inline bool need_to_print(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                            const int32_t level);

  //@brief Set the log-file's name.
  //@param[in] filename The log-file's name.
  //@param[in] flag Whether redirect the stdout and stderr to the descriptor of the log-file.
  //FALSE:redirect TRUE:no redirect.
  //@param[in] open_wf whether create warning log-file to store warning buffer.
  //@param[in] finename of xflush log-file's name.
  void set_file_name(const ObLogFDType type,
                     const char *filename,
                     const bool flag = false,
                     const bool open_wf = false);

  int reopen_monitor_log();

  //flag move to ObLogFileStruct, so this func do nothing
  void set_log_warn(bool log_warn) { UNUSED(log_warn); }
  void disable_thread_log_level() { disable_thread_log_level_ = true; }

  //@brief Check the log-file's status.
  void check_file();
  //@brief Check the log-file's status.
  void check_file(ObLogFileStruct &log_struct, const bool redirect_flag);

  //@brief Set whether checking the log-file at each message logging.
  //@param[in] v 1:with check, 0:without check
  inline void set_check(const bool v) { force_check_ = v;}

  //@brief Set the max log-file size. The unit is byte. Default value and max value are 1GB.
  void set_max_file_size(int64_t max_file_size);

  //@brief Get current time.
  static inline struct timeval get_cur_tv();

  //@brief Get the process-only ObLogger.
  static inline ObLogger &get_logger();

  //@brief Regist par-module name with par_mod_id.
  int register_mod(const uint64_t par_mod_id, const char *par_mod_name);
  //@brief Regist sub-module name with par_mod_id and sub_mod_id.
  int register_mod(const uint64_t par_mod_id, const uint64_t sub_mod_id, const char *sub_mod_name);

  int32_t get_log_level(const uint64_t par_mod_id) const;
  int32_t get_log_level(const uint64_t par_mod_id, const uint64_t sub_mod_id) const;

  //@brief Set global log-file's level and warning log-file's level.
  //@param[in] level The log-file's level.
  //@param[in] wf_level The warning log-file's level.
  //@param[in] version The time(us) change the log level.
  void set_log_level(const char *level, const char *wf_level = NULL, int64_t version = 0);
  //@brief Set global level.
  inline void set_log_level(const int32_t level, int64_t version = 0)
  {set_log_level(static_cast<int8_t>(level), version);}
  void set_log_level(const int8_t level, int64_t version = 0);
  int set_mod_log_levels(const char* level_str, int64_t version = 0);
  int set_monitor_log_level(const char* level_str);
  int set_xflush_log_level(const char* level_str);
  int get_log_level_from_str(const char* level_str, int8_t &level);

  inline void down_log_level(int64_t version = 0) {set_log_level(id_level_map_.get_level() + 1, version);}
  inline void up_log_level(int64_t version = 0) {set_log_level(id_level_map_.get_level() - 1, version);}

  inline int32_t get_level() const {return id_level_map_.get_level();}
  //@brief current log level, with session log level considered.
  inline int32_t get_log_level() const;
  inline int32_t get_monitor_log_level() const { return static_cast<int32_t>(monitor_level_); }
  inline int32_t get_xflush_log_level() const { return static_cast<int32_t>(xflush_level_); }
  inline const char *get_level_str() const { return errstr_[id_level_map_.get_level()]; }
  int get_level_str(const int8_t level_id, const char *&level_str) const;

  //@brief Get the int8_t type representing the level in string.
  int level_str2int(const char *level_name, int8_t &level_int);

  int parse_check(const char *str, const int32_t str_length);
  int parse_check(const char *str, const int32_t str_length, int32_t &valid_length);
  //@brief Parse the string like "ALL.*:INFO, SQL.ENG:DEBUG, COMMON.*:ERROR",
  //and set the global level map.
  int parse_set(const char *str, const int32_t str_length, int64_t version = 0);
  //for safe, this function name different with parse_set.
  int parse_set_with_valid_ret(const char *str, const int32_t str_length,
                               int32_t &valid_length, int64_t version = 0);
  //@brief Parse the string like "ALL.*:INFO, SQL.ENG:DEBUG, COMMON.*:ERROR",
  //and set the level map.
  int parse_set(const char *str,
                const int32_t str_length,
                int32_t &valid_length,
                ObLogIdLevelMap &id_level_map);

  //@brief get name_id_map_
  const ObLogNameIdMap &get_name_id_map() const { return name_id_map_; }
  //@brief get id_level_map_
  const ObLogIdLevelMap &get_id_level_map() const { return id_level_map_; }

  void set_logger_callback_handler(LoggerCallbackHandler callback_handler) { callback_handler_ = callback_handler; }

private:
  ObLogger();

  //@brief If version <= 0, return true.
  //If version > 0, return version > level_version_ and if true, update level_version_.
  bool check_and_set_level_version(int64_t version);

  //@brief get thread buffer mgr
  ObLogger::LogBufferMgr *get_buffer_mgr();

  void log_tail(const int32_t level, LogBuffer &log_buffer);

  void log_head_info(const ObLogFDType type,
                     const char *mod_name,
                     int32_t level,
                     LogLocation location,
                     LogBuffer &log_buffer);

  void log_data(const char *mod_name,
                const int32_t level,
                LogLocation location,
                LogBuffer &log_buffer);

  void log_data(const ObLogFDType type,
                const char *mod_name,
                const int32_t level,
                LogLocation location,
                LogBuffer &log_buffer);

  void insert_warning_buffer(const UserMsgLevel user_msg_level, const int errcode,
                             const char *data, const int64_t data_len);

  int get_mod_set(const char *par_mod,
                  const char *sub_mod,
                  const char *level,
                  ModSetting &mod_set);

  //@brief When setting mod_setting_list = NULL, do not return mod_setting_list.
  int parse_check(const char *str,
                  const int32_t str_length,
                  int32_t &valid_length,
                  void *mod_setting_list/*ObList<ModSetting>*/);

  int setting_list_processing(ObLogIdLevelMap &id_level_map,
                              void *mod_setting_list/*ObList<ModSetting>* */);

  //@brief Rename the log to a filename with time postfix. And open a new file with the old
  void rotate_log(const int64_t size, const bool redirect_flag, ObLogFileStruct &log_struct);
  //@brief Rename the log to a filename with fmt. And open a new file with the old, then add old file to file_list.
  //@param[in] filename the old filename to rotate.
  //@param[in] fmt the string to format the time with the function named 'strftime()'.
  //@param[in] whether redirect, FALSE:redirect TRUE:no redirect
  //@param[out] after retated log, open new file_fd
  //@param[out] after retated wf log, open new wf_file_fd
  void rotate_log(const char *filename,
                  const char *fmt,
                  const bool redirect_flag,
                  const bool open_wf_flag,
                  const bool enable_wf_flag,
                  int32_t &fd,
                  int32_t &wf_fd);

  static void *async_flush_log_handler(void *arg);
  void do_async_flush_to_file(ObLogItem **log_item, const int64_t count);
  void do_async_flush_log();

  int async_log_data_header(const ObLogFDType type,
                            ObLogItem &item,
                            const timeval &tv,
                            const char *mod_name,
                            const int32_t level,
                            const char *file,
                            const int32_t line,
                            const char *function);

  int try_upgrade_log_item(ObLogItem *&log_item, bool &upgrade_result);

  int async_log_data_body(ObLogItem &log_item, const char *info_string, const int64_t string_len);
  void check_log_end(ObLogItem &log_item, int64_t pos);
  int check_error_log(ObLogItem &log_item);
  int check_callback(ObLogItem &item);

  ObLogItemType calc_log_item_type(const int32_t level, const int64_t data_len = 0) const;
  int pop_from_free_queue(const int32_t level, ObLogItem *&log_item, const int64_t data_len = 0);
  int pop_from_free_queue(const int32_t level, ObLogItem *&log_item, const ObLogItemType type);
  void push_to_free_queue(ObLogItem *&log_item);
  int push_to_async_queue(ObLogItem &log_item);
  void inc_dropped_log_count(const int32_t level);
  bool is_monitor_file(const ObLogFDType type);

private:
  static const char *const errstr_[];
  // default log rate limiter if there's no tl_log_limiger
  //used for stat logging time and log dropped
  static __thread uint64_t curr_logging_seq_;
  static __thread uint64_t last_logging_seq_;
  static __thread int64_t last_logging_cost_time_us_;
  //whether to stop logging
  static __thread bool disable_logging_;

  ObLogFileStruct log_file_[MAX_FD_FILE];

  int64_t max_file_size_;
  pthread_mutex_t file_size_mutex_;

  //log level
  ObLogNameIdMap name_id_map_;
  ObLogIdLevelMap id_level_map_;//level of log-file
  int8_t monitor_level_;
  int8_t xflush_level_;
  int8_t wf_level_;//level of warning log-file
  int64_t level_version_;//version of log level

  bool disable_thread_log_level_;
  bool force_check_;//whether check log-file at each message logging.
  bool redirect_flag_;//whether redirect
  volatile bool can_print_;//when disk has no space, logger control
  volatile bool stop_flush_;//when destruct oblogger, stop it

  bool enable_async_log_;//if false, use sync way logging
  bool stop_append_log_;//whether stop product log
  LightyQueue *free_item_queue_[MAX_LOG_ITEM_TYPE];//producer get free buff from here and then fill data
  LightyQueue *async_log_queue_;//customer get log from here and then write to file
  //used for statistics
  int64_t dropped_log_count_[LOG_MAX_LEVEL];
  int64_t large_write_count_[MAX_FD_FILE];
  int64_t last_async_flush_count_per_sec_;
  pthread_t async_tid_;//async thread thread id

  LoggerCallbackHandler callback_handler_;
};

class ObLoggerTraceMode
{
public:
  ObLoggerTraceMode()
  { OB_LOGGER.set_trace_mode(true); }
  ~ObLoggerTraceMode()
  { OB_LOGGER.set_trace_mode(false); }

  void set_trace_mode(bool trace_mode)
  { OB_LOGGER.set_trace_mode(trace_mode); }
  //used print_trace_buffer(OB_LOG_LEVEL(INFO))
  void print_trace_buffer(int32_t level, const char*file, int32_t line, const char *function)
  {
    OB_LOGGER.need_to_print(level) ? OB_LOGGER.print_trace_buffer(level, file, line, function)
                                   : (void) 0;
  }
};

inline int8_t ObLogIdLevelMap::get_level(uint64_t par_mod_id) const
{
  return (par_mod_id < MAX_PAR_MOD_SIZE) ?
         log_level_[par_mod_id][0] : static_cast<int8_t>(OB_LOG_LEVEL_NP);
}

inline int8_t ObLogIdLevelMap::get_level(uint64_t par_mod_id, uint64_t sub_mod_id) const
{
  return (par_mod_id < MAX_PAR_MOD_SIZE && sub_mod_id < MAX_SUB_MOD_SIZE) ?
         log_level_[par_mod_id][sub_mod_id + 1] : static_cast<int8_t>(OB_LOG_LEVEL_NP);
}

inline void ObThreadLogLevel::init()
{
  ObThreadLogLevel *trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->id_level_map_ = NULL;
  }
}

inline void ObThreadLogLevel::init(const ObLogIdLevelMap *id_level_map)
{
  ObThreadLogLevel *trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->id_level_map_ = id_level_map;
    trace_log_level->level_ = (id_level_map == NULL
                               ? (int8_t)OB_LOG_LEVEL_NONE : id_level_map->non_mod_level_);
  }
}

inline void ObThreadLogLevel::init(const int8_t level)
{
  ObThreadLogLevel *trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->level_ = level;
  }
}

inline void ObThreadLogLevel::clear()
{
  ObThreadLogLevel *trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    trace_log_level->id_level_map_ = NULL;
    trace_log_level->level_ = OB_LOG_LEVEL_NONE;
  }
}

inline const ObLogIdLevelMap *ObThreadLogLevel::get()
{
  const ObLogIdLevelMap *ret = NULL;
  ObThreadLogLevel *trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    ret = trace_log_level->id_level_map_;
  }
  return ret;
}

inline int8_t ObThreadLogLevel::get_level()
{
  int8_t level = OB_LOG_LEVEL_NONE;
  ObThreadLogLevel *trace_log_level = get_thread_log_level();
  if (NULL != trace_log_level) {
    level = trace_log_level->level_;
  }
  return level;
}

inline ObThreadLogLevel *ObThreadLogLevel::get_thread_log_level()
{
  static __thread ObThreadLogLevel *TRACE_LOG_LEVEL = NULL;
  if (NULL == TRACE_LOG_LEVEL) {
    TRACE_LOG_LEVEL = new (std::nothrow) ObThreadLogLevel();
  }
  return TRACE_LOG_LEVEL;
}

inline void ObLogger::check_log_end(ObLogItem &log_item, int64_t pos)
{
  const int64_t buf_size = log_item.get_buf_size();
  if (buf_size > 0) {
    if (pos < 0) {
      pos = 0;
    } else if (pos > buf_size - 2) {
      pos = buf_size - 2;
    }
    char *data = log_item.get_buf();
    data[pos++] = '\n';
    data[pos] = '\0';
    log_item.set_data_len(pos);
  }
}

inline void ObLogger::log_user_message_info(
    const char *mod_name,
    UserMsgLevel user_msg_level,
    const int32_t level,
    const char *file,
    const int32_t line,
    const char *function,
    int errcode,
    const char *info_string)
{
  if (OB_NOT_NULL(info_string)) {
    insert_warning_buffer(user_msg_level, errcode, info_string, static_cast<int64_t>(strlen(info_string)));
    if (need_to_print(level)) {
      log_message_kv(mod_name, level, file, line, function, info_string, "ret", errcode);
    }
  }
}

inline bool ObLogger::need_to_print(const uint64_t par_mod_id, const int32_t level)
{
  return (level <= get_log_level(par_mod_id));
}

inline bool ObLogger::need_to_print(const uint64_t par_mod_id, const uint64_t sub_mod_id,
                                    const int32_t level)
{
  return (level <= get_log_level(par_mod_id, sub_mod_id));
}

inline int32_t ObLogger::get_log_level() const
{
  int8_t cur_level = OB_LOG_LEVEL_INFO;
  cur_level = id_level_map_.get_level();

  if (!disable_thread_log_level_) {
    const ObLogIdLevelMap *session_id_level_map = ObThreadLogLevel::get();
    int8_t thread_level = ObThreadLogLevel::get_level();
    if (NULL != session_id_level_map) {
      cur_level = session_id_level_map->get_level();
    } else if (OB_LOG_LEVEL_NONE != thread_level) {
      cur_level = thread_level;
    }
  }
  return cur_level;
}

inline int32_t ObLogger::get_log_level(const uint64_t par_mod_id) const
{
  int8_t cur_level = OB_LOG_LEVEL_INFO;
  cur_level = id_level_map_.get_level(par_mod_id);

  if (!disable_thread_log_level_) {
    const ObLogIdLevelMap *session_id_level_map = ObThreadLogLevel::get();
    int8_t thread_level = ObThreadLogLevel::get_level();
    if (NULL != session_id_level_map) {
      cur_level = session_id_level_map->get_level(par_mod_id);
    } else if (OB_LOG_LEVEL_NONE != thread_level) {
      cur_level = thread_level;
    }
  }
  return cur_level;
}

inline int32_t ObLogger::get_log_level(const uint64_t par_mod_id, const uint64_t sub_mod_id) const
{
  int8_t cur_level = OB_LOG_LEVEL_INFO;
  cur_level = id_level_map_.get_level(par_mod_id, sub_mod_id);

  if (!disable_thread_log_level_) {
    const ObLogIdLevelMap *session_id_level_map = ObThreadLogLevel::get();
    int8_t thread_level = ObThreadLogLevel::get_level();

    if (NULL != session_id_level_map) {
      cur_level = session_id_level_map->get_level(par_mod_id, sub_mod_id);
    } else if (OB_LOG_LEVEL_NONE != thread_level) {
      cur_level = thread_level;
    }
  }
  return cur_level;
}

inline struct timeval ObLogger::get_cur_tv()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv;
}

inline ObLogger& ObLogger::get_logger()
{
  static ObLogger logger;
  return logger;
}

inline int ObLogger::set_mod_log_levels(const char *level_str, int64_t version)
{
  return parse_set(level_str, static_cast<int32_t>(std::strlen(level_str)), version);
}

inline int64_t ObLogger::get_alloced_log_item_count(const ObLogItemType type)
{
  if (OB_LIKELY(type < MAX_LOG_ITEM_TYPE) && OB_LIKELY(type >= 0)) {
    return ObLogItemFactory::alloc_count_[type];
  } else {
    return 0;
  }
}

inline int64_t ObLogger::get_released_log_item_count(const ObLogItemType type)
{
  if (OB_LIKELY(type < MAX_LOG_ITEM_TYPE) && OB_LIKELY(type >= 0)) {
    return ObLogItemFactory::release_count_[type];
  } else {
    return 0;
  }
}

inline bool ObLogger::is_monitor_file(const ObLogFDType type)
{
  return FD_DIGEST_FILE == type
         || FD_ERROR_FILE == type
         || FD_SLOW_FILE == type
         || FD_STAT_FILE == type
         || FD_LIMIT_FILE == type
         || FD_POOL_FILE == type
         || FD_POOL_STAT_FILE == type;
}

}//common
}//oceanbase

#endif
