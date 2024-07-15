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

#ifndef OBPROXY_MAIN_H
#define OBPROXY_MAIN_H

#include <getopt.h>
#include "lib/ob_define.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/charset/ob_ctype.h"
#include "ob_proxy.h"

namespace oceanbase
{
namespace obproxy
{
class ObProxyMain
{

public:
  virtual ~ObProxyMain() {}

  static ObProxyMain *get_instance();

  static int do_detect_sig();
  static int do_monitor_mem();
  static int do_detect_sqlaudit();

  static void print_memory_usage();

  //parse argv, and start obproxy
  int start(const int argc, char *const argv[]);
  void print_usage() const;
  void destroy();
  int schedule_detect_task();
  int64_t get_startup_time() { return startup_time_us_; }

  ObProxy &get_proxy() { return obproxy_; }
private:
  //singleton mode
  ObProxyMain() : startup_time_us_(0), sig_detect_cont_(NULL), mem_monitor_cont_(NULL),
                  sqlaudit_detect_cont_(NULL), pos_(0)
  {
    memset(history_mem_size_, 0, sizeof(history_mem_size_));
  }

  static void sig_direct_handler(const int sig);
  static void sig_async_handler(const int sig);
  static void freeze_mem_alloc();
  static void unfreeze_mem_alloc();
  int add_sig_ignore_catched(struct sigaction &action, const int sig) const;
  int add_sig_default_catched(struct sigaction &action, const int sig) const;
  int add_sig_direct_catched(struct sigaction &action, const int sig, const int flag = 0) const;
  int add_sig_async_catched(struct sigaction &action, const int sig, const int flag = 0) const;

  int do_start_work(ObProxyOptions &opts);

  int parse_cmd_line(const int argc, char *const argv[],
                     ObProxyOptions &opts) const;

  static void print_glibc_memory_usage();
  static void print_sqlaudit_memory_usage();
  static void print_memory_usage(const int64_t hold, const int64_t used,
                                 const int64_t count, const char *name);
  void print_version() const;
  void print_releaseid() const;
  int print_args(const int argc, char *const argv[]) const;

  int get_opts_setting(struct option long_opts[], const int64_t long_opts_cnt,
                       char short_opts[], const int64_t short_opts_cnt) const;
  int parse_short_opt(const int32_t c, const char *value, ObProxyOptions &opts) const;

  int str_to_port(const char *sval, int32_t &port) const;
  int str_to_version(const char *sval, int64_t &version) const;
  int to_int64(const char *sval, int64_t &ival) const;
  int use_daemon();
  int get_log_file_name(const ObLogFDType type, char *file_name, const int64_t len);
  int handle_inherited_sockets(const int argc, char *const argv[]);
  int init_log();
  int init_signal();
  int close_all_fd(const int32_t listen_ipv4_fd, const int32_t listen_ipv6_fd, const int32_t rpc_listen_ipv4_fd = 0, const int32_t rpc_listen_ipv6_fd = 0);
  int init_data_type();

  class ObLogLevel
  {
  public:
    ObLogLevel(){}
    ~ObLogLevel(){}
    void down_log_level();
    void up_log_level();
  private:
    DISALLOW_COPY_AND_ASSIGN(ObLogLevel);
  };

private:
  static const int64_t HISTORY_MEMORY_RECORD_COUNT = 10;
  // fixed value 70M, the memory not alloc by ob_malloc
  static const int64_t OTHER_MEMORY_SIZE = 70 * 1024 * 1024;

  static ObProxyMain *instance_;

  int64_t startup_time_us_;

  ObProxy obproxy_;
  ObAppVersionInfo app_info_;

  obutils::ObAsyncCommonTask *sig_detect_cont_;
  obutils::ObAsyncCommonTask *mem_monitor_cont_;
  obutils::ObAsyncCommonTask *sqlaudit_detect_cont_;
  uint64_t pos_;
  int64_t history_mem_size_[HISTORY_MEMORY_RECORD_COUNT];

  DISALLOW_COPY_AND_ASSIGN(ObProxyMain);
};

#define GET_OBPROXY ObProxyMain::get_instance()->get_proxy()

} //end of namespace obproxy
} //end of namespace oceanbase
#endif /* OBPROXY_MAIN_H */
