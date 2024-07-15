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

#ifndef OBPROXY_HOT_UPGRADE_PROCESSOR_H
#define OBPROXY_HOT_UPGRADE_PROCESSOR_H

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/task/ob_timer.h"
#include "utils/ob_proxy_hot_upgrader.h"
#include "obutils/ob_proxy_config.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObMysqlProxy;
class ObRawMysqlClient;
}
namespace obutils
{
class ObProxyHotUpgrader
{
public:
  ObProxyHotUpgrader() {}
  ~ObProxyHotUpgrader() {}
  static int spawn_process();
  static int fill_exec_ctx(ObExecCtx &ctx);
  static int get_envp(char **&envp);
};

class ObAsyncCommonTask;
class ObProxyServerInfo;
class ObHotUpgradeProcessor
{
public:
  ObHotUpgradeProcessor();
  virtual ~ObHotUpgradeProcessor() { destroy(); }
  void destroy();

  int init(proxy::ObMysqlProxy &mysql_proxy);
  int check_timeout_rollback(); // check and do timeout rollback
  const char *get_proxy_ip() const { return proxy_ip_; };
  int32_t get_proxy_port() const { return proxy_port_; };
  char *get_proxy_self_md5() { return proxy_self_md5_; };

  bool is_timeout_rollback() const { return is_timeout_rollback_; };
  bool is_first_upgrade() const { return 0 == upgrade_failures_; };
  void reset_upgrade_failures() { upgrade_failures_ = 0; };
  bool is_self_md5_available() const { return is_self_md5_available_; };
  void set_self_md5_available() { is_self_md5_available_ = true; };
  bool is_self_binary() const { return is_self_binary_; };
  void set_is_self_binary(const bool flag) { is_self_binary_ = flag; };

  int state_wait_hu_cmd(const ObProxyServerInfo &proxy_info); // wait hot_upgrade cmd
  int state_fork_new_proxy();                                 // fork new proxy
  int state_wait_cr_cmd(const ObProxyServerInfo &proxy_info); // wait commit and rollback cmd
  int state_wait_cr_finish();                                 // wait commit and rollback finish

  static int do_repeat_task();
  int handle_hot_upgrade();
  ObAsyncCommonTask *get_hot_upgrade_cont() { return hu_cont_; }
  ObAsyncCommonTask *get_new_hot_upgrade_cont() { return hot_upgrade_cont_; }
  bool is_same_hot_binary(const common::ObString &name, const common::ObString &md5) const
  {
    return (name == hot_binary_name_ && md5 == hot_binary_md5_);
  }
  bool is_same_cold_binary(const common::ObString &name, const common::ObString &md5) const
  {
    return (name == cold_binary_name_ && md5 == cold_binary_md5_);
  }

  static int do_hot_upgrade_repeat_task();
  static void update_hot_upgrade_interval();
  int start_hot_upgrade_task();
  int do_hot_upgrade_work();
  void do_hot_upgrade_internal(); // need do hot upgrade for internal reason
  DECLARE_TO_STRING;

private:
  int prepare_binary();
  int check_binary_availability();
  int fetch_new_proxy_bin(const char *binary);
  int check_proxy_bin_md5(const char *binary);
  int check_proxy_bin_release(const char *binary);

  int state_parent_wait_cr_cmd(const ObProxyServerInfo &proxy_info);  // parent wait commit and rollback cmd
  int state_sub_wait_cr_cmd();                                        // sub wait commit and rollback cmd

  int rename_binary(const char *obproxy_tmp) const;
  int restore_binary() const;
  int dump_config() const;
  void schedule_timeout_rollback();
  void cancel_timeout_rollback();
  int backup_old_binary() const;

  int schedule_upgrade_task();
  int check_arguments(const ObProxyServerInfo &proxy_info); // check arguments before starting hot upgrade
  int init_raw_client(proxy::ObRawMysqlClient &raw_client, const ObProxyLoginUserType type);
  int send_cmd_and_check_response(const char *sql, const ObProxyLoginUserType type,
                                  const int64_t retry_times = 1, const int64_t expected_affected_rows = 0);
  int send_commit_via_subprocess();
  int check_subprocess_available();

public:
  static const int64_t OB_MAX_CHECK_SUBPROCESS_FAILURES = 32;
  //used for hot upgrade (with fork new process)
  char hot_binary_name_[OB_MAX_PROXY_BINARY_VERSION_LEN + 1];
  char hot_binary_md5_[OB_DEFAULT_PROXY_MD5_LEN + 1];

  //used for cold upgrade (upgrade bin without restart)
  char cold_binary_name_[OB_MAX_PROXY_BINARY_VERSION_LEN + 1];
  char cold_binary_md5_[OB_DEFAULT_PROXY_MD5_LEN + 1];

private:
  bool is_inited_;
  bool is_timeout_rollback_;
  bool is_self_md5_available_;
  bool is_self_binary_;
  ObHotUpgradeCmd cmd_;
  proxy::ObMysqlProxy *mysql_proxy_;
  ObAsyncCommonTask *hu_cont_;
  ObAsyncCommonTask *hot_upgrade_cont_;
  ObHotUpgraderInfo &info_;
  char proxy_ip_[common::MAX_IP_ADDR_LENGTH]; // ip primary key
  char proxy_self_md5_[OB_DEFAULT_PROXY_MD5_LEN + 1];
  int32_t proxy_port_;                    // port primary key
  int64_t upgrade_failures_;              // upgrade failures when handling 'hot_upgrade' cmd
  int64_t check_available_failures_;      // check available failures when handling 'restart' cmd
  ObHRTime timeout_rollback_timeout_at_;  // do timeout rollback timeout_at, during FORK_NEW_PROXY and WAIT_CR_CMD state
  ObHRTime wait_cr_finish_timeout_at_;    // wait commit(or rollback) finish timeout_at, during WAIT_CR_FINISH state

  DISALLOW_COPY_AND_ASSIGN(ObHotUpgradeProcessor);
};

ObHotUpgradeProcessor &get_global_hot_upgrade_processor();
int get_binary_md5(const char *binary, char *md5_buf, const int64_t md5_buf_len);

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_HOT_UPGRADE_PROCESSOR_H */
