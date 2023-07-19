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

#ifndef OBPROXY_HOT_UPGRADER_H
#define OBPROXY_HOT_UPGRADER_H

#include "utils/ob_proxy_lib.h"
#include "lib/net//ob_addr.h"

namespace oceanbase
{
namespace obproxy
{
#define OBPROXY_INHERITED_IPV4_FD "OBPROXY_INHERITED_FD"
#define OBPROXY_INHERITED_IPV6_FD "OBPROXY_INHERITED_IPV6_FD"
extern volatile int g_proxy_fatal_errcode;

enum ObReloadConfigStatus
{
  RCS_NONE = 0,
  RCS_RELOADING,
  RCS_RELOAD_FAIL,
  RCS_RELOAD_SUCC,
  RCS_MAX,
};

enum ObBatchUpgradeStatus
{
  BUS_UPGRADE_NONE = 0,
  BUS_UPGRADE_OFF,
  BUS_UPGRADE_PART,//upgrade part proxy
  BUS_UPGRADE_ON,  //do batch auto-upgrade
  BUS_UPGRADE_BIN, //do batch upgrade_bin
  BUS_MAX,
};

enum ObHotUpgradeCmd
{
  HUC_NONE = 0,
  HUC_HOT_UPGRADE,
  HUC_COMMIT,
  HUC_ROLLBACK,

  HUC_EXIT,             //used for a remote kill
  HUC_RESTART,          //used for a remote restart
  HUC_AUTO_UPGRADE,     //used for auto upgrade
  HUC_UPGRADE_BIN,      //used for upgrade binary

  HUC_LOCAL_EXIT,    //used for a config cmd exit
  HUC_LOCAL_RESTART, //used for a config cmd restart
  HUC_MAX,
};

enum ObHotUpgradeState
{
  HU_STATE_WAIT_HU_CMD = 0,   //wait hot_upgrade cmd
  HU_STATE_FORK_NEW_PROXY,    //fork new proxy
  HU_STATE_WAIT_CR_CMD,       //wait commit and rollback cmd
  HU_STATE_WAIT_CR_FINISH,    //wait commit and rollback finish
  HU_STATE_WAIT_LOCAL_CR_FINISH,    //wait commit and rollback finish
  HU_STATE_MAX,
};

//this is finish status, not start status
enum ObHotUpgradeStatus
{
  //both parent and sub used
  HU_STATUS_NONE = 0,
  //parent used
  HU_STATUS_START_HOT_UPGRADE,
  HU_STATUS_FAILURE_RETRIES_TOO_MANY,

  //used for a remote restart
  HU_STATUS_UNAVAILABLE_BINARY,

  HU_STATUS_INVALID_ARGUMENT,
  HU_STATUS_SCHEDULE_CREATE_NEW_PROXY_EVENT_FAIL,
  HU_STATUS_FETCH_BIN_FAIL,
  HU_STATUS_CHECK_BIN_RELEASE_FAIL,
  HU_STATUS_CHECK_BIN_MD5_FAIL,
  HU_STATUS_BACKUP_BIN_FAIL,
  HU_STATUS_CREATE_NEW_PROXY_FAIL,


  HU_STATUS_CREATE_NEW_PROXY_SUCC,
  HU_STATUS_RECV_COMMIT_AND_EXIT,
  HU_STATUS_ROLLBACK_SUCC,
  HU_STATUS_TIMEOUT_ROLLBACK_SUCC,

  //sub used
  HU_STATUS_NEW_PROXY_CREATED_SUCC,
  HU_STATUS_COMMIT_SUCC,
  HU_STATUS_RECV_ROLLBACK_AND_EXIT,
  HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT,

  //both parent and sub used
  HU_STATUS_EXITED,

  //used for a remote kill
  HU_STATUS_DO_QUICK_EXIT,
  //used for a remote restart
  HU_STATUS_START_DOING_RESTART,

  //used for upgrade binary
  HU_STATUS_START_UPGRADE_BINARY,
  HU_STATUS_UPGRADE_BINARY_SUCC,

  HU_STATUS_MAX,
};

enum ObProxyLoginUserType
{
  USER_TYPE_NONE,
  USER_TYPE_METADB,
  USER_TYPE_ROOTSYS,
  USER_TYPE_PROXYSYS,//root@proxysys
  USER_TYPE_PROXYRO,//proxyro@sys
  USER_TYPE_INSPECTOR,//inspector@proxysys
  USER_TYPE_SHARDING,
  USER_TYPE_MAX,
};

// variables relatived to hot upgrade
class ObHotUpgraderInfo
{
public:
  ObHotUpgraderInfo() { reset(); }
  ~ObHotUpgraderInfo() { reset(); }
  static int get_rc_status(const common::ObString &status_str, ObReloadConfigStatus &status);
  static int get_bu_status(const common::ObString &status_string, ObBatchUpgradeStatus &status);
  static int get_hu_cmd(const common::ObString &cmd_str, ObHotUpgradeCmd &cmd);
  static int get_hu_sub_status(const common::ObString &sub_status_str, ObHotUpgradeStatus &sub_status);
  static common::ObString get_rc_status_string(const ObReloadConfigStatus status);
  static common::ObString get_bu_status_string(const ObBatchUpgradeStatus status);
  static common::ObString get_cmd_string(const ObHotUpgradeCmd cmd);
  static common::ObString get_state_string(const ObHotUpgradeState state);
  static common::ObString get_status_string(const ObHotUpgradeStatus status);
  static bool is_vailed_cmd(const ObHotUpgradeCmd cmd) { return (HUC_NONE <= cmd && cmd < HUC_MAX); };
  static bool is_failed_status(const ObHotUpgradeStatus status);
  static bool is_upgrade_bin_succ_status(const ObHotUpgradeStatus status);

  void reset();
  void set_main_arg(const int32_t argc, char *const *argv);
  void disable_net_accept();
  void enable_net_accept() { need_conn_accept_ = true; }
  bool is_parent() const { return is_parent_; }
  bool need_reject_metadb() const { return USER_TYPE_METADB == user_rejected_;}
  bool need_reject_proxyro() const { return USER_TYPE_PROXYRO == user_rejected_;}
  bool need_reject_proxysys() const { return USER_TYPE_PROXYSYS == user_rejected_;}
  bool need_reject_user() const;
  void set_rejected_user(ObProxyLoginUserType type);
  void reset_rejected_user() { user_rejected_ = USER_TYPE_NONE; }
  void update_parent_status(const ObHotUpgradeStatus parent_status);
  void update_sub_status(const ObHotUpgradeStatus status);
  void update_both_status(const ObHotUpgradeStatus parent_status, const ObHotUpgradeStatus sub_status);
  void update_last_hu_status(const ObHotUpgradeStatus parent_status, const ObHotUpgradeStatus sub_status);
  ObHotUpgradeStatus get_parent_status() const { return (is_parent_ ? status_ : parent_status_); };
  ObHotUpgradeStatus get_sub_status() const { return (is_parent_ ? sub_status_ : status_); };
  ObHotUpgradeState get_state() const { return state_; };
  void update_state(const ObHotUpgradeState state) { state_ = state; };
  void reset_sub_pid() { sub_pid_ = common::OB_INVALID_INDEX; };
  int fill_inherited_info(const bool is_server_service_mode, const int64_t upgrade_version);

  bool is_auto_upgrade() const { return HUC_AUTO_UPGRADE == cmd_; };
  bool is_hot_upgrade() const { return HUC_HOT_UPGRADE == cmd_; };
  bool is_restart() const { return HUC_RESTART == cmd_ || HUC_LOCAL_RESTART == cmd_; };
  bool is_local_restart() const { return HUC_LOCAL_RESTART == cmd_; };
  bool is_exit() const { return HUC_EXIT == cmd_; };
  bool is_local_exit() const { return HUC_LOCAL_EXIT == cmd_; };
  bool is_local_cmd() const { return HUC_LOCAL_EXIT <= cmd_ && cmd_ < HUC_MAX; };

  bool is_in_idle_state() const { return HU_STATE_WAIT_HU_CMD == state_; };
  bool is_in_wait_cr_state() const { return HU_STATE_WAIT_CR_CMD == state_; };
  bool enable_batch_upgrade() const;
  bool enable_reload_config() const;
  bool is_in_single_service() const { return (HU_STATE_WAIT_HU_CMD == state_ || HU_STATE_FORK_NEW_PROXY == state_); };
  bool need_timeout_rollback_check() const { return (HU_STATE_FORK_NEW_PROXY == state_ || HU_STATE_WAIT_CR_CMD == state_); };
  bool is_sub_exited() const { return (OB_LIKELY(is_parent_) ? HU_STATUS_EXITED == sub_status_ : HU_STATUS_EXITED == status_); }
  bool is_parent_exited() const { return (OB_UNLIKELY(is_parent_) ? HU_STATUS_EXITED == status_ : HU_STATUS_EXITED == parent_status_); }
  bool is_doing_upgrade() const;
  bool need_report_info() const;
  bool need_report_status() const;
  bool is_graceful_exit_timeout(const ObHRTime cur_time) const;
  bool is_graceful_offline_timeout(const ObHRTime cur_time) const;
  DECLARE_TO_STRING;

public:
  static const int64_t MAX_UPGRADE_VERSION_BUF_SIZE = 64;
  static const int64_t MAX_RESTART_BUF_SIZE = 64;
  static const int64_t OB_MAX_INHERITED_ARGC = 4;

  int ipv4_fd_;                              // listen fd, which to be passed to sub process
  int ipv6_fd_;
  int received_sig_;
  ObReloadConfigStatus rc_status_;      // identify the current status for reload config
  volatile pid_t sub_pid_;              // sub process pid if fork succeed
  ObHotUpgradeCmd cmd_;                 // identify the current cmd in hot_upgrade
  volatile ObHotUpgradeState state_;    // identify the current state in hot_upgrade
  volatile ObHotUpgradeStatus status_;  // identify the current status in hot_upgrade
  union
  {
    volatile ObHotUpgradeStatus parent_status_; // identify the parent state in hot_upgrade if current is sub
    volatile ObHotUpgradeStatus sub_status_;    // identify the sub state in hot_upgrade if current is parent
  };
  volatile ObHotUpgradeStatus last_parent_status_;// identify last parent status in ob_all_proxy
  volatile ObHotUpgradeStatus last_sub_status_;   // identify last sub status in ob_all_proxy
  volatile bool is_parent_;                       // default false, indicate whether it is parent process
  bool is_inherited_;                             // is this process inherited from parent process
  volatile bool is_active_for_rolling_upgrade_;    // default true
  volatile bool need_conn_accept_;                // whether need accept new connection
  volatile ObProxyLoginUserType user_rejected_;     // default NONE, indicate use which user need rejected

  ObHRTime graceful_exit_start_time_;             // graceful exit start time when receiving graceful exit
  ObHRTime graceful_exit_end_time_;               // graceful exit end time, after it, force exit process
  ObHRTime graceful_offline_start_time_;          // graceful offline start time when receiving offline cmd
  ObHRTime graceful_offline_end_time_;            // graceful offline end time, after it, force close connection
  int64_t active_client_vc_count_;                // active client session count when receive graceful exit

  int64_t upgrade_version_;             // mainly used for connection id during server service mode
  char upgrade_version_buf_[MAX_UPGRADE_VERSION_BUF_SIZE];// used to pass upgrade version to sub process

  int32_t argc_;                        // main's argc, used to be passed to sub process
  char *const *argv_;                   // main's argv
  char *inherited_argv_[OB_MAX_INHERITED_ARGC];// argv used for inherited sub process
  volatile bool parent_hot_upgrade_flag_;
  lib::ObMutex hot_upgrade_mutex_;

  common::ObAddr local_addr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHotUpgraderInfo);
};

inline bool ObHotUpgraderInfo::need_reject_user() const
{
  return (USER_TYPE_METADB == user_rejected_
          || USER_TYPE_PROXYRO == user_rejected_
          || USER_TYPE_PROXYSYS == user_rejected_);
}

inline void ObHotUpgraderInfo::set_rejected_user(ObProxyLoginUserType type)
{
  user_rejected_ = ((USER_TYPE_NONE < type && type < USER_TYPE_MAX)
      ? type
      : USER_TYPE_NONE);
}

inline bool ObHotUpgraderInfo::enable_batch_upgrade() const
{
  return (HU_STATE_WAIT_HU_CMD == state_
          && (HUC_NONE == cmd_ || HUC_COMMIT == cmd_ || HUC_ROLLBACK == cmd_));
};

inline bool ObHotUpgraderInfo::enable_reload_config() const
{
  return (HU_STATE_WAIT_HU_CMD == state_
          && !is_local_cmd());
}

inline bool ObHotUpgraderInfo::is_failed_status(const ObHotUpgradeStatus status)
{
  return (HU_STATUS_UNAVAILABLE_BINARY <= status && status <= HU_STATUS_CREATE_NEW_PROXY_FAIL);
}

inline bool ObHotUpgraderInfo::is_upgrade_bin_succ_status(const ObHotUpgradeStatus status)
{
  return HU_STATUS_UPGRADE_BINARY_SUCC == status;
}

inline void ObHotUpgraderInfo::update_parent_status(const ObHotUpgradeStatus parent_status)
{
  if (OB_LIKELY(is_parent_)) {
    status_ = parent_status;
  } else {
    parent_status_ = parent_status;
  }
}

inline void ObHotUpgraderInfo::update_sub_status(const ObHotUpgradeStatus sub_status)
{
  if (OB_LIKELY(is_parent_)) {
    sub_status_ = sub_status;
  } else {
    status_ = sub_status;
  }
}

inline void ObHotUpgraderInfo::update_both_status(const ObHotUpgradeStatus parent_status,
    const ObHotUpgradeStatus sub_status)
{
  if (is_parent_) {
    status_ = parent_status;
    sub_status_ = sub_status;
  } else {
    parent_status_ = parent_status;
    status_ = sub_status;
  }
}

inline void ObHotUpgraderInfo::update_last_hu_status(const ObHotUpgradeStatus parent_status,
    const ObHotUpgradeStatus sub_status)
{
  last_parent_status_ = parent_status;
  last_sub_status_ = sub_status;
}

inline bool ObHotUpgraderInfo::need_report_info() const
{
  //parent no need report info when the follow status_ happened:
  //1. HU_STATUS_CREATE_NEW_PROXY_SUCC
  //2. HU_STATUS_RECV_COMMIT_AND_EXIT
  //sub no need report info when the follow status_ happened:
  //1. HU_STATUS_RECV_ROLLBACK_AND_EXIT
  //2. HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT

  bool ret = false;
  if (is_parent_) {
    ret = (HU_STATUS_CREATE_NEW_PROXY_SUCC != status_ && HU_STATUS_RECV_COMMIT_AND_EXIT != status_);
  } else {
    ret = (HU_STATUS_RECV_ROLLBACK_AND_EXIT != status_ && HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT != status_);
  }
  return ret;
}

inline bool ObHotUpgraderInfo::is_doing_upgrade() const
{
  const ObHotUpgradeStatus status = get_parent_status();
  return (HU_STATUS_START_HOT_UPGRADE == status
          || HU_STATUS_START_DOING_RESTART == status
          || HU_STATUS_START_UPGRADE_BINARY == status);
}

inline bool ObHotUpgraderInfo::need_report_status() const
{
  //proxy no need report status when the follow cases happened:
  //1. current status are equal to last status
  //or
  //2. last parent status is failed status, while current status is doing upgrade
  return !((get_parent_status() == last_parent_status_ && get_sub_status() == last_sub_status_)
           || (is_doing_upgrade() && is_failed_status(last_parent_status_)));
}

inline bool ObHotUpgraderInfo::is_graceful_exit_timeout(const ObHRTime cur_time) const
{
  return (OB_UNLIKELY(!need_conn_accept_)
          && OB_LIKELY(graceful_exit_end_time_ > 0)
          && OB_LIKELY(graceful_exit_end_time_ >= graceful_exit_start_time_)
          && graceful_exit_end_time_ < cur_time
          && common::OB_SUCCESS != g_proxy_fatal_errcode);
}

inline bool ObHotUpgraderInfo::is_graceful_offline_timeout(const ObHRTime cur_time) const
{
  return (OB_LIKELY(graceful_offline_end_time_ > 0)
          && OB_LIKELY(graceful_offline_end_time_ >= graceful_offline_start_time_)
          && graceful_offline_end_time_ < cur_time);
}

ObHotUpgraderInfo &get_global_hot_upgrade_info();

class ObExecCtx
{
public:
  ObExecCtx() { reset(); };
  ~ObExecCtx() { reset(); };
  void reset()
  {
    path_ = NULL;
    name_ = NULL;
    argv_ = NULL;
    envp_ = NULL;
  }

public:
  char         *path_;
  char  const  *name_;
  char *const  *argv_;
  char         **envp_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExecCtx);
};

extern ObHotUpgraderInfo g_hot_upgrade_info;
inline ObHotUpgraderInfo &get_global_hot_upgrade_info()
{
  return g_hot_upgrade_info;
}
} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OBPROXY_HOT_UPGRADER_H
