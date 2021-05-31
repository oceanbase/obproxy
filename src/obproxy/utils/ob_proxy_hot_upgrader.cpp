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

#define USING_LOG_PREFIX PROXY

#include "utils/ob_proxy_hot_upgrader.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
volatile int g_proxy_fatal_errcode = OB_SUCCESS;
volatile int64_t g_client_active_close_count = 0;

ObHotUpgraderInfo g_hot_upgrade_info;

void ObHotUpgraderInfo::reset()
{
  fd_ = OB_INVALID_INDEX;
  received_sig_ = OB_INVALID_INDEX;
  sub_pid_ = OB_INVALID_INDEX;
  rc_status_ = RCS_NONE;
  cmd_ = HUC_NONE;
  state_ = HU_STATE_WAIT_HU_CMD;
  status_ = HU_STATUS_NONE;
  parent_status_ = HU_STATUS_NONE;
  last_parent_status_ = HU_STATUS_NONE;
  last_sub_status_ = HU_STATUS_NONE;
  is_parent_ = false;
  user_rejected_ = USER_TYPE_NONE;
  need_conn_accept_ = true;
  graceful_exit_end_time_ = 0;
  graceful_exit_start_time_ = 0;
  active_client_vc_count_ = -1;
  upgrade_version_ = -1;
  argc_ = 0;
  argv_ = NULL;
  for (int64_t i = 0; i < OB_MAX_INHERITED_ARGC; ++i) {
    inherited_argv_[i] = NULL;
  }
  memset(upgrade_version_buf_, 0, sizeof(upgrade_version_buf_));
  is_inherited_ = false;
}

void ObHotUpgraderInfo::set_main_arg(const int32_t argc, char *const *argv)
{
  argc_ = argc;
  argv_ = argv;
  inherited_argv_[0] = OB_ISNULL(argv) ? NULL : argv_[0];
  inherited_argv_[1] = NULL;
}

DEF_TO_STRING(ObHotUpgraderInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inherited), K_(upgrade_version), K_(need_conn_accept), K_(user_rejected), K_(fd),
       K_(received_sig), K_(sub_pid), K_(graceful_exit_end_time), K_(graceful_exit_start_time),
       K_(active_client_vc_count), K_(local_addr),
       "rc_status", get_rc_status_string(rc_status_),
       "hu_cmd", get_cmd_string(cmd_),
       "state", get_state_string(state_),
       "hu_status", get_status_string(status_), K_(is_parent));
  if (is_parent_) {
    J_KV(", sub_status", get_status_string(sub_status_));
  } else {
    J_KV(", parent_status", get_status_string(parent_status_));
  }

  J_KV(", last_parent_status", get_status_string(last_parent_status_),
       "last_sub_status", get_status_string(last_sub_status_),
       K_(upgrade_version_buf),
       K_(argc));

  for (int32_t i = 0; i < argc_; ++i) {
    databuff_printf(buf, buf_len, pos, ", argv[%d]=\"%s\"", i, argv_[i]);
  }

  for (int64_t i = 0; i < OB_MAX_INHERITED_ARGC; ++i) {
    databuff_printf(buf, buf_len, pos, ", inherited_argv[%ld]=\"%s\"", i, inherited_argv_[i]);
  }
  J_OBJ_END();
  return pos;
}

ObString ObHotUpgraderInfo::get_cmd_string(const ObHotUpgradeCmd cmd)
{
  static const ObString cmd_string_array[HUC_MAX] =
  {
      ObString::make_string(""),
      ObString::make_string("hot_upgrade"),
      ObString::make_string("commit"),
      ObString::make_string("rollback"),
      ObString::make_string("exit"),
      ObString::make_string("restart"),
      ObString::make_string("auto_upgrade"),
      ObString::make_string("upgrade_bin"),

      ObString::make_string("local_exit"),
      ObString::make_string("local_restart"),
      ObString::make_string("local_commit"),
      ObString::make_string("local_rollback"),
  };

  ObString string;
  if (OB_LIKELY(cmd >= HUC_NONE) && OB_LIKELY(cmd < HUC_MAX)) {
    string = cmd_string_array[cmd];
  }
  return string;
}

int ObHotUpgraderInfo::get_hu_cmd(const ObString &cmd_str, ObHotUpgradeCmd &cmd)
{
  int ret = OB_SUCCESS;
  if (0 == cmd_str.compare(get_cmd_string(HUC_NONE))) {
    cmd = HUC_NONE;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_HOT_UPGRADE))) {
    cmd = HUC_HOT_UPGRADE;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_ROLLBACK))) {
    cmd = HUC_ROLLBACK;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_COMMIT))) {
    cmd = HUC_COMMIT;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_EXIT))) {
    cmd = HUC_EXIT;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_RESTART))) {
    cmd = HUC_RESTART;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_AUTO_UPGRADE))) {
    cmd = HUC_AUTO_UPGRADE;
  } else if (0 == cmd_str.compare(get_cmd_string(HUC_UPGRADE_BIN))) {
    cmd = HUC_UPGRADE_BIN;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unrecognized cmd", K(cmd_str), K(ret));
  }
  return ret;
}

int ObHotUpgraderInfo::get_hu_sub_status(const ObString &sub_status_str, ObHotUpgradeStatus &sub_status)
{
  int ret = OB_SUCCESS;
  if (0 == sub_status_str.compare(get_status_string(HU_STATUS_NONE))) {
    sub_status = HU_STATUS_NONE;
  } else if (0 == sub_status_str.compare(get_status_string(HU_STATUS_NEW_PROXY_CREATED_SUCC))) {
    sub_status = HU_STATUS_NEW_PROXY_CREATED_SUCC;
  } else if (0 == sub_status_str.compare(get_status_string(HU_STATUS_COMMIT_SUCC))) {
    sub_status = HU_STATUS_COMMIT_SUCC;
  } else if (0 == sub_status_str.compare(get_status_string(HU_STATUS_RECV_ROLLBACK_AND_EXIT))) {
    sub_status = HU_STATUS_RECV_ROLLBACK_AND_EXIT;
  } else if (0 == sub_status_str.compare(get_status_string(HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT))) {
    sub_status = HU_STATUS_RECV_TIMEOUT_ROLLBACK_AND_EXIT;
  } else if (0 == sub_status_str.compare(get_status_string(HU_STATUS_EXITED))) {
    sub_status = HU_STATUS_EXITED;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unrecognized cmd", K(sub_status_str), K(ret));
  }
  return ret;
}

ObString ObHotUpgraderInfo::get_rc_status_string(const ObReloadConfigStatus status)
{
  static const ObString rc_status_string_array[RCS_MAX] =
  {
    ObString::make_string(""),
    ObString::make_string("reloading"),
    ObString::make_string("reload failed"),
    ObString::make_string("reload success")
  };

  ObString string;
  if (OB_LIKELY(status >= RCS_NONE) && OB_LIKELY(status < RCS_MAX)) {
    string = rc_status_string_array[status];
  }
  return string;
}

int ObHotUpgraderInfo::get_rc_status(const ObString &status_str, ObReloadConfigStatus &status)
{
  int ret = OB_SUCCESS;
  if (0 == status_str.compare(get_rc_status_string(RCS_NONE))) {
    status = RCS_NONE;
  } else if (0 == status_str.compare(get_rc_status_string(RCS_RELOADING))) {
    status = RCS_RELOADING;
  } else if (0 == status_str.compare(get_rc_status_string(RCS_RELOAD_SUCC))) {
    status = RCS_RELOAD_SUCC;
  } else if (0 == status_str.compare(get_rc_status_string(RCS_RELOAD_FAIL))) {
    status = RCS_RELOAD_FAIL;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unrecognized reload config status", K(status_str), K(ret));
  }
  return ret;
}

ObString ObHotUpgraderInfo::get_bu_status_string(const ObBatchUpgradeStatus status)
{
  static const ObString bu_status_string_array[BUS_MAX] =
  {
    ObString::make_string(""),
    ObString::make_string("upgrade_off"),
    ObString::make_string("upgrade_part"),
    ObString::make_string("upgrade_on"),
    ObString::make_string("upgrade_bin"),
  };

  ObString string;
  if (OB_LIKELY(status >= BUS_UPGRADE_NONE) && OB_LIKELY(status < BUS_MAX)) {
    string = bu_status_string_array[status];
  }
  return string;
}

int ObHotUpgraderInfo::get_bu_status(const ObString &status_string, ObBatchUpgradeStatus &status)
{
  int ret = OB_SUCCESS;
  if (status_string == get_bu_status_string(BUS_UPGRADE_NONE)) {
    status = BUS_UPGRADE_NONE;
  } else if (status_string == get_bu_status_string(BUS_UPGRADE_OFF)) {
    status = BUS_UPGRADE_OFF;
  } else if (status_string == get_bu_status_string(BUS_UPGRADE_PART)) {
    status = BUS_UPGRADE_PART;
  } else if (status_string == get_bu_status_string(BUS_UPGRADE_ON)) {
    status = BUS_UPGRADE_ON;
  } else if (status_string == get_bu_status_string(BUS_UPGRADE_BIN)) {
    status = BUS_UPGRADE_BIN;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unrecognized batch upgrade status", K(status_string), K(ret));
  }
  return ret;
}

ObString ObHotUpgraderInfo::get_state_string(const ObHotUpgradeState state)
{
  static const ObString hu_state_string_array[HU_STATE_MAX] =
  {
      ObString::make_string("HU_STATE_WAIT_HU_CMD"),
      ObString::make_string("HU_STATE_FORK_NEW_PROXY"),
      ObString::make_string("HU_STATE_WAIT_CR_CMD"),
      ObString::make_string("HU_STATE_WAIT_CR_FINISH")
  };

  ObString string;
  if (OB_LIKELY(state >= HU_STATE_WAIT_HU_CMD) && OB_LIKELY(state < HU_STATE_MAX)) {
    string = hu_state_string_array[state];
  }
  return string;
}

ObString ObHotUpgraderInfo::get_status_string(const ObHotUpgradeStatus status)
{
  static const ObString hu_status_string_array[HU_STATUS_MAX] =
  {
      //both parent and sub used
      ObString::make_string(""),
      //parent used
      ObString::make_string("start hot upgrade"),
      ObString::make_string("failure retries too many"),

      //used for a remote restart
      ObString::make_string("unavailable binary"),

      ObString::make_string("invalid argument"),
      ObString::make_string("schedule create new proxy event failed"),
      ObString::make_string("fetch binary failed"),
      ObString::make_string("check binary release failed"),
      ObString::make_string("check binary md5 failed"),
      ObString::make_string("backup binary failed"),
      ObString::make_string("create new proxy failed"),

      ObString::make_string("create new proxy succeed"),
      ObString::make_string("received commit, do graceful exit"),
      ObString::make_string("rollback succeed"),
      ObString::make_string("timeout rollback succeed"),

      //sub used
      ObString::make_string("new proxy created succeed"),
      ObString::make_string("commit succeed"),
      ObString::make_string("received rollback, do graceful exit"),
      ObString::make_string("received timeout rollback, do graceful exit"),

      //both parent and sub used
      ObString::make_string("exited"),

      //used for a remote kill
      ObString::make_string("received exit cmd"),
      //used for a remote restart

      ObString::make_string("start doing restart"),

      //used for upgrade binary
      ObString::make_string("start upgrading binary"),
      ObString::make_string("upgrade binary succeed")
  };

  ObString string;
  if (OB_LIKELY(status >= HU_STATUS_NONE) && OB_LIKELY(status < HU_STATUS_MAX)) {
    string = hu_status_string_array[status];
  }
  return string;
}

int ObHotUpgraderInfo::fill_inherited_info(const bool is_server_service_mode, const int64_t upgrade_version)
{
  int ret = OB_SUCCESS;
  if (is_server_service_mode) {
    //if use server service mode, proxy_id and upgrade_version_ must be specified
    if ((OB_LIKELY(is_inherited_) && OB_UNLIKELY(upgrade_version < 0))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("upgrade_version should be specified in server service mode", K(upgrade_version), K(ret));
    } else if (NULL == inherited_argv_[1]) {
      upgrade_version_ = ((upgrade_version > 0) ? upgrade_version : 0);
      int64_t length = snprintf(upgrade_version_buf_, sizeof(upgrade_version_buf_), "-u%ld", upgrade_version_ + 1);
      if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= static_cast<int64_t>(sizeof(upgrade_version_buf_)))) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("buf not enought", K(length), K(upgrade_version_buf_), K(ret));
      } else {
        inherited_argv_[1] = upgrade_version_buf_;
        inherited_argv_[2] = NULL;
      }
    } else {/*do nothing*/}
  } else {
    upgrade_version_ = ((upgrade_version > 0) ? upgrade_version : 0);
  }
  return ret;
}

} // end of namespace obproxy
} // end of namespace oceanbase
