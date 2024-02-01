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

#define USING_LOG_PREFIX SHARE
#include "ob_server_status.h"

namespace oceanbase
{
using namespace common;
namespace share
{

ObServerStatus::ObServerStatus()
{
  reset();
}

ObServerStatus::~ObServerStatus()
{
}

void ObServerStatus::reset()
{
  id_ = OB_INVALID_ID;
  zone_.reset();
  memset(build_version_, 0, sizeof(build_version_));
  server_.reset();
  sql_port_ = 0;
  register_time_ = 0;
  last_hb_time_ = 0;
  block_migrate_in_time_ = 0;
  stop_time_ = 0;
  start_service_time_ = 0;
  admin_status_ = OB_SERVER_ADMIN_MAX;
  hb_status_ = OB_HEARTBEAT_MAX;
  merged_version_ = 0;
  with_rootserver_ = false;
  with_partition_ = false;
  resource_info_.reset();
}

bool ObServerStatus::is_status_valid() const
{
  return (admin_status_ >= 0 && admin_status_ < OB_SERVER_ADMIN_MAX)
      && (hb_status_ >= 0 && hb_status_ < OB_HEARTBEAT_MAX);
}

bool ObServerStatus::is_valid() const
{
  return OB_INVALID_ID != id_ && server_.is_valid() && is_status_valid()
      && register_time_ >= 0 && last_hb_time_ >= 0 && block_migrate_in_time_ >= 0
      && stop_time_ >= 0 && start_service_time_ >= 0;
}

static const char *g_server_display_status_str[] = {"inactive", "active", "deleting"};

int ObServerStatus::display_status_str(const DisplayStatus status, const char *&str)
{
  STATIC_ASSERT(OB_DISPLAY_MAX == ARRAYSIZEOF(g_server_display_status_str),
      "status string array size mismatch");
  int ret = OB_SUCCESS;
  if (status < 0 || status >= OB_DISPLAY_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(g_server_display_status_str,
      ARRAYSIZEOF(g_server_display_status_str), status, str))) {
    LOG_WDIAG("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::str2display_status(const char *str, ObServerStatus::DisplayStatus &status)
{
  int ret = OB_SUCCESS;
  if (NULL == str) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), KP(str));
  } else {
    status = OB_DISPLAY_MAX;
    if (0 == STRCASECMP(str, "inactive")) {
      status = OB_SERVER_INACTIVE;
    } else if (0 == STRCASECMP(str, "active")) {
      status = OB_SERVER_ACTIVE;
    } else if (0 == STRCASECMP(str, "deleting")) {
      status = OB_SERVER_DELETING;
    } else if (0 == STRCASECMP(str, "takeover_by_rs")) {
      status = OB_SERVER_INACTIVE;
    } else {
      status = OB_DISPLAY_MAX;
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WDIAG("display status str not found", K(ret), K(str));
    }
  }
  return ret;
}

int ObServerStatus::server_admin_status_str(const ServerAdminStatus status, const char *&str)
{
  static const char *strs[] = { "NORMAL", "DELETING" };
  STATIC_ASSERT(OB_SERVER_ADMIN_MAX == ARRAYSIZEOF(strs), "status string array size mismatch");
  int ret = OB_SUCCESS;
  if (status < 0 || status >= OB_SERVER_ADMIN_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(strs, ARRAYSIZEOF(strs), status, str))) {
    LOG_WDIAG("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::heartbeat_status_str(const HeartBeatStatus status, const char *&str)
{
  int ret = OB_SUCCESS;
  static const char *strs[] = {"alive", "lease_expired", "permanent_offline" };
  STATIC_ASSERT(OB_HEARTBEAT_MAX == ARRAYSIZEOF(strs), "status string array size mismatch");
  if (status < 0 || status >= OB_HEARTBEAT_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), K(status));
  } else if (OB_FAIL(get_status_str(strs, ARRAYSIZEOF(strs), status, str))) {
    LOG_WDIAG("get status str failed", K(ret), K(status));
  }
  return ret;
}

int ObServerStatus::get_status_str(const char *strs[], const int64_t strs_len,
    const int64_t status, const char *&str)
{
  int ret = OB_SUCCESS;
  str = NULL;
  if (NULL == strs || strs_len <= 0 || status < 0 || status >= strs_len) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), KP(strs), K(strs_len), K(status), K(strs_len));
  } else {
    str = strs[status];
  }
  return ret;
}

ObServerStatus::DisplayStatus ObServerStatus::get_display_status() const
{
  DisplayStatus status = OB_SERVER_ACTIVE;
  if (OB_SERVER_ADMIN_NORMAL == admin_status_) {
    if (is_alive()) {
      status = OB_SERVER_ACTIVE;
    } else {
      status = OB_SERVER_INACTIVE;
    }
  } else {
    status = OB_SERVER_DELETING;
  }
  return status;
}

int64_t ObServerStatus::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    const char *admin_str = NULL;
    const char *heartbeat_str = NULL;
    // ignore get_xxx_str error, and do not check NULL str
    int tmp_ret = server_admin_status_str(admin_status_, admin_str);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WDIAG("get server admin status str failed", K(tmp_ret), K_(admin_status));
    }
    if (OB_SUCCESS != (tmp_ret = heartbeat_status_str(hb_status_, heartbeat_str))) {
      LOG_WDIAG("get heartbeat status str failed", K(tmp_ret), K_(hb_status));
    }
    J_KV("id", id_,
        "zone", zone_,
        "build_version", build_version_,
        "server", server_,
        "sql_port", sql_port_,
        "register_time", register_time_,
        "last_hb_time", last_hb_time_,
        "block_migrate_in_time", block_migrate_in_time_,
        "stop_time", stop_time_,
        "start_service_time", start_service_time_,
        "admin_status", admin_str,
        "hb_status", heartbeat_str,
        "with_rootserver", with_rootserver_,
        "with_partition", with_partition_,
        "resource_info", resource_info_);
  }
  return pos;
}

}//end namespace share
}//end namespace oceanbase
