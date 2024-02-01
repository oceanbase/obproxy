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

#include "obutils/ob_proxy_table_processor.h"
#include "utils/ob_proxy_utils.h"
#include "utils/ob_layout.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "obutils/ob_proxy_table_processor_utils.h"
#include "obutils/ob_hot_upgrade_processor.h"
#include "obutils/ob_vip_tenant_processor.h"
#include "obutils/ob_resource_pool_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObProxyTableProcessor &get_global_proxy_table_processor()
{
  static ObProxyTableProcessor table_processor;
  return table_processor;
}

int ObProxyServerInfo::parse_hu_cmd(const ObString &cmd_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObHotUpgraderInfo::get_hu_cmd(cmd_str, hu_cmd_))) {
    LOG_WDIAG("fail to get hot upgrade cmd", K(cmd_str), K(hu_cmd_), K(ret));
  } else { }
  return ret;
}

const char *ObProxyServerInfo::get_new_binary_md5_str(const ObProxyKernelRelease version)
{
  const char *str = NULL;
  static const char *virsion_str_array[RELEASE_MAX] = {
    "new_binary_5u_md5",
    "new_binary_6u_md5",
    "new_binary_7u_md5",
    "new_binary_unknown_md5",
  };
  if (OB_LIKELY(version >= RELEASE_5U) && OB_LIKELY(version < RELEASE_MAX)) {
    str = virsion_str_array[version];
  }
  return str;
}

inline int ObProxyServerInfo::add_str_info(char *buf, const char *src, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src) || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(src), K(buf), K(len), K(ret));
  } else {
    int64_t w_len = snprintf(buf, len, "%s", src);
    if (OB_LIKELY(w_len < 0) || OB_UNLIKELY(w_len >= len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fill info error", K(ret));
    }
  }
  return ret;
}

int ObProxyServerInfo::add_app_name(const char *app_name)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_str_info(app_name_, app_name, OB_MAX_APP_NAME_LENGTH + 1))) {
    PROXY_LOG(WDIAG, "fail to fill appname", K(ret));
  }
  return ret;
}

int ObProxyServerInfo::add_binary_version(const char *binary_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_str_info(binary_version_, binary_version, OB_MAX_PROXY_BINARY_VERSION_LEN + 1))) {
    PROXY_LOG(WDIAG, "fail to fill binary_version", K(ret));
  }
  return ret;
}

int ObProxyServerInfo::add_new_binary_version(const char *version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_str_info(new_binary_version_, version, OB_MAX_PROXY_BINARY_VERSION_LEN + 1))) {
    PROXY_LOG(WDIAG, "fail to fill new_binary_version", K(ret));
  }
  return ret;
}

int ObProxyServerInfo::add_new_binary_md5(const char *md5)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_str_info(new_binary_md5_, md5, OB_MAX_PROXY_MD5_LEN))) {
    PROXY_LOG(WDIAG, "fail to fill new_binary_md5", K(ret));
  }
  return ret;
}

int ObProxyServerInfo::add_info(const char *info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_str_info(info_, info, OB_MAX_PROXY_INFO_LEN + 1))) {
    PROXY_LOG(WDIAG, "fail to fill proxy info", K(ret));
  }
  return ret;
}

DEF_TO_STRING(ObProxyServerInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(proxy_ip), K_(proxy_port), K_(regist_time_str), K_(proxy_id), K_(app_name),
      K_(binary_version), K_(new_binary_version), K_(new_binary_md5), K_(config_version),
      K_(current_pid),
      "rc_status", ObHotUpgraderInfo::get_rc_status_string(rc_status_),
      "hu_cmd", ObHotUpgraderInfo::get_cmd_string(hu_cmd_),
      K_(parent_hu_status), K_(sub_hu_status),
      K_(row_status), K_(uname_info), K_(info));
  J_OBJ_END();
  return pos;
}

int ObProxyKVTableInfo::parse_upgrade_start_time(const common::ObString &time_string, int64_t &minutes)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TIME_STRING_SIZE = 6;
  minutes = -1;
  if (OB_UNLIKELY(time_string.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("time_string is empty", K(time_string), K(ret));
  } else if (OB_UNLIKELY(time_string.length() >= MAX_TIME_STRING_SIZE)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WDIAG("invalid time_string", K(time_string), K(ret));
  } else {
    char time_str[MAX_TIME_STRING_SIZE];
    MEMCPY(time_str, time_string.ptr(), time_string.length());
    time_str[time_string.length()] = '\0';
    struct tm tm_value;
    if (OB_ISNULL(strptime(time_str, "%H:%M", &tm_value))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid time_str", K(time_str), K(time_string), K(ret));
    } else {
      minutes = tm_value.tm_min + tm_value.tm_hour * 60;
      LOG_DEBUG("succ to parse upgrade start time", K(minutes), K(time_string), K(ret));
    }
  }
  return ret;
}

DEF_TO_STRING(ObProxyKVTableInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(config_version), K_(vip_tenant_version),
       "app_status", ObHotUpgraderInfo::get_bu_status_string(app_status_),
       K_(app_start_minutes), K_(app_duration_minutes), K_(app_binary_name), K_(app_binary_md5),
       "all_status", ObHotUpgraderInfo::get_bu_status_string(all_status_),
       K_(all_start_minutes), K_(all_duration_minutes), K_(all_binary_name), K_(all_binary_md5),
       "is_found", ObArrayWrap<bool>(is_found_, INDEX_MAX));
  J_OBJ_END();
  return pos;
}

int ObProxyKVTableInfo::fill_rows(const ObProxyKVRowsName &rows, const common::ObString &kv_name,
    const common::ObString &kv_value)
{
  int ret = OB_SUCCESS;
  if (NULL != kv_value.ptr() && OB_UNLIKELY(kv_value.length() > OB_MAX_CONFIG_VALUE_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid kv_value", K(kv_value), K(ret));
  } else {
    //case sensitive
    if (!is_found_[INDEX_CONFIG_VERSION] && kv_name == rows.name_[INDEX_CONFIG_VERSION]) {
      is_found_[INDEX_CONFIG_VERSION] = true;
      if (!kv_value.empty() && OB_FAIL(get_int_value(kv_value, config_version_))) {
        LOG_WDIAG("fail to get int from string", K(kv_value), K_(config_version), K(ret));
      }
    } else if (!is_found_[INDEX_VIP_TENANT_VERSION] && kv_name == rows.name_[INDEX_VIP_TENANT_VERSION]) {
      is_found_[INDEX_VIP_TENANT_VERSION] = true;
      if (!kv_value.empty() && OB_FAIL(get_int_value(kv_value, vip_tenant_version_))) {
        LOG_WDIAG("fail to get int from string", K(kv_value), K_(vip_tenant_version), K(ret));
      }
    } else if (!is_found_[INDEX_APP_UPGRADE_SWITCH] && kv_name == rows.name_[INDEX_APP_UPGRADE_SWITCH]) {
      is_found_[INDEX_APP_UPGRADE_SWITCH] = true;
      if (OB_FAIL(ObHotUpgraderInfo::get_bu_status(kv_value, app_status_))) {
        LOG_WDIAG("fail to parse batch upgrade status", K(kv_value), K_(app_status), K(ret));
      }
    } else if (!is_found_[INDEX_APP_UPGRADE_START_TIME] && kv_name == rows.name_[INDEX_APP_UPGRADE_START_TIME]) {
      is_found_[INDEX_APP_UPGRADE_START_TIME] = true;
      if (!kv_value.empty() && OB_FAIL(parse_upgrade_start_time(kv_value, app_start_minutes_))) {
        LOG_WDIAG("fail to parse upgrade start time", K(kv_value), K_(app_start_minutes), K(ret));
      }
    } else if (!is_found_[INDEX_APP_UPGRADE_DURATION_MINUTES] && kv_name == rows.name_[INDEX_APP_UPGRADE_DURATION_MINUTES]) {
      is_found_[INDEX_APP_UPGRADE_DURATION_MINUTES] = true;
      if (!kv_value.empty() && OB_FAIL(get_int_value(kv_value, app_duration_minutes_))) {
        LOG_WDIAG("fail to get int from string", K(kv_value), K_(app_duration_minutes), K(ret));
      }
    } else if (!is_found_[INDEX_APP_UPGRADE_NEW_BINARY_NAME] && kv_name == rows.name_[INDEX_APP_UPGRADE_NEW_BINARY_NAME]) {
      is_found_[INDEX_APP_UPGRADE_NEW_BINARY_NAME] = true;
      if (OB_UNLIKELY(kv_value.length() >= static_cast<int32_t>(sizeof(app_binary_name_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid kv_value", K(kv_value.length()),  K(kv_name), K(ret));
      } else {
        MEMCPY(app_binary_name_, kv_value.ptr(), kv_value.length());
        app_binary_name_[kv_value.length()] = '\0';
      }
    } else if (!is_found_[INDEX_APP_UPGRADE_NEW_BINARY_MD5] && kv_name == rows.name_[INDEX_APP_UPGRADE_NEW_BINARY_MD5]) {
      is_found_[INDEX_APP_UPGRADE_NEW_BINARY_MD5] = true;
      if (OB_UNLIKELY(kv_value.length() >= static_cast<int32_t>(sizeof(app_binary_md5_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid kv_value", K(kv_value.length()), K(kv_name), K(ret));
      } else {
        MEMCPY(app_binary_md5_, kv_value.ptr(), kv_value.length());
        app_binary_md5_[kv_value.length()] = '\0';
      }
    } else if (!is_found_[INDEX_ALL_UPGRADE_SWITCH] && kv_name == rows.name_[INDEX_ALL_UPGRADE_SWITCH]) {
      is_found_[INDEX_ALL_UPGRADE_SWITCH] = true;
      if (OB_FAIL(ObHotUpgraderInfo::get_bu_status(kv_value, all_status_))) {
        LOG_WDIAG("fail to parse batch upgrade status", K(kv_value), K_(all_status), K(ret));
      }
    } else if (!is_found_[INDEX_ALL_UPGRADE_START_TIME] && kv_name == rows.name_[INDEX_ALL_UPGRADE_START_TIME]) {
      is_found_[INDEX_ALL_UPGRADE_START_TIME] = true;
      if (!kv_value.empty() && OB_FAIL(parse_upgrade_start_time(kv_value, all_start_minutes_))) {
        LOG_WDIAG("fail to parse upgrade start time", K(kv_value), K_(all_start_minutes), K(ret));
      }
    } else if (!is_found_[INDEX_ALL_UPGRADE_DURATION_MINUTES] && kv_name == rows.name_[INDEX_ALL_UPGRADE_DURATION_MINUTES]) {
      is_found_[INDEX_ALL_UPGRADE_DURATION_MINUTES] = true;
      if (!kv_value.empty() && OB_FAIL(get_int_value(kv_value, all_duration_minutes_))) {
        LOG_WDIAG("fail to get int from string", K(kv_value), K_(all_duration_minutes), K(ret));
      }
    } else if (!is_found_[INDEX_ALL_UPGRADE_NEW_BINARY_NAME] && kv_name == rows.name_[INDEX_ALL_UPGRADE_NEW_BINARY_NAME]) {
      is_found_[INDEX_ALL_UPGRADE_NEW_BINARY_NAME] = true;
      if (OB_UNLIKELY(kv_value.length() >= static_cast<int32_t>(sizeof(all_binary_name_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid kv_value", K(kv_value.length()),  K(kv_name), K(ret));
      } else {
        MEMCPY(all_binary_name_, kv_value.ptr(), kv_value.length());
        all_binary_name_[kv_value.length()] = '\0';
      }
    } else if (!is_found_[INDEX_ALL_UPGRADE_NEW_BINARY_MD5] && kv_name == rows.name_[INDEX_ALL_UPGRADE_NEW_BINARY_MD5]) {
      is_found_[INDEX_ALL_UPGRADE_NEW_BINARY_MD5] = true;
      if (OB_UNLIKELY(kv_value.length() >= static_cast<int32_t>(sizeof(all_binary_md5_)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid kv_value", K(kv_value.length()), K(kv_name), K(ret));
      } else {
        MEMCPY(all_binary_md5_, kv_value.ptr(), kv_value.length());
        all_binary_md5_[kv_value.length()] = '\0';
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unknown kv_name", K(kv_name), K(kv_value), K(ret));
    }
  }
  return ret;
}

//-----------------ObProxyTableProcessor----------------------------

ObProxyTableProcessor::ObProxyTableProcessor()
    : is_inited_(false), is_registered_(false), is_last_report_succeed_(true),
      table_check_cont_(NULL), mysql_proxy_(NULL), proxy_version_(NULL),
      hot_upgrade_processor_(get_global_hot_upgrade_processor()),
      config_manager_(), vt_processor_(get_global_vip_tenant_processor()), kv_rows_name_()
{
}

void ObProxyTableProcessor::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    mysql_proxy_ = NULL;
    proxy_version_ = NULL;
    is_inited_ = false;
    is_registered_ = false;
    is_last_report_succeed_ = true;
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObAsyncCommonTask::destroy_repeat_task(table_check_cont_))) {
      LOG_WDIAG("fail to destroy table_check_cont task", K(ret));
    }
  }
}

int ObProxyTableProcessor::init(ObMysqlProxy &mysql_proxy,
                                ObAppVersionInfo &proxy_version,
                                ObProxyReloadConfig &reload_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("already inited", K(ret));
  } else if (OB_FAIL(config_manager_.init(mysql_proxy, reload_config))) {
    LOG_WDIAG("fail to init config manger", K(&mysql_proxy), K(ret));
  } else if (OB_FAIL(hot_upgrade_processor_.init(mysql_proxy))) {
    LOG_WDIAG("fail to init hot upgrade processor", K(&mysql_proxy), K(ret));
  } else {
    mysql_proxy_ = &mysql_proxy;
    proxy_version_ = &proxy_version;
    is_inited_ = true;
  }

  return ret;
}

DEF_TO_STRING(ObProxyTableProcessor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(is_registered), KPC_(proxy_version), KP_(mysql_proxy),
       K_(vt_processor), K_(hot_upgrade_processor), K_(config_manager), KPC_(table_check_cont));
  J_OBJ_END();
  return pos;
}

int ObProxyTableProcessor::init_kv_rows_name()
{
  int ret = OB_SUCCESS;
  const ObProxyKernelRelease kernel_release = get_global_config_server_processor().get_kernel_release();
  if (OB_UNLIKELY(RELEASE_MAX == kernel_release)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("invalid kernel release", K(kernel_release), K(ret));
  } else {
    kv_rows_name_.name_[INDEX_VIP_TENANT_VERSION].assign_ptr(ObProxyTableInfo::PROXY_VIP_TENANT_VERSION_NAME,
        static_cast<int32_t>(STRLEN(ObProxyTableInfo::PROXY_VIP_TENANT_VERSION_NAME)));

    const ObString *all_proxy = &ObProxyTableInfo::PROXY_ALL_PROXY_HEADER;
    const ObString const_app_name(get_global_proxy_config().app_name_str_);
    const ObString *string = &ObProxyTableInfo::PROXY_UPGRADE_SWITCH_NAME;
    MEMCPY(kv_rows_name_.all_upgrade_switch_, all_proxy->ptr(), all_proxy->length());
    MEMCPY(kv_rows_name_.all_upgrade_switch_ + all_proxy->length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_ALL_UPGRADE_SWITCH].assign_ptr(
        kv_rows_name_.all_upgrade_switch_, all_proxy->length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_START_TIME_NAME;
    MEMCPY(kv_rows_name_.all_upgrade_start_time_, all_proxy->ptr(), all_proxy->length());
    MEMCPY(kv_rows_name_.all_upgrade_start_time_ + all_proxy->length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_ALL_UPGRADE_START_TIME].assign_ptr(
        kv_rows_name_.all_upgrade_start_time_, all_proxy->length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_DURATION_MINUTES_NAME;
    MEMCPY(kv_rows_name_.all_upgrade_duration_minutes_, all_proxy->ptr(), all_proxy->length());
    MEMCPY(kv_rows_name_.all_upgrade_duration_minutes_ + all_proxy->length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_ALL_UPGRADE_DURATION_MINUTES].assign_ptr(
        kv_rows_name_.all_upgrade_duration_minutes_, all_proxy->length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_NAME[kernel_release];
    MEMCPY(kv_rows_name_.all_upgrade_new_binary_name_, all_proxy->ptr(), all_proxy->length());
    MEMCPY(kv_rows_name_.all_upgrade_new_binary_name_ + all_proxy->length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_ALL_UPGRADE_NEW_BINARY_NAME].assign_ptr(
        kv_rows_name_.all_upgrade_new_binary_name_, all_proxy->length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_MD5[kernel_release];
    MEMCPY(kv_rows_name_.all_upgrade_new_binary_md5_, all_proxy->ptr(), all_proxy->length());
    MEMCPY(kv_rows_name_.all_upgrade_new_binary_md5_ + all_proxy->length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_ALL_UPGRADE_NEW_BINARY_MD5].assign_ptr(
        kv_rows_name_.all_upgrade_new_binary_md5_, all_proxy->length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_SWITCH_NAME;
    MEMCPY(kv_rows_name_.app_upgrade_switch_, const_app_name.ptr(), const_app_name.length());
    MEMCPY(kv_rows_name_.app_upgrade_switch_ + const_app_name.length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_APP_UPGRADE_SWITCH].assign_ptr(
        kv_rows_name_.app_upgrade_switch_, const_app_name.length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_START_TIME_NAME;
    MEMCPY(kv_rows_name_.app_upgrade_start_time_, const_app_name.ptr(), const_app_name.length());
    MEMCPY(kv_rows_name_.app_upgrade_start_time_ + const_app_name.length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_APP_UPGRADE_START_TIME].assign_ptr(
        kv_rows_name_.app_upgrade_start_time_, const_app_name.length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_DURATION_MINUTES_NAME;
    MEMCPY(kv_rows_name_.app_upgrade_duration_minutes_, const_app_name.ptr(), const_app_name.length());
    MEMCPY(kv_rows_name_.app_upgrade_duration_minutes_ + const_app_name.length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_APP_UPGRADE_DURATION_MINUTES].assign_ptr(
        kv_rows_name_.app_upgrade_duration_minutes_, const_app_name.length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_NAME[kernel_release];
    MEMCPY(kv_rows_name_.app_upgrade_new_binary_name_, const_app_name.ptr(), const_app_name.length());
    MEMCPY(kv_rows_name_.app_upgrade_new_binary_name_ + const_app_name.length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_APP_UPGRADE_NEW_BINARY_NAME].assign_ptr(
        kv_rows_name_.app_upgrade_new_binary_name_, const_app_name.length() + string->length());

    string = &ObProxyTableInfo::PROXY_UPGRADE_NEW_BINARY_MD5[kernel_release];
    MEMCPY(kv_rows_name_.app_upgrade_new_binary_md5_, const_app_name.ptr(), const_app_name.length());
    MEMCPY(kv_rows_name_.app_upgrade_new_binary_md5_ + const_app_name.length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_APP_UPGRADE_NEW_BINARY_MD5].assign_ptr(
        kv_rows_name_.app_upgrade_new_binary_md5_, const_app_name.length() + string->length());

    string = &ObProxyTableInfo::PROXY_CONFIG_VERSION_NAME;
    MEMCPY(kv_rows_name_.app_config_version_, const_app_name.ptr(), const_app_name.length());
    MEMCPY(kv_rows_name_.app_config_version_ + const_app_name.length(), string->ptr(), string->length());
    kv_rows_name_.name_[INDEX_CONFIG_VERSION].assign_ptr(
        kv_rows_name_.app_config_version_, const_app_name.length() + string->length());
  }
  return ret;
}

int ObProxyTableProcessor::register_proxy(const RegisterType type)
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  ObProxyServerInfo proxy_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not inited", K_(is_inited), K(ret));
  } else {
    if (OB_FAIL(build_proxy_info(proxy_info))) {
      LOG_WDIAG("fail to build proxy info", K(ret));
    } else {
      switch (type) {
        case RT_INIT: {
          if (OB_LIKELY(HU_STATUS_CREATE_NEW_PROXY_SUCC == info.get_parent_status())) {
            // the forked sub will update both parent and itself hot_upgrade_status in proxy initiation
            // in fact, it will never arrive here
            if (OB_FAIL(ObProxyTableProcessorUtils::update_proxy_info(*mysql_proxy_, proxy_info))) {
              LOG_WDIAG("fail to update proxy info", K(proxy_info), K(info), K(ret));
            } else {
              is_registered_ = true;
            }
          } else {
            LOG_INFO("we will register proxy now", K(proxy_info), K(info), K(ret));
            if (OB_FAIL(ObProxyTableProcessorUtils::add_or_modify_proxy_info(*mysql_proxy_, proxy_info))) {
              LOG_WDIAG("fail to add or modify proxy info", K(proxy_info), K(info), K(ret));
            } else {
              is_registered_ = true;
            }
          }
          break;
        }
        case RT_FORCE: {
          LOG_WDIAG("we will add_or_modify_proxy_info ", K(proxy_info), K(info));
          if (OB_FAIL(ObProxyTableProcessorUtils::add_or_modify_proxy_info(*mysql_proxy_, proxy_info))) {
            LOG_WDIAG("fail to add or modify proxy info", K(proxy_info), K(info), K(ret));
          } else {
            is_registered_ = true;
          }
          break;
        }
        case RT_CHECK: {
          // if timeout rollback, we need set update hu_cmd = rollback when register
          // parent's info incidentally, in case of the last cmd had not clear
          bool set_rollback = (HU_STATUS_TIMEOUT_ROLLBACK_SUCC == info.get_parent_status());
          if (OB_FAIL(ObProxyTableProcessorUtils::update_proxy_info(*mysql_proxy_, proxy_info,
                                                                    set_rollback))) {
            LOG_WDIAG("fail to update proxy info, unknown error code, we will retry update",
                     K(proxy_info), K(info), K(set_rollback), K(ret));
          } else {
            is_registered_ = true;
          }
          break;
        }
        case RT_NONE:
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("it should not enter here", K(type), K(info), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_last_report_succeed_ = true;
    LOG_DEBUG("succ to register proxy info", K(type), K(proxy_info), K(info));
  } else {
    is_last_report_succeed_ = false;
  }
  return ret;
}

int ObProxyTableProcessor::build_proxy_info(ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  const char *proxy_table_info = "(hot_upgrade_cmd: hot_upgrade, rollback, commit, auto_upgrade, upgrade_bin, exit or restart), "
                                 "(new_binary_version: new binary name for hot upgrade), "
                                 "(new_binary_[5u|6u|7u]_md5: md5sum of new binary)";
  if (OB_FAIL(ObProxyTableProcessorUtils::get_uname_info(proxy_info.uname_info_,
      static_cast<int64_t>(sizeof(proxy_info.uname_info_))))) {
    LOG_WDIAG("fail to get uname info", K(proxy_info.uname_info_), K(ret));

  } else if (OB_FAIL(proxy_info.add_binary_version(proxy_version_->full_version_info_str_))) {
    LOG_WDIAG("fail to add_binary_version", "binary_version", proxy_version_->full_version_info_str_, K(ret));

  } else if (OB_FAIL(proxy_info.add_info(proxy_table_info))) {
    LOG_WDIAG("fail to add_info", K(proxy_table_info), K(ret));
  } else if (OB_FAIL(proxy_info.add_app_name(config_manager_.get_proxy_config().app_name_str_))) {
    LOG_WDIAG("fail to add_app_name", "app_name", config_manager_.get_proxy_config().app_name_str_, K(ret));
  } else {
    MEMCPY(proxy_info.proxy_ip_, hot_upgrade_processor_.get_proxy_ip(), sizeof(proxy_info.proxy_ip_));
    proxy_info.proxy_port_ = hot_upgrade_processor_.get_proxy_port();
    proxy_info.proxy_id_ = config_manager_.get_proxy_config().proxy_id;

    const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
    proxy_info.config_version_ = config_manager_.get_config_version();
    proxy_info.current_pid_ = getpid();
    proxy_info.hu_cmd_ = info.cmd_;
    proxy_info.rc_status_ = info.rc_status_;
    proxy_info.parent_hu_status_ = info.get_parent_status();
    proxy_info.sub_hu_status_ = info.get_sub_status();
  }
  return ret;
}

int ObProxyTableProcessor::do_repeat_task()
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(!info.need_conn_accept_)) {
    // do nothing
  } else {
    ret = get_global_proxy_table_processor().do_check_work();
  }

  return ret;
}

void ObProxyTableProcessor::update_interval()
{
  ObAsyncCommonTask *cont = get_global_proxy_table_processor().get_table_check_cont();
  if (OB_LIKELY(NULL != cont)) {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().proxy_info_check_interval);
    cont->set_interval(interval_us);
  }
}

int ObProxyTableProcessor::start_check_table_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("obproxy table processor is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL != table_check_cont_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("table check cont has been scheduled", K(ret));
  } else {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(
                          get_global_proxy_config().proxy_info_check_interval);
    if (OB_ISNULL(table_check_cont_ = ObAsyncCommonTask::create_and_start_repeat_task(interval_us,
                                      "proxy_info_check_task",
                                      ObProxyTableProcessor::do_repeat_task,
                                      ObProxyTableProcessor::update_interval))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to create and start proxy table check task", K(ret));
    } else {
      LOG_INFO("succ to start proxy table check task", K(interval_us));
    }
  }
  return ret;
}

int ObProxyTableProcessor::set_check_interval()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("table processor is not inited", K(ret));
  } else if (OB_FAIL(ObAsyncCommonTask::update_task_interval(table_check_cont_))) {
    LOG_WDIAG("fail to set table check interval", K(ret));
  }
  return ret;
}

int ObProxyTableProcessor::get_proxy_info(ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  const char *proxy_ip = hot_upgrade_processor_.get_proxy_ip();
  const int32_t proxy_port = hot_upgrade_processor_.get_proxy_port();
  proxy_info.reset();
  if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_info(*mysql_proxy_, proxy_ip, proxy_port, proxy_info))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST == ret)) {
    // this means no record in observer's PROXY_INFO_TABLE_NAME, abnormal state
      proxy_info.row_status_ = PRS_NOT_EXSIT;
      LOG_WDIAG("no record found, abnormal state, we will register obproxy info "
               "after check timeout rollback", K(proxy_ip), K(proxy_port), K(ret));
    } else {
      proxy_info.row_status_ = PRS_UNAVAILABLE;
      LOG_WDIAG("fail to get proxy info, maybe return mysql error code", K(proxy_ip), K(proxy_port), K(proxy_info), K(ret));
    }
  } else {
    proxy_info.row_status_ = PRS_AVAILABLE;
    LOG_DEBUG("succ to get proxy info", K(proxy_info));
  }
  return ret;
}

int ObProxyTableProcessor::check_upgrade_state(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  switch (info.get_state()) {
    case HU_STATE_WAIT_HU_CMD: {
      //in wait hu cmd state, proxy wait "hot_upgrade", "auto_upgrade", "exit" or "restart" cmd
      //any of the follower happened, we should do state_wait_hu_cmd()
      //1. proxy_info is AVAILABLE
      //2. this is local cmd from config cmd
      if (OB_LIKELY(PRS_AVAILABLE == proxy_info.row_status_)) {
        if (OB_FAIL(hot_upgrade_processor_.state_wait_hu_cmd(proxy_info))) {
          LOG_WDIAG("fail to handle wait_hu_cmd state", K(proxy_info), K(info), K(ret));
        }
      } else {
        LOG_WDIAG("proxy_info is unavailable, can not handle state_wait_hu_cmd", K(proxy_info));
      }
      break;
    }
    case HU_STATE_FORK_NEW_PROXY: {
      //in fork new proxy state, proxy wait for the result of ObHotUpgraderNotifier
      //the result is stored in info_.status_.
      //here we care about info_.status_ instead of proxy_info
      if (OB_FAIL(hot_upgrade_processor_.state_fork_new_proxy())) {
        LOG_WDIAG("fail to handle fork_new_proxy state", K(info), K(ret));
      }
      break;
    }
    case HU_STATE_WAIT_CR_CMD: {
      //in wait cr cmd state, proxy wait "commit", "rollback", "auto_upgrade", "restart" or timeout_rollback happened
      //any of the followers happened, we should do state_wait_hu_cmd()
      //1. proxy_info is AVAILABLE
      //2. it is time to timeout rollback
      //3. this is auto_upgrade
      //4. this is restart from config cmd
      if (OB_LIKELY(PRS_AVAILABLE == proxy_info.row_status_)
          || hot_upgrade_processor_.is_timeout_rollback()
          || info.is_auto_upgrade()) {
        if (OB_FAIL(hot_upgrade_processor_.state_wait_cr_cmd(proxy_info))) {
          LOG_WDIAG("fail to handle wait_cr_cmd state", K(info), K(proxy_info), K(ret));
        }
      } else {
        LOG_WDIAG("proxy_info is unavailable, and it is not timeout_rollback");
      }
      break;
    }
    case HU_STATE_WAIT_CR_FINISH: {
      //in wait cr finish state, proxy wait for parent or sub proxy's exit
      //the result is stored in info_.parent_status_ or info_.sub_status_.
      //here we care about info_.?_status_ instead of proxy_info
      if (OB_FAIL(hot_upgrade_processor_.state_wait_cr_finish())) {
        LOG_WDIAG("fail to handle wait_cr_finish state", K(info), K(ret));
      }
      break;
    }
    case HU_STATE_WAIT_LOCAL_CR_FINISH:
      break;
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("it should not enter here", K(info));
    }
  }
  return ret;
}

int ObProxyTableProcessor::check_update_proxy_table(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  if (OB_UNLIKELY(!info.need_conn_accept_)) {
    //if already disable accept connect, no need do check_update_proxy_info
    LOG_DEBUG("it already disable accept connect, no need to update proxy table", K(info));
  } else {
    switch (proxy_info.row_status_) {
      case PRS_AVAILABLE: {
        if (OB_UNLIKELY(proxy_info.current_pid_ != getpid())) {
          if (OB_UNLIKELY(info.need_report_info())) {
            LOG_WDIAG("current proxy info has not register into table, we will register it", K(proxy_info));
            if (OB_FAIL(register_proxy(RT_CHECK))) {
              LOG_WDIAG("fail to register proxy, current proxy info in table is old", K(ret));
            }
          }
        } else {
          if (info.need_report_status()) {
            if (OB_FAIL(update_hu_status_and_cmd(proxy_info))) {
              LOG_WDIAG("fail to update update hot upgrade status and cmd", K(proxy_info), K(ret));
            }
          }
        }
        break;
      }
      case PRS_UNAVAILABLE: {
        if (info.need_report_status()) {
          if (OB_FAIL(update_hu_status_and_cmd(proxy_info))) {
            LOG_WDIAG("fail to update update hot upgrade status and cmd", K(info), K(ret));
          }
        }
        break;
      }
      case PRS_NOT_EXSIT: {
        if (OB_UNLIKELY(info.need_report_info())) {
          LOG_WDIAG("current proxy info is not exist, we need register it", K(info));
          if (OB_FAIL(register_proxy(RT_FORCE))) {
            LOG_WDIAG("fail to register proxy, current proxy info in table is old", K(info), K(ret));
          }
        } else {
          //do nothing
        }
        break;
      }
      case PRS_NONE: {
        if (OB_FAIL(register_proxy(RT_FORCE))) {
          LOG_WDIAG("fail to register proxy, current proxy info in table maybe old", K(info), K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("it should not happened", K(proxy_info), K(info));
      }
    }
  }
  return ret;
}

void ObProxyKVTableInfo::update_upgrade_switch(ObBatchUpgradeStatus &status,
    int64_t &start_minutes, int64_t &duration_minutes)
{
  if (BUS_UPGRADE_ON == status || BUS_UPGRADE_BIN == status) {
    if (start_minutes < 0 || duration_minutes < 0) {
      LOG_DEBUG("unexpected app_minutes, back to UPGRADE_OFF",
                K(start_minutes), K(duration_minutes));
      status = BUS_UPGRADE_OFF;
    } else {
      struct tm human_time;
      time_t cur_time;
      time(&cur_time);
      int64_t curr_minutes = -1;
      if (OB_UNLIKELY(NULL == localtime_r(&cur_time, &human_time))) {
        LOG_WDIAG("get localtime failed, back to UPGRADE_OFF", K(*this));
        status = BUS_UPGRADE_OFF;
      } else {
        curr_minutes = human_time.tm_hour * 60 +  human_time.tm_min;
        if (curr_minutes < start_minutes
            || curr_minutes > (start_minutes + duration_minutes)) {
          LOG_DEBUG("it is not time to do batch upgrade, back to UPGRADE_OFF",
                    "curr_hour", human_time.tm_hour, "curr_min", human_time.tm_min,
                    K(curr_minutes), K(start_minutes), K(duration_minutes));
          status = BUS_UPGRADE_OFF;
        } else {
          LOG_INFO("it is time to do batch upgrade",
                   "curr_hour", human_time.tm_hour, "curr_min", human_time.tm_min,
                   K(curr_minutes), K(start_minutes), K(duration_minutes), K(*this));
        }
      }
    }
  }
}

int ObProxyTableProcessor::check_proxy_kv_info(ObProxyKVTableInfo &kv_info)
{
  int ret = OB_SUCCESS;
  //1. get some rows info from ob_all_proxy_kv_table
  if (OB_FAIL(ObProxyTableProcessorUtils::get_proxy_kv_table_info(*mysql_proxy_, kv_rows_name_, kv_info))) {
    LOG_WDIAG("fail to get proxy kv table info, maybe return mysql error code", K(kv_info), K(ret));
  }

  //2. check update vip tenant cache
  if (OB_FAIL(check_update_vip_tenant_cache(kv_info.vip_tenant_version_))) {
    LOG_WDIAG("fail to check update vip tenant cache", K(ret));
  }

  //3. check reload config
  if (OB_FAIL(check_reload_config(kv_info.config_version_))) {
    LOG_WDIAG("fail to check reload config", K(ret));
  }

  //4. recheck upgrade_switch
  kv_info.recheck_upgrade_switch();

  return ret;
}

int ObProxyTableProcessor::check_proxy_info(const ObProxyKVTableInfo &kv_info)
{
  int ret = OB_SUCCESS;
  ObProxyServerInfo proxy_info;
  //1. check whether has been timeout rollback and do it
  if (OB_FAIL(hot_upgrade_processor_.check_timeout_rollback())) {
    LOG_WDIAG("fail to check timeout rollback", K(ret));
  }

  //2. get proxy info from ob_all_proxy
  if (OB_FAIL(get_proxy_info(proxy_info))) {
    LOG_WDIAG("fail to get proxy info",K(proxy_info), K(ret));
  }

  //3. update hot upgrade cmd and reload config staus
  if (PRS_AVAILABLE == proxy_info.row_status_) {
    get_global_hot_upgrade_info().cmd_ = proxy_info.hu_cmd_;
  }

  //4. check do batch upgrade
  if (OB_FAIL(check_do_batch_upgrade(kv_info, proxy_info))) {
    LOG_WDIAG("fail to check update vip tenant cache", K(kv_info), K(proxy_info), K(ret));
  }

  //5. check and handle hot upgrade state
  if (OB_FAIL(check_upgrade_state(proxy_info))) {
    LOG_WDIAG("fail to check upgrade state", K(proxy_info), K(ret));
  } else {/*do nothing*/}

  //6. check and update proxy table
  if (OB_FAIL(check_update_proxy_table(proxy_info))) {
    LOG_WDIAG("fail to check update proxy table", K(proxy_info), K(ret));
  }
  return ret;
}

int ObProxyTableProcessor::update_hu_status_and_cmd(const ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  const char *proxy_ip = hot_upgrade_processor_.get_proxy_ip();
  const int32_t proxy_port = hot_upgrade_processor_.get_proxy_port();

  const bool retries_too_many = (HU_STATUS_FAILURE_RETRIES_TOO_MANY == info.get_parent_status()
                                 && OB_LIKELY(ObHotUpgraderInfo::is_failed_status(info.last_parent_status_)));
  ObHotUpgradeCmd hu_cmd = HUC_MAX;
  //we need update cmd when the follows happened:
  //1. proxy_info is available
  //2. proxy_info.hu_cmd_ is not equal to info.cmd_
  //3. parent status is not CREATE_NEW_PROXY_FAIL
  if (OB_LIKELY(PRS_AVAILABLE == proxy_info.row_status_)
      && OB_UNLIKELY(proxy_info.hu_cmd_ != info.cmd_)
      && OB_LIKELY(HU_STATUS_CREATE_NEW_PROXY_FAIL != info.get_parent_status())) {
    hu_cmd = info.cmd_;
  }

  LOG_INFO("current hot_upgrade status need update", K(proxy_info.row_status_), K(hu_cmd), K(retries_too_many), K(info));
  if (OB_FAIL(ObProxyTableProcessorUtils::update_proxy_status_and_cmd(*mysql_proxy_, proxy_ip,
      proxy_port, info.get_parent_status(), info.get_sub_status(), hu_cmd, retries_too_many))) {
    LOG_WDIAG("fail to update hot upgrade status", K(info), K(ret));
  }
  return ret;
}

bool ObProxyTableProcessor::is_meta_cluster_available()
{
  return get_global_resource_pool_processor().is_cluster_resource_avail(ObString::make_string(OB_META_DB_CLUSTER_NAME));
}

int ObProxyTableProcessor::do_check_work()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  bool is_meta_mysql_avail = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("proxy process is not inited", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!is_meta_cluster_available())) {
    LOG_INFO("meta cluster is not available, do nothing here");
  } else if (OB_UNLIKELY(!mysql_proxy_->is_inited())) {
    LOG_INFO("meta client proxy is not inited, do nothing here");
  } else {
    is_meta_mysql_avail = true;
  }

  if (OB_SUCC(ret)) {
    //proxy need check kv table when it is not local cmd
    ObProxyKVTableInfo kv_info;

    if (is_meta_mysql_avail) {
      //1. must registered proxy
      if (!is_registered_) {
        if (OB_FAIL(register_proxy(ObProxyTableProcessor::RT_INIT))) {
          LOG_WDIAG("fail to register proxy info");
        }
      }

      //2. check proxy kv info from tables without care is_registered_
      if (OB_FAIL(check_proxy_kv_info(kv_info))) {
        LOG_WDIAG("fail to check proxy info", K(kv_info), K(ret));
      }

      //3. get proxy info from tables if needed
      if (need_check_proxy_info_table(kv_info)) {
        if (OB_FAIL(check_proxy_info(kv_info))) {
          LOG_WDIAG("fail to check proxy info", K(kv_info), K(ret));
        }
      } else {
        LOG_DEBUG("there is no need to get proxy info", K(is_meta_mysql_avail), K(kv_info));
      }
    }
  }
  LOG_DEBUG("do check work finished", "cost time(us)", ObTimeUtility::current_time() - now);
  return ret;
}

int ObProxyTableProcessor::update_local_config(int64_t new_config_version)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(config_manager_.update_proxy_config(new_config_version))) {
    LOG_WDIAG("fail to update proxy config", K(ret));
  } else {
    config_manager_.get_proxy_config().refresh_config = false;
    LOG_INFO("succ to update proxy config", K(new_config_version), "local_config_version", config_manager_.get_config_version());
  }
  return ret;
}

int ObProxyTableProcessor::load_remote_config()
{
  int ret = OB_SUCCESS;
  int64_t new_config_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("proxy process is not inited", K_(is_inited), K(ret));
  } else if (OB_FAIL(ObProxyTableProcessorUtils::get_config_version(*mysql_proxy_, new_config_version))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_WDIAG("config version do not exist, continue", K(ret));
      ret = OB_SUCCESS;
    } else {
      LOG_WDIAG("fail to get config version", K(ret));
    }
  } else {
    LOG_INFO("succ to get newest config version", K(new_config_version));
  }

  if (FAILEDx(update_local_config(new_config_version))) {
    LOG_WDIAG("fail to update proxy config", K(new_config_version), K(ret));
  }
  return ret;
}

int ObProxyTableProcessor::check_reload_config(const int64_t new_config_version)
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  const bool need_update_local_config = (new_config_version > config_manager_.get_config_version()
                                         || config_manager_.get_proxy_config().refresh_config);
  if (info.enable_reload_config() && need_update_local_config) {
    if (OB_FAIL(update_local_config(new_config_version))) {
      info.rc_status_ = RCS_RELOAD_FAIL;
      LOG_WDIAG("fail to update local config", K(ret));
    } else {
      info.rc_status_ = RCS_RELOAD_SUCC;
    }

    const char *proxy_ip = hot_upgrade_processor_.get_proxy_ip();
    const int32_t proxy_port = hot_upgrade_processor_.get_proxy_port();
    const int64_t config_version = config_manager_.get_config_version();
    if (OB_FAIL(ObProxyTableProcessorUtils::update_proxy_table_config(
            *mysql_proxy_, proxy_ip, proxy_port, config_version, info.rc_status_))) {
      LOG_WDIAG("fail to update proxy table config info", K(config_version),
               K(info.rc_status_), K(ret));
    }
  } else {
    LOG_DEBUG("there is no need to update local config", K(new_config_version),
              "local_config_version", config_manager_.get_config_version());
  }
  return ret;
}

int ObProxyTableProcessor::update_vip_tenant_cache()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("proxy table processor is not inited", K_(is_inited), K(ret));
  } else {
    ObVipTenantCache::VTHashMap &cache_map_tmp = vt_processor_.get_cache_map_tmp();
    ObVipTenantCache::clear_cache_map(cache_map_tmp);
    if (OB_FAIL(ObProxyTableProcessorUtils::get_vip_tenant_info(*mysql_proxy_, cache_map_tmp))) {
      LOG_WDIAG("fail to get vip tenant info", K(ret));
    } else if (OB_FAIL(vt_processor_.update_cache_map())) {
      LOG_WDIAG("fail to update cache map", K(ret));
    } else {
      LOG_DEBUG("succ update vip tenant cache", "count", vt_processor_.get_vt_cache_count());
    }

    if (OB_FAIL(ret)) {
      ObVipTenantCache::clear_cache_map(cache_map_tmp);
    }
  }
  LOG_DEBUG("update vip tenant cache finished", "cost time(us)", ObTimeUtility::current_time() - now);
  return ret;
}

int ObProxyTableProcessor::check_update_vip_tenant_cache(const int64_t new_vt_cache_version)
{
  int ret = OB_SUCCESS;
  const bool need_convert_vip_to_tname = config_manager_.get_proxy_config().need_convert_vip_to_tname;
  const int64_t local_vt_cache_version = config_manager_.get_proxy_config().local_vip_tenant_version;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("it is not inited", K_(is_inited), K(ret));
  } else if ((!need_convert_vip_to_tname || new_vt_cache_version <= local_vt_cache_version)
             && !get_global_proxy_config().enable_qa_mode) {
    LOG_DEBUG("there is no need to update vip tenant cache", K(need_convert_vip_to_tname),
              K(new_vt_cache_version), K(local_vt_cache_version));
  } else if (OB_FAIL(update_vip_tenant_cache())) {
    LOG_WDIAG("fail to update vip tenant cache", K(ret));
  } else {
    config_manager_.get_proxy_config().local_vip_tenant_version = new_vt_cache_version;
    LOG_INFO("succ to update vip tenant cache", K(new_vt_cache_version), K(local_vt_cache_version));
  }
  return ret;
}

bool ObProxyTableProcessor::need_do_batch_upgrade(const ObProxyKVTableInfo &kv_info, ObProxyServerInfo &proxy_info)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  char *proxy_self_md5_str = hot_upgrade_processor_.get_proxy_self_md5();
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();

  if (!hot_upgrade_processor_.is_self_md5_available()) {
    // if self md5 is not available, calculate it
    char *obproxy_path = NULL;
    ObFixedArenaAllocator<ObLayout::MAX_PATH_LENGTH> allocator;
    if (OB_FAIL(FileDirectoryUtils::create_full_path(get_global_layout().get_bin_dir()))) {
      LOG_WDIAG("fail to makedir", "dir", get_global_layout().get_bin_dir(), K(ret));
    } else if (OB_FAIL(ObLayout::merge_file_path(get_global_layout().get_bin_dir(), "obproxy", allocator, obproxy_path))) {
      LOG_WDIAG("fail to merge file path of obproxy", K(ret));
    } else if (OB_FAIL(get_binary_md5(obproxy_path, proxy_self_md5_str, OB_DEFAULT_PROXY_MD5_LEN + 1))) {
      LOG_WDIAG("fail to get binary md5", K(obproxy_path), K(proxy_self_md5_str), K(ret));
    } else if (!is_available_md5(ObString::make_string(proxy_self_md5_str))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("binary md5 is still unavailable", K(obproxy_path), K(proxy_self_md5_str), K(ret));
    } else {
      hot_upgrade_processor_.set_self_md5_available();
      LOG_INFO("succ to get proxy self md5", K(obproxy_path), K(proxy_self_md5_str), K(hot_upgrade_processor_));
    }
    //Attention::If self_md5 is not available, it must not equal to new_binary_md5.
    //It also means we do batch upgrade continue!
  }

  const ObString &app_binary_name = ObString::make_string(kv_info.app_binary_name_).trim();
  const ObString &app_binary_md5 = ObString::make_string(kv_info.app_binary_md5_).trim();

  //when the follow happened, we need do batch upgrade
  //1. bu_status is UPGRADING
  //2. binary name or md5 is different last used
  //3. binary name is available (length and version)
  //4. binary md5 is not equal to self md5
  //5. binary md5 is available (length and letter)
  if (((BUS_UPGRADE_BIN == kv_info.app_status_ && !hot_upgrade_processor_.is_same_cold_binary(app_binary_name, app_binary_md5))
        || (BUS_UPGRADE_ON == kv_info.app_status_ && !hot_upgrade_processor_.is_same_hot_binary(app_binary_name, app_binary_md5)))
      && app_binary_name.length() > 0
      && app_binary_name.length() <= OB_MAX_PROXY_BINARY_VERSION_LEN
      && app_binary_md5 != proxy_self_md5_str
      && is_available_md5(app_binary_md5)
      && OB_SUCCESS == get_global_config_server_processor().check_kernel_release(app_binary_name)) {
    bret = true;
    MEMCPY(proxy_info.new_binary_version_, app_binary_name.ptr(), app_binary_name.length());
    proxy_info.new_binary_version_[app_binary_name.length()] = '\0';
    MEMCPY(proxy_info.new_binary_md5_, app_binary_md5.ptr(), app_binary_md5.length());
    proxy_info.new_binary_md5_[app_binary_md5.length()] = '\0';
    proxy_info.row_status_  = PRS_AVAILABLE;
    if (BUS_UPGRADE_BIN == kv_info.app_status_) {
      info.cmd_ = HUC_UPGRADE_BIN;
    } else {
      info.cmd_ = HUC_AUTO_UPGRADE;
    }
    LOG_INFO("need do batch upgrade by app", K(info), K(kv_info), K(proxy_info));
  }

  //if do not batch upgrade by app, try all_proxy
  if (!bret) {
    const ObString &all_binary_name = ObString::make_string(kv_info.all_binary_name_).trim();
    const ObString &all_binary_md5 = ObString::make_string(kv_info.all_binary_md5_).trim();
    if (((BUS_UPGRADE_BIN == kv_info.all_status_ && !hot_upgrade_processor_.is_same_cold_binary(all_binary_name, all_binary_md5))
          || (BUS_UPGRADE_ON == kv_info.all_status_ && !hot_upgrade_processor_.is_same_hot_binary(all_binary_name, all_binary_md5)))
        && all_binary_name.length() > 0
        && all_binary_name.length() <= OB_MAX_PROXY_BINARY_VERSION_LEN
        && all_binary_md5 != proxy_self_md5_str
        && is_available_md5(all_binary_md5)
        && OB_SUCCESS == get_global_config_server_processor().check_kernel_release(all_binary_name)) {
      bret = true;
      MEMCPY(proxy_info.new_binary_version_, all_binary_name.ptr(), all_binary_name.length());
      proxy_info.new_binary_version_[all_binary_name.length()] = '\0';
      MEMCPY(proxy_info.new_binary_md5_, all_binary_md5.ptr(), all_binary_md5.length());
      proxy_info.new_binary_md5_[all_binary_md5.length()] = '\0';
      proxy_info.row_status_  = PRS_AVAILABLE;
      if (BUS_UPGRADE_BIN == kv_info.all_status_) {
        get_global_hot_upgrade_info().cmd_ = HUC_UPGRADE_BIN;
      } else {
        get_global_hot_upgrade_info().cmd_ = HUC_AUTO_UPGRADE;
      }
      LOG_INFO("need do batch upgrade by all proxy", K(info), K(kv_info), K(proxy_info));
    }
  }

  LOG_DEBUG("current info", K(bret), K(proxy_self_md5_str),
            "new_binary_md5", proxy_info.new_binary_md5_,
            "new_binary_name", proxy_info.new_binary_version_,
            "hot_binary_name", hot_upgrade_processor_.hot_binary_name_,
            "hot_binary_md5", hot_upgrade_processor_.hot_binary_md5_,
            "cold_binary_name", hot_upgrade_processor_.cold_binary_name_,
            "cold_binary_md5", hot_upgrade_processor_.cold_binary_md5_,
            K(kv_info));
  return bret;
}

int ObProxyTableProcessor::check_do_batch_upgrade(const ObProxyKVTableInfo &kv_info, ObProxyServerInfo &proxy_info)
{
  int ret = OB_SUCCESS;
  ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  // we do btach upgrade when these follow happened:
  //1. enable batch upgrade
  //2. need do batch upgrade
  if (!info.enable_batch_upgrade() || !need_do_batch_upgrade(kv_info, proxy_info)) {
    LOG_DEBUG("there is no need do batch upgrade", "cmd", ObHotUpgraderInfo::get_cmd_string(info.cmd_));
  }
  return ret;
}

bool ObProxyTableProcessor::need_check_proxy_info_table(const ObProxyKVTableInfo &kv_info)
{
  bool bret = false;
  const ObHotUpgraderInfo &info = get_global_hot_upgrade_info();
  //when the follow happened, we need check info table
  //1. proxy under the app need upgrade_part or upgrade_on or upgrade_bin
  //2. proxy is not in idle state
  //3. regester proxy info failed
  //4. last report failed
  //5. all_proxy need upgrade_on or upgrade_bin
  if (BUS_UPGRADE_PART == kv_info.app_status_
      || BUS_UPGRADE_ON == kv_info.app_status_
      || BUS_UPGRADE_BIN == kv_info.app_status_
      || !info.is_in_idle_state()
      || !is_registered_
      || !is_last_report_succeed_
      || BUS_UPGRADE_ON == kv_info.all_status_
      || BUS_UPGRADE_BIN == kv_info.all_status_ ) {
    LOG_DEBUG("need to get proxy info", K_(is_registered), K_(is_last_report_succeed), K(kv_info), K(info));
    bret = true;
  }
  return bret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
