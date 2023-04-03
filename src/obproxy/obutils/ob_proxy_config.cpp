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
#include "obutils/ob_proxy_config.h"
#include "utils/ob_layout.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/client/ob_client_utils.h"
#include "obutils/ob_proxy_config_utils.h"
#include "common/ob_version.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

ObProxyConfig &get_global_proxy_config()
{
  static ObProxyConfig g_proxy_config;
  return g_proxy_config;
}

void ObProxyConfig::print() const
{
  LOG_INFO("===================== *begin proxy config report * =====================");
  //we need lock it whenever handle item->value
  CRLockGuard guard(rwlock_);
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    _LOG_INFO("| %-36s = %s", it->first.str(), it->second->str());
  }
  LOG_INFO("===================== *end proxy config report* =======================");
}

void ObProxyConfig::print_need_reboot_config() const
{
  LOG_INFO("===================== *begin proxy need reboot config report * =====================");
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (it->second->need_reboot()) {
      _LOG_INFO("| %-36s = %s", it->first.str(), it->second->str());
    }
  }
  LOG_INFO("===================== *end proxy need reboot config report* =======================");
}

int ObProxyConfig::init_need_reboot_config()
{
  int ret = OB_SUCCESS;
  ObConfigContainer::const_iterator it = container_.begin();
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (it->second->need_reboot()) {
      it->second->set_initial_value();
    }
  }
  return ret;
}

int ObProxyConfig::reset()
{
  int ret = OB_SUCCESS;
  ObProxyConfig *orig_config = NULL;
  if (OB_ISNULL(orig_config = new (std::nothrow) ObProxyConfig())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new memory for ObProxyConfig", K(ret));
  } else {
    set_app_name(orig_config->app_name);

    const ObConfigContainer &container = orig_config->get_container();
    ObConfigContainer::const_iterator it = container.begin();
    for (; OB_SUCC(ret) && it != container.end(); ++it) {
      if (OB_FAIL(update_config_item(ObString::make_string(it->first.str()), ObString::make_string(it->second->str())))) {
        LOG_WARN("fail to update config item", "name", it->first.str(), "value", it->second->str(), K(ret));
      }
    }
  }

  if (OB_LIKELY(NULL != orig_config)) {
    delete orig_config;
    orig_config = NULL;
  }
  return ret;
}

int ObProxyConfig::check_all() const
{
  int ret = OB_SUCCESS;
  //we need lock it whenever handle item->value
  CRLockGuard guard(rwlock_);
  ObConfigContainer::const_iterator it = container_.begin();
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (OB_UNLIKELY(!it->second->check())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("proxy config item invalid!", "name", it->first.str(), "value", it->second->str(), K(ret));
    }
  }
  return ret;
}

int ObProxyConfig::check_proxy_serviceable() const
{
  int ret = OB_SUCCESS;
  //1. service mode no need check
  if (OB_SUCC(ret)) {
    ObProxyServiceMode mode = OB_MAX_SERVICE_MODE;
    if (OB_UNLIKELY(!is_service_mode_available(mode)) || OB_UNLIKELY(OB_MAX_SERVICE_MODE == mode)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("service mode is not available", K(mode),
               "proxy_service_mode", proxy_service_mode.str(), K(ret));
    } else if (OB_SERVER_SERVICE_MODE == mode && OB_UNLIKELY(proxy_id.get_value() <= 0)) {
      // get_value() return the real-time value
      // get() return the real-time value if it does not need reboot, otherwise it return initial_value
      // here we need use real-time value.
      // if use server service mode, proxy_id must be specified
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("proxy_id must be specified when use server service mode",
               "proxy_id", proxy_id.get_value(), K(ret));
    } else {
      //do nothing
    }
  }

  //2. check routing_mode
  if (OB_SUCC(ret)) {
    ObServerRoutingMode mode = OB_MAX_ROUTING_MODE;
    if (OB_UNLIKELY(!is_routing_mode_available(mode)) || OB_UNLIKELY(OB_MAX_ROUTING_MODE == mode)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("routing mode is not available", K(mode),
               "server_routing_mode", server_routing_mode.str(), K(ret));
    }
  }

  //3. app name should not be null or error length
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(app_name.str()));
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_MAX_APP_NAME_LENGTH)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("app name is not available", K(len), K(OB_MAX_APP_NAME_LENGTH), K(app_name.str()), K(ret));
    }
  }

  //4. check syslog level
  if (OB_SUCC(ret)) {
    int8_t level = 0;
    CRLockGuard guard(rwlock_);
    if (OB_FAIL(OB_LOGGER.parse_check(syslog_level.str(), static_cast<int32_t>(STRLEN(syslog_level.str()))))) {
      LOG_WARN("fail to parse check syslog_level", K(syslog_level.str()), K(ret));
    } else if (OB_FAIL(OB_LOGGER.get_log_level_from_str(monitor_log_level.str(), level))) {
      LOG_WARN("fail to get_log_level_from_str", K(monitor_log_level.str()), K(ret));
    } else if (OB_FAIL(OB_LOGGER.get_log_level_from_str(xflush_log_level.str(), level))) {
      LOG_WARN("fail to get_log_level_from_str", K(xflush_log_level.str()), K(ret));
    }
  }

  //5. strlist item
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    if (OB_UNLIKELY(!rootservice_list.valid()) || OB_UNLIKELY(!username_separator.valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("string list item is not valid", K(rootservice_list.str()), K(username_separator.str()), K(ret));
    }
  }

  //6. proxy idc name name should not error length
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(proxy_idc_name.str()));
    if (OB_UNLIKELY(len > OB_PROXY_MAX_IDC_NAME_LENGTH)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("proxy idc name is not available", K(len), K(OB_PROXY_MAX_IDC_NAME_LENGTH), K(proxy_idc_name.str()), K(ret));
    }
  }

  //7. mysql-version should be digital
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(mysql_version.str()));

    if ((len != 0) && OB_UNLIKELY(OB_FAIL(check_version_valid(len, mysql_version.str())))) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("mysql version is not valid, must be digital", K(mysql_version.str()), K(ret));
    }
  }
  return ret;
}

int ObProxyConfig::dump_config_to_local()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (OB_FAIL(ObProxyConfigUtils::dump2file(*this))) {
    LOG_WARN("fail to dump config bin to file", K(ret));
  }

  LOG_DEBUG("finish dump config to local", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  return ret;
}

int ObProxyConfig::dump_config_to_sqlite()
{
  int ret = OB_SUCCESS;
  CRLockGuard guard(rwlock_);
  ObConfigContainer::const_iterator it = container_.begin();
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    const char *sql = "replace into proxy_config(name, value, config_level) values('%s', '%s', 'LEVEL_GLOBAL')";
    char buf[1024];
    char *err_msg = NULL;
    int64_t len = static_cast<int64_t>(snprintf(buf, 1024, sql, it->first.str(), it->second->str()));
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= 1024)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to fill sql", K(buf), K(len), K(ret));
    } else if (SQLITE_OK != sqlite3_exec(proxy_config_db_, buf, NULL, 0, &err_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("exec replace into proxy config failed", K(ret), "sql", buf, "err_msg", err_msg);
    }

    if (NULL != err_msg) {
      sqlite3_free(err_msg);
    }
  }
  return ret;
}

int ObProxyConfig::set_value_safe(ObConfigItem *item, const ObString &value,
    const bool allow_invalid_value/*true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input argument", K(value), K(ret));
  } else {
    //0.we need lock it in case of multi-thread set_value()
    CWLockGuard guard(rwlock_);

    //1.save original value
    const int64_t value_str_length = STRLEN(item->str());
    MEMCPY(original_value_str_, item->str(), value_str_length);
    original_value_str_[value_str_length] = '\0';

    //2.update config item value
    if (value.empty()) {
      //if value is empty, value.ptr_ must equal to NULL. we need use null_string instead
      const char *null_string = "";
      if (OB_UNLIKELY(!item->set_value(null_string))) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("fail to set config item to null_string", "name", item->name(), K(ret));
      }
    } else {
      if (OB_UNLIKELY(!item->set_value(value))) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("fail to set config item", "name", item->name(), K(value), K(ret));
      }
    }

    //3.check config item value
    if (OB_SUCC(ret)) {
     if (OB_UNLIKELY(!item->check())) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid config", "name", item->name(), "value", item->str(), K(ret));
      }
    }

    //4.when failed, recover original config
    if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(!item->set_value(original_value_str_))) {
        LOG_ERROR("fail to recover config item", "name", item->name(), K(original_value_str_));
      } else if (!allow_invalid_value) {
        LOG_WARN("invalid value", K(value), K(original_value_str_), K(allow_invalid_value), K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObProxyConfig::update_config_item(const ObString &key_name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_name.empty())
      || OB_UNLIKELY(key_name.length() >= static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN))
      || (!value.empty() && OB_UNLIKELY(static_cast<int64_t>(value.length()) >= OB_MAX_CONFIG_VALUE_LEN))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key_name), K(value), K(ret));
  } else {
    //if allow_invalid_key_value is true, we can tolerate the invalid key and value
    //if allow_invalid_key_value is false, the key and value must be valid and set succeed
    const bool allow_invalid_key_value = false;
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WARN("unknown key_name", K(key_name), K(ret));
    } else if (OB_ISNULL(*item)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid item, it should not happened",K(key_name), K(ret));
    } else if (OB_FAIL(set_value_safe(*item, value, allow_invalid_key_value))) {
      LOG_WARN("fail to set config item safety", "name", (*item)->name(), K(value), K(ret));
    }
  }

  return ret;
}

int ObProxyConfig::update_user_config_item(const ObString &key_name, const ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_name.empty())
      || OB_UNLIKELY(key_name.length() >= static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN))
      || (!value.empty() && OB_UNLIKELY(static_cast<int64_t>(value.length()) >= OB_MAX_CONFIG_VALUE_LEN))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key_name), K(value), K(ret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      LOG_INFO("config item not exist, maybe new add item", K(key_name), K(ret));
    } else if (OB_ISNULL(*item)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid item, it should not happened",K(key_name), K(ret));
    } else if (OB_UNLIKELY(!ObProxyConfigUtils::is_user_visible((**item)))) {
      LOG_WARN("this is not user visible item, config in metadb is unexpected, ignore it", K(key_name), K(value));
    } else if (OB_FAIL(set_value_safe(*item, value))) {
      LOG_WARN("fail to set config item safety", "name", (*item)->name(), K(value), K(ret));
    }
  }

  return ret;
}

int ObProxyConfig::get_old_config_value(const common::ObString &key_name, char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_name.empty())
      || OB_UNLIKELY(key_name.length() >= static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN))
      || OB_ISNULL(buf)
      || OB_UNLIKELY(buf_size < OB_MAX_CONFIG_VALUE_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key_name), K(buf_size), K(buf), K(ret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WARN("unknown key_name", K(key_name), K(ret));
    } else {
      //we need lock it whenever handle item->value
      CRLockGuard guard(rwlock_);
      const int32_t length = snprintf(buf, buf_size, "%s", (*item)->str());
      if (length < 0 || length >= buf_size) {
        ret = OB_BUF_NOT_ENOUGH;
        OB_LOG(WARN, "buffer not enough", K(length), K(buf_size), "value", (*item)->str(), K(ret));
      }
    }
  }
  return ret;
}

int ObProxyConfig::get_config_item(const common::ObString &key_name, ObConfigItem &ret_item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_name.empty())
      || OB_UNLIKELY(key_name.length() >= static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key_name), K(ret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WARN("unknown key_name", K(key_name), K(ret));
    } else {
      //we need lock it whenever handle item->value
      CRLockGuard guard(rwlock_);
      ret_item = **item;
    }
  }
  return ret;
}

void ObProxyConfig::update_log_level(const bool level_flag)
{
  int ret = OB_SUCCESS;
  const int32_t old_level= OB_LOGGER.get_level();
  if (level_flag) {
    OB_LOGGER.up_log_level();
  } else {
    OB_LOGGER.down_log_level();
  }
  const ObString name("syslog_level");
  const ObString value(OB_LOGGER.get_level_str());
  if (OB_FAIL(update_config_item(name, value))) {
    LOG_WARN(" fail to update sys log level", K(ret));
  } else if (OB_FAIL(dump_config_to_local())) {
    LOG_WARN("dump config fail, inc log level fail", K(ret));
  } else if (OB_FAIL(dump_config_to_sqlite())) {
    LOG_WARN("dump config failed, inc log level fail", K(ret));
  } {
    //do nothing
  }

  if (OB_FAIL(ret)) {
    OB_LOGGER.set_log_level(old_level);
  }
}

int ObProxyConfig::fill_proxy_config(ObMysqlResultHandler &result_handler)
{
  int ret = OB_SUCCESS;
  ObString app_name;
  ObString key_name;
  ObString value;
  while (OB_SUCC(ret) && OB_SUCC(result_handler.next())) {
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "app_name", app_name);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "name", key_name);
    PROXY_EXTRACT_VARCHAR_FIELD_MYSQL(result_handler, "value", value);

    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_user_config_item(key_name, value))) {
        LOG_WARN("fail to update user config item", K(app_name), K(key_name), K(value));
      } else {
        LOG_DEBUG("succ to update user config item", K(app_name), K(key_name), K(value));
        app_name.reset();
        key_name.reset();
        value.reset();
      }
    }
  }

  if (OB_LIKELY(OB_ITER_END == ret)) {
    ret = OB_SUCCESS;
  } else {
    LOG_WARN("failed to get result from result set", K(ret));
  }
  return ret;
}

}
}
}
