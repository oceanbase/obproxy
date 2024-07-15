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
    LOG_WDIAG("fail to new memory for ObProxyConfig", K(ret));
  } else {
    set_app_name(orig_config->app_name);

    const ObConfigContainer &container = orig_config->get_container();
    ObConfigContainer::const_iterator it = container.begin();
    for (; OB_SUCC(ret) && it != container.end(); ++it) {
      if (OB_FAIL(update_config_item(ObString::make_string(it->first.str()), ObString::make_string(it->second->str())))) {
        LOG_WDIAG("fail to update config item", "name", it->first.str(), "value", it->second->str(), K(ret));
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
      LOG_WDIAG("proxy config item invalid!", "name", it->first.str(), "value", it->second->str(), K(ret));
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
      LOG_WDIAG("service mode is not available", K(mode),
               "proxy_service_mode", proxy_service_mode.str(), K(ret));
    } else if (OB_SERVER_SERVICE_MODE == mode && OB_UNLIKELY(proxy_id.get_value() <= 0)) {
      // get_value() return the real-time value
      // get() return the real-time value if it does not need reboot, otherwise it return initial_value
      // here we need use real-time value.
      // if use server service mode, proxy_id must be specified
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("proxy_id must be specified when use server service mode",
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
      LOG_WDIAG("routing mode is not available", K(mode),
               "server_routing_mode", server_routing_mode.str(), K(ret));
    }
  }

  //3. app name should not be null or error length
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(app_name.str()));
    if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_MAX_APP_NAME_LENGTH)) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("app name is not available", K(len), K(OB_MAX_APP_NAME_LENGTH), K(app_name.str()), K(ret));
    }
  }

  //4. check syslog level
  if (OB_SUCC(ret)) {
    int8_t level = 0;
    CRLockGuard guard(rwlock_);
    if (OB_FAIL(OB_LOGGER.parse_check(syslog_level.str(), static_cast<int32_t>(STRLEN(syslog_level.str()))))) {
      LOG_WDIAG("fail to parse check syslog_level", K(syslog_level.str()), K(ret));
    } else if (OB_FAIL(OB_LOGGER.get_log_level_from_str(monitor_log_level.str(), level))) {
      LOG_WDIAG("fail to get_log_level_from_str", K(monitor_log_level.str()), K(ret));
    } else if (OB_FAIL(OB_LOGGER.get_log_level_from_str(xflush_log_level.str(), level))) {
      LOG_WDIAG("fail to get_log_level_from_str", K(xflush_log_level.str()), K(ret));
    }
  }

  //5. strlist item
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    if (OB_UNLIKELY(!rootservice_list.valid()) || OB_UNLIKELY(!username_separator.valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("string list item is not valid", K(rootservice_list.str()), K(username_separator.str()), K(ret));
    }
  }

  //6. proxy idc name name should not error length
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(proxy_idc_name.str()));
    if (OB_UNLIKELY(len > OB_PROXY_MAX_IDC_NAME_LENGTH)) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("proxy idc name is not available", K(len), K(OB_PROXY_MAX_IDC_NAME_LENGTH), K(proxy_idc_name.str()), K(ret));
    }
  }

  //7. mysql-version should be digital
  if (OB_SUCC(ret)) {
    CRLockGuard guard(rwlock_);
    const int64_t len = static_cast<int64_t>(strlen(mysql_version.str()));

    if ((len != 0) && OB_UNLIKELY(OB_FAIL(check_version_valid(len, mysql_version.str())))) {
      ret = OB_INVALID_CONFIG;
      LOG_WDIAG("mysql version is not valid, must be digital", K(mysql_version.str()), K(ret));
    }
  }
  return ret;
}

bool ObProxyConfig::is_memory_visible(const common::ObString &key_name)
{
  bool bret = false;
  if (OB_UNLIKELY(key_name.empty())
      || OB_UNLIKELY(key_name.length() >= static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN))) {
    LOG_WDIAG("invalid argument", K(key_name), K(bret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);
    // 无需加锁，因为memory_visiable不会被修改
    if (OB_ISNULL(item = container_.get(key))) {
      LOG_WDIAG("unknown key_name", K(key_name), K(bret));
    } else if (OB_ISNULL(*item)) {
      LOG_WDIAG("invalid item, it should not happened",K(key_name), K(bret));
    } else {
      bret = ObProxyConfigUtils::is_memory_visible(**item);
    }
  }
  return bret;
}

int ObProxyConfig::load_sqlite_config_init_callback(void *data, int argc, char **argv, char **column_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data) || OB_ISNULL(argv) || OB_ISNULL(column_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("sqlite argument is null unexpected", K(ret));
  } else {
    // 这里传入的是全局proxy_config，只会修改其value，不涉及其他属性
    ObProxyConfig &proxy_config = *static_cast<ObProxyConfig*>(data);
    ObConfigItem item;
    for (int64_t i = 0; OB_SUCC(ret) && i < argc; ++i) {
      ObString col_name(column_name[i]);
      if (col_name == "name") {
        if (NULL == argv[i]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("name is null unexpected from sqlite", K(ret));
        } else {
          item.set_name(argv[i]);
        }
      } else if (col_name == "value") {
        if (NULL == argv[i]) {
          // do nothing, allowed to value is null
        } else {
          item.set_value(argv[i]);
        }
      } else {  // impossible
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected column name from sqlite", K(col_name), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WDIAG("fail to parse config from sqlite", K(ret));
    } else if (proxy_config.is_memory_visible(item.name())) {
      // 内存级别配置，不使用磁盘数据，采用默认配置
      // do nothing
    } else if (OB_FAIL(proxy_config.update_config_item(item.name(), item.str()))) {
      if (OB_ERR_SYS_CONFIG_UNKNOWN == ret || OB_INVALID_CONFIG == ret) {
        // 兼容配置，1. 如果读取到不存在的配置，直接跳过，不插入；2. 配置是无效值，会回滚到默认配置
        LOG_WDIAG("config is invaild, will ues default config value and ignore this error",
                  K(item), K(ret));
        ret = OB_SUCCESS;
      } else {
        LOG_WDIAG("fail to update config item", K(item.name()), K(item.str()), K(ret));
      }
    }
  }

  return ret;
}

int ObProxyConfig::dump_config_to_local()
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();

  if (OB_FAIL(ObProxyConfigUtils::dump2file(*this))) {
    LOG_WDIAG("fail to dump config bin to file", K(ret));
  }

  LOG_DEBUG("finish dump config to local", "cost time(us)", ObTimeUtility::current_time() - now, K(ret));
  return ret;
}

int ObProxyConfig::dump_config_to_sqlite()
{
  int ret = OB_SUCCESS;
  sqlite3 *proxy_config_db = get_global_config_processor().get_sqlite_db();
  if (OB_ISNULL(proxy_config_db)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer from sqlite", K(ret));
  } else {
    CRLockGuard guard(rwlock_);
    ObConfigContainer::const_iterator it = container_.begin();
    const int64_t max_sql_len = 1024 * 4 + 256;   // value最长4k，name最长128
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      const char *sql = "replace into proxy_config(name, value, config_level) values('%s', '%s', 'LEVEL_GLOBAL')";
      char buf[max_sql_len];
      char *err_msg = NULL;
      int64_t len = static_cast<int64_t>(snprintf(buf, max_sql_len, sql, it->first.str(), it->second->str()));
      if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= max_sql_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to fill sql", K(buf), K(len), K(max_sql_len), K(ret));
      } else if (SQLITE_OK != sqlite3_exec(proxy_config_db, buf, NULL, 0, &err_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("exec replace proxy config into sqlite failed", K(ret), "sql", buf, "err_msg", err_msg);
      }

      if (NULL != err_msg) {
        sqlite3_free(err_msg);
      }
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
    LOG_WDIAG("invalid input argument", K(value), K(ret));
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
        LOG_WDIAG("fail to set config item to null_string", "name", item->name(), K(ret));
      }
    } else {
      if (OB_UNLIKELY(!item->set_value(value))) {
        ret = OB_INVALID_CONFIG;
        LOG_WDIAG("fail to set config item", "name", item->name(), K(value), K(ret));
      }
    }

    //3.check config item value
    if (OB_SUCC(ret)) {
     if (OB_UNLIKELY(!item->check())) {
        ret = OB_INVALID_CONFIG;
        LOG_WDIAG("invalid config", "name", item->name(), "value", item->str(), K(ret));
      }
    }

    //4.when failed, recover original config
    if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(!item->set_value(original_value_str_))) {
        LOG_EDIAG("fail to recover config item", "name", item->name(), K(original_value_str_));
      } else if (!allow_invalid_value) {
        LOG_WDIAG("invalid value", K(value), K(original_value_str_), K(allow_invalid_value), K(ret));
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
    LOG_WDIAG("invalid argument", K(key_name), K(value), K(ret));
  } else {
    //if allow_invalid_key_value is true, we can tolerate the invalid key and value
    //if allow_invalid_key_value is false, the key and value must be valid and set succeed
    const bool allow_invalid_key_value = false;
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WDIAG("unknown key_name", K(key_name), K(ret));
    } else if (OB_ISNULL(*item)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid item, it should not happened",K(key_name), K(ret));
    } else if (OB_FAIL(set_value_safe(*item, value, allow_invalid_key_value))) {
      LOG_WDIAG("fail to set config item safety", "name", (*item)->name(), K(value), K(ret));
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
    LOG_WDIAG("invalid argument", K(key_name), K(value), K(ret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      LOG_INFO("config item not exist, maybe new add item", K(key_name), K(ret));
    } else if (OB_ISNULL(*item)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid item, it should not happened",K(key_name), K(ret));
    } else if (OB_UNLIKELY(!ObProxyConfigUtils::is_user_visible((**item)))) {
      LOG_WDIAG("this is not user visible item, config in metadb is unexpected, ignore it", K(key_name), K(value));
    } else if (OB_FAIL(set_value_safe(*item, value))) {
      LOG_WDIAG("fail to set config item safety", "name", (*item)->name(), K(value), K(ret));
    }
  }

  return ret;
}

int ObProxyConfig::get_config_value(const common::ObString &key_name, common::ObConfigVariableString &value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key_name.empty())
      || OB_UNLIKELY(key_name.length() >= static_cast<int32_t>(OB_MAX_CONFIG_NAME_LEN))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(key_name), K(value), K(ret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WDIAG("unknown key_name", K(key_name), K(ret));
    } else {
      //we need lock it whenever handle item->value
      CRLockGuard guard(rwlock_);
      if (OB_FAIL(value.rewrite((*item)->str()))) {
        OB_LOG(WDIAG, "fail to write config to value",
              K(key_name), K(value), "value", (*item)->str(), K(ret));
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
    LOG_WDIAG("invalid argument", K(key_name), K(ret));
  } else {
    ObConfigItem *const *item = NULL;
    ObConfigStringKey key(key_name);

    if (OB_ISNULL(item = container_.get(key))) {
      ret = OB_ERR_SYS_CONFIG_UNKNOWN;
      LOG_WDIAG("unknown key_name", K(key_name), K(ret));
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
    LOG_WDIAG(" fail to update sys log level", K(ret));
  } else if (OB_FAIL(dump_config_to_sqlite())) {
    LOG_WDIAG("dump config failed, inc log level fail", K(ret));
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
        LOG_WDIAG("fail to update user config item", K(app_name), K(key_name), K(value));
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
    LOG_WDIAG("failed to get result from result set", K(ret));
  }
  return ret;
}

#ifdef ERRSIM
int ObProxyConfig::parse_error_inject_config() const
{
  common::ObString param = error_inject.str();
  int ret = OB_SUCCESS;
  int len = param.length();
  common::ObString buf;
  // clear error inject config
  TP_RESET();
  if (len == 0) {
  } else {
    buf.assign(param.ptr(), len);
    bool finish = false;
    while (OB_SUCC(ret) && !finish) {
      common::ObString group_str;
      common::ObString item_str;
      int64_t err_inject_conf[4] = {0};
      group_str = buf.split_on(';');
      if (group_str.empty()) {
        finish = true;
        group_str = buf;
      }
      bool inner_finish = false;
      for (int i = 0; i < 4 && !group_str.empty() && OB_SUCC(ret); i++) {
        item_str = group_str.split_on(',');
        char *stop_index = NULL;
        if (inner_finish) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WDIAG("invalid err inject param");
        } else {
          if (item_str.empty()) {
            item_str = group_str;
            inner_finish = true;
          }
          if (item_str.empty()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WDIAG("invalid err inject param", K(ret));
          } else {
            err_inject_conf[i] = strtol(item_str.ptr(), &stop_index, 10);
            if (stop_index != (item_str.ptr() + item_str.length())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WDIAG("invalid err inject param", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        TP_SET_EVENT(err_inject_conf[0], err_inject_conf[1], err_inject_conf[2], err_inject_conf[3]);
      }
    }
  }
  if (OB_FAIL(ret)) {
    TP_RESET();
  }
  return ret;
}
#endif

}
}
}
