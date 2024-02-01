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
#include "obutils/ob_proxy_config_manager.h"
#include "utils/ob_proxy_lib.h"
#include "utils/ob_layout.h"
#include "utils/ob_proxy_table_define.h"
#include "obutils/ob_proxy_reload_config.h"
#include "proxy/mysqllib/ob_resultset_fetcher.h"
#include "proxy/client/ob_client_utils.h"
#include "proxy/client/ob_mysql_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

const char *ObProxyConfigManager::GET_PROXY_CONFIG_SQL =
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ app_name, name, value FROM %s where app_name = 'all_proxy' "
    "UNION ALL "
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ app_name, name, value FROM %s where app_name = '%s' "
    "LIMIT %ld";

ObProxyConfigManager::ObProxyConfigManager()
   : is_inited_(false), mysql_proxy_(NULL),
     proxy_config_(get_global_proxy_config()), reload_config_func_(NULL)
{
}

int ObProxyConfigManager::init(ObMysqlProxy &mysql_proxy,
                               ObProxyReloadConfig &reload_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("already inited", K(ret));
  } else {
    mysql_proxy_ = &mysql_proxy;
    reload_config_func_ = &reload_config;
    is_inited_ = true;
  }

  return ret;
}

void ObProxyConfigManager::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    reload_config_func_ = NULL;
    mysql_proxy_ = NULL;
    is_inited_ = false;
  }
}

DEF_TO_STRING(ObProxyConfigManager)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), KPC_(mysql_proxy));
  J_OBJ_END();
  return pos;
}

int ObProxyConfigManager::reload_proxy_config()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(proxy_config_.check_all())) {
    LOG_WDIAG("fail to check proxy config, can't reload", K(ret));
  } else if (OB_FAIL(proxy_config_.check_proxy_serviceable())) {
    LOG_WDIAG("fail to check proxy string_item config, can't reload", K(ret));
  } else if (OB_FAIL((*reload_config_func_)(proxy_config_))) {
    LOG_WDIAG("fail to reload config", K(ret));
  }
  return ret;
}

int ObProxyConfigManager::update_proxy_config(const int64_t new_config_version)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("proxy config manager not inited", K_(is_inited), K(ret));
  } else if (OB_ISNULL(mysql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("unexpected error happened, mysql_proxy_ must not be NULL", K(ret));
  } else {
    ObMysqlResultHandler result_handler;
    char sql[OB_SHORT_SQL_LENGTH];
    sql[0] = '\0';
    int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, GET_PROXY_CONFIG_SQL,
                        ObProxyTableInfo::PROXY_CONFIG_TABLE_NAME,
                        ObProxyTableInfo::PROXY_CONFIG_TABLE_NAME,
                        proxy_config_.app_name_str_,
                        INT64_MAX);
    if (OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH) || OB_UNLIKELY(len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to fill sql", K(len), K(OB_SHORT_SQL_LENGTH), K(ret));
    } else if (OB_FAIL(mysql_proxy_->read(sql, result_handler))) {
      LOG_WDIAG("fail to get proxy config", K(sql), K(ret));
    } else if (OB_FAIL(update_local_config(new_config_version, result_handler))) {
      LOG_WDIAG("fail to update config", K(sql), K(ret));
    } else { }
  }
  LOG_TRACE("update proxy config finished", "cost time(us)", ObTimeUtility::current_time() - now);
  return ret;
}

int ObProxyConfigManager::update_local_config(const int64_t new_config_version,
    ObMysqlResultHandler &result_handler)
{
  int ret = OB_SUCCESS;
  bool has_serialized = false;
  bool has_dump_config = false;
  char *orig_buf = NULL;
  int64_t write_pos = 0;
  int64_t read_pos = 0;
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = ObModIds::OB_PROXY_FILE;

  //1. backup old config
  if (OB_ISNULL(orig_buf = static_cast<char *>(ob_malloc(OB_PROXY_CONFIG_BUFFER_SIZE, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("ob tc malloc memory for buf fail", K(ret));
  } else {
    obsys::CRLockGuard guard(proxy_config_.rwlock_);
    if (OB_FAIL(proxy_config_.serialize(orig_buf, OB_PROXY_CONFIG_BUFFER_SIZE, write_pos))) {
      LOG_WDIAG("fail to serialize proxy config", K(ret));
    } else if (OB_UNLIKELY(write_pos > OB_PROXY_CONFIG_BUFFER_SIZE)) {
      ret = OB_SERIALIZE_ERROR;
      LOG_WDIAG("fail to serialize old config", K(write_pos), K(OB_PROXY_CONFIG_BUFFER_SIZE), K(ret));
    } else {
      has_serialized = true;
      LOG_DEBUG("succ to serialize old config", K(write_pos), K(OB_PROXY_CONFIG_BUFFER_SIZE));
    }
  }

  //2. fill, check, dump new value
  if (OB_SUCC(ret)) {
    //if OB_INVALID_VERSION == new_config_version, local config version will no changed
    if (common::OB_INVALID_VERSION != new_config_version) {
      proxy_config_.current_local_config_version = new_config_version;
    }
    if (OB_FAIL(proxy_config_.fill_proxy_config(result_handler ))) {
      LOG_WDIAG("fail to fill proxy config", K(ret));
    } else if (OB_FAIL(proxy_config_.check_proxy_serviceable())) {
      LOG_WDIAG("fail to check proxy string_item config", K(ret));
    } else if (OB_FAIL(proxy_config_.dump_config_to_local())) {
      LOG_WDIAG("fail to dump config to local", K(ret));
    } else {
      has_dump_config = true;
      LOG_DEBUG("succ to dump config to local");
    }
  }

  //3. reload config to memory
  if (OB_SUCC(ret)) {
    if (OB_FAIL((*reload_config_func_)(proxy_config_))) {
      LOG_WDIAG("fail to reload config", K(ret));
    } else {
      LOG_DEBUG("succ to update local config");
    }
  }

  //4. rollback
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (has_serialized) {
      obsys::CWLockGuard guard(proxy_config_.rwlock_);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = (proxy_config_.deserialize(orig_buf, write_pos, read_pos))))) {
        LOG_WDIAG("fail to deserialize old config", K(write_pos), K(read_pos), K(tmp_ret));
      } else if (OB_UNLIKELY(write_pos != read_pos)) {
        tmp_ret = OB_DESERIALIZE_ERROR;
        LOG_WDIAG("deserialize proxy config failed", K(write_pos), K(read_pos), K(tmp_ret));
      } else {
        LOG_DEBUG("succ to deserialize old config", K(write_pos), K(read_pos));
      }
    }
    if (has_dump_config && OB_LIKELY(OB_SUCCESS == tmp_ret)) {
      if(OB_UNLIKELY(OB_SUCCESS != (tmp_ret = (*reload_config_func_)(proxy_config_)))) {
        LOG_WDIAG("fail to reload old config", K(tmp_ret));
      } else {
        LOG_DEBUG("succ to reload old config");
      }
      if(OB_UNLIKELY(OB_SUCCESS != (tmp_ret = proxy_config_.dump_config_to_local()))) {
        LOG_WDIAG("fail to dump old config to local", K(tmp_ret));
      } else {
        LOG_DEBUG("succ to dump old config to local");
      }
    }

    //if fail to update proxy config, print all config items to log file
    proxy_config_.print_need_reboot_config();
    proxy_config_.print();
  }

  if (OB_LIKELY(NULL != orig_buf)) {
    ob_free(orig_buf);
    orig_buf = NULL;
  }
  return ret;
}

} //end namespace obutils
} //end namespace obproxy
} //end namespace oceanbase
