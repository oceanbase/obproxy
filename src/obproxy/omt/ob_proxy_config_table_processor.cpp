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
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX PROXY

#include "omt/ob_proxy_config_table_processor.h"
#include "obutils/ob_config_processor.h"
#include "obutils/ob_proxy_config.h"
#include "obutils/ob_proxy_reload_config.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "cmd/ob_internal_cmd_processor.h"

static const char *config_name_array[] = {"proxy_route_policy", "proxy_idc_name", "enable_cloud_full_username",
                                          "enable_client_ssl", "enable_server_ssl"};
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
namespace oceanbase
{
namespace obproxy
{
namespace omt
{

ObProxyConfigItem* ObProxyConfigItem::clone()
{
  ObProxyConfigItem *ret = NULL;
  if (OB_ISNULL(ret = op_alloc(ObProxyConfigItem))) {
    LOG_WARN("op_alloc ObProxyConfigItem failed");
  } else {
    ret->vip_addr_ = vip_addr_;
    ret->tenant_name_ = tenant_name_;
    ret->cluster_name_ = cluster_name_;
    ret->config_level_ = config_level_;
    ret->config_item_ = config_item_;
  }

  return ret;
}

ObProxyConfigItem::ObProxyConfigItem(const ObProxyConfigItem& item)
{
  vip_addr_ = item.vip_addr_;
  cluster_name_ = item.cluster_name_;
  tenant_name_ = item.tenant_name_;
  config_level_ = item.config_level_;
  config_item_ = config_item_;
}

ObProxyConfigItem& ObProxyConfigItem::operator =(const ObProxyConfigItem &item)
{
  if (this != &item) {
    vip_addr_ = item.vip_addr_;
    cluster_name_ = item.cluster_name_;
    tenant_name_ = item.tenant_name_;
    config_level_ = item.config_level_;
    config_item_ = item.config_item_;
  }
  return *this;
}

int64_t ObProxyConfigItem::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(vip_addr), K_(tenant_name), K_(cluster_name), K_(config_level), K_(config_item));
  J_OBJ_END();
  return pos;
}

uint64_t ObProxyConfigItem::get_hash() const
{
  uint64_t hash = 0;
  void *data = (void*)(&vip_addr_);
  hash = murmurhash(data, sizeof(ObVipAddr), 0);
  hash = tenant_name_.hash(hash);
  hash = cluster_name_.hash(hash);
  hash = murmurhash(config_item_.name(), static_cast<int32_t>(strlen(config_item_.name())), hash);
  return hash;
}

int ObProxyConfigTableProcessor::execute(void *arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is null unexpected", K(ret));
  } else {
    ObCloudFnParams *params = reinterpret_cast<ObCloudFnParams*>(arg);
    if (NULL == params->fields_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fields is null unexpected", K(ret));
    } else if (params->stmt_type_ == OBPROXY_T_REPLACE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().set_proxy_config(params->fields_))) {
        LOG_WARN("set proxy config failed", K(ret));
      }
    } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
      if (OB_FAIL(get_global_proxy_config_table_processor().delete_proxy_config(params->fields_))) {
        LOG_WARN("delete proxy config failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(params->stmt_type_), K(ret));
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::commit(void *arg, bool is_success)
{
  UNUSED(arg);
  if (is_success) {
    get_global_proxy_config_table_processor().inc_index();
    get_global_proxy_config_table_processor().inc_config_version();
  } else {
    LOG_WARN("proxy config commit failed", K(is_success));
  }

  get_global_proxy_config_table_processor().clean_hashmap(
    get_global_proxy_config_table_processor().get_backup_hashmap());

  return OB_SUCCESS;
}

void ObProxyConfigTableProcessor::inc_index()
{
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  index_ = (index_ + 1) % 2;
}

ObProxyConfigTableProcessor &get_global_proxy_config_table_processor()
{
  static ObProxyConfigTableProcessor g_proxy_config_table_processor;
  return g_proxy_config_table_processor;
}

int ObProxyConfigTableProcessor::init()
{
  int ret = OB_SUCCESS;
  ObConfigHandler handler;
  handler.execute_func_ = &ObProxyConfigTableProcessor::execute;
  handler.commit_func_ = &ObProxyConfigTableProcessor::commit;
  if (OB_FAIL(get_global_config_processor().register_callback("proxy_config", handler))) {
    LOG_WARN("register proxy config table callback failed", K(ret));
  }

   return ret;
}

void ObProxyConfigTableProcessor::clean_hashmap(ProxyConfigHashMap &config_map)
{
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  clean_hashmap_with_lock(config_map);
}

void ObProxyConfigTableProcessor::clean_hashmap_with_lock(ProxyConfigHashMap &config_map)
{
  ProxyConfigHashMap::iterator last = config_map.end();
  ProxyConfigHashMap::iterator tmp;
  for (ProxyConfigHashMap::iterator it = config_map.begin(); it != last;) {
    tmp = it;
    ++it;
    tmp->destroy();
  }
  config_map.reset();
}

int ObProxyConfigTableProcessor::backup_hashmap_with_lock()
{
  int ret = OB_SUCCESS;
  int64_t backup_index = (index_ + 1) % 2;
  ProxyConfigHashMap &backup_map = proxy_config_map_array_[backup_index];
  ProxyConfigHashMap &current_map = proxy_config_map_array_[index_];
  clean_hashmap_with_lock(backup_map);
  ProxyConfigHashMap::iterator last = current_map.end();
  for (ProxyConfigHashMap::iterator it = current_map.begin(); OB_SUCC(ret) && it != last; ++it) {
    ObProxyConfigItem *item = static_cast<ObProxyConfigItem*>(&(*it))->clone();
    if (NULL == item) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("clone ObProxyConfigItem failed", K(ret));
    } else if (OB_FAIL(backup_map.unique_set(item))) {
      LOG_WARN("backup map set refactored failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    clean_hashmap_with_lock(backup_map);
  }

  return ret;
}

int ObProxyConfigTableProcessor::set_proxy_config(void *arg)
{
  int ret = OB_SUCCESS;
  ObProxyConfigItem *item = NULL;
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(backup_hashmap_with_lock())) {
    LOG_WARN("backup hashmap failed", K(ret));
  } else if (OB_ISNULL(item = op_alloc(ObProxyConfigItem))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op alloc ObProxyConfigItem failed", K(ret));
  } else {
    ObString vip;
    int64_t vport = 0;
    int64_t vid = -1;
    ObString name;
    SqlFieldResult *sql_fields = static_cast<SqlFieldResult*>(arg);
    for (int i = 0; OB_SUCC(ret) && i < sql_fields->field_num_; i++) {
      SqlField &sql_field = sql_fields->fields_.at(i);
      if (0 == sql_field.column_name_.string_.case_compare("vip")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          vip = sql_field.column_value_.config_string_;
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("vport")) {
        if (TOKEN_STR_VAL == sql_field.value_type_) {
          vport = atoi(sql_field.column_value_.config_string_.ptr());
        } else if (TOKEN_INT_VAL == sql_field.value_type_) {
          vport = sql_field.column_int_value_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid value type", K(sql_field.value_type_), K(ret));
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("vid")) {
        if (TOKEN_STR_VAL == sql_field.value_type_) {
          vid = atoi(sql_field.column_value_.config_string_.ptr());
        } else if (TOKEN_INT_VAL == sql_field.value_type_) {
          vid = sql_field.column_int_value_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid value type", K(sql_field.value_type_), K(ret));
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("config_level")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->config_level_.assign(sql_field.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("tenant_name")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->tenant_name_.assign(sql_field.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("cluster_name")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->cluster_name_.assign(sql_field.column_value_.config_string_.ptr());
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("name")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          ObString &name = sql_field.column_value_.config_string_;
          item->config_item_.set_name(name.ptr());
        }
      } else if (0 == sql_field.column_name_.string_.case_compare("value")) {
        if (TOKEN_STR_VAL != sql_field.value_type_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
        } else {
          item->config_item_.set_value(sql_field.column_value_.config_string_);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column name", K(sql_field.column_name_.string_));
      }
    }

    if (OB_SUCC(ret)) {
      item->vip_addr_.set(ObAddr::convert_ipv4_addr(vip.ptr()), static_cast<int32_t>(vport), vid);
      if (0 != strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr())
          && !is_config_in_service(item->config_item_.name())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("multi level config not supported", K(item), K(ret));
      } else {
        ProxyConfigHashMap &backup_map = proxy_config_map_array_[(index_ + 1) % 2];
        // Unique_set needs to be guaranteed to be the last step,
        // and the previous failure needs to release the item memory
        ObProxyConfigItem* tmp_item = backup_map.remove(*item);
        if (NULL != tmp_item) {
          tmp_item->destroy();
          tmp_item = NULL;
        }

        if (need_sync_to_file_ && 0 == strcasecmp("LEVEL_GLOBAL", item->config_level_.ptr()) &&
            OB_FAIL(alter_proxy_config(item->config_item_.name(), item->config_item_.str()))) {
          LOG_WARN("alter proxyconfig failed", K(ret));
        } else if (OB_FAIL(backup_map.unique_set(item))) {
          LOG_WARN("backup map unique_set failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (NULL != item) {
        item->destroy();
        item = NULL;
      }
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::delete_proxy_config(void *arg)
{
  // todo: Intercept and delete global configuration items
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(proxy_config_lock_);
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(backup_hashmap_with_lock())) {
    LOG_WARN("backup hashmap failed", K(ret));
  } else {
    SqlFieldResult *fields = static_cast<SqlFieldResult*>(arg);
    ProxyConfigHashMap &backup_map = proxy_config_map_array_[(index_ + 1) % 2];
    ProxyConfigHashMap::iterator last = backup_map.end();
    for (ProxyConfigHashMap::iterator it = backup_map.begin(); OB_SUCC(ret) && it != last;) {
      ObProxyConfigItem &item = *it;
      bool need_delete = true;
      ++it;
      for (int i = 0; OB_SUCC(ret) && need_delete && i < fields->field_num_; i++) {
        SqlField &sql_field = fields->fields_.at(i);
        if (0 == sql_field.column_name_.string_.case_compare("vip")) {
          char ip_buf[256];
          item.vip_addr_.addr_.ip_to_string(ip_buf, 256);
          if (TOKEN_STR_VAL != sql_field.value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
          } else if (0 != strcasecmp(ip_buf, sql_field.column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("vport")) {
          int64_t vport = 0;
          if (TOKEN_STR_VAL == sql_field.value_type_) {
            vport = atoi(sql_field.column_value_.config_string_.ptr());
          } else if (TOKEN_INT_VAL == sql_field.value_type_) {
            vport = sql_field.column_int_value_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid value type", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (vport != item.vip_addr_.addr_.port_) {
              need_delete = false;
            }
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("vid")) {
          int64_t vid = 0;
          if (TOKEN_STR_VAL == sql_field.value_type_) {
            vid = atoi(sql_field.column_value_.config_string_.ptr());
          } else if (TOKEN_INT_VAL == sql_field.value_type_) {
            vid = sql_field.column_int_value_;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid value type", K(ret));
          }
          if (OB_SUCC(ret)) {
            if (vid != item.vip_addr_.vid_) {
              need_delete = false;
            }
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("config_level")) {
          if (TOKEN_STR_VAL != sql_field.value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_level_.ptr(), sql_field.column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("tenant_name")) {
          if (TOKEN_STR_VAL != sql_field.value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
          } else if (0 != strcasecmp(item.tenant_name_.ptr(), sql_field.column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("cluster_name")) {
          if (TOKEN_STR_VAL != sql_field.value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
          } else if (0 != strcasecmp(item.cluster_name_.ptr(), sql_field.column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("name")) {
          if (TOKEN_STR_VAL != sql_field.value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_item_.name(), sql_field.column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else if (0 == sql_field.column_name_.string_.case_compare("value")) {
          if (TOKEN_STR_VAL != sql_field.value_type_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpeted type", K(sql_field.value_type_), K(ret));
          } else if (0 != strcasecmp(item.config_item_.str(), sql_field.column_value_.config_string_.ptr())) {
            need_delete = false;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected column name", K(sql_field.column_name_.string_));
        }
      }

      if (OB_SUCC(ret) && need_delete) {
        if (item.config_level_ == "LEVEL_GLOBAL") {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("delete global config unsupported", K(ret));
        } else {
          backup_map.remove(&item);
          item.destroy();
        }
      }
    }
  }

  return ret;
}

int ObProxyConfigTableProcessor::get_config_item(const obutils::ObVipAddr &addr,
                                                 const ObString &cluster_name,
                                                 const ObString &tenant_name,
                                                 const common::ObString &name,
                                                 ObProxyConfigItem &item)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(proxy_config_lock_);
  ObProxyConfigItem *proxy_config_item = NULL;
  ProxyConfigHashMap &current_map = proxy_config_map_array_[index_];

  ObProxyConfigItem key;
  key.vip_addr_ = addr;
  key.config_item_.set_name(name.ptr());
  if (OB_FAIL(key.cluster_name_.assign(cluster_name))) {
    LOG_WARN("assign cluster_name failed", K(ret));
  } else if (OB_FAIL(key.tenant_name_.assign(tenant_name))) {
    LOG_WARN("assign tenant name failed", K(ret));
  } else if (OB_FAIL(current_map.get_refactored(key, proxy_config_item))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get config item failed", K(addr), K(cluster_name), K(tenant_name), K(ret));
    }
  } else if (OB_ISNULL(proxy_config_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy config item is null unexpected", K(ret));
  } else {
    item = *proxy_config_item;
    LOG_DEBUG("get config item succ", K(addr), K(cluster_name), K(tenant_name), K(item));
  }

  return ret;
}

int ObProxyConfigTableProcessor::alter_proxy_config(const common::ObString &key_string,
                                                    const common::ObString &value_string)
{
  int ret = OB_SUCCESS;
  char *old_value = NULL;
  bool has_update_config = false;
  bool has_dump_config = false;
  ObProxyReloadConfig *reload_config = NULL;
  ObString tmp_value_string = value_string;
  if (OB_UNLIKELY(key_string.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(key_string), K(value_string), K(ret));
  } else {
    char value_str[common::OB_MAX_CONFIG_VALUE_LEN + 1];
    if ((0 == key_string.case_compare("observer_sys_password")
      || 0 == key_string.case_compare("obproxy_sys_password")
      || 0 == key_string.case_compare("observer_sys_password1"))
      && !value_string.empty()) {
      char passwd_staged1_buf[ENC_STRING_BUF_LEN];
      ObString passwd_string(ENC_STRING_BUF_LEN, passwd_staged1_buf);
      if (OB_FAIL(ObEncryptedHelper::encrypt_passwd_to_stage1(value_string, passwd_string))) {
        LOG_WARN("encrypt_passwd_to_stage1 failed", K(ret));
      } else {
        MEMCPY(value_str, passwd_staged1_buf + 1, 40);
        value_str[40] = '\0';
        tmp_value_string.assign(value_str, 40);
        LOG_DEBUG("alter password", K(key_string), K(value_string));
      }
    }
    if (OB_ISNULL(reload_config = get_global_internal_cmd_processor().get_reload_config())) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fail to get reload config", K(ret));
    } else if (OB_ISNULL(old_value = static_cast<char *>(op_fixed_mem_alloc(OB_MAX_CONFIG_VALUE_LEN)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for old_value", "size", OB_MAX_CONFIG_VALUE_LEN, K(ret));
    } else if (OB_FAIL(get_global_proxy_config().get_old_config_value(key_string, old_value, OB_MAX_CONFIG_VALUE_LEN))) {
      LOG_WARN("fail to get old config value", K(key_string), K(ret));
    } else if (OB_FAIL(get_global_proxy_config().update_config_item(key_string, tmp_value_string))) {
      LOG_WARN("fail to update config", K(key_string), K(tmp_value_string), K(ret));
    } else {
      has_update_config = true;
      LOG_DEBUG("succ to update config", K(key_string), K(value_string), K(old_value));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_global_proxy_config().check_proxy_serviceable())) {
        LOG_WARN("fail to check proxy serviceable", K(ret));
      } else if (OB_FAIL(get_global_proxy_config().dump_config_to_local())) {
        LOG_WARN("fail to dump_config_to_local", K(ret));
      } else {
        has_dump_config = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL((*reload_config)(get_global_proxy_config()))) {
        WARN_ICMD("fail to reload config, but config has already dumped!!", K(ret));
      } else {
        DEBUG_ICMD("succ to update config", K(key_string), K(value_string));
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (has_update_config) {
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_proxy_config().update_config_item(
            key_string, ObString::make_string(old_value))))) {
          LOG_WARN("fail to back to old config", K(key_string), K(old_value), K(tmp_ret));
        } else {
          LOG_DEBUG("succ to back to old config", K(key_string), K(old_value));
        }
      }
      if (has_dump_config && OB_LIKELY(OB_SUCCESS == tmp_ret)) {
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_global_proxy_config().dump_config_to_local()))) {
          LOG_WARN("fail to dump old config", K(tmp_ret));
        } else {
          LOG_DEBUG("succ to dump old config");
        }
      }
    }
  }

  return ret;
}

bool ObProxyConfigTableProcessor::is_config_in_service(const ObString &config_name)
{
  bool is_in_service = false;
  for(int64_t i = 0; i < (sizeof(config_name_array) / sizeof(const char*)); i++) {
    if (config_name == config_name_array[i]) {
      is_in_service = true;
      break;
    }
  }

  return is_in_service;
}

} // end of omt
} // end of obprxy
} // end of oceanbase
