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

#include <sqlite/sqlite3.h>
#include "omt/ob_ssl_config_table_processor.h"
#include "obutils/ob_config_processor.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "utils/ob_proxy_utils.h"
#include "iocore/net/ob_ssl_processor.h"
#include "obutils/ob_proxy_config.h"

using namespace obsys;
using namespace oceanbase::json;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::net;

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

int ObSSLConfigTableProcessor::execute(void *arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("arg is null unexpected", K(ret));
  } else {
    ObCloudFnParams *params = reinterpret_cast<ObCloudFnParams*>(arg);
    ObString tenant_name = params->tenant_name_;
    ObString cluster_name = params->cluster_name_;
    ObString name;
    ObString value;
    if (NULL == params->fields_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fields is null unexpected", K(ret));
    } else if (cluster_name.empty() || cluster_name.length() > OB_PROXY_MAX_CLUSTER_NAME_LENGTH
              || tenant_name.empty() || tenant_name.length() > OB_MAX_TENANT_NAME_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("execute failed, tenant_name or cluster_name is null", K(ret), K(cluster_name), K(tenant_name));
    } else {
      for (int64_t i = 0; i < params->fields_->field_num_; i++) {
        SqlField *sql_field = params->fields_->fields_.at(i);
        if (0 == sql_field->column_name_.config_string_.case_compare("value")) {
          value = sql_field->column_value_.config_string_;
        } else if (0 == sql_field->column_name_.config_string_.case_compare("name")) {
          name = sql_field->column_value_.config_string_;
        }
      }

      if (params->stmt_type_ == OBPROXY_T_REPLACE) {
        if (OB_FAIL(get_global_ssl_config_table_processor().set_ssl_config(cluster_name, tenant_name, name, value))) {
          LOG_WDIAG("set ssl config failed", K(ret), K(cluster_name), K(tenant_name), K(name), K(value));
        }
      } else if (params->stmt_type_ == OBPROXY_T_DELETE) {
        if (!name.empty() || !value.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("delete failed, name or value is not null", K(ret), K(name), K(value));
        } else if (OB_FAIL(get_global_ssl_config_table_processor().delete_ssl_config(cluster_name, tenant_name))) {
          LOG_WDIAG("delete ssl config failed", K(ret), K(cluster_name), K(tenant_name));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unexpected stmt type", K(ret), K(params->stmt_type_));
      }
    }
  }

  return ret;
}

int ObSSLConfigTableProcessor::commit(void* arg, bool is_success)
{
  UNUSED(arg);
  if (is_success) {
    get_global_ssl_config_table_processor().inc_index();
    get_global_ssl_config_table_processor().handle_delete();
    if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
      get_global_ssl_config_table_processor().print_config();
    }
  } else {
    LOG_WDIAG("ssl config commit failed");
  }

  return OB_SUCCESS;
}

ObSSLConfigTableProcessor &get_global_ssl_config_table_processor()
{
  static ObSSLConfigTableProcessor g_ssl_config_table_processor;
  return g_ssl_config_table_processor;
}

int ObSSLConfigTableProcessor::init()
{
  int ret = OB_SUCCESS;
  ObConfigHandler handler;
  handler.execute_func_ = &ObSSLConfigTableProcessor::execute;
  handler.commit_func_ = &ObSSLConfigTableProcessor::commit;
  if (OB_FAIL(ssl_config_map_array_[0].create(32, ObModIds::OB_PROXY_SSL_RELATED))) {
    LOG_WDIAG("create hash map failed", K(ret));
  } else if (OB_FAIL(ssl_config_map_array_[1].create(32, ObModIds::OB_PROXY_SSL_RELATED))) {
    LOG_WDIAG("create hash map failed", K(ret));
  } else if (OB_FAIL(get_global_config_processor().register_callback("ssl_config", handler))) {
    LOG_WDIAG("register ssl config table callback failed", K(ret));
  }

  return ret;
}

int ObSSLConfigTableProcessor::set_ssl_config(const ObString &cluster_name,
                                              const ObString &tenant_name,
                                              const ObString &name,
                                              const ObString &value)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(ssl_config_lock_);
  if (OB_UNLIKELY(cluster_name.empty() || tenant_name.empty() || name.empty() || value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("set ssl config failed, invalid argument", K(cluster_name), K(tenant_name), K(name), K(value), K(ret));
  } else if (OB_FAIL(backup_hash_map())) {
    LOG_WDIAG("backup hash map failed", K(ret));
  } else {
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    SSLConfigInfo ssl_config_info;
    ssl_config_info.reset();
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WDIAG("paster tenant and cluser name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      SSLConfigHashMap &backup_map = ssl_config_map_array_[(index_ + 1) % 2];
      if (OB_FAIL(backup_map.get_refactored(key_string, ssl_config_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WDIAG("map get refactored failed", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (0 == name.compare("key_info")) {
          Parser parser;
          json::Value *json_value = NULL;
          ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);
          if (OB_FAIL(parser.init(&json_allocator))) {
            LOG_WDIAG("json parser init failed", K(ret));
          } else if (OB_FAIL(parser.parse(value.ptr(), value.length(), json_value))) {
            LOG_WDIAG("json parse failed", K(ret), K(value));
          } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(json_value, json::JT_OBJECT))) {
            LOG_WDIAG("check config info type failed", K(ret));
          } else {
            ssl_config_info.is_key_info_valid_ = false;
            ObProxyVariantString source_type;
            ObProxyVariantString ca_info;
            ObProxyVariantString public_key;
            ObProxyVariantString private_key;
            DLIST_FOREACH(p, json_value->get_object()) {
              if (0 == p->name_.case_compare("sourceType")) {
                if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
                  LOG_WDIAG("check config info type failed", K(ret));
                } else {
                  source_type.set_value(p->value_->get_string());
                }
              } else if (0 == p->name_.case_compare("CA")) {
                if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
                  LOG_WDIAG("check config info type failed", K(ret));
                } else {
                  ca_info.set_value(p->value_->get_string());
                }
              } else if (0 == p->name_.case_compare("publicKey")) {
                if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
                  LOG_WDIAG("check config info type failed", K(ret));
                } else {
                  public_key.set_value(p->value_->get_string());
                }
              } else if (0 == p->name_.case_compare("privateKey")) {
                if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
                  LOG_WDIAG("check config info type failed", K(ret));
                } else {
                  private_key.set_value(p->value_->get_string());
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(g_ssl_processor.update_key(cluster_name, tenant_name, source_type,
                                                     ca_info, public_key, private_key))) {
                LOG_WDIAG("update key failed", K(cluster_name), K(tenant_name), K(source_type),
                               K(ca_info), K(public_key), K(private_key), K(ret));
              } else {
                ssl_config_info.is_key_info_valid_ = true;
              }
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid name for ssl config", K(name), K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(backup_map.set_refactored(key_string, ssl_config_info, 1))) {
        LOG_WDIAG("set refactored failed", K(ret));
      }
    }
  }

  return ret;
}

int ObSSLConfigTableProcessor::delete_ssl_config(ObString &cluster_name, ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(ssl_config_lock_);
  if (cluster_name.empty() || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("cluster name or tenant name empty unexpected", K(ret));
  } else if (OB_FAIL(backup_hash_map())) {
    LOG_WDIAG("backup hash map failed", K(ret));
  } else {
    LOG_DEBUG("delete ssl config", K(cluster_name), K(tenant_name));
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WDIAG("paste tenant and cluster name failed", K(ret), K(tenant_name), K(cluster_name));
    } else {
      SSLConfigHashMap &backup_map = ssl_config_map_array_[(index_ + 1) % 2];
      if (OB_FAIL(backup_map.erase_refactored(key_string))) {
        LOG_WDIAG("addr hash map erase failed", K(ret));
      } else {
        delete_info_ = key_string;
      }
    }
  }

  return ret;
}

void SSLConfigInfo::reset()
{
  is_key_info_valid_ = false;
}

int64_t SSLConfigInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_key_info_valid));
  J_OBJ_END();
  return pos;
}

int ObSSLConfigTableProcessor::backup_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t backup_index = (index_ + 1) % 2;
  SSLConfigHashMap &backup_map = ssl_config_map_array_[backup_index];
  SSLConfigHashMap &current_map = ssl_config_map_array_[index_];
  if (OB_FAIL(backup_map.reuse())) {
    LOG_WDIAG("backup map failed", K(ret));
  } else {
    SSLConfigHashMap::iterator iter = current_map.begin();
    for(; OB_SUCC(ret) && iter != current_map.end(); ++iter) {
      if (OB_FAIL(backup_map.set_refactored(iter->first, iter->second))) {
        LOG_WDIAG("backup map set refactored failed", K(ret));
      }
    }
  }
  return ret;
}

void ObSSLConfigTableProcessor::inc_index()
{
  DRWLock::WRLockGuard guard(ssl_config_lock_);
  index_ = (index_ + 1) % 2;
}

void ObSSLConfigTableProcessor::handle_delete()
{
  DRWLock::WRLockGuard guard(ssl_config_lock_);
  if (!delete_info_.is_empty()) {
    g_ssl_processor.release_ssl_ctx(delete_info_);
    delete_info_.reset();
  }
}

bool ObSSLConfigTableProcessor::is_ssl_key_info_valid(const common::ObString &cluster_name,
                                                      const common::ObString &tenant_name)
{
  bool bret = false;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
    get_global_ssl_config_table_processor().print_config();
  }
  SSLConfigInfo ssl_config_info;
  ssl_config_info.reset();
  ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH> key_string;
  DRWLock::RDLockGuard guard(ssl_config_lock_);
  SSLConfigHashMap &current_map = ssl_config_map_array_[index_];
  // Get tenant configuration
  if (OB_SUCC(ret)) {
    if (OB_FAIL(paste_tenant_and_cluster_name(tenant_name, cluster_name, key_string))) {
      LOG_WDIAG("paste tenant and cluster_name failed", K(ret), K(tenant_name), K(cluster_name));
    } else if (OB_FAIL(current_map.get_refactored(key_string, ssl_config_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WDIAG("ssl ctx map get refactored failed", K(ret), K(cluster_name), K(tenant_name));
      }
    } else {
      LOG_DEBUG("get ssl config from tenant succ", K(ssl_config_info));
    }
  }
  // Get the cluster configuration
  if (OB_HASH_NOT_EXIST == ret) {
    if (OB_FAIL(paste_tenant_and_cluster_name("*", cluster_name, key_string))) {
      LOG_WDIAG("paste tenant and cluster_name failed", K(ret), K(tenant_name), K(cluster_name));
    } else if (OB_FAIL(current_map.get_refactored(key_string, ssl_config_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WDIAG("ssl ctx map get refactored failed", K(ret), K(cluster_name), K(tenant_name));
      }
    } else {
      LOG_DEBUG("get ssl config from cluster succ", K(ssl_config_info));
    }
  }
  // Get global configuration
  if (OB_HASH_NOT_EXIST == ret) {
    if (OB_FAIL(paste_tenant_and_cluster_name("*", "*", key_string))) {
      LOG_WDIAG("paste tenant and cluster_name failed", K(ret), K(tenant_name), K(cluster_name));
    } else if (OB_FAIL(current_map.get_refactored(key_string, ssl_config_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WDIAG("ssl ctx map get refactored failed", K(ret), K(cluster_name), K(tenant_name));
      }
    } else {
      LOG_DEBUG("get ssl config from global succ", K(ssl_config_info));
    }
  }

  if (OB_HASH_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
  }

  if (OB_SUCC(ret)) {
    bret = ssl_config_info.is_key_info_valid_;
    LOG_DEBUG("get ssl config", K(cluster_name), K(tenant_name), K(bret));
  }
  return bret;
}

void ObSSLConfigTableProcessor::print_config()
{
  SSLConfigHashMap &current_map = ssl_config_map_array_[index_];
  SSLConfigHashMap::iterator iter = current_map.begin();
  for(; iter != current_map.end(); ++iter) {
    LOG_DEBUG("ssl config map info", K(iter->first), K(iter->second));
  }
}

} // end of omt
} // end of obproxy
} // end of oceanbase
