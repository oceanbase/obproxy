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

#include "ob_conn_table_processor.h"
#include "obutils/ob_proxy_json_config_info.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

static const char* JSON_OBPROXY_VIP       = "vip";
static const char* JSON_OBPROXY_VALUE     = "value";
static const uint32_t column_num          = 4;

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::json;

extern int build_tenant_cluster_vip_name(const ObString &tenant_name, const ObString &cluster_name, const ObString &vip_name,
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH> &key_string);

int ObConnTableProcessor::commit(bool is_success)
{
  int ret = OB_SUCCESS;
  if (!is_success) {
    ret = conn_rollback();
  }
  conn_backup_status_ = false;
  return ret;
}

void ObConnTableProcessor::destroy()
{
  DRWLock::WRLockGuard guard(rwlock_);
  vt_conn_cache_.destroy();
}

bool ObConnTableProcessor::check_and_inc_conn(
    ObString& cluster_name, ObString& tenant_name, ObString& ip_name)
{
  int ret = OB_SUCCESS;
  bool throttle = false;
  ObVipTenantConn* vt_conn = NULL;
  int64_t cur_used_connections = 0;

  if (OB_FAIL(inc_conn(cluster_name, tenant_name, ip_name, cur_used_connections))) {
    throttle = true;
    LOG_WDIAG("fail to get or create used conn", K(cluster_name), K(tenant_name), K(ip_name), K(ret));
  }

  if (OB_SUCC(ret)) {
    DRWLock::RDLockGuard guard(rwlock_);
    if (OB_FAIL(get_vt_conn_object(cluster_name, tenant_name, ip_name, vt_conn))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Scheme review comments: Connections without configuration information are allowed to access
        LOG_DEBUG("get vip tenant connect failed", K(ret));
      } else {
        // Other errors, access denied
        dec_conn(cluster_name, tenant_name, ip_name);
        throttle = true;
      }
    } else {
      if (cur_used_connections <= vt_conn->max_connections_) {
        LOG_DEBUG("vip tenant connect info", K(cur_used_connections), KPC(vt_conn));
      } else {
        LOG_WDIAG("used connections reach throttle", K(cur_used_connections), K(vt_conn->max_connections_), KPC(vt_conn));
        dec_conn(cluster_name, tenant_name, ip_name);
        throttle = true;
      }
    }
  }

  return throttle;
}

int ObConnTableProcessor::inc_conn(ObString& cluster_name, ObString& tenant_name, ObString& ip_name,
    int64_t& cur_used_connections)
{
  int ret = OB_SUCCESS;
  ObString key_name;
  ObUsedConn* used_conn = NULL;
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH> key_string;
  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, ip_name, key_string))) {
    LOG_WDIAG("build tenant cluser vip name failed", K(tenant_name), K(cluster_name), K(ip_name), K(ret));
  } else {
    key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(get_or_create_used_conn(key_name, used_conn, cur_used_connections))) {
      LOG_WDIAG("create used conn failed", K(key_name), K(ret));
    } else {
      used_conn->dec_ref();
    }
  }
  return ret;
}

void ObConnTableProcessor::dec_conn(
    ObString& cluster_name, ObString& tenant_name, ObString& ip_name)
{
  int ret = OB_SUCCESS;
  ObUsedConn* used_conn = NULL;
  int64_t cur_used_connections = 0;
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH> key_string;

  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, ip_name, key_string))) {
    LOG_WDIAG("build tenant cluster vip name failed", K(ret), K(tenant_name), K(cluster_name), K(ip_name));
  } else {
    ObString key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(get_used_conn(key_name, false, used_conn, cur_used_connections))) {
      LOG_WDIAG("fail to get used conn in map", K(key_name), K(ret));
    } else {
      if (OB_NOT_NULL(used_conn)) {
        if (ATOMIC_FAA(&used_conn->max_used_connections_, -1) > 1) {
          LOG_DEBUG("dec conn", KPC(used_conn));
        } else {
          DRWLock::WRLockGuard guard(used_conn_rwlock_);
          if (0 == used_conn->max_used_connections_ && used_conn->is_in_map_) {
            erase_used_conn(key_name, used_conn);
            LOG_DEBUG("erase used conn", K(key_name));
          }
        }
        used_conn->dec_ref();
      }
    }
  }
}

int ObConnTableProcessor::get_vt_conn_object(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name, ObVipTenantConn*& vt_conn)
{
  int ret = OB_SUCCESS;

  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::MAX_IP_ADDR_LENGTH> key_string;
  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, vip_name, key_string))) {
    LOG_WDIAG("build tenant cluser vip name failed", K(ret), K(tenant_name), K(cluster_name), K(vip_name));
  } else {
    ObString key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(vt_conn_cache_.get(key_name, vt_conn))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("vip tenant connect not in cache", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
      } else {
        LOG_WDIAG("fail to get vip tenant connect in cache", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
      }
    } else {
      LOG_DEBUG("succ to get vip tenant connect in cache", K(cluster_name), K(tenant_name), K(vip_name), KPC(vt_conn));
    }
  }

  return ret;
}

int ObConnTableProcessor::alloc_and_init_vt_conn(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name,
    uint32_t max_connections, ObVipTenantConn*& vt_conn)
{
  int ret = OB_SUCCESS;
  ObVipTenantConn* tmp_vt_conn = NULL;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("name is empty", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
  } else if (OB_ISNULL(tmp_vt_conn = op_alloc(ObVipTenantConn))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObVipTenantConn", K(ret));
  } else if (OB_FAIL(tmp_vt_conn->set(cluster_name, tenant_name, vip_name, max_connections))) {
    LOG_WDIAG("fail to set vip tenant connect info", K(ret));
  } else {
    vt_conn = tmp_vt_conn;
    LOG_DEBUG("succ to set vip tenant connect object", KPC(vt_conn));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_vt_conn)) {
    ob_free(tmp_vt_conn);
    tmp_vt_conn = NULL;
  }

  return ret;
}

// local vip json format:
//[
//  {
//    "vip" : "127.0.0.1",
//    "value" : "1000"
//  },
//  {
//    "vip" : "0.0.0.1",
//    "value" : "2000"
//  }
//]
int ObConnTableProcessor::fill_local_vt_conn_cache(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name)
{
  int ret = OB_SUCCESS;
  Parser parser;
  json::Value *info_config = NULL;
  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);

  int max_connections = 0;
  ObString vip_list_str;
  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty()) || OB_UNLIKELY(vip_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("name is empty", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
  } else if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WDIAG("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(vip_name.ptr(), vip_name.length(), info_config))) {
    LOG_WDIAG("parse json failed", K(ret), "vip_json_str", get_print_json(vip_name));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(info_config, json::JT_ARRAY))) {
    LOG_WDIAG("check config info type failed", K(ret));
  } else {
    DLIST_FOREACH(it, info_config->get_array()) {
      if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it, json::JT_OBJECT))) {
        LOG_WDIAG("check config info type failed", K(ret));
      } else {
        DLIST_FOREACH(p, it->get_object()) {
          if (p->name_ == JSON_OBPROXY_VIP) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
              LOG_WDIAG("check config info type failed", K(ret));
            } else {
              vip_list_str = p->value_->get_string();
            }
          } else if (p->name_ == JSON_OBPROXY_VALUE) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_NUMBER))) {
              LOG_WDIAG("check config info type failed", K(ret));
            } else {
              max_connections = (uint32_t)p->value_->get_number();
            }
          }
        }
      }

      ObVipTenantConn* vt_conn = NULL;
      if (OB_SUCC(ret)) {
        DRWLock::WRLockGuard guard(rwlock_);
        if (OB_FAIL(get_vt_conn_object(cluster_name, tenant_name, vip_list_str, vt_conn))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            if (OB_FAIL(alloc_and_init_vt_conn(cluster_name, tenant_name, vip_list_str, max_connections, vt_conn))) {
              LOG_WDIAG("fail to alloc and init vip tenant connect", K(ret));
            } else if (OB_FAIL(vt_conn_cache_.set(vt_conn))) {
              LOG_WDIAG("fail to insert one vip tenant conn into conn_map", K(vt_conn), K(ret));
              if (OB_LIKELY(NULL != vt_conn)) {
                vt_conn->destroy();
                vt_conn = NULL;
              }
            } else {
              LOG_DEBUG("succ to insert one vip tenant connect into conn_map", K(*vt_conn));
            }
          }
        } else {
          vt_conn->max_connections_ = max_connections;
          LOG_DEBUG("update vip tenant connect in cache", K(vt_conn));
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_LIKELY(NULL != info_config)) {
          info_config = NULL;
        }
      }
    }
  }

  return ret;
}

int ObConnTableProcessor::backup_local_vt_conn_cache()
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_FAIL(vt_conn_cache_.backup())) {
    LOG_WDIAG("backup connect cache failed");
  } else {
    conn_backup_status_ = true;
  }

  return ret;
}

int ObConnTableProcessor::conn_handle_replace_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str, ObString& value_str)
{
  UNUSED(name_str);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("tenant or cluster is null", K(ret), K(cluster_name), K(tenant_name));
  } else if (OB_FAIL(backup_local_vt_conn_cache())) {
    LOG_WDIAG("backup vip tenant connect cache failed", K(ret));
  } else if (OB_FAIL(fill_local_vt_conn_cache(cluster_name, tenant_name, value_str))) {
    LOG_WDIAG("update vip tenant connect cache failed", K(ret));
  } else {
    LOG_DEBUG("update vip tenant connect cache succed", "count", get_conn_map_count());
  }

  return ret;
}

int ObConnTableProcessor::conn_handle_delete_config(ObString& cluster_name, ObString& tenant_name)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("tenant_name or cluster_name is null", K(ret), K(cluster_name), K(tenant_name));
  } else {
    if (OB_FAIL(backup_local_vt_conn_cache())) {
      LOG_WDIAG("backup vip tenant connect cache failed", K(ret));
    } else {
      DRWLock::WRLockGuard guard(rwlock_);
      vt_conn_cache_.erase(cluster_name, tenant_name);
    }
  }

  return ret;
}

int ObConnTableProcessor::conn_rollback()
{
  int ret = OB_SUCCESS;
  LOG_INFO("conn rollback");
  DRWLock::WRLockGuard guard(rwlock_);
  // The backup can be rolled back only if the backup is successful
  if (OB_LIKELY(conn_backup_status_)) {
    if (OB_FAIL(vt_conn_cache_.recover())) {
      LOG_WDIAG("recover connect cache failed");
    }
  }

  return ret;
}

int ObConnTableProcessor::create_used_conn(ObString& key_name,
    ObUsedConn*& used_conn, int64_t& cur_used_connections)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(used_conn_rwlock_);
  ObUsedConn* tmp_used_conn = NULL;
  if (OB_FAIL(used_conn_cache_.get(key_name, tmp_used_conn))) {
    if (OB_HASH_NOT_EXIST == ret) {
      char *buf = NULL;
      int64_t alloc_size = sizeof(ObUsedConn);
      if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc memory for used conn", K(alloc_size), K(ret));
      } else {
        used_conn = new (buf) ObUsedConn(key_name);
        used_conn->inc_ref();
        if (OB_FAIL(used_conn_cache_.set(used_conn))) {
          LOG_WDIAG("fail to set used conn map", K(key_name), KPC(used_conn), K(ret));
        } else {
          used_conn->is_in_map_ = true;
        }
      }

      if (OB_SUCC(ret)) {
        used_conn->inc_ref();
        cur_used_connections  = ATOMIC_AAF(&used_conn->max_used_connections_, 1);
      } else if (OB_NOT_NULL(used_conn)) {
        used_conn->dec_ref();
        used_conn = NULL;
      }
    }
  } else {
    used_conn = tmp_used_conn;
    used_conn->inc_ref();
    cur_used_connections  = ATOMIC_AAF(&used_conn->max_used_connections_, 1);
  }

  return ret;
}

int ObConnTableProcessor::get_used_conn(ObString& key_name, bool is_need_inc_used_connections,
    ObUsedConn*& used_conn, int64_t& cur_used_connections)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(used_conn_rwlock_);
  ObUsedConn* tmp_used_conn = NULL;
  if (OB_FAIL(used_conn_cache_.get(key_name, tmp_used_conn))) {
  } else {
    used_conn = tmp_used_conn;
    used_conn->inc_ref();
    if (is_need_inc_used_connections) {
      cur_used_connections  = ATOMIC_AAF(&used_conn->max_used_connections_, 1);
    }
    LOG_DEBUG("succ to get used conn from map", K(key_name), KPC(used_conn), K(cur_used_connections));
  }

  return ret;
}

int ObConnTableProcessor::get_or_create_used_conn(ObString& key_name,
    ObUsedConn*& used_conn, int64_t& cur_used_connections)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_used_conn(key_name, true, used_conn, cur_used_connections))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_used_conn(key_name, used_conn, cur_used_connections))) {
        LOG_WDIAG("fail to create used conn", K(key_name), K(ret));
      } else {
        LOG_DEBUG("succ to create used conn", K(key_name), K(cur_used_connections), KPC(used_conn));
      }
    } else {
      LOG_WDIAG("fail to get used conn", K(key_name), K(ret));
    }
  }
  return ret;
}

int ObConnTableProcessor::erase_used_conn(ObString& key_name, ObUsedConn* used_conn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(used_conn_cache_.erase(key_name))) {
    LOG_WDIAG("erase used conn failed", K(key_name));
  } else {
    used_conn->is_in_map_ = false;
    used_conn->dec_ref();
  }

  return ret;
}

ObConnTableProcessor &get_global_conn_table_processor()
{
  static ObConnTableProcessor conn_table_processor;
  return conn_table_processor;
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
