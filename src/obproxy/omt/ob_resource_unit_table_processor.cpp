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

#include "ob_resource_unit_table_processor.h"
#include "obutils/ob_proxy_json_config_info.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::json;

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

ObResourceUnitTableProcessor::ObResourceUnitTableProcessor()
  : is_inited_(false), backup_status_(false), rwlock_(), used_conn_rwlock_()
{
}

void ObResourceUnitTableProcessor::destroy()
{
  if (OB_LIKELY(is_inited_)) {
    is_inited_ = false;
    DRWLock::WRLockGuard guard(rwlock_);
    vt_conn_cache_.destroy();
  }
}

int ObResourceUnitTableProcessor::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    ObConfigHandler handler;
    ObString table_name = ObString::make_string("resource_unit");
    handler.execute_func_  = &execute;
    handler.commit_func_   = &commit;
    handler.config_type_   = OBPROXY_CONFIG_CLOUD;
    if (OB_FAIL(get_global_config_processor().register_callback(table_name, handler))) {
      LOG_WARN("register callback info failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::execute(void* cloud_params)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(cloud_params)) {
    LOG_WARN("params should not be null");
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObCloudFnParams* params = (ObCloudFnParams*)cloud_params;
    ObString name_str;
    ObString value_str;
    ObString cluster_name  = params->cluster_name_;
    ObString tenant_name   = params->tenant_name_;
    SqlFieldResult* fields = params->fields_;
    ObProxyBasicStmtType stmt_type = params->stmt_type_;
    if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant_name or cluster_name is null", K(ret), K(cluster_name), K(tenant_name));
    } else if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fields is null unexpected", K(ret));
    } else {
      // The storage format is:[cluster|tenant|name|value]
      for (int64_t i = 0; i < fields->field_num_; i++) {
        SqlField& sql_field = fields->fields_.at(i);
        if (0 == sql_field.column_name_.string_.case_compare("name")) {
          name_str = sql_field.column_value_.config_string_;
        } else if (0 == sql_field.column_name_.string_.case_compare("value")) {
          value_str = sql_field.column_value_.config_string_;
        }
      }

      if (OBPROXY_T_REPLACE == stmt_type) {
        if (OB_FAIL(get_global_resource_unit_table_processor().handle_replace_config(
          cluster_name, tenant_name, name_str, value_str))) {
          LOG_WARN("fail to handle replace cmd");
        }
      } else if (OBPROXY_T_DELETE == stmt_type) {
        if (OB_FAIL(get_global_resource_unit_table_processor().handle_delete_config(
          cluster_name, tenant_name, name_str))) {
          LOG_WARN("fail to handle delete cmd");
        }
      } else {
        LOG_WARN("operation is unexpected");
        ret = OB_NOT_SUPPORTED;
      }
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::commit(bool is_success)
{
  int ret = OB_SUCCESS;

  if (!is_success) {
    ret = get_global_resource_unit_table_processor().rollback();
  }
  get_global_resource_unit_table_processor().set_backup_status(false);

  return ret;
}

bool ObResourceUnitTableProcessor::check_and_inc_conn(
    ObString& cluster_name, ObString& tenant_name, ObString& ip_name)
{
  int ret = OB_SUCCESS;
  bool throttle = false;
  ObVipTenantConn* vt_conn = NULL;
  int64_t cur_used_connections = 0;

  if (OB_FAIL(inc_conn(cluster_name, tenant_name, ip_name, cur_used_connections))) {
    throttle = true;
    LOG_WARN("fail to get or create used conn", K(cluster_name), K(tenant_name), K(ip_name), K(ret));
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
        LOG_WARN("used connections reach throttle", K(cur_used_connections), K(vt_conn->max_connections_), KPC(vt_conn));
        dec_conn(cluster_name, tenant_name, ip_name);
        throttle = true;
      }
    }
  }

  return throttle;
}

int ObResourceUnitTableProcessor::inc_conn(ObString& cluster_name, ObString& tenant_name, ObString& ip_name,
    int64_t& cur_used_connections)
{
  int ret = OB_SUCCESS;
  ObString key_name;
  ObUsedConn* used_conn = NULL;
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::OB_IP_STR_BUFF> key_string;
  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, ip_name, key_string))) {
    LOG_WARN("build tenant cluser vip name failed", K(tenant_name), K(cluster_name), K(ip_name), K(ret));
  } else {
    key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(get_or_create_used_conn(key_name, used_conn, cur_used_connections))) {
      LOG_WARN("create used conn failed", K(key_name), K(ret));
    } else {
      used_conn->dec_ref();
    }
  }
  return ret;
}

void ObResourceUnitTableProcessor::dec_conn(
    ObString& cluster_name, ObString& tenant_name, ObString& ip_name)
{
  int ret = OB_SUCCESS;
  ObUsedConn* used_conn = NULL;
  int64_t cur_used_connections = 0;
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::OB_IP_STR_BUFF> key_string;

  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, ip_name, key_string))) {
    LOG_WARN("build tenant cluster vip name failed", K(ret), K(tenant_name), K(cluster_name), K(ip_name));
  } else {
    ObString key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(get_used_conn(key_name, false, used_conn, cur_used_connections))) {
      LOG_WARN("fail to get used conn in map", K(key_name), K(ret));
    } else {
      if (OB_NOT_NULL(used_conn)) {
        if (ATOMIC_FAA(&used_conn->max_used_connections_, -1) > 1) {
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

int ObResourceUnitTableProcessor::get_vt_conn_object(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name, ObVipTenantConn*& vt_conn)
{
  int ret = OB_SUCCESS;

  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::OB_IP_STR_BUFF> key_string;
  if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, vip_name, key_string))) {
    LOG_WARN("build tenant cluser vip name failed", K(ret), K(tenant_name), K(cluster_name), K(vip_name));
  } else {
    ObString key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(vt_conn_cache_.get(key_name, vt_conn))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        LOG_DEBUG("vip tenant connect not in cache", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
      } else {
        LOG_WARN("fail to get vip tenant connect in cache", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
      }
    } else {
      LOG_DEBUG("succ to get vip tenant connect in cache", K(cluster_name), K(tenant_name), K(vip_name), KPC(vt_conn));
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::build_tenant_cluster_vip_name(const ObString &tenant_name, const ObString &cluster_name,
    const ObString &vip_name, ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF> &key_string)
{
  int ret = OB_SUCCESS;
  char buf[OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF];
  int64_t len = 0;
  len = static_cast<int64_t>(snprintf(buf, OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF, "%.*s#%.*s|%.*s",
    tenant_name.length(), tenant_name.ptr(),
    cluster_name.length(), cluster_name.ptr(),
    vip_name.length(), vip_name.ptr()));
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill buf", K(ret), K(tenant_name), K(cluster_name), K(vip_name));
  } else if (OB_FAIL(key_string.assign(buf))) {
    LOG_WARN("assign failed", K(ret));
  }

  return ret;
}

int ObResourceUnitTableProcessor::alloc_and_init_vt_conn(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name,
    uint32_t max_connections, ObVipTenantConn*& vt_conn)
{
  int ret = OB_SUCCESS;
  ObVipTenantConn* tmp_vt_conn = NULL;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name is empty", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
  } else if (OB_ISNULL(tmp_vt_conn = op_alloc(ObVipTenantConn))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObVipTenantConn", K(ret));
  } else if (OB_FAIL(tmp_vt_conn->set(cluster_name, tenant_name, vip_name, max_connections))) {    
    LOG_WARN("fail to set vip tenant connect info", K(ret));
  } else {
    vt_conn = tmp_vt_conn;
    LOG_DEBUG("succ to set vip tenant connect object", K(*vt_conn));
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
int ObResourceUnitTableProcessor::fill_local_vt_conn_cache(
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
    LOG_WARN("name is empty", K(cluster_name), K(tenant_name), K(vip_name), K(ret));
  } else if (OB_FAIL(parser.init(&json_allocator))) {
    LOG_WARN("json parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(vip_name.ptr(), vip_name.length(), info_config))) {
    LOG_WARN("parse json failed", K(ret), "vip_json_str", get_print_json(vip_name));
  } else if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(info_config, json::JT_ARRAY))) {
    LOG_WARN("check config info type failed", K(ret));
  } else {
    DLIST_FOREACH(it, info_config->get_array()) {
      if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(it, json::JT_OBJECT))) {
        LOG_WARN("check config info type failed", K(ret));
      } else {
        DLIST_FOREACH(p, it->get_object()) {
          if (p->name_ == JSON_OBPROXY_VIP) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
              LOG_WARN("check config info type failed", K(ret));
            } else {
              vip_list_str = p->value_->get_string();
            }
          } else if (p->name_ == JSON_OBPROXY_VALUE) {
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_NUMBER))) {
              LOG_WARN("check config info type failed", K(ret));
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
              LOG_WARN("fail to alloc and init vip tenant connect", K(ret));
            } else if (OB_FAIL(vt_conn_cache_.set(vt_conn))) {
              LOG_WARN("fail to insert one vip tenant conn into conn_map", K(vt_conn), K(ret));
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

int ObResourceUnitTableProcessor::backup_local_vt_conn_cache()
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard guard(rwlock_);
  if (OB_FAIL(vt_conn_cache_.backup())) {
    LOG_WARN("backup connect cache failed");
  } else {
    backup_status_ = true;
  }

  return ret;
}

int ObResourceUnitTableProcessor::handle_replace_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str, ObString& value_str)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(name_str.empty()) || OB_UNLIKELY(value_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name or value is null", K(ret), K(name_str), K(value_str));
  } else {
    if (0 == name_str.case_compare("resource_max_connections")) {
      if (OB_FAIL(conn_handle_replace_config(cluster_name, tenant_name, name_str, value_str))) {
        LOG_WARN("fail to replace vip tenant connection");
      }
    } else if (0 == name_str.case_compare("resource_memory")) {
      // TODO:qianyuan.rqy
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("now not support", K(name_str), K(ret));
    } else if (0 == name_str.case_compare("resource_cpu")) {
      // TODO:qianyuan.rqy
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("now not support", K(name_str), K(ret));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is unexpected", K(name_str));
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::handle_delete_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str) 
{
  int ret = OB_SUCCESS;

  if (name_str.empty()) {
    // Remove all resource isolation configurations
    if (OB_FAIL(conn_handle_delete_config(cluster_name, tenant_name))) {
      LOG_WARN("fail to execute vip tenant connection");
    }
    // TODO:The configuration information of other features needs to be deleted later
  } else if (0 == name_str.case_compare("resource_max_connections")) {
    if (OB_FAIL(conn_handle_delete_config(cluster_name, tenant_name))) {
      LOG_WARN("fail to execute vip tenant connection");
    }
  } else if (0 == name_str.case_compare("resource_memory")) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("now not support", K(name_str), K(ret));
  } else if (0 == name_str.case_compare("resource_cpu")) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("now not support", K(name_str), K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is unexpected", K(name_str));
  }

  return ret;
}

int ObResourceUnitTableProcessor::conn_handle_replace_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str, ObString& value_str)
{
  UNUSED(name_str);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant or cluster is null", K(ret), K(cluster_name), K(tenant_name));
  } else if (OB_FAIL(backup_local_vt_conn_cache())) {
    LOG_WARN("backup vip tenant connect cache failed", K(ret));
  } else if (OB_FAIL(fill_local_vt_conn_cache(cluster_name, tenant_name, value_str))) {
    LOG_WARN("update vip tenant connect cache failed", K(ret));
  } else {
    LOG_DEBUG("update vip tenant connect cache succed", "count", get_conn_map_count());
  }

  return ret;
}

int ObResourceUnitTableProcessor::conn_handle_delete_config(ObString& cluster_name, ObString& tenant_name) 
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_name or cluster_name is null", K(ret), K(cluster_name), K(tenant_name));
  } else {
    if (OB_FAIL(backup_local_vt_conn_cache())) {
      LOG_WARN("backup vip tenant connect cache failed", K(ret));
    } else {
      DRWLock::WRLockGuard guard(rwlock_);
      vt_conn_cache_.erase(cluster_name, tenant_name);
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::rollback()
{
  int ret = OB_SUCCESS;

  DRWLock::WRLockGuard guard(rwlock_);
  // The backup can be rolled back only if the backup is successful
  if (OB_LIKELY(backup_status_)) {
    if (OB_FAIL(vt_conn_cache_.recover())) {
      LOG_WARN("recover connect cache failed");
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::create_used_conn(ObString& key_name,
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
        LOG_WARN("fail to alloc memory for used conn", K(alloc_size), K(ret));
      } else {
        used_conn = new (buf) ObUsedConn(key_name);
        used_conn->inc_ref();
        if (OB_FAIL(used_conn_cache_.set(used_conn))) {
          LOG_WARN("fail to set used conn map", K(key_name), KPC(used_conn), K(ret));
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

int ObResourceUnitTableProcessor::get_used_conn(ObString& key_name, bool is_need_inc_used_connections,
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

int ObResourceUnitTableProcessor::get_or_create_used_conn(ObString& key_name,
    ObUsedConn*& used_conn, int64_t& cur_used_connections)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_used_conn(key_name, true, used_conn, cur_used_connections))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_used_conn(key_name, used_conn, cur_used_connections))) {
        LOG_WARN("fail to create used conn", K(key_name), K(ret));
      } else {
        LOG_DEBUG("succ to create used conn", K(key_name), K(cur_used_connections), KPC(used_conn));
      }
    } else {
      LOG_WARN("fail to get used conn", K(key_name), K(ret));
    }
  }
  return ret;
}

int ObResourceUnitTableProcessor::erase_used_conn(ObString& key_name, ObUsedConn* used_conn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(used_conn_cache_.erase(key_name))) {
    LOG_WARN("erase used conn failed", K(key_name));
  } else {
    used_conn->is_in_map_ = false;
    used_conn->dec_ref();
  }

  return ret;
}

ObResourceUnitTableProcessor &get_global_resource_unit_table_processor()
{
  static ObResourceUnitTableProcessor t_processor;
  return t_processor;
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
