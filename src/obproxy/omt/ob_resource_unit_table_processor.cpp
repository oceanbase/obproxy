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
#include "ob_conn_table_processor.h"
#include "ob_cpu_table_processor.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

const char* conn_name = "resource_max_connections";
const char* cpu_name  = "resource_cpu";

int ObResourceUnitTableProcessor::get_config_params(void* args,
    ObString& cluster_str, ObString& tenant_str, ObString& name_str, ObString& value_str, ObProxyBasicStmtType& stmt_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(args)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("params is null", K(ret));
  } else {
    ObFnParams* params = (ObFnParams*)args;
    stmt_type = params->stmt_type_;
    SqlFieldResult* fields = params->fields_;
    int64_t index = params->row_index_;
    if (OB_ISNULL(fields)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("fields is null", K(ret));
    } else {
      // The storage format is:[cluster|tenant|name|value]
      for (int64_t i = 0; OB_SUCC(ret) && i < fields->field_num_; i++) {
        SqlField* sql_field = fields->fields_.at(i);
        if (index >= sql_field->column_values_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("index out of range, invalid value for resource_unit",
                    K(index), K(sql_field->column_values_.count()), K_(sql_field->column_name), K(ret));
        } else if (0 == sql_field->column_name_.config_string_.case_compare("name")) {
          name_str = sql_field->column_values_.at(index).column_value_;
        } else if (0 == sql_field->column_name_.config_string_.case_compare("value")) {
          value_str = sql_field->column_values_.at(index).column_value_;
        } else if (0 == sql_field->column_name_.config_string_.case_compare("cluster_name")) {
          cluster_str = sql_field->column_values_.at(index).column_value_;
          if (OB_UNLIKELY(cluster_str.empty())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WDIAG("cluster_name is null", K(ret));
          }
        } else if (0 == sql_field->column_name_.config_string_.case_compare("tenant_name")) {
          tenant_str = sql_field->column_values_.at(index).column_value_;
          if (OB_UNLIKELY(tenant_str.empty())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WDIAG("tenant_name is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceUnitTableProcessor::execute(void* args)
{
  int ret = OB_SUCCESS;
  ObString cluster_name;
  ObString tenant_name;
  ObString name_str;
  ObString value_str;
  ObProxyBasicStmtType stmt_type;
  if (OB_FAIL(get_config_params(args, cluster_name, tenant_name, name_str, value_str, stmt_type))) {
    LOG_WDIAG("fail to get config params", K(ret), K(cluster_name), K(tenant_name), K(name_str), K(value_str), K(stmt_type));
  } else {
    ObFnParams* params = (ObFnParams*)args;
    bool need_to_backup = (0 == params->row_index_);
    LOG_DEBUG("execute cloud config", K(cluster_name), K(tenant_name), K(name_str), K(value_str), K(stmt_type), K(need_to_backup));
    if (OBPROXY_T_REPLACE == stmt_type) {
      if (OB_FAIL(get_global_resource_unit_table_processor().handle_replace_config(
        cluster_name, tenant_name, name_str, value_str, need_to_backup))) {
        LOG_WDIAG("fail to handle replace cmd", K(ret), K(cluster_name), K(tenant_name), K(name_str), K(value_str));
      }
    } else if (OBPROXY_T_DELETE == stmt_type) {
      if (OB_FAIL(get_global_resource_unit_table_processor().handle_delete_config(
        cluster_name, tenant_name, name_str, need_to_backup))) {
        LOG_WDIAG("fail to handle delete cmd", K(ret), K(cluster_name), K(tenant_name), K(name_str));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("operation is unexpected", K(ret), K(stmt_type));
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::commit(void* args, bool is_success)
{
  int ret = OB_SUCCESS;
  UNUSED(args);
  /* ObString cluster_name;
  // ObString tenant_name;
  // ObString name_str;
  // ObString value_str;
  // ObProxyBasicStmtType stmt_type;
  if (OB_FAIL(get_config_params(args, cluster_name, tenant_name, name_str, value_str, stmt_type))) {
    LOG_WDIAG("fail to get config params", K(ret), K(cluster_name), K(tenant_name), K(name_str), K(value_str), K(stmt_type));
  } else {
    LOG_DEBUG("commit cloud config", K(cluster_name), K(tenant_name), K(name_str), K(value_str), K(stmt_type), K(is_success));
    */
  LOG_DEBUG("commit cloud config");
  // 用2bit表示，修改的name种类
  if (get_global_resource_unit_table_processor().get_name_type() & 1) {
    if (OB_FAIL(get_global_conn_table_processor().commit(is_success))) {
      LOG_WDIAG("fail to handle connection commit", K(ret));
    }
  }
  if (get_global_resource_unit_table_processor().get_name_type() & 2) {
    get_global_cpu_table_processor().commit(is_success);
  }

  get_global_resource_unit_table_processor().reset_name_type();
  return ret;
}

int ObResourceUnitTableProcessor::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(ret));
  } else {
    ObConfigHandler handler;
    ObString table_name = ObString::make_string("resource_unit");
    handler.execute_func_  = &execute;
    handler.commit_func_   = &commit;
    if (OB_FAIL(get_global_config_processor().register_callback(table_name, handler))) {
      LOG_WDIAG("register callback info failed", K(ret));
    } else if (OB_FAIL(get_global_cpu_table_processor().init())) {
      LOG_WDIAG("init cpu processor failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::handle_replace_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str, ObString& value_str, const bool need_to_backup)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(name_str.empty()) || OB_UNLIKELY(value_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("name or value is null", K(ret), K(name_str), K(value_str));
  } else {
    if (name_str == conn_name) {
      name_type_ |= 1;  // 第1个bit置1
      if (OB_FAIL(get_global_conn_table_processor().conn_handle_replace_config(cluster_name, tenant_name, name_str, value_str, need_to_backup))) {
        LOG_WDIAG("fail to replace connection config", K(ret), K(cluster_name), K(tenant_name), K(name_str), K(value_str));
      }
    } else if (name_str == cpu_name) {
      name_type_ |= 2;  // 第2个bit置1
      if (OB_FAIL(get_global_cpu_table_processor().cpu_handle_replace_config(cluster_name, tenant_name, name_str, value_str, need_to_backup))) {
        LOG_WDIAG("fail to replace cpu config", K(ret), K(cluster_name), K(tenant_name), K(name_str), K(value_str));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("operation is unexpected", K(ret), K(name_str));
    }
  }

  return ret;
}

int ObResourceUnitTableProcessor::handle_delete_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str, const bool need_to_backup)
{
  int ret = OB_SUCCESS;
  // note: Now only supports the deletion of cluster and tenant information
  if (name_str.empty()) {
    if (OB_FAIL(get_global_conn_table_processor().conn_handle_delete_config(cluster_name, tenant_name, need_to_backup))) {
      LOG_WDIAG("fail to handle delete conn config", K(ret), K(cluster_name), K(tenant_name));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_global_cpu_table_processor().cpu_handle_delete_config(cluster_name, tenant_name, need_to_backup))) {
        LOG_WDIAG("fail to handle delete cpu config", K(ret), K(cluster_name), K(tenant_name), K(name_str));
      }
    }
  } else if (name_str == conn_name) {
    if (OB_FAIL(get_global_conn_table_processor().conn_handle_delete_config(cluster_name, tenant_name, need_to_backup))) {
      LOG_WDIAG("fail to handle delete conn config", K(ret), K(cluster_name), K(tenant_name), K(name_str));
    }
  } else if (name_str == cpu_name) {
    if (OB_FAIL(get_global_cpu_table_processor().cpu_handle_delete_config(cluster_name, tenant_name, need_to_backup))) {
      LOG_WDIAG("fail to handle delete cpu config", K(ret), K(cluster_name), K(tenant_name), K(name_str));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WDIAG("operation is unexpected", K(ret), K(name_str));
  }

  return ret;
}

int build_tenant_cluster_vip_name(const ObString &tenant_name, const ObString &cluster_name,
    const ObString &vip_name, ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH> &key_string)
{
  int ret = OB_SUCCESS;
  char buf[OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH];
  int64_t len = 0;
  len = static_cast<int64_t>(snprintf(buf, OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH, "%.*s#%.*s|%.*s",
    tenant_name.length(), tenant_name.ptr(),
    cluster_name.length(), cluster_name.ptr(),
    vip_name.length(), vip_name.ptr()));
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + MAX_IP_ADDR_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to fill buf", K(ret), K(tenant_name), K(cluster_name), K(vip_name));
  } else if (OB_FAIL(key_string.assign(buf))) {
    LOG_WDIAG("assign failed", K(ret));
  }

  return ret;
}

ObResourceUnitTableProcessor &get_global_resource_unit_table_processor()
{
  static ObResourceUnitTableProcessor resource_unit_table_processor;
  return resource_unit_table_processor;
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
