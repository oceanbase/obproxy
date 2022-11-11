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

#include "ob_cpu_table_processor.h"
#include "obutils/ob_proxy_json_config_info.h"
#include "obproxy/utils/ob_proxy_utils.h"

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
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::json;

extern int build_tenant_cluster_vip_name(const ObString &tenant_name, const ObString &cluster_name, const ObString &vip_name,
    ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF> &key_string);

void ObCpuTableProcessor::commit(bool is_success)
{
  if (!is_success) {
    cpu_rollback();
  } else {
    cpu_commit();
  }
  is_cpu_backup_succ_ = false;
}

int ObCpuTableProcessor::init()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = cgroup_ctrl_.init())) {
    // cgroup failure does not affect initialization
    LOG_WARN("fail to init tenant cgroup ctrl", K(tmp_ret));
  }
  return ret;
}

void ObCpuTableProcessor::destroy()
{
  DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
  tenant_cpu_cache_.destroy();
}

int ObCpuTableProcessor::cpu_handle_replace_config(
    ObString& cluster_name, ObString& tenant_name, ObString& name_str, ObString& value_str)
{
  UNUSED(name_str);
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant or cluster is null", K(ret), K(cluster_name), K(tenant_name));
  } else if (FALSE_IT(backup_local_cpu_cache())) {
  } else if (OB_FAIL(fill_local_cpu_cache(cluster_name, tenant_name, value_str))) {
    LOG_WARN("update vip tenant cpu cache failed", K(ret));
  } else {
    LOG_INFO("update vip tenant cpu cache succ", "count", get_cpu_map_count(), K(cluster_name), K(tenant_name), K(value_str));
  }

  return ret;
}

int ObCpuTableProcessor::cpu_handle_delete_config(ObString& cluster_name, ObString& tenant_name)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(cluster_name.empty()) || OB_UNLIKELY(tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_name or cluster_name is null", K(ret), K(cluster_name), K(tenant_name));
  } else {
    backup_local_cpu_cache();
    DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
    tenant_cpu_cache_.erase(cluster_name, tenant_name);
    LOG_INFO("erase vip tenant cpu cache succ", "count", get_cpu_map_count(), K(cluster_name), K(tenant_name));
  }

  return ret;
}

void ObCpuTableProcessor::backup_local_cpu_cache()
{
  DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
  tenant_cpu_cache_.backup();
  is_cpu_backup_succ_ = true;
}

int ObCpuTableProcessor::check_json_value(bool has_set_vip, ObString vip_list_str,
    bool has_set_value, ObString value)
{
  int ret = OB_SUCCESS;
  if (has_set_vip && vip_list_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check json value failed", K(ret), K(has_set_vip), K(vip_list_str));
  }
  if (OB_SUCC(ret)) {
    if (!has_set_value || value.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check json value failed", K(ret), K(has_set_value), K(value));
    }
  }
  return ret;
}

int ObCpuTableProcessor::fill_local_cpu_cache(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name)
{
  int ret = OB_SUCCESS;
  Parser parser;
  json::Value *info_config = NULL;
  ObArenaAllocator json_allocator(ObModIds::OB_JSON_PARSER);

  ObString max_cpu_usage = 0;
  double double_max_cpu_usage = 0.0;
  ObString vip_list_str;
  bool has_set_vip = false;
  bool has_set_value = false;
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
              has_set_vip = true;
            }
          } else if (p->name_ == JSON_OBPROXY_VALUE) {
            // note: Since json only supports integers but not doubles for numeric parsing, the JT_STRING type is used here
            if (OB_FAIL(ObProxyJsonUtils::check_config_info_type(p->value_, json::JT_STRING))) {
              LOG_WARN("check config info type failed", K(ret));
            } else {
              max_cpu_usage = p->value_->get_string();
              has_set_value = true;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(check_json_value(has_set_vip, vip_list_str, has_set_value, max_cpu_usage))) {
          LOG_WARN("check json value failed", K(cluster_name), K(tenant_name), K(vip_list_str), K(max_cpu_usage), K(ret));
        } else if (OB_FAIL(get_double_value(max_cpu_usage, double_max_cpu_usage))) {
          LOG_WARN("fail to get double", K(max_cpu_usage), K(double_max_cpu_usage), K(ret));
        } else if (OB_FAIL(handle_cpu_config(cluster_name, tenant_name, vip_list_str, double_max_cpu_usage))) {
          LOG_WARN("update config faild", K(cluster_name), K(tenant_name), K(vip_list_str), K(double_max_cpu_usage), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_cpu_config())) {
        LOG_WARN("check config faild", K(ret));
      } else if (OB_FAIL(update_cpu_config())) {
        LOG_WARN("update cpu config faild", K(ret));
      }
    }
  }

  return ret;
}

int ObCpuTableProcessor::handle_cpu_config(
    ObString& cluster_name, ObString& tenant_name, ObString& vip_name, double max_cpu_usage)
{
  int ret = OB_SUCCESS;
  ObString key_name;
  ObTenantCpu* tenant_cpu = NULL;
  common::ObFixedLengthString<OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::OB_IP_STR_BUFF> key_string;
  if (max_cpu_usage <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("max cpu usage error", K(max_cpu_usage), K(ret));
  } else if (OB_FAIL(build_tenant_cluster_vip_name(tenant_name, cluster_name, vip_name, key_string))) {
    LOG_WARN("build tenant cluser vip name failed", K(tenant_name), K(cluster_name), K(vip_name), K(ret));
  } else {
    key_name = ObString::make_string(key_string.ptr());
    if (OB_FAIL(get_or_create_tenant_cpu(cluster_name, tenant_name, vip_name, key_name, max_cpu_usage, tenant_cpu))) {
      LOG_WARN("create tenant cpu failed", K(key_name), K(ret));
    } else {
      tenant_cpu->max_cpu_usage_ = max_cpu_usage;
      tenant_cpu->max_thread_num_ = (int64_t)ceil(tenant_cpu->max_cpu_usage_);
      if (INSTANCE_CREATE_STATUS != tenant_cpu->instance_status_) {
        tenant_cpu->instance_status_ = INSTANCE_UPDATE_STATUS;
      }
      tenant_cpu->dec_ref();
    }
  }
  return ret;
}

int ObCpuTableProcessor::check_cpu_config()
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(tenant_cpu_rwlock_);
  if (OB_FAIL(tenant_cpu_cache_.check())) {
    LOG_WARN("check cpu config failed", K(ret));
  }
  return ret;
}

int ObCpuTableProcessor::update_cpu_config()
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
  if (OB_FAIL(tenant_cpu_cache_.update())) {
    LOG_WARN("update cpu config failed", K(ret));
  }
  return ret;
}

int ObCpuTableProcessor::create_tenant_cpu(ObString& cluster_name, ObString& tenant_name,
    ObString& vip_name, ObString& key_name, double max_cpu_usage, ObTenantCpu*& tenant_cpu)
{
  int ret = OB_SUCCESS;
  DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
  ObTenantCpu* tmp_tenant_cpu = NULL;
  if (OB_FAIL(tenant_cpu_cache_.get(key_name, tmp_tenant_cpu))) {
    if (OB_HASH_NOT_EXIST == ret) {
      char *buf = NULL;
      int64_t alloc_size = sizeof(ObTenantCpu);
      if (OB_ISNULL(buf = static_cast<char *>(op_fixed_mem_alloc(alloc_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for tenant cpu", K(alloc_size), K(ret));
      } else {
        tenant_cpu = new (buf) ObTenantCpu(cluster_name, tenant_name, vip_name, key_name, max_cpu_usage, cgroup_ctrl_);
        tenant_cpu->inc_ref();
        if (OB_FAIL(tenant_cpu_cache_.set(tenant_cpu))) {
          LOG_WARN("fail to set tenant cpu map", K(key_name), KPC(tenant_cpu), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        tenant_cpu->inc_ref();
      } else if (OB_NOT_NULL(tenant_cpu)) {
        tenant_cpu->dec_ref();
        tenant_cpu = NULL;
      }
    }
  } else {
    tenant_cpu = tmp_tenant_cpu;
    tenant_cpu->inc_ref();
  }

  return ret;
}

int ObCpuTableProcessor::get_tenant_cpu(ObString& key_name, ObTenantCpu*& tenant_cpu)
{
  int ret = OB_SUCCESS;
  DRWLock::RDLockGuard guard(tenant_cpu_rwlock_);
  ObTenantCpu* tmp_tenant_cpu = NULL;
  if (OB_FAIL(tenant_cpu_cache_.get(key_name, tmp_tenant_cpu))) {
  } else {
    tenant_cpu = tmp_tenant_cpu;
    tenant_cpu->inc_ref();
    LOG_DEBUG("succ to get tenant cpu from map", K(key_name), KPC(tenant_cpu));
  }

  return ret;
}

int ObCpuTableProcessor::get_or_create_tenant_cpu(ObString& cluster_name, ObString& tenant_name,
    ObString& vip_name, ObString& key_name, double max_cpu_usage, ObTenantCpu*& tenant_cpu)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tenant_cpu(key_name, tenant_cpu))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(create_tenant_cpu(cluster_name, tenant_name, vip_name, key_name, max_cpu_usage, tenant_cpu))) {
        LOG_WARN("fail to create tenant cpu", K(key_name), K(ret));
      } else {
        LOG_DEBUG("succ to create tenant cpu", K(key_name), K(max_cpu_usage), KPC(tenant_cpu));
      }
    } else {
      LOG_WARN("fail to get tenant cpu", K(key_name), K(ret));
    }
  }
  return ret;
}

void ObCpuTableProcessor::cpu_rollback()
{
  LOG_INFO("cpu rollback");
  DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
  if (OB_LIKELY(is_cpu_backup_succ_)) {
    tenant_cpu_cache_.recover();
  }
}

void ObCpuTableProcessor::cpu_commit()
{
  LOG_INFO("cpu commit");
  DRWLock::WRLockGuard guard(tenant_cpu_rwlock_);
  tenant_cpu_cache_.commit();
}

ObCpuTableProcessor &get_global_cpu_table_processor()
{
  static ObCpuTableProcessor cpu_table_processor;
  return cpu_table_processor;
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
