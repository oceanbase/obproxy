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

#include "ob_vip_tenant_cpu.h"
#include "lib/oblog/ob_log.h"
#include "iocore/net/ob_net_def.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;

ObTenantCpu::ObTenantCpu(ObString& cluster_name, ObString& tenant_name,
    ObString& vip_name, ObString& full_name, double max_cpu_usage, ObCgroupCtrl &cgroup_ctrl)
    : max_cpu_usage_(max_cpu_usage), backup_max_cpu_usage_(max_cpu_usage),
    cgroup_ctrl_(cgroup_ctrl), index_(0), is_inited_(false), instance_status_(INSTANCE_CREATE_STATUS)
{
  if (cluster_name.length() < OB_PROXY_MAX_CLUSTER_NAME_LENGTH) {
    MEMCPY(cluster_name_str_, cluster_name.ptr(), cluster_name.length());
    cluster_name_.assign_ptr(cluster_name_str_, cluster_name.length());
  }
  if (tenant_name.length() < OB_MAX_TENANT_NAME_LENGTH) {
    MEMCPY(tenant_name_str_, tenant_name.ptr(), tenant_name.length());
    tenant_name_.assign_ptr(tenant_name_str_, tenant_name.length());
  }
  if (vip_name.length() < OB_IP_STR_BUFF) {
    MEMCPY(vip_name_str_, vip_name.ptr(), vip_name.length());
    vip_name_.assign_ptr(vip_name_str_, vip_name.length());
  }
  if (full_name.length() < OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF) {
    MEMCPY(full_name_str_, full_name.ptr(), full_name.length());
    full_name_.assign_ptr(full_name_str_, full_name.length());
  }
  max_thread_num_ = (int64_t)ceil(max_cpu_usage_);
  backup_max_thread_num_ = max_thread_num_;
  memset(&cpu_link_, 0, sizeof(cpu_link_));
}

int ObTenantCpu::init()
{
  int ret = OB_SUCCESS;
  ObString tenant_id = full_name_;
  if (cgroup_ctrl_.is_valid()) {
    if (!is_inited_) {
      if (OB_FAIL(cgroup_ctrl_.create_tenant_cgroup(tenant_id))) {
        LOG_WARN("create tenant cgroup failed", K(ret), K(tenant_id));
      } else {
        is_inited_ = true;
      }
    }
  } else {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("cgroup is not valid", K(ret));
  }
  return ret;
}

void ObTenantCpu::destroy()
{
  LOG_INFO("tenant cpu will be destroyed", KPC(this));
  if (is_inited_) {
    remove_tenant_cgroup();
  }
  ObEThread* ethread = NULL;
  for (int i = 0; i < thread_array_.count(); ++i) {
    ethread = NULL;
    thread_array_.at(i, ethread);
    if (OB_NOT_NULL(ethread)) {
      ethread->use_status_ = false;
    }
  }
  reset();
  int64_t total_len = sizeof(ObTenantCpu);
  op_fixed_mem_free(this, total_len);
}

void ObTenantCpu::remove_tenant_cgroup()
{
  LOG_INFO("tenant cgroup will be destroyed", KPC(this));
  int tmp_ret = OB_SUCCESS;
  ObString tenant_id = full_name_;
  if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.remove_tenant_cgroup(tenant_id))) {
    LOG_WARN("remove tenant cgroup failed", K(tmp_ret), K(tenant_id));
  }
  is_inited_ = false;
}

void ObTenantCpu::reset()
{
  tenant_name_.reset();
  cluster_name_.reset();
  vip_name_.reset();
  full_name_.reset();
  max_thread_num_ = 0;
  backup_max_thread_num_ = 0;
  max_cpu_usage_ = 0.0;
  backup_max_cpu_usage_ = 0.0;
  thread_array_.reset();
}

int ObTenantCpu::acquire_more_worker(const int64_t tid)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObString tenant_id = full_name_;
  if (cgroup_ctrl_.is_valid() && OB_SUCCESS != (tmp_ret = cgroup_ctrl_.add_thread_to_cgroup(tenant_id, tid))) {
    LOG_WARN("add thread to cgroup failed", K(ret), K(tenant_id), K(tid));
  }
  return ret;
}

int ObTenantCpu::set_unit_max_cpu(double cpu)
{
  int ret = OB_SUCCESS;
  ObString tenant_id = full_name_;
  if (cgroup_ctrl_.is_valid()) {
    int32_t cfs_period_us = 0;
    if (OB_FAIL(cgroup_ctrl_.get_cpu_cfs_period(tenant_id, cfs_period_us))) {
      LOG_WARN("get cpu cfs period failed", K(ret), K(tenant_id), K(cfs_period_us));
    } else if (OB_FAIL(cgroup_ctrl_.set_cpu_cfs_quota(tenant_id, (uint32_t)(cfs_period_us * cpu)))) {
      LOG_WARN("set cpu cfs quota failed", K(ret), K(tenant_id), K(cfs_period_us * cpu));
    }
  } else {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("cgroup is not valid", K(ret));
  }
  return ret;
}

void ObTenantCpuCache::destroy()
{
  clear_cpu_map(vt_cpu_map_);
}

int64_t ObTenantCpuCache::get_cpu_map_count()
{
  int64_t ret = -1;
  ret = vt_cpu_map_.count();
  return ret;
}

int ObTenantCpuCache::get(ObString& key_name, ObTenantCpu*& tenant_cpu)
{
  int ret = OB_SUCCESS;
  if (key_name.empty() || key_name.length() > OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + OB_IP_STR_BUFF) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("name invalid", K(key_name), K(ret));
  } else if (OB_FAIL(vt_cpu_map_.get_refactored(key_name, tenant_cpu))) {
    LOG_DEBUG("fail to get tenant cpu", K(key_name), K(ret));
  }
  return ret;
}

int ObTenantCpuCache::set(ObTenantCpu* tenant_cpu)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_cpu)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant cpu is null", K(ret));
  } else if (OB_FAIL(vt_cpu_map_.unique_set(tenant_cpu))) {
    LOG_WARN("fail to set tenant cpu", K(tenant_cpu), K(ret));
  }
  return ret;
}

int ObTenantCpuCache::erase(ObString& cluster_name, ObString& tenant_name)
{
  int ret = OB_SUCCESS;
  LOG_INFO("erase cpu config");
  if (cluster_name.empty() || tenant_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(ret), K(cluster_name), K(tenant_name));
  } else {
    VTCpuHashMap::iterator last = vt_cpu_map_.end();
    for (VTCpuHashMap::iterator it = vt_cpu_map_.begin(); it != last;) {
      if (it->cluster_name_ == cluster_name && it->tenant_name_ == tenant_name) {
        VTCpuHashMap::iterator tmp = it;
        ++it;
        vt_cpu_map_.erase_refactored(tmp->full_name_);
        tmp->dec_ref();
      } else {
        ++it;
      }
    }
  }

  return ret;
}

void ObTenantCpuCache::backup()
{
  LOG_INFO("backup cpu config");
  VTCpuHashMap::iterator last = vt_cpu_map_.end();
  for (VTCpuHashMap::iterator it = vt_cpu_map_.begin(); it != last; ++it) {
    it->backup_max_cpu_usage_ = it->max_cpu_usage_;
    it->backup_max_thread_num_ = it->max_thread_num_;
  }
}

int ObTenantCpuCache::recover()
{
  int ret = OB_SUCCESS;
  VTCpuHashMap::iterator last = vt_cpu_map_.end();
  for (VTCpuHashMap::iterator it = vt_cpu_map_.begin(); it != last && OB_SUCC(ret);) {
    if (INSTANCE_CREATE_STATUS == it->instance_status_) {
      VTCpuHashMap::iterator tmp = it;
      ++it;
      vt_cpu_map_.erase_refactored(tmp->full_name_);
      tmp->dec_ref();
    } else {
      if (INSTANCE_UPDATE_STATUS == it->instance_status_) {
        it->max_cpu_usage_ = it->backup_max_cpu_usage_;
        it->max_thread_num_ = it->backup_max_thread_num_;
        if (OB_FAIL(it->set_unit_max_cpu(it->max_cpu_usage_))) {
          LOG_WARN("set max cpu failed", K(ret));
        }
      }
      ++it;
    }
  }
  return ret;
}

int ObTenantCpuCache::check()
{
  LOG_INFO("check cpu config");
  int ret = OB_SUCCESS;
  double max_cpu_value = 0.0;
  int64_t max_thread_hold = 0;
  int64_t cpu_num = g_event_processor.get_cpu_count();

  VTCpuHashMap::iterator it = vt_cpu_map_.begin();
  for (; it != vt_cpu_map_.end(); ++it) {
    max_cpu_value += it->max_cpu_usage_;
    max_thread_hold += std::max(it->thread_array_.count(), it->max_thread_num_);
  }
  if ((max_cpu_value - (double)cpu_num  > 0) || (max_thread_hold > MAX_THREADS_IN_EACH_TYPE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check config params failed", K(max_cpu_value), K(max_thread_hold), K(cpu_num));
  }
  return ret;
}

int ObTenantCpuCache::update()
{
  int ret = OB_SUCCESS;
  LOG_INFO("update cpu config");
  VTCpuHashMap::iterator last = vt_cpu_map_.end();
  for (VTCpuHashMap::iterator it = vt_cpu_map_.begin(); it != last && OB_SUCC(ret); ++it) {
    if (INSTANCE_CREATE_STATUS == it->instance_status_ || INSTANCE_UPDATE_STATUS == it->instance_status_) {
      if (OB_FAIL(it->init())) {
        LOG_WARN("init tenant cgroup failed", K(ret));
      } else if (OB_FAIL(it->set_unit_max_cpu(it->max_cpu_usage_))) {
        LOG_WARN("set max cpu failed", K(ret));
      }
    }
  }
  return ret;
}

void ObTenantCpuCache::commit()
{
  VTCpuHashMap::iterator last = vt_cpu_map_.end();
  for (VTCpuHashMap::iterator it = vt_cpu_map_.begin(); it != last; ++it) {
    it->instance_status_ = INSTANCE_COMMIT_STATUS;
  }
}

void ObTenantCpuCache::clear_cpu_map(VTCpuHashMap &cache_map)
{
  VTCpuHashMap::iterator last = cache_map.end();
  VTCpuHashMap::iterator tmp;
  for (VTCpuHashMap::iterator it = cache_map.begin(); it != last;) {
    tmp = it;
    ++it;
    tmp->dec_ref();
  }
  cache_map.reset();
}

void ObTenantCpuCache::dump_cpu_map(VTCpuHashMap &cache_map)
{
  VTCpuHashMap::iterator last = cache_map.end();
  for (VTCpuHashMap::iterator it = cache_map.begin(); it != last; ++it) {
    LOG_INFO("key: ", K(it->full_name_));
    LOG_INFO("value: ", K(it->max_cpu_usage_));
  }
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
