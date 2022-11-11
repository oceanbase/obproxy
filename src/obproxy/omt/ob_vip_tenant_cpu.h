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

#ifndef OB_VIP_TENANT_CPU_H
#define OB_VIP_TENANT_CPU_H

#include "lib/hash/ob_build_in_hashmap.h"
#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_buf_allocator.h"
#include "iocore/eventsystem/ob_ethread.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_drw_lock.h"
#include "ob_cgroup_ctrl.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

enum ObTenantCpuStatus {
  INSTANCE_CREATE_STATUS = 0,
  INSTANCE_UPDATE_STATUS,
  INSTANCE_COMMIT_STATUS
};

class ObTenantCpu : public common::ObSharedRefCount {
public:
  ObTenantCpu(
    common::ObString& cluster_name,
    common::ObString& tenant_name,
    common::ObString& vip_name,
    common::ObString& full_name,
    double max_cpu_usage,
    ObCgroupCtrl &cgroup_ctrl);
  virtual ~ObTenantCpu() { reset(); };
  virtual void free() { destroy(); }
  int init();
  void destroy();
  void remove_tenant_cgroup();
  void reset();
  event::ObEThread* get_tenant_schedule_ethread() {
    return thread_array_[index_++ % thread_array_.count()];
  }

  int set_unit_max_cpu(double cpu);
  int acquire_more_worker(const int64_t tid);
  
  TO_STRING_KV(K_(tenant_name), K_(cluster_name), K_(vip_name), K_(full_name),
      K_(max_thread_num), K_(backup_max_thread_num), K_(max_cpu_usage), K_(backup_max_cpu_usage),
      K_(index), K_(is_inited), K_(instance_status));

public:
  common::ObString tenant_name_;
  common::ObString cluster_name_;
  common::ObString vip_name_;
  common::ObString full_name_;
  int64_t max_thread_num_;
  int64_t backup_max_thread_num_;
  double max_cpu_usage_;
  double backup_max_cpu_usage_;
  ObCgroupCtrl &cgroup_ctrl_;
  int64_t index_;
  bool is_inited_;
  ObTenantCpuStatus instance_status_;
  common::ObSEArray<event::ObEThread*, 16> thread_array_;
  
  LINK(ObTenantCpu, cpu_link_);

private:
  char tenant_name_str_[common::OB_MAX_TENANT_NAME_LENGTH];  
  char cluster_name_str_[OB_PROXY_MAX_CLUSTER_NAME_LENGTH];
  char vip_name_str_[common::OB_IP_STR_BUFF];
  char full_name_str_[OB_PROXY_MAX_TENANT_CLUSTER_NAME_LENGTH + common::OB_IP_STR_BUFF];
  DISALLOW_COPY_AND_ASSIGN(ObTenantCpu);
};

class ObTenantCpuCache
{
public:
  ObTenantCpuCache() : vt_cpu_map_() { }
  virtual ~ObTenantCpuCache() { }

  void destroy();

public:
  static const int64_t HASH_BUCKET_SIZE = 64;

  struct VTCacheHashing
  {
    typedef const common::ObString &Key;
    typedef ObTenantCpu Value;
    typedef ObDLList(ObTenantCpu, cpu_link_) ListHead;

    static uint64_t hash(Key key) { return key.hash(); }
    static Key key(Value *value) { return value->full_name_; }
    static bool equal(Key lhs, Key rhs) { return lhs == rhs; }
  };
  typedef common::hash::ObBuildInHashMap<VTCacheHashing, HASH_BUCKET_SIZE> VTCpuHashMap;

public:
  int set(ObTenantCpu* vt);
  int get(common::ObString& key_name, ObTenantCpu*& vt_conn);
  int erase(common::ObString& cluster_name, common::ObString& tenant_name);
  void backup();
  int recover();
  int check();
  int update();
  void commit();
  int64_t get_cpu_map_count();
  void clear_cpu_map(VTCpuHashMap &cache_map);
  void dump_cpu_map(VTCpuHashMap &cache_map);

private:
  VTCpuHashMap vt_cpu_map_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantCpuCache);
};

}  // namespace omt
}  // namespace obproxy
}  // namespace oceanbase

#endif  // OB_VIP_TENANT_CPU_H