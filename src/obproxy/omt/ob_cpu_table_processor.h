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

#ifndef OBPROXY_CPU_TABLE_PROCESSOR_H
#define OBPROXY_CPU_TABLE_PROCESSOR_H

#include "obutils/ob_config_processor.h"
#include "lib/lock/ob_drw_lock.h"
#include "ob_vip_tenant_cpu.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

class ObCpuTableProcessor
{
public:
  ObCpuTableProcessor() : is_cpu_backup_succ_(false), tenant_cpu_rwlock_() {}
  virtual ~ObCpuTableProcessor() { destroy(); }
  int init();
  void destroy();
  void commit(bool is_success);
  int check_json_value(bool has_set_vip, common::ObString vip_list_str, bool has_set_value, common::ObString value);
  int cpu_handle_replace_config(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& name_str, common::ObString& value_str, const bool need_to_backup);
  int cpu_handle_delete_config(common::ObString& cluster_name, common::ObString& tenant_name, const bool need_to_backup);
  void backup_local_cpu_cache();
  int fill_local_cpu_cache(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& vip_name);
  int create_tenant_cpu(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& vip_name, common::ObString& key_name, double max_cpu_usage, ObTenantCpu*& tenant_cpu);
  int get_tenant_cpu(common::ObString& key_name, ObTenantCpu*& tenant_cpu);
  int get_or_create_tenant_cpu(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& vip_name, common::ObString& key_name, double max_cpu_usage, ObTenantCpu*& tenant_cpu);
  void cpu_rollback();
  void cpu_commit();
  int64_t get_cpu_map_count() { return tenant_cpu_cache_.get_cpu_map_count(); }
  int handle_cpu_config(common::ObString& cluster_name, common::ObString& tenant_name, common::ObString& vip_name, double max_cpu_usage);
  int check_cpu_config();
  int update_cpu_config();

  TO_STRING_KV(K_(is_cpu_backup_succ));

private:
  bool is_cpu_backup_succ_;  // false: backup failed; true: backup successful
  ObCgroupCtrl cgroup_ctrl_;
  common::DRWLock tenant_cpu_rwlock_;
  ObTenantCpuCache tenant_cpu_cache_;

  DISALLOW_COPY_AND_ASSIGN(ObCpuTableProcessor);
};

ObCpuTableProcessor &get_global_cpu_table_processor();

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_CPU_TABLE_PROCESSOR_H */
