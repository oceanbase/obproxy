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

#ifndef OB_CGROUP_CTRL_H
#define OB_CGROUP_CTRL_H

#include <stdint.h>
#include <sys/types.h>
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

class ObCgroupCtrl
{
public:
  ObCgroupCtrl()
      : valid_(false)
  {}
  virtual ~ObCgroupCtrl() {}
  int init();
  // void destroy() { /* tid is automatically removed from cgroup tasks after the process exits */ }
  bool is_valid() { return valid_; }

  // Create tenant cgroup group and initialize
  int create_tenant_cgroup(const common::ObString& tenant_id);

  // delete tenant cgroup rules
  int remove_tenant_cgroup(const common::ObString& tenant_id);

  // Add tid to the specified tenant cgroup. will deprecate
  int add_thread_to_cgroup(const common::ObString tenant_id, const int64_t tid);

  // Set the cpu.shares of the specified tenant cgroup group
  // int set_cpu_shares(const ObString tenant_id, const int32_t cpu_shares);
  // int get_cpu_shares(const ObString tenant_id, int32_t &cpu_shares);

  // Set the cpu.cfs_quota_us of the specified tenant cgroup group
  int set_cpu_cfs_quota(const common::ObString tenant_id, const int32_t cfs_quota_us);
  // Get the period value of a tenant's group for calculating cfs_quota_us
  int get_cpu_cfs_period(const common::ObString tenant_id, int32_t &cfs_period_us);
  // Get the cpuacct.usage of a cgroup group
  // int get_cpu_usage(const uint64_t tenant_id, int32_t &cpu_usage);
private:
  // The initialization script of obproxy will resume the cgroup soft link,
  // and the directory layout is as follows:
  //  ---bin/
  //   |-etc/
  //   |-...
  //   |_cgroup --> /sys/fs/cgroup/obproxy/obproxy_name
  //                        |
  //                        |_ user
  //                            |- tenant_cluster_vip1
  //                            |- tenant_cluster_vip2
  //                            |_ ...
  //
  const char *root_cgroup_  = "cgroup";
  const char *other_cgroup_ = "cgroup/other";
  const char *user_cgroup_  = "cgroup/user";
  static const int32_t PATH_BUFSIZE = 512;
  static const int32_t VALUE_BUFSIZE = 32;
  // Before using ObCgroupCtrl, you need to judge whether the group_ctrl object is valid. If it is false, the cgroup mechanism will be skipped.
  //  The possible reasons for false are that the cgroup directory does not have permission to operate, the operating system does not support cgroups, etc.
  bool valid_;

private:
  int init_cgroup_root_dir(const char *cgroup_path);
  int init_cgroup_dir(const char *cgroup_path);
  int write_string_to_file(const char *filename, const char *content);
  int get_string_from_file(const char *filename, char content[VALUE_BUFSIZE]);
};

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase

#endif  // OB_CGROUP_CTRL_H
