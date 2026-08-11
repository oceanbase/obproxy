/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  // void destroy() { /* 进程退出后tid会自动从cgroup tasks中删除 */ }
  bool is_valid() { return valid_; }

  // 创建租户cgroup组并初始化
  int create_tenant_cgroup(const common::ObString& tenant_id);

  // 删除租户cgroup规则
  int remove_tenant_cgroup(const common::ObString& tenant_id);

  // 添加tid到指定租户cgroup组. will deprecate
  int add_thread_to_cgroup(const common::ObString tenant_id, const int64_t tid);

  // 设定指定租户cgroup组的cpu.shares
  // int set_cpu_shares(const ObString tenant_id, const int32_t cpu_shares);
  // int get_cpu_shares(const ObString tenant_id, int32_t &cpu_shares);

  // 设定指定租户cgroup组的cpu.cfs_quota_us
  int set_cpu_cfs_quota(const common::ObString tenant_id, const int32_t cfs_quota_us);
  // 获取某个租户的group 的 period 值，用于计算 cfs_quota_us
  int get_cpu_cfs_period(const common::ObString tenant_id, int32_t &cfs_period_us);
  // 获取某个cgroup组的cpuacct.usage
  // int get_cpu_usage(const uint64_t tenant_id, int32_t &cpu_usage);
private:
  // obproxy 的初始化脚本会简历 cgroup 软连接，目录布局如下：
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
  // 使用 ObCgroupCtrl 之前需要判断 group_ctrl 对象是否 valid，若为 false 则跳过 cgroup 机制
  //  为 false 可能的原因是 cgroup 目录没有操作权限、操作系统不支持 cgroup 等。
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
