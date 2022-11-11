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

#include "ob_cgroup_ctrl.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"

#include <stdlib.h>
#include <stdio.h>

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace omt
{

// Create cgroup initial directory structure
int ObCgroupCtrl::init()
{
  int ret = OB_SUCCESS;
  // cgroup
  //  ├── tenant_cluster_vip1
  //  |   ├── cpu.cfs_period_us
  //  |   ├── cpu.cfs_quota_us
  //  |   └── cpu.tasks
  //  └── tenant_cluster_vip2
  //      ├── cpu.cfs_period_us
  //      ├── cpu.cfs_quota_us
  //      └── cpu.tasks
  // 1. Initialize the obproxy root cgroup
  if (OB_FAIL(init_cgroup_root_dir(root_cgroup_))) {
    LOG_WARN("init cgroup dir failed", K(ret), K(root_cgroup_));
  // 2. Create a user cgroup for the user tenant
  } else if (init_cgroup_dir(user_cgroup_)) {
    LOG_WARN("init tenants cgroup dir failed", K(ret), K(user_cgroup_));
  } else if (init_cgroup_dir(other_cgroup_)) {
    LOG_WARN("init other cgroup dir failed", K(ret), K(other_cgroup_));
  } else {
    valid_ = true;
    LOG_DEBUG("init cgroup dir succ");
  }

  return ret;
}

int ObCgroupCtrl::create_tenant_cgroup(const ObString& tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_cg_dir[PATH_BUFSIZE];
  bool exist_cgroup = false;
  const char *top_cgroup = user_cgroup_;
  snprintf(tenant_cg_dir, PATH_BUFSIZE, "%s/%.*s", top_cgroup, tenant_id.length(), tenant_id.ptr());
  if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_cg_dir, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(tenant_cg_dir), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir(tenant_cg_dir))) {
    // note: Support concurrent creation of the same directory, all return OB_SUCCESS
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(tenant_cg_dir), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::remove_tenant_cgroup(const ObString& tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_path[PATH_BUFSIZE];
  char tenant_task_path[PATH_BUFSIZE];
  char other_task_path[PATH_BUFSIZE];
  const char *top_cgroup = user_cgroup_;
  snprintf(tenant_path, PATH_BUFSIZE, "%s/%.*s", top_cgroup, tenant_id.length(), tenant_id.ptr());
  snprintf(tenant_task_path, PATH_BUFSIZE, "%s/%.*s/tasks", top_cgroup, tenant_id.length(), tenant_id.ptr());
  snprintf(other_task_path, PATH_BUFSIZE, "%s/tasks", other_cgroup_);
  FILE* tenant_task_file = NULL;
  if (OB_ISNULL(tenant_task_file = fopen(tenant_task_path, "r"))) {
    ret = OB_IO_ERROR;
    LOG_WARN("open tenant task path failed", K(ret), K(tenant_task_path), K(errno), KERRMSG);
  } else {
    char tid_buf[VALUE_BUFSIZE];
    while (fgets(tid_buf, VALUE_BUFSIZE, tenant_task_file)) {
      if (OB_FAIL(write_string_to_file(other_task_path, tid_buf))) {
        LOG_WARN("remove tenant task failed", K(ret), K(other_task_path), K(tenant_id));
        break;
      }
    }
    fclose(tenant_task_file);
  }
  if (OB_SUCC(ret) && OB_FAIL(FileDirectoryUtils::delete_directory(tenant_path))) {
    LOG_WARN("remove tenant cgroup directory failed", K(ret), K(tenant_path), K(tenant_id));
  } else {
    LOG_INFO("remove tenant cgroup directory success", K(tenant_path), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::add_thread_to_cgroup(const ObString tenant_id, const int64_t tid)
{
  int ret = OB_SUCCESS;
  char task_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE];
  snprintf(task_path, PATH_BUFSIZE, "%s/%.*s/tasks", user_cgroup_, tenant_id.length(), tenant_id.ptr());
  snprintf(tid_value, VALUE_BUFSIZE, "%d", (uint32_t)tid);
  if(OB_FAIL(write_string_to_file(task_path, tid_value))) {
    LOG_WARN("add tid to cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
  } else {
    LOG_DEBUG("add tid to cgroup succ", K(task_path), K(tid_value), K(tenant_id));
  }

  return ret;
}

int ObCgroupCtrl::set_cpu_cfs_quota(const ObString tenant_id, const int32_t cfs_quota_us)
{
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  const char *top_cgroup = user_cgroup_;
  snprintf(cfs_path, PATH_BUFSIZE, "%s/%.*s/cpu.cfs_quota_us", top_cgroup, tenant_id.length(), tenant_id.ptr());
  snprintf(cfs_value, VALUE_BUFSIZE, "%d", cfs_quota_us);
  if(OB_FAIL(write_string_to_file(cfs_path, cfs_value))) {
    LOG_WARN("set cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    LOG_INFO("set cpu cfs quota success", K(cfs_path), K(cfs_value), K(tenant_id));
  }
  return ret;
}

// TODO: This value can be unchanged, and the same value is shared globally,
// so there is no need to read from the file system every time.
int ObCgroupCtrl::get_cpu_cfs_period(const ObString tenant_id, int32_t &cfs_period_us)
{
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  const char *top_cgroup = user_cgroup_;
  snprintf(cfs_path, PATH_BUFSIZE, "%s/%.*s/cpu.cfs_period_us",
           top_cgroup, tenant_id.length(), tenant_id.ptr());
  if(OB_FAIL(get_string_from_file(cfs_path, cfs_value))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    cfs_period_us = atoi(cfs_value);
  }
  return ret;
}

int ObCgroupCtrl::init_cgroup_root_dir(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  char current_path[PATH_BUFSIZE];
  char value_buf[VALUE_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(cgroup_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(cgroup_path), K(ret));
  } else if (!exist_cgroup) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("no cgroup directory found. disable cgroup support", K(cgroup_path), K(ret));
  } else {
    // set mems and cpus
    //  The cpu may be discontinuous, and it is very complicated to detect it
    //  This always inherits the parent's settings, no need to detect it
    //  Subsequent child cgroups no longer need to set mems and cpus
    if (OB_SUCC(ret)) {
      snprintf(current_path, PATH_BUFSIZE, "%s/cgroup.clone_children", cgroup_path);
      snprintf(value_buf, VALUE_BUFSIZE, "1");
      if (OB_FAIL(write_string_to_file(current_path, value_buf))) {
        LOG_WARN("fail set value to file", K(current_path), K(ret));
      }
    }
  }
  return ret;
}

int ObCgroupCtrl::init_cgroup_dir(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  char current_path[PATH_BUFSIZE];
  char value_buf[VALUE_BUFSIZE];
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::create_directory(cgroup_path))) {
    LOG_WARN("create tenant cgroup dir failed", K(ret), K(cgroup_path));
  } else {
    // set mems and cpus
    //  The cpu may be discontinuous, and it is very complicated to detect it
    //  This always inherits the parent's settings, no need to detect it
    //  Subsequent child cgroups no longer need to set mems and cpus
    if (OB_SUCC(ret)) {
      snprintf(current_path, PATH_BUFSIZE, "%s/cgroup.clone_children", cgroup_path);
      snprintf(value_buf, VALUE_BUFSIZE, "1");
      if (OB_FAIL(write_string_to_file(current_path, value_buf))) {
        LOG_WARN("fail set value to file", K(current_path), K(ret));
      }
      LOG_DEBUG("debug print clone", K(current_path), K(value_buf));
    }
  }
  return ret;
}

int ObCgroupCtrl::write_string_to_file(const char *filename, const char *content)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  long int tmp_ret = -1;
  if ((fd = ::open(filename, O_WRONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((tmp_ret = ::write(fd, content, strlen(content))) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("write file error",
        K(filename), K(content), K(ret), K(errno), KERRMSG, K(ret));
  } else {
    // do nothing
  }
  if (fd > 0 && 0 != ::close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("close file error",
        K(filename), K(fd), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObCgroupCtrl::get_string_from_file(const char *filename, char content[VALUE_BUFSIZE])
{
  int ret = OB_SUCCESS;
  int fd = -1;
  long int tmp_ret = -1;
  if ((fd = ::open(filename, O_RDONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((tmp_ret = ::read(fd, content, VALUE_BUFSIZE)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("read file error",
        K(filename), K(content), K(ret), K(errno), KERRMSG, K(ret));
  } else {
    // do nothing
  }
  if (fd > 0 && 0 != ::close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("close file error",
        K(filename), K(fd), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

} // end of namespace omt
} // end of namespace obproxy
} // end of namespace oceanbase
