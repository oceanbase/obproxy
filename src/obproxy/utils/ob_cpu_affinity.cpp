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
 */

#define USING_LOG_PREFIX PROXY
#include "utils/ob_proxy_lib.h"
#include "utils/ob_cpu_affinity.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{

ObCpuTopology::ObCpuTopology()
    : is_inited_(false),
      core_number_(0),
      cpu_number_(0),
      cores_()
{
  for (int64_t i = 0; i < MAX_CORE_NUMBER; i++) {
    cores_[i].cpu_number_ = 0;
  }
}

int ObCpuTopology::init()
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(fp = popen("lscpu -p", "r"))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get lscpu info", K(ret));
  } else {
    char buf[BUFSIZ];
    int64_t cpu_id = 0;
    int64_t core_id = 0;

    char *p = NULL;
    char *p_core = NULL;

    while ((NULL != fgets(buf, BUFSIZ, fp)) && OB_SUCC(ret)) {
      if (buf[0] == '#') {
        continue;
      }

      p = strchr(buf, ',');
      *p = '\0';
      cpu_id = atoll(buf);

      p_core = p + 1;
      p = strchr(p_core + 1, ',');
      *p = '\0';
      core_id = atoll(p_core);

      if (core_id + 1 > core_number_) {
        core_number_ = core_id + 1;
      }

      ++cpu_number_;

      if (OB_LIKELY(core_number_ < MAX_CORE_NUMBER)) {
        cores_[core_id].cpues_[(cores_[core_id].cpu_number_++) % MAX_CPU_NUMBER_PER_CORE] = cpu_id;
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_ERROR("too many cores", K(core_number_), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      int64_t j = 0;
      int64_t n = 0;
      for (int64_t i = 0; i < core_number_; i++) {
        j = 0;
        n = cores_[i].cpu_number_;
        for (j = 0; j < n; j++) {
          _LOG_INFO("core_id:%3ld => cpu_id:%3ld", i, cores_[i].cpues_[j]);
        }
      }
    }
  }

  if (NULL != fp) {
    pclose(fp);
    fp = NULL;
  }

  return ret;
}

int64_t ObCpuTopology::get_core_number() const
{
  return core_number_;
}

int64_t ObCpuTopology::get_cpu_number() const
{
  return cpu_number_;
}

ObCpuTopology::CoreInfo *ObCpuTopology::get_core_info(const int64_t core_id)
{
  CoreInfo *core_info = NULL;
  if (core_id < core_number_) {
    core_info = &cores_[core_id];
  }
  return core_info;
}

int ObCpuTopology::bind_cpu(const int64_t cpu_id, const pthread_t thread_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCpuTopology not init", K(ret));
  } else if (OB_UNLIKELY(cpu_id < 0) || OB_UNLIKELY(thread_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(thread_id), K(ret));
  } else {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);
    if (OB_UNLIKELY(0 != pthread_setaffinity_np(thread_id, sizeof(cpu_set_t), &cpuset))) {
      ret = ob_get_sys_errno();
      LOG_WARN("fail to pthread_setaffinity_np", K(cpu_id), K(thread_id), K(ret));
    } else {
      LOG_INFO("success to bind_cpu", K(cpu_id), K(thread_id));
    }
  }
  return ret;
}

} // end of obproxy
} // end of oeanbase
