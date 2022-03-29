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
 *
 */

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_event_system.h"
#include <unistd.h>
#include "utils/ob_cpu_affinity.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObEventProcessor g_event_processor;

int ObEventProcessor::spawn_event_threads(
    const int64_t thread_count, const char *et_name,
    const int64_t stacksize, ObEventThreadType &etype)
{
  int ret = OB_SUCCESS;
  char thr_name[MAX_THREAD_NAME_LENGTH];
  ObEventThreadType new_thread_group_id = MAX_EVENT_TYPES;
  etype = MAX_EVENT_TYPES;

  if (OB_UNLIKELY((event_thread_count_ + thread_count) > MAX_EVENT_THREADS) || OB_UNLIKELY(thread_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(thread_count), K(event_thread_count_), K(MAX_EVENT_THREADS), K(ret));
  } else if (OB_UNLIKELY(thread_group_count_ >= MAX_EVENT_TYPES)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(thread_group_count_), K(MAX_EVENT_TYPES), K(ret));
  } else if (OB_ISNULL(et_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters, et_name is null", K(ret));
  } else if (OB_UNLIKELY(stacksize < 0)) {//when equal to 0, use the default thread size 8M;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(stacksize), K(ret));
  } else {
    new_thread_group_id = (ObEventThreadType)thread_group_count_;

    ObEThread *t = NULL;
    for (int64_t i = 0; i < thread_count && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(t = new(std::nothrow) ObEThread(REGULAR, event_thread_count_ + i))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to allocator memory for REGULAR ObEThread", K(i), K(thread_count), K(ret));
      } else if (OB_FAIL(t->init())) {
        LOG_WARN("fail to init event", K(i), K(ret));
        delete t;
        t = NULL;
      } else {
        all_event_threads_[event_thread_count_ + i] = t;
        event_thread_[new_thread_group_id][i] = t;
        t->set_event_thread_type(new_thread_group_id);
      }
    }

    if (OB_SUCC(ret)) {
      thread_count_for_type_[new_thread_group_id] = thread_count;
      int32_t length = 0;
      for (int64_t i = 0; i < thread_count && OB_SUCC(ret); ++i) {
        length = snprintf(thr_name, sizeof(thr_name), "[%s %ld]", et_name, i);
        if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= static_cast<int32_t>(sizeof(thr_name)))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail format thread name", K(length), K(ret));
        } else if (OB_FAIL(event_thread_[new_thread_group_id][i]->start(thr_name, stacksize))) {
          LOG_WARN("fail to start event thread", K(new_thread_group_id), K(i), K(thread_count), K(ret));
        } else {/*do nothing*/}
      }
      ++thread_group_count_;
      event_thread_count_ += thread_count;
    }
  }

  if (OB_SUCC(ret)) {
    etype = new_thread_group_id;
    LOG_DEBUG("succ to creat thread group", K(et_name), K(thread_count),
                    K(new_thread_group_id));
  }
  return ret;
}

inline int ObEventProcessor::init_one_event_thread(const int64_t index)
{
  int ret = OB_SUCCESS;
  ObEThread *t = NULL;

  if (OB_ISNULL(t = new(std::nothrow) ObEThread(REGULAR, index))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocator memory for REGULAR ObEThread", K(index), K(ret));
  } else if (OB_FAIL(t->init())) {
    LOG_WARN("fail to init ObEThread", K(index), K(ret));
    delete t;
    t = NULL;
  } else {
    if (0 == index) {
      this_thread() = t;
      global_mutex = t->mutex_;
      t->cur_time_ = get_hrtime_internal();
    }
    if (OB_SUCC(ret)) {
      all_event_threads_[index] = t;
      event_thread_[ET_CALL][index] = t;
      t->set_event_thread_type(ET_CALL);
    }
  }
  return ret;
}

int64_t ObEventProcessor::get_cpu_count()
{
  int64_t cpu_num = -1;
  int64_t cpu_conf_num = -1;
  int64_t cpu_onln_num = -1;

  cpu_conf_num = sysconf(_SC_NPROCESSORS_CONF);
  cpu_onln_num = sysconf(_SC_NPROCESSORS_ONLN);

  cpu_num = std::min(cpu_conf_num, cpu_onln_num);

  cpu_num = std::min(cpu_num, MAX_THREADS_IN_EACH_TYPE);
  if (OB_UNLIKELY(cpu_num <= 0)) {
    LOG_WARN("fail to get cpu num", K(cpu_num), K(cpu_conf_num), K(cpu_onln_num), K(MAX_THREADS_IN_EACH_TYPE));
  } else {
    LOG_INFO("get cpu num succeed", K(cpu_num), K(cpu_conf_num), K(cpu_onln_num), K(MAX_THREADS_IN_EACH_TYPE));
  }
  return cpu_num;
}

int ObEventProcessor::start(const int64_t net_thread_count, const int64_t stacksize,
    const bool enable_cpu_topology/*false*/, const bool automatic_match_work_thread/*true*/)
{
  int ret = OB_SUCCESS;
  char thr_name[MAX_THREAD_NAME_LENGTH];
  ObCpuTopology *cpu_topology = NULL;
  int64_t cpu_num = -1;

  if (OB_UNLIKELY(started_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start event processor twice", K(ret));
  } else if (OB_UNLIKELY(net_thread_count <= 0) || OB_UNLIKELY(net_thread_count > MAX_EVENT_THREADS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(net_thread_count), K(ret));
  } else if (OB_UNLIKELY(stacksize < 0)) {//when equal to 0, use the default thread size 8M;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(stacksize), K(ret));
  } else {

    if (!automatic_match_work_thread
        || OB_UNLIKELY((cpu_num = get_cpu_count()) <= 0)
        || cpu_num > net_thread_count) {
      cpu_num = net_thread_count;
    }

    event_thread_count_ = cpu_num;
    thread_group_count_ = 1;

    for (int64_t i = 0; i < cpu_num && OB_SUCC(ret); ++i) {
      if (OB_FAIL(init_one_event_thread(i))) {
        LOG_WARN("fail to init ObEThread", K(i), K(cpu_num), K(ret));
      }
    }

    bool bind_cpu = false;
    ObCpuTopology::CoreInfo *core_info = NULL;
    int64_t core_number = 0;
    int64_t cpu_number = 0;
    if (OB_SUCC(ret)) {
      if (enable_cpu_topology) {
        if (OB_ISNULL(cpu_topology = new (std::nothrow) ObCpuTopology())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          PROXY_NET_LOG(WARN, "fail to new ObCpuTopology", K(ret));
        } else if (OB_FAIL(cpu_topology->init())) {
          PROXY_NET_LOG(WARN, "fail to init cpu_topology", K(ret));
        } else {
          core_number = cpu_topology->get_core_number();
          cpu_number = cpu_topology->get_cpu_number();

          if (core_number > 0 && (core_number >= event_thread_count_ || 0 == (event_thread_count_ % cpu_number))) {
            bind_cpu = true;
            PROXY_NET_LOG(INFO, "we will bind cpu to work thread", K(core_number), K(cpu_number), K_(event_thread_count));
          } else {
            PROXY_NET_LOG(INFO, "we can't bind cpu to work thread", K(core_number), K(cpu_number), K_(event_thread_count));
          }
        }

        if (OB_FAIL(ret)) {
          // although CPU binding fails, we can continue to run
          bind_cpu = false;
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret)) {
      thread_count_for_type_[ET_CALL] = cpu_num;

      int64_t core_id = -1;
      int64_t cpu_id = -1;

      for (int64_t i = 0; i < event_thread_count_ && OB_SUCC(ret); ++i) {
        int32_t length = snprintf(thr_name, sizeof(thr_name), "[ET_NET %ld]", i);
        if (OB_UNLIKELY(length <= 0) || OB_UNLIKELY(length >= static_cast<int32_t>(sizeof(thr_name)))) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("fail format thread name", K(length), K(ret));
        } else if ((0 != i) && OB_FAIL(all_event_threads_[i]->start(thr_name, stacksize))) {
          LOG_WARN("fail to start event thread", K(thr_name), K(ret));
        } else {
          if (bind_cpu) {
            core_id = i % core_number;
            if (OB_ISNULL(core_info = cpu_topology->get_core_info(core_id))) {
              ret = OB_ENTRY_NOT_EXIST;
              LOG_WARN("fail to get core_info", K(core_info), K(core_id), K(ret));
            } else if (OB_UNLIKELY(core_info->cpu_number_ <= 0)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get core_info", K(core_info->cpu_number_), K(core_id), K(ret));
            } else {
              cpu_id =  (i / core_number) % (core_info->cpu_number_);
              if (0 == i) {
                if (OB_FAIL(cpu_topology->bind_cpu(core_info->cpues_[cpu_id], pthread_self()))) {
                  LOG_WARN("fail to bind_cpu", K(core_id), K(cpu_id), "thread_id", pthread_self());
                }
              } else {
                if (OB_FAIL(cpu_topology->bind_cpu(core_info->cpues_[cpu_id], all_event_threads_[i]->tid_))) {
                  LOG_WARN("fail to bind_cpu", K(core_id), K(cpu_id), "thread_id", all_event_threads_[i]->tid_);
                }
              }
            }
            if (OB_FAIL(ret)) {
              // although CPU binding fails, we can continue to run
              ret = OB_SUCCESS;
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    started_ = true;
    LOG_INFO("succ to start event thread group id", K(cpu_num), K(stacksize));
  }

  if (NULL != cpu_topology) {
    delete cpu_topology;
    cpu_topology = NULL;
  }

  return ret;
}

ObEvent *ObEventProcessor::spawn_thread(
    ObContinuation *cont, const char *thr_name, const int64_t stacksize,
    ObDedicateThreadType dedicate_thread_type)
{
  int ret = OB_SUCCESS;
  ObEvent *event = NULL;
  if (OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters, ObContinuation is NULL", K(ret));
  } else if (OB_ISNULL(thr_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters, et_name is null", K(ret));
  } else if (OB_UNLIKELY(stacksize < 0)) {//when equal to 0, use the default thread size 8M;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(stacksize), K(ret));
  } else if (OB_UNLIKELY(dedicate_thread_count_ >= MAX_EVENT_THREADS)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parameters", K(dedicate_thread_count_), K(ret));
  } else {
    if (OB_ISNULL(event = op_reclaim_alloc(ObEvent))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory for event", K(ret));
    } else if (OB_FAIL(event->init(*cont, 0, 0))) {
      LOG_WARN("fail init ObEvent", K(*event), K(ret));
    } else if (OB_ISNULL(all_dedicate_threads_[dedicate_thread_count_] = new(std::nothrow) ObEThread(DEDICATED, event))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to allocate memory for ethread, DEDICATED");
    } else if (OB_FAIL(all_dedicate_threads_[dedicate_thread_count_]->init())) {
      LOG_ERROR("fail to init ObEThread, DEDICATED");
      delete all_dedicate_threads_[dedicate_thread_count_];
      all_dedicate_threads_[dedicate_thread_count_] = NULL;
    } else {
      all_dedicate_threads_[dedicate_thread_count_]->set_dedicate_type(dedicate_thread_type);
      all_dedicate_threads_[dedicate_thread_count_]->id_ = dedicate_thread_count_;
      event->ethread_ = all_dedicate_threads_[dedicate_thread_count_];
      event->continuation_->mutex_ = all_dedicate_threads_[dedicate_thread_count_]->mutex_;
      event->mutex_ = event->continuation_->mutex_;
      if (OB_FAIL(all_dedicate_threads_[dedicate_thread_count_]->start(thr_name, stacksize))) {
        LOG_WARN("fail to start event thread, DEDICATED", K(dedicate_thread_count_), K(ret));
      }
      ++dedicate_thread_count_;
    }

    if (OB_FAIL(ret) && OB_LIKELY(NULL != event)) {
      event->free();
      event = NULL;
    }
  }
  return event;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
