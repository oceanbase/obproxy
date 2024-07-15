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

#ifndef OBPROXY_THREAD_ALLOCATOR_H
#define OBPROXY_THREAD_ALLOCATOR_H

#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

static const int64_t g_thread_freelist_high_watermark       = 512;
static const int64_t g_thread_freelist_low_watermark        = 32;
static const int64_t g_thread_freelist_low_watermark_for_8k = 0;
static const int64_t size_8k                                = 8 * 1024;

struct ObProxyThreadAllocator
{
  ObProxyThreadAllocator() : allocated_(0), freelist_(NULL) { }
  ~ObProxyThreadAllocator() { }

  int64_t allocated_;
  void *freelist_;
};

template<class T>
inline T *thread_alloc(ObProxyThreadAllocator &l,
                       void (*init_func) (T &proto, T &instance) = NULL)
{
  T *ret = NULL;
  common::ObSparseClassAllocator<T> *allocator =
      common::ObSparseClassAllocator<T>::get(init_func, common::OPNum<T>::LOCAL_NUM);

  if (OB_LIKELY(NULL != l.freelist_) && OB_LIKELY(NULL != allocator)) {
    ret = reinterpret_cast<T *>(l.freelist_);
    l.freelist_ = *reinterpret_cast<T **>(l.freelist_);
    --(l.allocated_);
    *reinterpret_cast<void **>(ret) = *reinterpret_cast<void **>(&(allocator->proto_.type_object_));
  }
  if (OB_ISNULL(ret) && OB_LIKELY(NULL != allocator)) {
    ret = allocator->alloc();
  }
  return ret;
}

template<class T>
inline T *thread_alloc_init(ObProxyThreadAllocator &l,
                            void (*init_func) (T &proto, T &instance) = NULL)
{
  T *ret = NULL;
  common::ObSparseClassAllocator<T> *allocator =
      common::ObSparseClassAllocator<T>::get(init_func, common::OPNum<T>::LOCAL_NUM);

  if (OB_LIKELY(NULL != l.freelist_) && OB_LIKELY(NULL != allocator)) {
    ret = reinterpret_cast<T *>(l.freelist_);
    l.freelist_ = *reinterpret_cast<T **>(l.freelist_);
    --(l.allocated_);
    if (NULL == init_func) {
      memcpy(ret, &allocator->proto_.type_object_, sizeof(T));
    } else {
      (*init_func) (allocator->proto_.type_object_, *ret);
    }
  }
  if (OB_ISNULL(ret) && OB_LIKELY(NULL != allocator)) {
    ret = allocator->alloc();
  }
  return ret;
}

inline void *thread_alloc_void(common::ObFixedMemAllocator &a, ObProxyThreadAllocator &l)
{
  void *ret = NULL;
  if (OB_LIKELY(NULL != l.freelist_)) {
    ret = reinterpret_cast<void *>(l.freelist_);
    l.freelist_ = *reinterpret_cast<void **>(l.freelist_);
    --(l.allocated_);
  }
  if (OB_ISNULL(ret)) {
    ret = a.alloc_void();
  }
  return ret;
}

template<class T>
inline void thread_freeup(ObProxyThreadAllocator &l)
{
  T *v = NULL;
  common::ObSparseClassAllocator<T> *allocator =
      common::ObSparseClassAllocator<T>::get(NULL, common::OPNum<T>::LOCAL_NUM);

  while (NULL != l.freelist_ && l.allocated_ > g_thread_freelist_low_watermark && OB_LIKELY(NULL != allocator)) {
    v = reinterpret_cast<T *>(l.freelist_);
    l.freelist_ = *reinterpret_cast<T **>(l.freelist_);
    --(l.allocated_);
    *reinterpret_cast<void **>(v) = *reinterpret_cast<void **>(&(allocator->proto_.type_object_));
    allocator->free(v);
    v = NULL;
  }
}

inline void thread_freeup_void(common::ObFixedMemAllocator &a, ObProxyThreadAllocator &l)
{
  void *v = NULL;
  int64_t low_watermark = g_thread_freelist_low_watermark;
  if (a.get_obj_size() == size_8k) {
    low_watermark = g_thread_freelist_low_watermark_for_8k;
  }

  while (NULL != l.freelist_ && l.allocated_ > low_watermark) {
    v = reinterpret_cast<void *>(l.freelist_);
    l.freelist_ = *reinterpret_cast<void **>(l.freelist_);
    --(l.allocated_);
    a.free_void(v);
  }
}

#define op_thread_alloc(type, freelist, init_func) event::thread_alloc<type>(freelist, init_func)
#define op_thread_alloc_init(type, freelist, init_func) event::thread_alloc_init<type>(freelist, init_func)

#define op_thread_free(type, p, freelist) do {                 \
  *reinterpret_cast<void **>(p) = reinterpret_cast<void *>(freelist.freelist_); \
  freelist.freelist_ = p;                                      \
  ++(freelist.allocated_);                                     \
  if (freelist.allocated_ > g_thread_freelist_high_watermark)  \
  {                                                            \
    event::thread_freeup<type>(freelist);                      \
  }                                                            \
} while (0)

#define op_thread_fixed_mem_alloc(a, freelist) event::thread_alloc_void(a, freelist)
#define op_thread_fixed_mem_free(a, p, freelist) do {                 \
  *reinterpret_cast<void **>(p) = reinterpret_cast<void *>(freelist.freelist_); \
  freelist.freelist_ = p;                                      \
  ++(freelist.allocated_);                                     \
  if (freelist.allocated_ > g_thread_freelist_high_watermark)  \
  {                                                            \
    event::thread_freeup_void(a, freelist);                    \
  }                                                            \
} while (0)

struct ObThreadAllocator
{
  ObProxyThreadAllocator mio_allocator_;
  ObProxyThreadAllocator io_block_allocator_;
  ObProxyThreadAllocator io_data_allocator_;
  ObProxyThreadAllocator block_8k_allocator_;
  ObProxyThreadAllocator sm_allocator_;
  ObProxyThreadAllocator rpc_sm_allocator_;
  ObProxyThreadAllocator ob_rpc_req_allocator_;
  ObProxyThreadAllocator rpc_request_handle_sm_allocator_;
};

inline ObThreadAllocator &get_thread_allocator()
{
  static __thread ObThreadAllocator *thread_allocator = NULL;
  if (OB_ISNULL(thread_allocator)
      && OB_ISNULL(thread_allocator = new (std::nothrow) ObThreadAllocator())) {
    OB_LOG(EDIAG, "failed to new ObThreadAllocator");
  }
  return *thread_allocator;
}

inline ObProxyThreadAllocator &get_mio_allocator()
{
  return get_thread_allocator().mio_allocator_;
}

inline ObProxyThreadAllocator &get_io_block_allocator()
{
  return get_thread_allocator().io_block_allocator_;
}

inline ObProxyThreadAllocator &get_io_data_allocator()
{
  return get_thread_allocator().io_data_allocator_;
}

inline ObProxyThreadAllocator &get_8k_block_allocator()
{
  return get_thread_allocator().block_8k_allocator_;
}

inline ObProxyThreadAllocator &get_sm_allocator()
{
  return get_thread_allocator().sm_allocator_;
}

inline ObProxyThreadAllocator &get_rpc_sm_allocator()
{
  return get_thread_allocator().rpc_sm_allocator_;
}

inline ObProxyThreadAllocator &get_rpc_req_allocator()
{
  return get_thread_allocator().ob_rpc_req_allocator_;
}

inline ObProxyThreadAllocator &get_rpc_request_handle_sm_allocator()
{
  return get_thread_allocator().rpc_request_handle_sm_allocator_;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_THREAD_ALLOCATOR_H
