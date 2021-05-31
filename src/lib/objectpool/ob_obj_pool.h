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

#ifndef __COMMON_OB_OBJ_POOL__
#define __COMMON_OB_OBJ_POOL__

#include "lib/list/ob_list.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread_local/ob_tl_store.h"

namespace oceanbase
{
namespace common
{
template <typename T>
class ObObjPool
{
public:
  explicit ObObjPool(int32_t mod);
  ~ObObjPool();
  int init();
  void destroy();
  T *alloc();
  int free(T *obj);
private:
  ObTlStore<common::ObList<T *> > list_;
  common::PageArena<char> allocator_;
  common::ObSpinLock allocator_lock_;
};

template <typename T>
ObObjPool<T>::ObObjPool(int32_t mod)
{
  allocator_.set_mod_id(mod);
}
template <typename T>
int ObObjPool<T>::init()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = list_.init())) {
    _OB_LOG(WARN, "ObObjPool init failed, ret=%d", ret);
  }
  return ret;
}
template <typename T>
void ObObjPool<T>::destroy()
{
  list_.destroy();
}
template <typename T>
ObObjPool<T>::~ObObjPool()
{
  destroy();
}
template <typename T>
T *ObObjPool<T>::alloc()
{
  T *ret = NULL;
  int err = OB_SUCCESS;
  {
    if (list_.get() != NULL) {
      if (OB_SUCCESS == (err = list_.get()->pop_front(ret))) {
        // nothing
      }
    }
  }
  if (OB_SUCCESS != err) {
    common::ObSpinLockGuard guard(allocator_lock_);
    void *buffer = allocator_.alloc(sizeof(T));
    if (NULL != buffer) {
      ret = new(buffer) T();
    }
  }
  return ret;
}
template <typename T>
int ObObjPool<T>::free(T *obj)
{
  int ret = OB_SUCCESS;
  if (list_.get() != NULL) {
    if (OB_SUCCESS != (ret = list_.get()->push_back(obj))) {
      _OB_LOG(INFO, "push to list failed free failed, ret=%d", ret);
    }
  }
  return ret;
}
}
}
#endif
