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

#ifndef _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_
#define _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_

#include "lib/thread_local/ob_tsi_utils.h"
#include "lib/random/ob_random.h"
#include "lib/ob_define.h"
#include "object_set.h"

namespace oceanbase
{
namespace lib
{
class ObTenantAllocator;

template <int N>
class ObjectMgr
{
public:
  explicit ObjectMgr(ObTenantAllocator &allocator);

  AObject *alloc_object(uint64_t size, const ObMemAttr &attr);
  void free_object(AObject *block);
  AObject *realloc_object(
      AObject *obj, const uint64_t size, const ObMemAttr &attr);
  void reset();

  common::ObModItem get_mod_usage(int mod_id) const;
  void print_usage() const;

private:
  ObjectSet sets_[N];
  common::ObRandom rand_;
}; // end of class ObjectMgr

template <int N>
ObjectMgr<N>::ObjectMgr(ObTenantAllocator &allocator)
{
  for (int i = 0; i < N; ++i) {
    sets_[i].set_tenant_allocator(allocator);
  }
}

template <int N>
AObject *ObjectMgr<N>::alloc_object(uint64_t size, const ObMemAttr &attr)
{
  AObject *obj = NULL;
  bool found = false;
  const uint64_t start = common::get_itid();
  for (uint64_t i = 0; NULL == obj && i < N && !found; i++) {
    uint64_t idx = (start + i) % N;
    if (sets_[idx].trylock()) {
      obj = sets_[idx].alloc_object(size, attr);
      sets_[idx].unlock();
      found = true;
      break;
    }
  }
  if (!found && NULL == obj) {
    const uint64_t idx = start % N;
    sets_[idx].lock();
    obj = sets_[idx].alloc_object(size, attr);
    sets_[idx].unlock();
  }
  return obj;
}

template <int N>
void ObjectMgr<N>::free_object(AObject *obj)
{
  abort_unless(NULL != obj);
  abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
               || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
  abort_unless(obj->in_use_);

  ABlock *block = obj->block();

  abort_unless(block->check_magic_code());
  abort_unless(block->in_use_);
  abort_unless(block->obj_set_ != NULL);

  ObjectSet *set = block->obj_set_;
  abort_unless(set);
  set->lock();
  set->free_object(obj);
  set->unlock();
}

template <int N>
AObject *ObjectMgr<N>::realloc_object(
    AObject *obj, const uint64_t size, const ObMemAttr &attr)
{
  AObject *new_obj = NULL;

  if (NULL != obj) {
    abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                 || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);

    ABlock *block = obj->block();

    abort_unless(block->check_magic_code());
    abort_unless(block->in_use_);
    abort_unless(block->obj_set_ != NULL);

    ObjectSet *set = block->obj_set_;
    abort_unless(set);
    if (set != NULL) {
      set->lock();
      new_obj = set->realloc_object(obj, size, attr);
      set->unlock();
    }
  } else {
    new_obj = alloc_object(size, attr);
  }

  return new_obj;
}

template <int N>
void ObjectMgr<N>::reset()
{
  for (int i = 0; i < N; ++i) {
    sets_[i].lock();
    sets_[i].reset();
    sets_[i].unlock();
  }
}

template <int N>
common::ObModItem ObjectMgr<N>::get_mod_usage(int mod_id) const
{
  common::ObModItem item;
  common::ObModItem one_item;
  for (int i = 0; i < N; ++i) {
    one_item = sets_[i].get_mod_usage(mod_id);
    item += one_item;
    one_item.reset();
  }
  return item;
}

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOC_OBJECT_MGR_H_ */
