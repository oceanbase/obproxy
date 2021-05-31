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

#ifndef _OB_TENANT_ALLOCATOR_H_
#define _OB_TENANT_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/queue/ob_link.h"
#include "lib/alloc/object_mgr.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace lib
{
class ObTenantAllocator
    : public common::ObIAllocator,
      private common::ObLink
{
public:
  explicit ObTenantAllocator(uint64_t tenant_id)
      : tenant_id_(tenant_id), has_deleted_(false), obj_mgr_(*this),
        limit_(INT64_MAX), hold_bytes_(0), last_print_timestamp_(0)
  {}

  uint64_t get_tenant_id()
  {
    return tenant_id_;
  }
  void set_tenant_deleted()
  {
    has_deleted_ = true;
  }
  bool has_tenant_deleted()
  {
    return has_deleted_;
  }
  inline ObTenantAllocator *&get_next()
  {
    return reinterpret_cast<ObTenantAllocator*&>(next_);
  }

  virtual void *alloc(const int64_t size)
  {
    return alloc(size, ObMemAttr());
  }

  virtual void *alloc(const int64_t size, const ObMemAttr &attr)
  {
    abort_unless(attr.tenant_id_ == tenant_id_);
    const int64_t now = common::ObTimeUtility::current_time();
    void *ptr = NULL;
    if (attr.prio_ == OB_HIGH_ALLOC
        || CHUNK_MGR.get_hold() <= CHUNK_MGR.get_limit()) {
      AObject *obj = obj_mgr_.alloc_object(size, attr);
      if (NULL != obj) {
        ptr = obj->data_;
      }
    }

    if (OB_ISNULL(ptr)) {
      if (now - last_print_timestamp_ > 10 * 1000000) {
        last_print_timestamp_ = now;
        print_usage();
      }
    } else {
      if (now - last_print_timestamp_ > 100 * 1000000) {
        last_print_timestamp_ = now;
        print_usage();
      }
    }

    return ptr;
  }

  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
  {
    void *nptr = NULL;
    AObject *obj = NULL;
    if (NULL != ptr) {
      obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
      abort_unless(obj->is_valid());
      abort_unless(obj->in_use_);
      abort_unless(obj->block()->is_valid());
      abort_unless(obj->block()->in_use_);
    }
    obj = obj_mgr_.realloc_object(obj, size, attr);
    if (obj != NULL) {
      nptr = obj->data_;
    }
    return nptr;
  }

  virtual void free(void *ptr)
  {
    if (NULL != ptr) {
      AObject *obj = reinterpret_cast<AObject*>((char*)ptr - AOBJECT_HEADER_SIZE);
      abort_unless(NULL != obj);
      abort_unless(obj->MAGIC_CODE_ == AOBJECT_MAGIC_CODE
                   || obj->MAGIC_CODE_ == BIG_AOBJECT_MAGIC_CODE);
      abort_unless(obj->in_use_);

      ABlock *block = obj->block();
      abort_unless(block->check_magic_code());
      abort_unless(block->in_use_);
      abort_unless(block->obj_set_ != NULL);

      ObjectSet *set = block->obj_set_;
      set->lock();
      set->free_object(obj);
      set->unlock();
    }
  }

  // statistic related
  void set_limit(int64_t bytes)
  {
    limit_ = bytes;
  }
  int64_t get_limit() const
  {
    return limit_;
  }
  inline int update_hold(int64_t bytes, const ObMemAttr &attr)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(attr);

    if (bytes <= 0) {
      IGNORE_RETURN ATOMIC_AAF(&hold_bytes_, bytes);
    } else if (hold_bytes_ + bytes <= limit_) {
      const int64_t nvalue = ATOMIC_AAF(&hold_bytes_, bytes);
      if (nvalue > limit_) {
        IGNORE_RETURN ATOMIC_AAF(&hold_bytes_, -bytes);
        ret = common::OB_EXCEED_MEM_LIMIT;
      }
    } else {
      ret = common::OB_EXCEED_MEM_LIMIT;
    }
    return ret;
  }
  int64_t get_hold() const
  {
    return hold_bytes_;
  }
  common::ObModItem get_mod_usage(int mod_id)
  {
    return obj_mgr_.get_mod_usage(mod_id);
  }

  void print_memory_usage() const { print_usage(); }

private:
  void print_usage() const;

private:
  uint64_t tenant_id_;
  bool has_deleted_;
  ObjectMgr<32> obj_mgr_;

  volatile int64_t limit_;
  volatile int64_t hold_bytes_;
  volatile int64_t last_print_timestamp_;
}; // end of class ObTenantAllocator

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_TENANT_ALLOCATOR_H_ */
