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

#define USING_LOG_PREFIX LIB

#include "ob_malloc_allocator.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/alloc/object_set.h"
#include "lib/allocator/ob_mem_leak_checker.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

ObMallocAllocator *ObMallocAllocator::instance_ = NULL;

ObMallocAllocator::ObMallocAllocator()
    : locks_(), allocators_(), reserved_(0), urgent_(0), disabled_mod_id_(-1)
{
}

ObMallocAllocator::~ObMallocAllocator()
{}

int ObMallocAllocator::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tenant_allocator(OB_SYS_TENANT_ID))) {
  } else if (OB_FAIL(create_tenant_allocator(OB_SERVER_TENANT_ID))) {
  } else {
    allocators_[OB_SYS_TENANT_ID]->set_limit(DEFAULT_MAX_SYS_MEMORY);
    allocators_[OB_SERVER_TENANT_ID]->set_limit(INT64_MAX);
  }
  return ret;
}

void *ObMallocAllocator::alloc(const int64_t size)
{
  ObMemAttr attr;
  return alloc(size, attr);
}

void *ObMallocAllocator::alloc(
    const int64_t size, const oceanbase::lib::ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  ObIAllocator *allocator = NULL;
  
  if (OB_UNLIKELY(disabled_mod_id_ == attr.mod_id_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("mod is disabled alloc", K_(disabled_mod_id), K(attr.tenant_id_));
  } else if (OB_UNLIKELY(0 == attr.tenant_id_)
      || OB_UNLIKELY(INT64_MAX == attr.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid argument", K(lbt()), K(attr.tenant_id_), K(ret));
  } else if (OB_UNLIKELY(attr.tenant_id_ >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = attr.tenant_id_ % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    allocator = get_tenant_allocator(attr.tenant_id_);
    if (!OB_ISNULL(allocator)) {
      ptr = allocator->alloc(size, attr);
    }
  } else {
    allocator = allocators_[attr.tenant_id_];
    if (!OB_ISNULL(allocator)) {
      ptr = allocator->alloc(size, attr);
    }
  }
  // If there isn't tenant allocator, create one than re-allocate.
  if (OB_SUCC(ret) && OB_ISNULL(ptr) && OB_ISNULL(allocator)) {
    if (OB_FAIL(create_tenant_allocator(attr.tenant_id_))) {
      LOG_EDIAG("create tenant allocator fail");
    } else {
      if (OB_UNLIKELY(attr.tenant_id_ >= PRESERVED_TENANT_COUNT)) {
        const int64_t slot = attr.tenant_id_ % PRESERVED_TENANT_COUNT;
        obsys::CRLockGuard guard(locks_[slot]);
        allocator = get_tenant_allocator(attr.tenant_id_);
        if (NULL != allocator) {
          ptr = allocator->alloc(size, attr);
        }
      } else {
        allocator = allocators_[attr.tenant_id_];
        if (!OB_ISNULL(allocator)) {
          ptr = allocator->alloc(size, attr);
        }
      }
    }
  }
  return ptr;
}

void *ObMallocAllocator::realloc(
    const void *ptr, const int64_t size, const oceanbase::lib::ObMemAttr &attr)
{
  // Won't create tenant allocator!!
  void *nptr = NULL;
  if (OB_UNLIKELY(disabled_mod_id_ == attr.mod_id_)) {
    LOG_WDIAG("mod is disabled alloc", K_(disabled_mod_id), K(attr.tenant_id_));
  } else if (OB_UNLIKELY(attr.tenant_id_ >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = attr.tenant_id_ % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObIAllocator *allocator = get_tenant_allocator(attr.tenant_id_);
    if (NULL != allocator) {
      nptr = allocator->realloc(ptr, size, attr);
    }
  } else {
    ObIAllocator *allocator = allocators_[attr.tenant_id_];
    if (NULL != allocator) {
      nptr = allocator->realloc(ptr, size, attr);
    }
  }
  return nptr;;
}

void ObMallocAllocator::free(void *ptr)
{
  // directly free object instead of using tenant allocator.
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
    
    common::get_global_mem_leak_checker().on_free(obj->mod_id_, get_global_mod_set().get_mod_name(obj->mod_id_), ptr);
    ObjectSet *set = block->obj_set_;
    set->lock();
    set->free_object(obj);
    set->unlock();
  }
}

ObTenantAllocator *ObMallocAllocator::get_tenant_allocator(uint64_t tenant_id) const
{
  ObTenantAllocator *allocator = NULL;
  if (tenant_id < PRESERVED_TENANT_COUNT) {
    allocator = allocators_[tenant_id];
  } else {
    // TODO: lock slot
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    ObTenantAllocator * const *cur = &allocators_[slot];
    while (NULL != *cur && (*cur)->get_tenant_id() < tenant_id) {
      cur = &(*cur)->get_next();
    }
    if (NULL != *cur && (*cur)->get_tenant_id() == tenant_id) {
      allocator = *cur;
    }
  }
  return allocator;
}

int ObMallocAllocator::create_tenant_allocator(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (0 == tenant_id || INT64_MAX == tenant_id) {
    ret = OB_INVALID_ARGUMENT;    LOG_EDIAG("invalid argument", K(lbt()), K(tenant_id), K(ret));
    LOG_EDIAG("invalid argument", K(lbt()), K(tenant_id), K(ret));
 } else if (tenant_id < PRESERVED_TENANT_COUNT) {
    if (NULL == allocators_[tenant_id]) {
      ObMemAttr attr;
      attr.tenant_id_ = 0;
      void *buf = allocators_[0]->alloc(sizeof (ObTenantAllocator), attr);
      if (NULL != buf) {
        ObTenantAllocator *allocator = new (buf) ObTenantAllocator(tenant_id);
        bool bret = ATOMIC_BCAS(&allocators_[tenant_id], NULL, allocator);
        if (!bret) {
          allocator->~ObTenantAllocator();
          allocators_[0]->free(buf);
          // it's successful the same.
        }
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
  } else {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    if (NULL == allocators_[slot]) {
      ret = create_tenant_allocator(slot);
    }
    if (OB_SUCC(ret)) {
      obsys::CWLockGuard guard(locks_[slot]);
      ObTenantAllocator **cur = &allocators_[slot];
      while ((NULL != *cur) && (*cur)->get_tenant_id() < tenant_id) {
        cur = &((*cur)->get_next());
      }
      if (NULL != cur) {
        if (NULL == (*cur)
            || (*cur)->get_tenant_id() > tenant_id) {
          ObMemAttr attr;
          attr.tenant_id_ = 0;
          void *buf = allocators_[0]->alloc(sizeof (ObTenantAllocator), attr);
          if (NULL != buf) {
            *cur = new (buf) ObTenantAllocator(tenant_id);
          } else {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
        }
      }
    }
  }
  return ret;
}

int ObMallocAllocator::delete_tenant_allocator(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant_id);
  return ret;
}

int ObMallocAllocator::set_root_allocator(ObTenantAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!OB_ISNULL(allocators_[0])) {
    ret = OB_ENTRY_EXIST;
  } else {
    allocators_[0] = allocator;
  }
  return ret;
}

ObMallocAllocator *ObMallocAllocator::get_instance()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ObMallocAllocator::instance_)) {
    ObMallocAllocator *malloc_allocator = NULL;
    ObTenantAllocator *allocator = new (std::nothrow) ObTenantAllocator(0);
    if (!OB_ISNULL(allocator)) {
      allocator->set_limit(1L << 30);
      ObMemAttr attr;
      attr.tenant_id_ = 0;
      void *buf = allocator->alloc(sizeof (ObMallocAllocator), attr);
      if (!OB_ISNULL(buf)) {
        malloc_allocator = new (buf) ObMallocAllocator();
        ret = malloc_allocator->set_root_allocator(allocator);
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }

    if (OB_FAIL(ret)) {
      if (!OB_ISNULL(malloc_allocator)) {
        malloc_allocator->~ObMallocAllocator();
        allocator->free(malloc_allocator);
        malloc_allocator = NULL;
      }
      if (!OB_ISNULL(allocator)) {
        delete allocator;
        allocator = NULL;
      }
    } else {
      ObMallocAllocator::instance_ = malloc_allocator;
    }
  }
  return ObMallocAllocator::instance_;
}

int ObMallocAllocator::set_tenant_limit(uint64_t tenant_id, int64_t bytes)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObTenantAllocator *allocator = get_tenant_allocator(tenant_id);
    if (!OB_ISNULL(allocator)) {
      allocator->set_limit(bytes);
    } else {
      ret = OB_TENANT_NOT_EXIST;
    }
  } else {
    ObTenantAllocator *allocator = allocators_[tenant_id];
    if (!OB_ISNULL(allocator)) {
      allocator->set_limit(bytes);
    } else {
      ret = OB_TENANT_NOT_EXIST;
    }
  }
  return ret;
}

int64_t ObMallocAllocator::get_tenant_limit(uint64_t tenant_id) const
{
  int64_t limit = 0;
  if (OB_UNLIKELY(tenant_id >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObTenantAllocator *allocator = get_tenant_allocator(tenant_id);
    if (!OB_ISNULL(allocator)) {
      limit = allocator->get_limit();
    }
  } else {
    ObTenantAllocator *allocator = allocators_[tenant_id];
    if (!OB_ISNULL(allocator)) {
      limit = allocator->get_limit();
    }
  }
  return limit;
}

int64_t ObMallocAllocator::get_tenant_hold(uint64_t tenant_id) const
{
  int64_t hold = 0;
  if (OB_UNLIKELY(tenant_id >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObTenantAllocator *allocator = get_tenant_allocator(tenant_id);
    if (!OB_ISNULL(allocator)) {
      hold = allocator->get_hold();
    }
  } else {
    ObTenantAllocator *allocator = allocators_[tenant_id];
    if (!OB_ISNULL(allocator)) {
      hold = allocator->get_hold();
    }
  }
  return hold;
}

void ObMallocAllocator::get_tenant_mod_usage(
    uint64_t tenant_id, int mod_id, ObModItem &item) const
{
  if (OB_UNLIKELY(tenant_id >= PRESERVED_TENANT_COUNT)) {
    const int64_t slot = tenant_id % PRESERVED_TENANT_COUNT;
    obsys::CRLockGuard guard(locks_[slot]);
    ObTenantAllocator *allocator = get_tenant_allocator(tenant_id);
    if (!OB_ISNULL(allocator)) {
      item = allocator->get_mod_usage(mod_id);
    }
  } else {
    ObTenantAllocator *allocator = allocators_[tenant_id];
    if (!OB_ISNULL(allocator)) {
      item = allocator->get_mod_usage(mod_id);
    }
  }
}

void ObMallocAllocator::print_tenant_memory_usage(uint64_t tenant_id) const
{
  ObTenantAllocator *allocator = get_tenant_allocator(tenant_id);
  if (OB_LIKELY(NULL != allocator)) {
    allocator->print_memory_usage();
  }
}

int64_t ObMallocAllocator::get_mod_dist(
    int mod_id, oceanbase::lib::ObTenantMemory tenant_memory[], int64_t count)
{
  int64_t idx = 0;
  for (uint64_t i = 0; idx < count && i < PRESERVED_TENANT_COUNT; ++i) {
    if (OB_ISNULL(allocators_[i])) {
    } else if (OB_ISNULL(allocators_[i]->get_next())) {
      if (allocators_[i]->get_tenant_id() != 0) {
        tenant_memory[idx].tenant_id_ = allocators_[i]->get_tenant_id();
        tenant_memory[idx].limit_ = allocators_[i]->get_limit();
        tenant_memory[idx].hold_ = allocators_[i]->get_hold();
        tenant_memory[idx].item_ = allocators_[i]->get_mod_usage(mod_id);
        idx++;
      }
    } else {
      obsys::CRLockGuard guard(locks_[i]);
      ObTenantAllocator *allocator = allocators_[i];
      while (!OB_ISNULL(allocator) && idx < count) {
        if (allocator->get_tenant_id() != 0) {
          tenant_memory[idx].tenant_id_ = allocator->get_tenant_id();
          tenant_memory[idx].limit_ = allocator->get_limit();
          tenant_memory[idx].hold_ = allocator->get_hold();
          tenant_memory[idx].item_ = allocator->get_mod_usage(mod_id);
          idx++;
        }
        allocator = allocator->get_next();
      }
    }
  }
  return idx;
}

void ObMallocAllocator::set_urgent(int64_t bytes)
{
  CHUNK_MGR.set_urgent(bytes);
}

int64_t ObMallocAllocator::get_urgent() const
{
  return CHUNK_MGR.get_urgent();
}

void ObMallocAllocator::set_reserved(int64_t bytes)
{
  reserved_ = bytes;
}

int64_t ObMallocAllocator::get_reserved() const
{
  return reserved_;
}
