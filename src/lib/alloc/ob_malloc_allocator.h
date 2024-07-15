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

#ifndef _OB_MALLOC_ALLOCATOR_H_
#define _OB_MALLOC_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/alloc/ob_tenant_allocator.h"
#include "lib/alloc/alloc_func.h"
#include "lib/lock/tbrwlock.h"

namespace oceanbase
{
namespace common
{
class ObModItem;
} // end of namespace common

namespace lib
{
struct ObTenantMemory
{
  uint64_t tenant_id_;
  int64_t limit_;
  int64_t hold_;
  common::ObModItem item_;
};

// It's the implement of ob_malloc/ob_free/ob_realloc interface.  The
// class separates allocator for each tenant thus tenant vaolates each
// other.
class ObMallocAllocator
    : public common::ObIAllocator
{
  static const uint64_t PRESERVED_TENANT_COUNT = 10000;

public:
  ObMallocAllocator();
  virtual ~ObMallocAllocator();

  int init();

  void *alloc(const int64_t size);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  void *realloc(const void *ptr, const int64_t size, const ObMemAttr &attr);
  void free(void *ptr);

  int set_root_allocator(ObTenantAllocator *allocator);
  static ObMallocAllocator *get_instance();

  int create_tenant_allocator(uint64_t tenant_id);
  int delete_tenant_allocator(uint64_t tenant_id);

  // statistic relating
  void set_urgent(int64_t bytes);
  int64_t get_urgent() const;
  void set_reserved(int64_t bytes);
  int64_t get_reserved() const;
  int set_tenant_limit(uint64_t tenant_id, int64_t bytes);
  int64_t get_tenant_limit(uint64_t tenant_id) const;
  int64_t get_tenant_hold(uint64_t tenant_id) const;
  void get_tenant_mod_usage(uint64_t tenant_id, int mod_id, common::ObModItem &item) const;
  int64_t get_mod_dist(int mod_id, ObTenantMemory tenant_memory[], int64_t count);

  void print_tenant_memory_usage(uint64_t tenant_id) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMallocAllocator);

  ObTenantAllocator *get_tenant_allocator(uint64_t tenant_id) const;

private:
  obsys::CRWLock locks_[PRESERVED_TENANT_COUNT];
  ObTenantAllocator *allocators_[PRESERVED_TENANT_COUNT];
  int64_t reserved_;
  int64_t urgent_;

  static ObMallocAllocator *instance_;
}; // end of class ObMallocAllocator

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_MALLOC_ALLOCATOR_H_ */
