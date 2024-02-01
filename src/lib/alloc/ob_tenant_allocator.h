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
        limit_(INT64_MAX), hold_bytes_(0)
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

  virtual void *alloc(const int64_t size, const ObMemAttr &attr);
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr);
  virtual void free(void *ptr);

  // statistic related
  void set_limit(int64_t bytes)
  {
    limit_ = bytes;
  }
  int64_t get_limit() const
  {
    return limit_;
  }
  int update_hold(int64_t bytes, const ObMemAttr &attr);

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
}; // end of class ObTenantAllocator

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _OB_TENANT_ALLOCATOR_H_ */
