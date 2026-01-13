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

#include "alloc_func.h"

#include <malloc.h>

#include "lib/alloc/achunk_mgr.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
namespace lib
{

void set_memory_limit(int64_t bytes)
{
  CHUNK_MGR.set_limit(bytes);
}

int64_t get_memory_limit()
{
  return CHUNK_MGR.get_limit();
}

int64_t get_memory_hold()
{
  return CHUNK_MGR.get_hold();
}

int64_t get_memory_used()
{
  int64_t total = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (OB_NOT_NULL(allocator)) {
    ObTenantAllocator *tenant_allocator = allocator->get_tenant_allocator(common::OB_SERVER_TENANT_ID);
    if (OB_NOT_NULL(tenant_allocator)) {
      total += tenant_allocator->get_used();
    }
  }

  int64_t glibc_used = get_glibc_memory_used();

  return total + glibc_used;
}

int64_t get_glibc_memory_hold()
{
#ifdef EL9_PLATFORM
  struct mallinfo2 mi = mallinfo2();
#else
  struct mallinfo mi = mallinfo();
#endif
  int64_t hold = mi.arena + mi.hblkhd;

  return hold;
}

int64_t get_glibc_memory_used()
{
#ifdef EL9_PLATFORM
  struct mallinfo2 mi = mallinfo2();
#else
  struct mallinfo mi = mallinfo();
#endif
  int64_t hold = mi.arena + mi.hblkhd;
  int64_t used = hold - mi.fordblks;
  return used;
}

int64_t get_memory_avail()
{
  return get_memory_limit() - get_memory_hold();
}

void set_tenant_memory_limit(uint64_t tenant_id, int64_t bytes)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->set_tenant_limit(tenant_id, bytes);
  }
}

int64_t get_tenant_memory_limit(uint64_t tenant_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_limit(tenant_id);
  }
  return bytes;
}

int64_t get_tenant_memory_hold(uint64_t tenant_id)
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes = allocator->get_tenant_hold(tenant_id);
  }
  return bytes;
}

void get_tenant_mod_memory(
    uint64_t tenant_id, int mod_id,
    common::ObModItem &item)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->get_tenant_mod_usage(tenant_id, mod_id, item);
  }
}

int64_t get_mod_memory_dist(
    int mod_id, ObTenantMemory tenant_memory[], int64_t count)
{
  int64_t retcount = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    retcount = allocator->get_mod_dist(mod_id, tenant_memory, count);
  }
  return retcount;
}

int get_rpc_mod_memory()
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  common::ObModItem item;
  if (!OB_ISNULL(allocator)) {
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_PROXY_RPC_TABLE_QUERY_MAP, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_PROXY_RPC_REQ_CTX_MAP, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_PROXY_RPC_PARSE, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_RPC, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_RPC_PROCESSOR, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_RPC_TABLE_PROC, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_RPC_TABLE_CLIENT, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_RPC_TABLE_BATCH_OPERATION, item);
    allocator->add_tenant_mod_usage(common::OB_SERVER_TENANT_ID, common::ObModIds::OB_RPC_TABLE_LS_OPERATION_RESULT, item);
  }
  return item.hold_;
}

void ob_set_reserved_memory(const int64_t bytes)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->set_reserved(bytes);
  }
}

void ob_set_urgent_memory(const int64_t bytes)
{
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    allocator->set_urgent(bytes);
  }
}

int64_t ob_get_reserved_urgent_memory()
{
  int64_t bytes = 0;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    bytes += allocator->get_urgent();
    bytes += allocator->get_reserved();
  }
  return bytes;
}



} // end of namespace lib
} // end of namespace oceanbase
