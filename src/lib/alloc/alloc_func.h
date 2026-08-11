/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _ALLOC_FUNC_H_
#define _ALLOC_FUNC_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
struct ObModItem;
} // end of namespace common

namespace lib
{
struct ObTenantMemory;

// statistic relating
void set_memory_limit(int64_t bytes);
int64_t get_memory_limit();
int64_t get_memory_hold();
int64_t get_memory_used();
int64_t get_memory_avail();
int64_t get_glibc_memory_hold();
int64_t get_glibc_memory_used();
void set_tenant_memory_limit(uint64_t tenant_id, int64_t bytes);
int64_t get_tenant_memory_limit(uint64_t tenant_id);
int64_t get_tenant_memory_hold(uint64_t tenant_id);
void get_tenant_mod_memory(
    uint64_t tenant_id, int mod_id, common::ObModItem &ObModItem);
int64_t get_mod_memory_dist(
    int mod_id, ObTenantMemory tenant_memory[], int64_t count);
int get_rpc_mod_memory();
void ob_set_reserved_memory(const int64_t bytes);
void ob_set_urgent_memory(const int64_t bytes);
int64_t ob_get_reserved_urgent_memory();

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _ALLOC_FUNC_H_ */
