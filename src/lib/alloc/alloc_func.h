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

#ifndef _ALLOC_FUNC_H_
#define _ALLOC_FUNC_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObModItem;
} // end of namespace common

namespace lib
{
struct ObTenantMemory;

// statistic relating
void set_memory_limit(int64_t bytes);
int64_t get_memory_limit();
int64_t get_memory_hold();
int64_t get_memory_avail();

void set_tenant_memory_limit(uint64_t tenant_id, int64_t bytes);
int64_t get_tenant_memory_limit(uint64_t tenant_id);
int64_t get_tenant_memory_hold(uint64_t tenant_id);
void get_tenant_mod_memory(
    uint64_t tenant_id, int mod_id, common::ObModItem &ObModItem);

int64_t get_mod_memory_dist(
    int mod_id, ObTenantMemory tenant_meomry[], int64_t count);

void ob_set_reserved_memory(const int64_t bytes);
void ob_set_urgent_memory(const int64_t bytes);
int64_t ob_get_reserved_urgent_memory();

} // end of namespace lib
} // end of namespace oceanbase

#endif /* _ALLOC_FUNC_H_ */
