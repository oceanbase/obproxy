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

#ifndef __OB_COMMON_OB_BLOCK_ALLOCATOR_H__
#define __OB_COMMON_OB_BLOCK_ALLOCATOR_H__
#include <stdint.h>
#include <sys/mman.h>
namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObModSet;
class ObMemLeakChecker;
extern const ObModSet &get_global_mod_set();
/// @fn print memory usage of each module
extern void print_malloc_stats(bool print_glibc_malloc_stats);
extern void ob_purge_memory_pool();
void set_mem_leak_checker_mod_id(int64_t mod_id);
void reset_mem_leak_checker_mod_id(int64_t mod_id);
extern ObMemLeakChecker &get_mem_leak_checker();

/// set the memory as read-only
/// @note the ptr should have been returned by ob_malloc, and only the small block is supported now
/// @param prot See the manpage of mprotect
// int ob_mprotect(void *ptr, int prot);

}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_BLOCK_ALLOCATOR_H__ */
