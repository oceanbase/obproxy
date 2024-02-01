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

#define USING_LOG_PREFIX LIB_ALLOC

#include <sys/mman.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/list/ob_free_list.h"
#include "lib/utility/utility.h"
#include "lib/hash_func/ob_hash_func.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/alloc/ob_malloc_allocator.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

void __attribute__((weak)) memory_limit_callback()
{
  LOG_DEBUG("common memory_limit_callback");
}

static bool is_aligned(uint64_t x, uint64_t align)
{
  return 0 == (x & (align - 1));
}

static uint64_t up2align(uint64_t x, uint64_t align)
{
  return (x + (align - 1)) & ~(align - 1);
}

#define __DM_MALLOC 1
#define __DM_MMAP 2
#define __DM_MMAP_ALIGNED 3
#define __DIRECT_MALLOC__ __DM_MMAP_ALIGNED

#if __DIRECT_MALLOC__ == __DM_MALLOC
void *direct_malloc(int64_t size)
{
  return ::malloc(size);
}
void direct_free(void *p, int64_t size)
{
  UNUSED(size);
  ::free(p);
}
#elif __DIRECT_MALLOC__ == __DM_MMAP
void *direct_malloc(int64_t size)
{
  void *p = NULL;
  if (MAP_FAILED == (p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                              0))) {
    p = NULL;
  }
  return p;
}
void direct_free(void *p, int64_t size)
{
  if (NULL != p) {
    munmap(p, size);
  }
}
#elif __DIRECT_MALLOC__ == __DM_MMAP_ALIGNED
const static uint64_t MMAP_BLOCK_ALIGN = 1ULL << 21;

inline void *mmap_aligned(uint64_t size, uint64_t align)
{
  void *ret = NULL;
  if (MAP_FAILED == (ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                                0))) {
    ret = NULL;
  } else if (is_aligned((uint64_t)ret, align)) {
  } else {
    munmap(ret, size);
    if (MAP_FAILED == (ret = mmap(NULL, size + align, PROT_READ | PROT_WRITE,
                                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0))) {
      ret = NULL;
    } else {
      uint64_t aligned_addr = up2align((uint64_t)ret, align);
      // Compute the header/trailer size to avoid using ret after munmap.
      uint64_t header_size = aligned_addr - (uint64_t) ret;
      uint64_t trailer_size = (uint64_t) ret + align - aligned_addr;

      munmap(ret, header_size);
      munmap((void *) (aligned_addr + size), trailer_size);
      ret = (void *) aligned_addr;
    }
  }
  return ret;
}

void *direct_malloc(int64_t size)
{
  return mmap_aligned(size, MMAP_BLOCK_ALIGN);
}

void direct_free(void *p, int64_t size)
{
  if (NULL != p) {
    munmap(p, size);
  }
}
#endif // __DIRECT_MALLOC__

namespace oceanbase
{
namespace common
{

ObIAllocator *global_default_allocator = NULL;
extern void tsi_factory_init();
extern void tsi_factory_destroy();
int ob_init_memory_pool(int64_t block_size)
{
  UNUSED(block_size);
  return OB_SUCCESS;
}

const ObModSet &get_global_mod_set()
{
  static ObModSet set;
  return set;
}

void print_malloc_stats(bool print_glibc_malloc_stats)
{
  get_global_mod_set().print_mod_memory_usage(print_glibc_malloc_stats);
  // ((ObTSIBlockAllocator &)get_global_tc_allocator()).print_stats(print_glibc_malloc_stats);
}

void  __attribute__((constructor(MALLOC_INIT_PRIORITY))) init_global_memory_pool()
{
  int ret = OB_SUCCESS;
  ObMallocAllocator *allocator = ObMallocAllocator::get_instance();
  if (OB_ISNULL(allocator) || OB_FAIL(allocator->init())) {
  }
  global_default_allocator = ObMallocAllocator::get_instance();
  tsi_factory_init();
}

void  __attribute__((destructor(MALLOC_INIT_PRIORITY))) deinit_global_memory_pool()
{
  tsi_factory_destroy();
}

} // end namespace common
} // end namespace oceanbase
