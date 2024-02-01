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

#include "lib/allocator/ob_mod_define.h"

#include <malloc.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase
{
namespace common
{
int64_t ObModItem::format_string(
    char *buf, int64_t len, const char *name) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos,
                  "[MEMORY] hold=% '15ld used=% '15ld count=% '8ld avg_used=% '15ld mod=%s\n",
                  hold_, used_, count_, (0 == count_) ? 0 : used_ / count_, name);
  return pos;
}

ObLocalModSet *ObModSet::get_local()
{
  static __thread bool within_creating = false;
  ObLocalModSet *p = NULL;
  const int64_t id = get_itid();
  if (id < OB_MAX_THREAD_NUM && id >= 0) {
    if (OB_ISNULL(mod_sets_[id]) && !within_creating) {
      within_creating = true;
      mod_sets_[id] = new (std::nothrow) ObLocalModSet();
      within_creating = false;
    }
    p = mod_sets_[id];
  }
  return p;
}


int ObModSet::get_mod_id(const common::ObString &mod_name) const
{
  int id = -1;
  bool found = false;
  for (int i = 0; !found && i < MOD_COUNT_LIMIT; i ++) {
    if (mod_names_[i] != NULL) {
      if ((strlen(mod_names_[i]) == (uint64_t)(mod_name.length()))
          && (0 == strncasecmp(mod_names_[i], mod_name.ptr(), mod_name.length()))) {
        id = i;
        found = true;
      }
    }
  }
  return id;
}

static int print_mod(char *buf, int64_t size, int64_t &pos, const char *name, const ObModItem &mod)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  if (0 == mod.hold_) {
  } else if ((count = mod.format_string(buf + pos, size - pos, name)) <= 0) {
    ret = OB_BUF_NOT_ENOUGH;
    _OB_LOG(EDIAG, "buf not enough, size=%ld, pos=%ld", size, pos);
  } else {
    pos += count;
  }
  return ret;
}

void ObModSet::print_mod_memory_usage(bool print_glibc) const
{
  int ret = OB_SUCCESS;
  char buf[1 << 16] = {};
  int64_t pos = 0;

  if (!OB_FAIL(print_mod_memory_usage(buf, sizeof (buf), pos))) {
    _OB_LOG(INFO, "=== memory info ===\n%s", buf);
  }

  PC_REPORT();
  if (print_glibc) {
    _OB_LOG(INFO, "=== malloc_stats ===");
    malloc_stats();
    _OB_LOG(INFO, "=== main heap info ===");
#ifdef EL9_PLATFORM
    struct mallinfo2 info = mallinfo2();
    _OB_LOG(INFO, "mmap_chunks=%ld", info.hblks);
    _OB_LOG(INFO, "mmap_bytes=%ld", info.hblkhd);
    _OB_LOG(INFO, "sbrk_sys_bytes=%ld", info.arena);
    _OB_LOG(INFO, "sbrk_used_chunk_bytes=%ld", info.uordblks);
    _OB_LOG(INFO, "sbrk_not_in_use_chunks=%ld", info.ordblks);
    _OB_LOG(INFO, "sbrk_not_in_use_chunk_bytes=%ld", info.fordblks);
    _OB_LOG(INFO, "sbrk_top_most_releasable_chunk_bytes=%ld", info.keepcost);
#else
    struct mallinfo info = mallinfo();
    _OB_LOG(INFO, "mmap_chunks=%d", info.hblks);
    _OB_LOG(INFO, "mmap_bytes=%d", info.hblkhd);
    _OB_LOG(INFO, "sbrk_sys_bytes=%d", info.arena);
    _OB_LOG(INFO, "sbrk_used_chunk_bytes=%d", info.uordblks);
    _OB_LOG(INFO, "sbrk_not_in_use_chunks=%d", info.ordblks);
    _OB_LOG(INFO, "sbrk_not_in_use_chunk_bytes=%d", info.fordblks);
    _OB_LOG(INFO, "sbrk_top_most_releasable_chunk_bytes=%d", info.keepcost);
#endif
    _OB_LOG(INFO, "=== detailed malloc_info ===");

    //malloc_info(0, stderr);
  }
  ObObjFreeListList::get_freelists().dump();
}

int ObModSet::print_mod_memory_usage(char *buf, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(databuff_printf(buf, len, pos, "== allocator mod ==\n"))) {
  }
  for (int32_t idx = 0; OB_SUCCESS == ret && idx < MOD_COUNT_LIMIT; idx ++) {
    if (is_allocator_mod(idx)) {
      ret = print_mod(buf, len, pos, mod_names_[idx], get_mod(idx));
    }
  }

  if (OB_SUCC(ret)) {
    ret = databuff_printf(buf, len, pos, "== user mod ==\n");
    for (int32_t idx = 0; OB_SUCCESS == ret && idx < MOD_COUNT_LIMIT; idx ++) {
      if (!is_allocator_mod(idx)) {
        ret = print_mod(buf, len, pos, mod_names_[idx], get_mod(idx));
      }
    }
  }
  return ret;
}

}; // end namespace common
}; // end namespace oceanbase
