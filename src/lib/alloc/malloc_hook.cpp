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

#include "malloc_hook.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase {
int64_t lib::memalign_size = 0;
namespace lib {
__thread int glibc_hook_opt = GHO_NOHOOK;
}  // jit
}  // oceanbase

using namespace oceanbase::common;
using namespace oceanbase::lib;



EXTERN_C_BEGIN

#ifndef USING_ASAN

extern void *__libc_malloc(size_t size);
extern void __libc_free(void *ptr);
extern void *__libc_realloc(void *ptr, size_t size);

static ObMemAttr mattr(OB_SERVER_TENANT_ID, ObModIds::OB_BUFFER);
#define up_align(x, y) (((x) + ((y) - 1)) / (y) * (y))
//static __thread bool is_fopen = false;

/*void hook_malloc_print(void *ptr, bool is_ob)*/
//{
  //if (is_fopen == false) {
    //FILE *fd = NULL;
    //char buf[1000];
    //is_fopen = true;
    //fd = fopen("hook_malloc.log", "a+");
    //char *bt = lbt();
    //is_fopen = false;
    //if (fd == NULL) {
      //return;
    //}
    //int n = 0;
    //if (is_ob) {
      //n = snprintf(buf, 1000, "jit_malloc_ob:%p, trace:%s\n", ptr, bt);
    //} else {
      //n = snprintf(buf, 1000, "jit_malloc_libc:%p, trace:%s\n", ptr, bt);
    //}
    //is_fopen = true;
    //fwrite(buf, sizeof(char), n, fd);
    //fclose(fd);
    //is_fopen = false;
  //} else {
    ////do nothing
  //}
/*}*/

size_t malloc_usable_size(void *ptr)
{
  if (OB_LIKELY(ptr != NULL)) {
    HookHeader *header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    if (OB_UNLIKELY(header->MAGIC_CODE_ != HOOK_MAGIC_CODE)) {
    #ifndef NDEBUG
      // debug mode
      abort();
    #else
      _OB_LOG(ERROR,
              "unexpected magic, memory broken or mismatched hook function is invoked!!!");
      return 0;
    #endif
    }

    /*
     * ob_malloc do not have usable_size interface.
     * glic hava malloc_usable_size interface, but do not have hook method, so can't call malloc_usable_size of glic
     * In summary, here directly rerutn data_size
     */
    return header->data_size_;
  } else {
    return 0;
  }
}

void *calloc(size_t nmemb, size_t size)
{
  void *ptr = NULL;
  if (OB_LIKELY(nmemb != 0) && OB_LIKELY(size != 0)) {
    void *pheader = NULL;
    size_t all_size = HOOK_HEADER_SIZE + nmemb * size;
    if (glibc_hook_opt != GHO_NOHOOK) {
      do {
        pheader = ob_malloc(all_size, mattr);
        if (OB_UNLIKELY(NULL == pheader)) {
          usleep(10000);  // 10ms
        }
      } while (OB_UNLIKELY(NULL == pheader) &&
               OB_UNLIKELY(glibc_hook_opt == GHO_NONULL));
    } else {
      pheader = __libc_malloc(all_size);
    }
    if (OB_LIKELY(pheader != NULL)) {
      MEMSET(pheader, 0, all_size);
      HookHeader *header = new (pheader) HookHeader();
      header->data_size_ = static_cast<uint32_t>(all_size - HOOK_HEADER_SIZE);
      header->from_glibc_ = GHO_NOHOOK == glibc_hook_opt;
      ptr = header->data_;
    }
  }
  return ptr;
}

void *malloc(size_t size)
{
  void *ptr = NULL;
  if (OB_LIKELY(size != 0)) {
    void *pheader = NULL;
    size_t all_size = HOOK_HEADER_SIZE + size;
    if (glibc_hook_opt != GHO_NOHOOK) {
      do {
        pheader = ob_malloc(all_size, mattr);
        if (OB_UNLIKELY(NULL == pheader)) {
          usleep(10000); // 10ms
        }
      } while (OB_UNLIKELY(NULL == pheader) &&
               OB_UNLIKELY(glibc_hook_opt == GHO_NONULL));
    } else {
      pheader = __libc_malloc(all_size);
    }
    if (OB_LIKELY(pheader != NULL)) {
      HookHeader *header = new (pheader) HookHeader();
      header->data_size_ = static_cast<uint32_t>(size);
      header->from_glibc_ = GHO_NOHOOK == glibc_hook_opt;
      ptr = header->data_;
    }
  }
  return ptr;
}

void free(void *ptr)
{
  if (OB_LIKELY(ptr != NULL)) {
    HookHeader *header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    if (OB_UNLIKELY(header->MAGIC_CODE_ != HOOK_MAGIC_CODE)) {
    #ifndef NDEBUG
      // debug mode
      abort();
    #else
      _OB_LOG(ERROR,
              "unexpected magic, memory broken or mismatched hook function is invoked!!!");
      return;
    #endif
    }
    void *orig_ptr = (char*)header - header->offset_;
    // Temporary code, used to count the usage of the align interface
    if (0xabcd1234 == *(int64_t*)&header->padding__[0]) {
      ATOMIC_FAA(&oceanbase::lib::memalign_size, -header->data_size_);
    }
    if (!header->from_glibc_) {
      ob_free(orig_ptr);
    } else {
      __libc_free(orig_ptr);
    }
  }
}

void *realloc(void *ptr, size_t size)
{
  void *nptr = NULL;
  HookHeader *old_header = NULL;
  void *orig_ptr = NULL;
  if (ptr != NULL) {
    old_header = reinterpret_cast<HookHeader*>((char*)ptr - HOOK_HEADER_SIZE);
    if (OB_UNLIKELY(old_header->MAGIC_CODE_ != HOOK_MAGIC_CODE)) {
    #ifndef NDEBUG
      // debug mode
      abort();
    #else
      _OB_LOG(ERROR,
              "unexpected magic, memory broken or mismatched hook function is invoked!!!");
      return NULL;
    #endif
    }
    orig_ptr = (char*)old_header - old_header->offset_;
  }
  if (NULL == ptr ||
      (old_header->from_glibc_ == (GHO_NOHOOK == glibc_hook_opt))) {
    void *pheader = NULL;
    size_t all_size = size != 0 ? HOOK_HEADER_SIZE + size : 0;
    if (glibc_hook_opt != GHO_NOHOOK) {
      do {
        pheader = ob_realloc(orig_ptr, all_size, mattr);
        if (OB_UNLIKELY(NULL == pheader)) {
          usleep(10000); // 10ms
        }
      } while (OB_UNLIKELY(NULL == pheader) &&
               OB_UNLIKELY(glibc_hook_opt == GHO_NONULL));
    } else {
      pheader = __libc_realloc(orig_ptr, all_size);
    }
    if (OB_LIKELY(pheader != NULL)) {
      HookHeader *new_header = new (pheader) HookHeader();
      new_header->data_size_ = static_cast<uint32_t>(all_size - HOOK_HEADER_SIZE);
      new_header->from_glibc_ = GHO_NOHOOK == glibc_hook_opt;
      nptr = new_header->data_;
    }
  } else {
    // When ptr is not empty, and hook and non-hook need to be called separately
    if (size != 0) {
      nptr = malloc(size);
      if (nptr != NULL) {
        memmove(nptr, ptr, MIN(old_header->data_size_, size));
      }
    }
    free(ptr);
  }
  return nptr;
}

void *memalign(size_t align, size_t size)
{
  void *ptr = NULL;
  if (OB_LIKELY(size != 0)) {
    // Make sure alignment is power of 2
    {
      size_t a = 8;
      while (a < align)
        a <<= 1;
      align = a;
    }
    void *tmp_ptr = NULL;
    size_t all_size = 2 * MAX(align, HOOK_HEADER_SIZE) + size;
    if (glibc_hook_opt != GHO_NOHOOK) {
      do {
        tmp_ptr = ob_malloc(all_size, mattr);
        if (OB_UNLIKELY(NULL == tmp_ptr)) {
          usleep(10000); // 10ms
        }
      } while (OB_UNLIKELY(NULL == tmp_ptr) &&
               OB_UNLIKELY(glibc_hook_opt == GHO_NONULL));
    } else {
      tmp_ptr = __libc_malloc(all_size);
    }
    if (OB_LIKELY(tmp_ptr != NULL)) {
      ATOMIC_FAA(&oceanbase::lib::memalign_size, size);
      char *start = (char *)tmp_ptr + HOOK_HEADER_SIZE;
      char *align_ptr = (char *)up_align(
          reinterpret_cast<int64_t>(start), align);
      char *pheader = align_ptr - HOOK_HEADER_SIZE;
      size_t offset = pheader - (char*)tmp_ptr;
      HookHeader *header = new (pheader) HookHeader();
      *(int64_t*)&header->padding__[0] = 0xabcd1234;
      header->data_size_ = static_cast<uint32_t>(size);
      header->from_glibc_ = GHO_NOHOOK == glibc_hook_opt;
      header->offset_ = offset;
      ptr = header->data_;
    }
  }
  return ptr;
}

void *valloc(size_t size)
{
  return memalign(sysconf(_SC_PAGESIZE), size);
}

int posix_memalign(void **memptr, size_t alignment, size_t size)
{
  int err = 0;

  if (OB_UNLIKELY(NULL == memptr)) {
    err = -EINVAL;
  } else {
    *memptr = NULL;
    void *ptr = memalign(alignment, size);
    if (OB_UNLIKELY(NULL == ptr)) {
      err = -ENOMEM;
    } else {
      *memptr = ptr;
    }
  }

  return err;
}

#endif

EXTERN_C_END
