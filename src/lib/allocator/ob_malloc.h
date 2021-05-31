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

#ifndef OCEANBASE_COMMON_OB_MALLOC_H_
#define OCEANBASE_COMMON_OB_MALLOC_H_
#include <stdint.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_tc_malloc.h"
#include "lib/time/ob_time_utility.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
namespace common
{
inline void ob_print_mod_memory_usage(bool print_to_std = false,
                                      bool print_glibc_malloc_stats = false)
{
  UNUSEDx(print_to_std, print_glibc_malloc_stats);
}

extern ObMemAttr default_memattr;

inline void *ob_malloc(const int64_t nbyte, const ObMemAttr &attr = default_memattr)
{
  void *ptr = NULL;
  ObIAllocator *allocator = lib::ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    ptr = allocator->alloc(nbyte, attr);
  }
  return ptr;
}

inline void ob_free(void *ptr)
{
  ObIAllocator *allocator = lib::ObMallocAllocator::get_instance();
  abort_unless(!OB_ISNULL(allocator));
  allocator->free(ptr);
  ptr = NULL;
}

inline void *ob_realloc(void *ptr, const int64_t nbyte, const ObMemAttr &attr)
{
  void *nptr = NULL;
  ObIAllocator *allocator = lib::ObMallocAllocator::get_instance();
  if (!OB_ISNULL(allocator)) {
    nptr = allocator->realloc(ptr, nbyte, attr);
  }
  return nptr;
}

void *ob_malloc_align(
    const int64_t alignment, const int64_t nbyte,
    const ObMemAttr &attr = default_memattr);
void ob_free_align(void *ptr);

// Deprecated interface
inline void *ob_malloc(const int64_t nbyte, const int64_t mod_id)
{
  ObMemAttr attr;
  attr.mod_id_ = mod_id;
  return ob_malloc(nbyte, attr);
}

// Deprecated interface
void *ob_malloc_align(
    const int64_t alignment,
    const int64_t nbyte, const int64_t mod_id);

////////////////////////////////////////////////////////////////
class ObMalloc : public ObIAllocator
{
public:
  ObMalloc() : mod_id_(0) {};
  explicit ObMalloc(int64_t mod_id) : mod_id_(mod_id) {};
  virtual ~ObMalloc() {};
public:
  void set_mod_id(int64_t mod_id) {mod_id_ = mod_id;};
  void *alloc(const int64_t sz) {return ob_malloc_align(32, sz, mod_id_);};
  void *alloc(const int64_t size, const ObMemAttr &attr)
  { return ob_malloc_align(32, size, attr); }
  void free(void *ptr) {ob_free_align(ptr);};
private:
  int64_t mod_id_;
};
typedef ObMalloc ObTCMalloc;

class ObMemBuf
{
public:
  ObMemBuf()
    : buf_ptr_(NULL),
      buf_size_(OB_MALLOC_NORMAL_BLOCK_SIZE),
      mod_id_(ObModIds::OB_MOD_DO_NOT_USE_ME)
  {

  }

  explicit ObMemBuf(const int64_t default_size)
    : buf_ptr_(NULL),
      buf_size_(default_size),
      mod_id_(ObModIds::OB_MOD_DO_NOT_USE_ME)
  {

  }

  virtual ~ObMemBuf()
  {
    if (NULL != buf_ptr_) {
      ob_free(buf_ptr_);
      buf_ptr_ = NULL;
    }
  }

  inline char *get_buffer()
  {
    return buf_ptr_;
  }

  int64_t get_buffer_size() const
  {
    return buf_size_;
  }

  int ensure_space(const int64_t size, const int64_t mod_id = 0);

private:
  char *buf_ptr_;
  int64_t buf_size_;
  int64_t mod_id_;
};

class ObMemBufAllocatorWrapper
{
public:
  ObMemBufAllocatorWrapper(ObMemBuf &mem_buf, const int64_t mod_id = 0)
      : mem_buf_(mem_buf), mod_id_(mod_id) {}
public:
  inline char *alloc(int64_t sz)
  {
    char *ptr = NULL;
    if (OB_SUCCESS == mem_buf_.ensure_space(sz, mod_id_)) {
      ptr = mem_buf_.get_buffer();
    }
    return ptr;
  }
  inline void free(char *ptr)
  {
    UNUSED(ptr);
  }
private:
  ObMemBuf &mem_buf_;
  int64_t mod_id_;
};


class ObRawBufAllocatorWrapper
{
public:
  ObRawBufAllocatorWrapper(char *mem_buf, int64_t mem_buf_len) : mem_buf_(mem_buf),
                                                                 mem_buf_len_(mem_buf_len) {}
public:
  inline char *alloc(int64_t sz)
  {
    char *ptr = NULL;
    if (mem_buf_len_ >= sz) {
      ptr = mem_buf_;
    }
    return ptr;
  }
  inline void free(char *ptr)
  {
    UNUSED(ptr);
  }
private:
  char *mem_buf_;
  int64_t mem_buf_len_;
};

template <typename T>
void ob_delete(T *&ptr)
{
  if (NULL != ptr) {
    ptr->~T();
    ob_free(ptr);
    ptr = NULL;
  }
}

}
}

#define OB_NEW(T, mod_id, ...)                  \
  ({                                            \
    T* ret = NULL;                              \
    void *buf = ob_malloc(sizeof(T), mod_id);   \
    if (NULL != buf)                            \
    {                                           \
      ret = new(buf) T(__VA_ARGS__);            \
    }                                           \
    ret;                                        \
  })

#define OB_NEWx(T, pool, ...)                   \
  ({                                            \
    T* ret = NULL;                              \
    if (pool) {                                 \
    void *buf = pool->alloc(sizeof(T));         \
    if (NULL != buf)                            \
    {                                           \
      ret = new(buf) T(__VA_ARGS__);            \
    }                                           \
    }                                           \
    ret;                                        \
  })

#define OB_DELETE(T, mod_id, ptr)               \
  do{                                           \
    if (NULL != ptr)                            \
    {                                           \
      ptr->~T();                                \
      ob_free(ptr);                             \
      ptr = NULL;                               \
    }                                           \
  } while(0)



#endif /* OCEANBASE_SRC_COMMON_OB_MALLOC_H_ */
