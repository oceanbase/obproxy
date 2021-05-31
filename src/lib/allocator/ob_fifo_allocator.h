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
 *
 * **************************************************************
 *
 * This file defines ObFIFOAllocator.
 *
 * FIFO mode memory allocator
 * need a ObIAllocator as base allocator of ObFIFOAllocator
 * support alignment, default 16byte.
 *
 * Implement details:
 * 1. Save current using page lists, special page and free page lists.
 * 2. alloc from normal page when size <= normal page size.
 * 3. record base address, size and MagicNumber in header.
 * 4. inc the ref count in normal page (dec it when free, and when ref count is 0, return page to base Allocator).
 * 5. alloc from base Allocator directlly when size > normal page size.
 * ObFIFOAllocator will save all spcial page list.
 */

#ifndef OCEANBASE_LIB_ALLOCATOR_FIFO_
#define OCEANBASE_LIB_ALLOCATOR_FIFO_

#include <pthread.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/ob_define.h"

class ObFIFOAllocatorSpecialPageListTest;
class ObFIFOAllocatorSpecialPageParamTest;
class ObFIFOAllocatorAlignParamTest;
class ObFIFOAllocatorNormalPageListTest;
class UsedTotalChecker;

namespace oceanbase
{
namespace common
{
class ObFIFOAllocator : public common::ObIAllocator
{
  friend class ::ObFIFOAllocatorSpecialPageParamTest;
  friend class ::ObFIFOAllocatorAlignParamTest;
  friend class ::ObFIFOAllocatorSpecialPageListTest;
  friend class ::ObFIFOAllocatorNormalPageListTest;
  friend class UsedTotalChecker;
public:
  //private:
  struct BasePageHeader
  {
    BasePageHeader *next_;
    union
    {
      int64_t ref_count_;
      int64_t flag_;
    };
  };

  struct SpecialPageHeader : public BasePageHeader
  {
    // real_size_ is used to compute special_total_.
    // attention: aligned space may be before AND after user data.
    int64_t real_size_;
  };

  struct NormalPageHeader : public BasePageHeader
  {
    char *offset_;
  };

  // this is used to manage 16 bytes memory before ptr(return to user).
  struct AllocHeader
  {
    int32_t magic_num_;
    int32_t size_;
    void *page_addr_;
  };
private:
  // Alloc Header Magic Number. f1f0allc for FIFOALLC
  static const int64_t ALLOC_HEADER = 0xf1f0a11c;
  // Mark already free(double free detect). for[FreeBefore] f5eebef0
  static const int64_t ALREADY_FREE = 0xf5eebef0;
  static const int64_t SPECIAL_FLAG = -1;
public:
  ObFIFOAllocator();
  ~ObFIFOAllocator();

  int init(ObIAllocator *allocator, const int64_t page_size,
           const int64_t page_hold_limit = 32);
  void reset();
  void reuse() { /* not support */ }
  void *alloc(const int64_t size);
  void *alloc_align(const int64_t size, const int64_t align);
  void free(void *p);
  void set_mod_id(int64_t mod_id);

  inline int64_t used() const
  {
    return normal_used_ + special_used_;
  }

  inline int64_t total() const
  {
    return (page_using_count_  + page_hold_count_) * page_size_ + special_total_;
  }

  // just for debug.
  NormalPageHeader *get_current_using()
  {
    return current_using_;
  }

private:
  void *try_alloc(const int64_t size, const int64_t align);
  void *alloc_normal(const int64_t size, const int64_t align);
  void *alloc_special(const int64_t size, const int64_t align);
  void alloc_new_normalpage();
  BasePageHeader *get_owner_pageheader(void *p);
  void free_special(SpecialPageHeader *special_page, int64_t size);
  void free_normal(NormalPageHeader *normal_page, int64_t size);
  bool check_overwrite(void *p, int64_t &size);
  bool check_allocation_magic(void *p, int64_t &size);
  bool check_alloc_param_valid(const int64_t size, const int64_t align);

  bool is_normalpage_enough(const int64_t size, const int64_t align)
  {
    int64_t max_free_size = 0; // max free size on a normal page.
    int64_t start_offset = 0;
    start_offset = (sizeof(NormalPageHeader) + sizeof(AllocHeader) + align - 1);
    max_free_size = page_size_ - start_offset;
    return (max_free_size >= size);
  }

private:
  bool is_inited_;
  ObIAllocator *allocator_;
  NormalPageHeader *page_free_list_head_;
  NormalPageHeader *page_using_list_head_;
  NormalPageHeader *page_using_list_tail_;
  NormalPageHeader *current_using_;
  SpecialPageHeader *page_special_list_head_;
  SpecialPageHeader *page_special_list_tail_;

  int64_t page_hold_limit_;
  int64_t page_hold_count_;
  int64_t page_size_;
  int64_t mod_id_;
  pthread_t owner_thread_;

  static bool enable_assert_leak_;

  int64_t page_using_count_;
  int64_t special_used_;
  int64_t special_total_;
  int64_t normal_used_;
  mutable ObSpinLock lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFIFOAllocator);
};

}
}

#endif
