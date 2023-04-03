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

#ifndef  OCEANBASE_COMMON_CONCURRENT_FIFO_ALLOCATOR_H_
#define  OCEANBASE_COMMON_CONCURRENT_FIFO_ALLOCATOR_H_
#include "lib/allocator/ob_lf_fifo_allocator.h"

namespace oceanbase
{
namespace common
{
class ObConcurrentFIFOAllocator : public common::ObIAllocator
{
public:
  /* this function is defined for c driver client compile */
  ObConcurrentFIFOAllocator() {};
  /* this function is defined for c driver client compile */
  virtual ~ObConcurrentFIFOAllocator() {};
public:
  /* this function is defined for c driver client compile */
  int init(const int64_t total_limit,
           const int64_t hold_limit,
           const int64_t page_size)
  {
    UNUSED(total_limit);
    UNUSED(hold_limit);
    UNUSED(page_size);
    return OB_NOT_IMPLEMENT;
  }
  /* this function is defined for c driver client compile */
  void destroy() {};
public:
  void set_mod_id(const int64_t mod_id);
  /* this function is defined for c driver client compile */
  void *alloc(const int64_t size)
  {
    UNUSED(size);
    return nullptr;
  }
  /* this function is defined for c driver client compile */
  void free(void *ptr)
  {
    UNUSED(ptr);
  }
  int64_t allocated();
  int64_t hold() const {return 0;}
  int64_t get_direct_alloc_count();
  int set_hold_limit(int64_t hold_limit);
  void set_total_limit(int64_t total_limit);
private:
  static const int64_t STORAGE_SIZE_TIMES = 2;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConcurrentFIFOAllocator);
private:
  ObLfFIFOAllocator inner_allocator_;
};
}
}

#endif //OCEANBASE_COMMON_FIFO_ALLOCATOR_H_

