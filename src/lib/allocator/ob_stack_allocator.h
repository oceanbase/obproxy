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

#ifndef __OB_COMMON_OB_STACK_ALLOCATOR_H__
#define __OB_COMMON_OB_STACK_ALLOCATOR_H__
#include "lib/ob_define.h"
#include "pthread.h"
#include "lib/tbsys.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
class DefaultBlockAllocator: public ObIAllocator
{
public:
  DefaultBlockAllocator();
  virtual ~DefaultBlockAllocator();
public:
  void set_mod_id(int64_t mod);
  int set_limit(const int64_t limit);
  int64_t get_allocated() const;
  void *alloc(const int64_t size);
  void free(void *p);
private:
  int64_t mod_;
  int64_t limit_;
  volatile int64_t allocated_;
};

class StackAllocator: public ObIAllocator
{
  struct Block
  {
    int init(const int64_t limit);
    int64_t remain() const;
    int64_t magic_;
    Block *next_;
    int64_t limit_;
    int64_t pos_;
    int64_t checksum_;
  };
public:
  StackAllocator();
  virtual ~StackAllocator();
public:
  int init(ObIAllocator *allocator, const int64_t block_size);
  int reset(const bool slow = false);
  int reserve(const int64_t size);
  int shrink(const char *p, const int64_t size, const int64_t new_size);
  void *alloc(const int64_t size);
  void free(void *p);
  // void set_mod_id(int64_tmod_id) {allocator_->set_mod_id(mod_id);};
  int start_batch_alloc();
  int end_batch_alloc(const bool rollback);
protected:
  int set_reserved_block(Block *block);
  int reserve_block(const int64_t size);
  int alloc_block(Block *&block, const int64_t size);
  int free_block(Block *block);
  int alloc_head(const int64_t size);
  int free_head();
  int save_top(int64_t &top) const;
  int restore_top(const int64_t top, const bool slow = false);
private:
  ObIAllocator *allocator_;
  int64_t block_size_;
  int64_t top_;
  int64_t saved_top_;
  Block *head_;
  Block *reserved_;
};

class TSIStackAllocator
{
public:
  struct BatchAllocGuard
  {
    BatchAllocGuard(TSIStackAllocator &allocator, int &err): allocator_(allocator), err_(err)
    {
      if (OB_SUCCESS != allocator_.start_batch_alloc()) {
        _OB_LOG(EDIAG, "start_batch_alloc() fail");
      }
    }
    virtual ~BatchAllocGuard()
    {
      if (OB_SUCCESS != allocator_.end_batch_alloc(OB_SUCCESS != err_)) {
        _OB_LOG(EDIAG, "end_batch_alloc(OB_SUCCESS != err[%d]) fail", err_);
      }
    }
    TSIStackAllocator &allocator_;
    int &err_;
  };
public:
  TSIStackAllocator();
  virtual ~TSIStackAllocator();
  int init(ObIAllocator *block_allocator, int64_t block_size);
  int reset(const bool slow = false);
  int reserve(const int64_t size);
  int shrink(const char *p, const int64_t size, const int64_t new_size);
  void *alloc(const int64_t size);
  void free(void *p);
  int start_batch_alloc();
  int end_batch_alloc(const bool rollback);
  StackAllocator *get();
private:
  int64_t block_size_;
  ObIAllocator *block_allocator_;
  StackAllocator allocator_array_[OB_MAX_THREAD_NUM];
};
}; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_STACK_ALLOCATOR_H__ */
