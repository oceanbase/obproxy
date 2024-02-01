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

#include "lib/allocator/ob_stack_allocator.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
DefaultBlockAllocator::DefaultBlockAllocator()
  : mod_(ObModIds::BLOCK_ALLOC),
    limit_(INT64_MAX),
    allocated_(0)
{}

DefaultBlockAllocator::~DefaultBlockAllocator()
{
  if (allocated_ != 0) {
    _OB_LOG(WDIAG, "allocated_[%ld] != 0", allocated_);
  }
}

void DefaultBlockAllocator::set_mod_id(int64_t mod)
{
  mod_ = mod;
}

int DefaultBlockAllocator::set_limit(const int64_t limit)
{
  int ret = OB_SUCCESS;
  if (limit < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    limit_ = limit;
    _OB_LOG(INFO, "block_allocator.set_limit(%ld)", limit);
  }
  return ret;
}

int64_t DefaultBlockAllocator::get_allocated() const
{
  return allocated_;
}

void *DefaultBlockAllocator::alloc(const int64_t size)
{
  int ret = OB_SUCCESS;
  void *p = NULL;
  int64_t alloc_size = size + sizeof(alloc_size);
  if (0 >= size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (allocated_ + alloc_size > limit_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(EDIAG, "allocated[%ld] + size[%ld] > limit[%ld]", allocated_, size, limit_);
  } else if (__sync_add_and_fetch(&allocated_, alloc_size) > limit_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    IGNORE_RETURN __sync_add_and_fetch(&allocated_, -alloc_size);
    _OB_LOG(EDIAG, "allocated[%ld] + size[%ld] > limit[%ld]", allocated_, alloc_size, limit_);
  } else if (NULL == (p = ob_malloc(alloc_size, mod_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    IGNORE_RETURN __sync_add_and_fetch(&allocated_, -alloc_size);
    _OB_LOG(EDIAG, "ob_tc_malloc(size=%ld) failed", alloc_size);
  } else {
    *((int64_t *)p) = alloc_size;
    p = (char *)p + sizeof(alloc_size);
  }
  UNUSED(ret);
  return p;
}

void DefaultBlockAllocator::free(void *p)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = 0;
  if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    alloc_size = *((int64_t *)((char *)p - sizeof(alloc_size)));
  }
  if (OB_SUCCESS != ret) {
  } else if (__sync_add_and_fetch(&allocated_, -alloc_size) < 0) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(EDIAG, "free(size=%ld): allocated[%ld] < 0 after free", alloc_size, allocated_);
  } else {
    ob_free((void *)((char *)p - sizeof(int64_t)));
  }
}

StackAllocator::StackAllocator()
  : allocator_(NULL),
    block_size_(0),
    top_(0),
    saved_top_(-1),
    head_(NULL),
    reserved_(NULL)
{}

StackAllocator::~StackAllocator()
{
  IGNORE_RETURN reset();
}

int StackAllocator::Block::init(const int64_t limit)
{
  int ret = OB_SUCCESS;
  magic_ = 0x7878787887878787;
  next_ = NULL;
  limit_ = limit;
  pos_ = sizeof(*this);
  checksum_ = -limit;
  return ret;
}

int64_t StackAllocator::Block::remain() const
{
  return limit_ - pos_;
}

int StackAllocator::init(ObIAllocator *allocator, const int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator || 0 >= block_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != allocator_ && 0 < block_size_) {
    ret = OB_INIT_TWICE;
  } else {
    allocator_ = allocator;
    block_size_ = block_size;
  }
  return ret;
}

int StackAllocator::reset(const bool slow)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(restore_top(0, slow))) {
    _OB_LOG(EDIAG, "restore_top(0)=>%d", ret);
  } else if (OB_FAIL(set_reserved_block(NULL))) {
    _OB_LOG(EDIAG, "set_reserved_block()=>%d", ret);
  }
  return ret;
}

int StackAllocator::alloc_block(Block *&block, const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t limit = max(block_size_, size + sizeof(Block));
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
  } else if (NULL == (block = (Block *)allocator_->alloc(limit))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(WDIAG, "allocator->alloc(size=%ld)=>NULL", size);
  } else if (OB_FAIL(block->init(limit))) {
    _OB_LOG(EDIAG, "block->init()=>%d", ret);
  }
  return ret;
}

int StackAllocator::free_block(Block *block)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
  } else if (NULL == block) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(EDIAG, "free_block(block=NULL)");
  } else {
    allocator_->free(block);
    block = NULL;
  }
  return ret;
}

int StackAllocator::alloc_head(const int64_t size)
{
  int ret = OB_SUCCESS;
  Block *new_block = reserved_;
  if (OB_ISNULL(new_block)) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(WDIAG, "invalid new block");
  } else if (new_block->remain() < size
             && OB_FAIL(alloc_block(new_block, size))) {
    _OB_LOG(WDIAG, "allocator->alloc(size=%ld)=>%d", size, ret);
  } else {
    if (new_block == reserved_) {
      reserved_ = NULL;
    }
    new_block->next_ = head_;
    top_ += new_block->pos_;
    head_ = new_block;
  }
  return ret;
}

int StackAllocator::free_head()
{
  int ret = OB_SUCCESS;
  Block *head = NULL;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
  } else if (NULL == head_ || (top_ -= head_->pos_) < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    head = head_;
    head_ = head_->next_;
  }
  if (OB_SUCCESS != ret) {
  } else if (OB_FAIL(free_block(head))) {
    _OB_LOG(EDIAG, "free_block()=>%d", ret);
  }
  return ret;
}

int StackAllocator::set_reserved_block(Block *block)
{
  int ret = OB_SUCCESS;
  if (NULL != reserved_ && OB_FAIL(free_block(reserved_))) {
    _OB_LOG(EDIAG, "free(%p)=>%d", reserved_, ret);
  } else {
    reserved_ = block;
  }
  return ret;
}

int StackAllocator::reserve_block(const int64_t size)
{
  int ret = OB_SUCCESS;
  Block *new_block = NULL;
  if (OB_FAIL(alloc_block(new_block, size))) {
    _OB_LOG(WDIAG, "allocator->alloc(size=%ld)=>%d", size, ret);
  } else if (OB_FAIL(set_reserved_block(new_block))) {
    _OB_LOG(EDIAG, "set_reserved_block(new_block=%p)=>%d", new_block, ret);
  }

  if (OB_SUCCESS != ret) {
    int ret_tmp = OB_SUCCESS;
    if (OB_SUCCESS != (ret_tmp = free_block(new_block))) {
      _OB_LOG(EDIAG, "free_block(new_block=%p)=>%d", new_block, ret_tmp);
    }
  }
  return ret;
}

int StackAllocator::reserve(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(head_)
      || head_->remain() >= size
      || OB_ISNULL(reserved_)
      || reserved_->remain() >= size) {
    _OB_LOG(EDIAG, "unexpected error");
  } else if (OB_FAIL(reserve_block(size))) {
    _OB_LOG(EDIAG, "reserve_block(size=%ld)=>%d", size, ret);
  }
  return ret;
}

int StackAllocator::shrink(const char *p, const int64_t size, const int64_t new_size)
{
  int ret = OB_SUCCESS;
  if (NULL == p || size < new_size || new_size < 0 || OB_ISNULL(head_)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (p + size != (char *)head_ + head_->pos_) {
    ret = OB_NOT_SUPPORTED;
    _OB_LOG(WDIAG, "try shrink after another alloc, not supported: end[%ld] != allocator_end[%ld]",
              (int64_t)p + size, (int64_t)head_ + head_->pos_);
  } else if (head_->pos_ < size) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(EDIAG, "head->pos[%ld] < size[%ld]", head_->pos_, size);
  } else {
    head_->pos_ -= size - new_size;
    top_ -= size - new_size;
  }
  return ret;
}

void *StackAllocator::alloc(const int64_t size)
{
  int ret = OB_SUCCESS;
  void *p = NULL;
  if (size <= 0 || OB_ISNULL(head_)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(EDIAG, "alloc(size=%ld): INVALID_ARGUMENT", size);
  } else if (head_->remain() < size && OB_FAIL(alloc_head(size))) {
    _OB_LOG(EDIAG, "alloc_block(size=%ld)=>%d", size, ret);
  } else {
    p = (char *)head_ + head_->pos_;
    head_->pos_ += size;
    top_ += size;
  }
  return p;
}

void StackAllocator::free(void *p)
{
  UNUSED(p);
}

int StackAllocator::start_batch_alloc()
{
  int ret = OB_SUCCESS;
  if (saved_top_ >= 0) {
    ret = OB_ERROR;
  } else if (OB_FAIL(save_top(saved_top_))) {
    _OB_LOG(EDIAG, "save_top(top=%ld)=>%d", top_, ret);
  }
  return ret;
}

int StackAllocator::end_batch_alloc(const bool rollback)
{
  int ret = OB_SUCCESS;
  if (saved_top_ < 0) {
    _OB_LOG(TRACE, "no need to end_batch_alloc");
  } else if (rollback && OB_FAIL(restore_top(saved_top_, false))) {
    _OB_LOG(EDIAG, "restor_top(saved_top=%ld)=>%d", saved_top_, ret);
  } else {
    saved_top_ = -1;
  }
  return ret;
}

int StackAllocator::save_top(int64_t &top) const
{
  int ret = OB_SUCCESS;
  if (saved_top_ >= 0) {
    ret = OB_ERROR;
  } else {
    top = top_;
  }
  return ret;
}

int StackAllocator::restore_top(const int64_t top, const bool slow)
{
  int ret = OB_SUCCESS;
  int64_t last_top = top_;
  int64_t BATCH_FREE_LIMIT = 1 << 30;
  if (top < 0 || top > top_ || OB_ISNULL(head_)) {
    ret = OB_INVALID_ARGUMENT;
  }
  while (OB_SUCCESS == ret && top_ > top) {
    if (NULL == head_) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(EDIAG, "restor_top(top=%ld): head_ == NULL", top);
    } else if (top + head_->pos_ <= top_) {
      ret = free_head();
    } else if (head_->pos_ - (int64_t)sizeof(*head_) < top_ - top) {
      ret = OB_ERR_UNEXPECTED;
      _OB_LOG(EDIAG, "restore_top(head->pos_=%ld, top=%ld, top_=%ld)=>%d", head_->pos_, top, top_, ret);
    } else {
      head_->pos_ -= top_ - top;
      top_ = top;
    }
    if (slow && last_top > top_ + BATCH_FREE_LIMIT) {
      last_top = top_;
      usleep(10000);
    }
  }
  return ret;
}

TSIStackAllocator::TSIStackAllocator() : block_size_(0), block_allocator_(NULL)
{}

TSIStackAllocator::~TSIStackAllocator()
{
  reset();
}

int TSIStackAllocator::init(ObIAllocator *block_allocator, int64_t block_size)
{
  int ret = OB_SUCCESS;
  if (NULL == block_allocator || 0 >= block_size) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL != block_allocator_) {
    ret = OB_INIT_TWICE;
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < (int64_t)ARRAYSIZEOF(allocator_array_); i++) {
    ret = allocator_array_[i].init(block_allocator, block_size);
  }
  if (OB_SUCCESS == ret) {
    block_allocator_ = block_allocator;
    block_size_ = block_size;
  }
  return ret;
}

int TSIStackAllocator::reset(const bool slow)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == ret && i < (int64_t)ARRAYSIZEOF(allocator_array_); i++) {
    ret = allocator_array_[i].reset(slow);
  }
  return ret;
}

int TSIStackAllocator::reserve(const int64_t size)
{
  int ret = OB_SUCCESS;
  StackAllocator *allocator = NULL;
  if (NULL == (allocator = get())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(EDIAG, "get_tsi_allocator() failed");
  } else {
    ret = allocator->reserve(size);
  }
  return ret;
}

void *TSIStackAllocator::alloc(const int64_t size)
{
  StackAllocator *allocator = NULL;
  void *p = NULL;
  if (NULL == (allocator = get())) {
    _OB_LOG(EDIAG, "get_tsi_allocator() failed");
  } else if (NULL == (p = allocator->alloc(size))) {
    _OB_LOG(EDIAG, "allocator->alloc(%ld)=>NULL", size);
  }
  return p;
}

int TSIStackAllocator::shrink(const char *p, const int64_t size, const int64_t new_size)
{
  int ret = OB_SUCCESS;
  StackAllocator *allocator = NULL;
  if (NULL == (allocator = get())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(EDIAG, "get_tsi_allocator() failed");
  } else {
    ret = allocator->shrink(p, size, new_size);
  }
  return ret;
}
void TSIStackAllocator::free(void *p)
{
  StackAllocator *allocator = NULL;
  if (NULL == (allocator = get())) {
    _OB_LOG(EDIAG, "get_tsi_allocator() failed");
  } else {
    allocator->free(p);
    p = NULL;
  }
}

int TSIStackAllocator::start_batch_alloc()
{
  int ret = OB_SUCCESS;
  StackAllocator *allocator = NULL;
  if (NULL == (allocator = get())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(EDIAG, "get_tsi_allocator() failed");
  } else {
    ret = allocator->start_batch_alloc();
  }
  return ret;
}

int TSIStackAllocator::end_batch_alloc(const bool rollback)
{
  int ret = OB_SUCCESS;
  StackAllocator *allocator = NULL;
  if (NULL == (allocator = get())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(EDIAG, "get_tsi_allocator() failed");
  } else {
    ret = allocator->end_batch_alloc(rollback);
  }
  return ret;
}

StackAllocator *TSIStackAllocator::get()
{
  int64_t idx = get_itid();
  return (idx >= 0 && idx < (int64_t)ARRAYSIZEOF(allocator_array_)) ? allocator_array_ + idx : NULL;
}
}; // end namespace common
}; // end namespace oceanbase
