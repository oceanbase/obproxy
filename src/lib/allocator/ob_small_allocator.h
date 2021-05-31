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
 * *************************************************************
 *
 * This is a memory allocator designed for small fixed size memory.
 * It allocates large chunk memory from ob_tc_malloc(), and splits them
 * into small objects of fixed size set by user.
 * This allocator supports fast multi-thread access, because it provides
 * a thread local cache for each user thread.
 * Allocate and free memory in separate threads is allowed, though it's
 * less efficient.
 */

#ifndef OCEANBASE_COMMON_OB_SMALL_ALLOCATOR_H_
#define OCEANBASE_COMMON_OB_SMALL_ALLOCATOR_H_

#include <cstdlib>
#include <pthread.h>
#include "lib/ob_define.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/thread_local/ob_tsi_utils.h"
#include "ob_allocator.h"

namespace oceanbase
{
namespace common
{
class ObSmallAllocator
{
public:
  static const int64_t DEFAULT_MIN_OBJ_COUNT_ON_BLOCK = 1;

  // The two block sizes are normal and large block size of ob_tc_malloc().
  const static int64_t LARGE_BLOCK_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;
private:
  const static int64_t OBJECT_ALIGN_SIZE = 8;
  const static int64_t BLOCK_MAGIC = 0xA1BABA0CEA17BA5E;
  const static int32_t OBJECT_ALLOC_MAGIC = 0x10CEA171;
  const static int32_t OBJECT_FREE_MAGIC = 0x111BA5E1;
  // The get_local_blk_() don't use a block whose utilization is
  // greater than this limit, and after this retry count it will
  // allocate a new block from system.
  const static double BLOCK_UTILIZATION_LIMIT = 0.8;
  const static int64_t GET_BLOCK_RETRY = 3;
  // Max thread local block number.
  const static int64_t MAX_TL_BLK_CNT = OB_MAX_THREAD_NUM;

public:
  ObSmallAllocator();
  virtual ~ObSmallAllocator();

  /**
   * Initialize & destroy the allocator.
   * The obj_size is the size of small fixed size memory.
   * The mod_id is the module ID in oceanbase.
   * The block_size is the size of large memory chunk that
   * allocated from system, and the small fixed size memory
   * are picked from them.
   */
  int init(const int64_t obj_size,
           const int64_t mod_id = 0,
           const uint64_t tenant_id = OB_SERVER_TENANT_ID,
           const int64_t block_size = OB_MALLOC_NORMAL_BLOCK_SIZE,
           const int64_t min_obj_count_on_block = DEFAULT_MIN_OBJ_COUNT_ON_BLOCK,
           const int64_t limit_num = INT64_MAX);
  bool is_inited() const { return inited_; }
  int destroy();

  /**
   * Allocate & free memory.
   * The alloc() might fail when the underlying allocator runs out of
   * memory, then the alloc() returns NULL.
   */
  void *alloc() { return alloc_(); }
  void free(void *ptr) { free_(ptr); }

  /**
   * Print utilization and other statistics.
   */
  int64_t to_string(char *buf, const int64_t limit) const;
  int64_t get_alloc_size() const;
  int64_t get_remain_size() const;

private:
  // Disable copy and signment.
  ObSmallAllocator(const ObSmallAllocator &);
  ObSmallAllocator &operator= (const ObSmallAllocator &);

private:
  /**
   * The small fixed size memory object.
   * The offset_ kept in object is the distance between the object
   * and its block, which enables locating the block by an object.
   * The unnamed union and the char buf_[0] is a trick to save memory.
   */
  struct Object
  {
    // Constructor conflicts with offsetof(Object, buf_) and const char buf_[0].
    void init(int32_t offset)
    {
      magic_ = OBJECT_FREE_MAGIC;
      offset_ = offset;
      next_ = NULL;
    }

    int32_t magic_;
    int32_t offset_;
    union
    {
      const char buf_[0];
      Object *next_;
    };
  };

  /**
   * The large memory chunk allocated from system.
   * A block is either held by a thread or kept in the global queue
   * waiting for reuse, as specified by BlockType.
   * Its unused objects are chained in free_obj_, and the object
   * utilization is tracked by 2 counters.
   */
  enum BlockType {Local, Global};
  struct Block
  {
    Block() :
    magic_(BLOCK_MAGIC),
           free_obj_(NULL),
           obj_alloced_(0),
           obj_count_(0),
           prev_(NULL),
           next_(NULL),
           type_(Local)
    {}

    int64_t magic_;
    Object *free_obj_;
    volatile int64_t obj_alloced_;
    int64_t obj_count_;
    ObSpinLock lock_;  // only protects variables above
    Block *prev_;
    Block *next_;
    volatile BlockType type_;
    char buf_[0];

    int64_t to_string(char *buf, const int64_t limit) const;
  };

private:
  /**
   * Allocate a chunk of fixed size memory.
   * Returns NULL if the underlying allocator lacks memory.
   */
  void *alloc_();
  /**
   * Free a chunk of memory.
   */
  void free_(void *ptr);
  /**
   * Pick a memory object from a valid block.
   * If the block is invalid or used up, it returns NULL.
   */
  Object *get_obj_(Block *blk);
  /**
   * Revert an memory object to its host block.
   * If all objects are returned to the block, and the block is
   * not obtained by any thread, the block is released to system.
   * The block and object should be valid.
   */
  void revert_obj_(Block *blk, Object *obj);
  /**
   * Locate a memory object from the address refered by a void pointer.
   * If the address refers to an invalid area, NULL is returned.
   */
  Object *locate_obj_(void *ptr);
  /**
   * Get a block that held by current thread.
   * If current thread doesn't hold any block or the block is already
   * used up, it will pick a vacant one from global queue or allocate
   * one from system.
   * This function might fail when the underlying allocator runs out
   * of memory, and then it returns NULL.
   */
  Block *get_local_blk_();
  /**
   * Locate a block from an object that the block holds.
   * If the object refers to an invalid block, NULL is returned.
   */
  Block *locate_blk_(Object *obj);
  /**
   * Allocate new blocks or free them.
   * The underlying allocator is ob_tc_malloc() and ob_tc_free().
   * When the underlying allocator lacks memory, NULL is returned.
   * The block and memory objects are well initialized by alloc_blk_().
   */
  Block *alloc_blk_();
  void free_blk_(Block *blk);
  /**
   * The queue and dlist operations.
   * The pop_front_() would return NULL if dlist is empty. Other
   * operations won't fail.
   */
  Block *pop_front_(Block *&head);
  Block *pop_front_(Block *&head, Block *&tail);
  void push_front_(Block *&head, Block *blk);
  void push_front_(Block *&head, Block *&tail, Block *blk);
  void push_back_(Block *&head, Block *&tail, Block *blk);
  void remove_(Block *&head, Block *blk);
  void remove_(Block *&head, Block *&tail, Block *blk);
  /**
   * Memory object utilization. These are atomic operations.
   */
  void add_used_obj_(int64_t delta);
  void add_total_obj_(int64_t delta);
  void add_local_blk_(int64_t delta);
  void add_total_blk_(int64_t delta);
  double obj_util_();
  /**
   * Memory object utilization in a block.
   */
  double blk_obj_util_(const Block *blk);

private:
  bool inited_;
  // The thread local blocks.
  // The tl_blks_ array allows fast access, and the
  // tl_blk_list_ allows the destruction of thread local blocks.
  Block *tl_blks_[MAX_TL_BLK_CNT];
  Block *tl_blk_list_;
  // The global block queue. A block released by tl_blk_ would
  // stay here until another thread reuses it.
  Block *blk_queue_;
  Block *blk_queue_tail_;
  // Global lock. It protects the global queue and block list.
  ObSpinLock glock_;
  // Memory object utilization.
  int64_t used_obj_;
  int64_t total_obj_;
  int64_t local_blk_;
  int64_t total_blk_;
  // Memory attr.
  ObMemAttr memattr_;
  // The real size allocated for objects and blocks.
  int64_t object_size_;
  int64_t alloc_size_;
  int64_t block_size_;
  int64_t obj_num_limit_;
};
}
}

#endif  //  OCEANBASE_COMMON_OB_SMALL_ALLOCATOR_H_
