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

#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase
{
namespace common
{
volatile int64_t ObObjFreeListList::once_ = 0;
const int64_t ObObjFreeListList::MAX_NUM_FREELIST = 1024;
static const uint8_t ITEM_MAGIC = 0xFF;

#define POOL_LOG(tag, f, thread_cache) \
  f->show_info(__FILE__, __LINE__, tag, thread_cache)

inline void ObObjFreeList::show_info(const char *file, const int line,
                                     const char *tag, ObThreadCache *thread_cache)
{
  _OB_LOG(INFO, "[%02ld][%s:%05d][%s] mem_total:%6.2fM total:%-8ld free:%-4ld avg:%-6.1f"
          " malloc:%-4ld obj_cnt_base:%-4ld obj_cnt_per_chunk:%-4ld obj_size:%-6ld max_chunk_count:%-6ld"
          " thread_chunk_count:%-6ld chunk_size:%ld name:%s lbt:%s",
          thread_cache_idx_, file, line, tag,
          (static_cast<double>(ObObjFreeListList::get_freelists().mem_total_)/1024/1024),
          thread_cache->nr_total_,
          thread_cache->nr_free_,
          thread_cache->nr_average_,
          thread_cache->nr_malloc_,
          obj_count_base_,
          obj_count_per_chunk_,
          type_size_,
          chunk_count_,
          thread_cache->nr_free_chunks_,
          chunk_byte_size_,
          name_,
          lbt());
}

static inline int64_t upper_round(const int64_t value, const int64_t align)
{
  return (value + align - 1L) & ~(align - 1L);
}

static inline ObThreadCache *&get_thread_cache(const int64_t thread_cache_idx)
{
  static __thread ObThreadCache *g_thread_caches[ObObjFreeListList::MAX_NUM_FREELIST];
  return g_thread_caches[thread_cache_idx];
}

inline ObChunkInfo *&ObObjFreeList::get_chunk_info(void *item)
{
  return *reinterpret_cast<ObChunkInfo **>(reinterpret_cast<char *>(item) + type_size_ - sizeof(void *));
}

ObObjFreeList::ObObjFreeList()
    : freelists_(NULL), tc_allocator_(NULL), is_inited_(false),
      only_global_(false), mod_id_(0),
      thread_cache_idx_(0), name_(NULL), type_size_(0), type_size_base_(0),
      alignment_(0), obj_count_base_(0), obj_count_per_chunk_(0),
      chunk_byte_size_(0), chunk_count_(0), used_(0), allocated_(0),
      allocated_base_(0), used_base_(0), nr_thread_cache_(0), thread_cache_(NULL)
{

}

#ifdef DOUBLE_FREE_CHECK
inline int64_t ObObjFreeList::get_chunk_item_magic_idx(
   void *item, ObChunkInfo **chunk_info, const bool do_check /* = false */)
{
  int64_t idx = 0;

  if (NULL == *chunk_info) {
    *chunk_info = get_chunk_info(item);
  }
  idx = (reinterpret_cast<int64_t>(item) - reinterpret_cast<int64_t>((*chunk_info)->head_)) / type_size_;
  if (do_check && (idx >= obj_count_per_chunk_
      || (0 != (reinterpret_cast<int64_t>(item) - reinterpret_cast<int64_t>((*chunk_info)->head_)) % type_size_))) {
    _OB_LOG(EDIAG, "Invalid address:%p, chunk_addr:%p, type_size:%ld, obj_count:%ld, idx:%ld",
             item, (*chunk_info)->head_, type_size_, obj_count_per_chunk_, idx);
  }

  return idx;
}

inline void ObObjFreeList::set_chunk_item_magic(ObChunkInfo *chunk_info, void *item)
{
  int64_t idx = 0;
  idx = get_chunk_item_magic_idx(item, &chunk_info);
  if (0 != chunk_info->item_magic_[idx]) {
    OB_LOG(EDIAG, "the chunk is free, but the magic is not zero", "magic", chunk_info->item_magic_[idx]);
  }
  chunk_info->item_magic_[idx] = ITEM_MAGIC;
}

inline void ObObjFreeList::clear_chunk_item_magic(ObChunkInfo *chunk_info, void *item)
{
  int64_t idx = 0;
  idx = get_chunk_item_magic_idx(item, &chunk_info, true);
  if (ITEM_MAGIC != chunk_info->item_magic_[idx]) {
    OB_LOG(EDIAG, "the chunk is used, but without the right magic",
           "actual_magic", chunk_info->item_magic_[idx], "expected_magic", ITEM_MAGIC);
  }
  chunk_info->item_magic_[idx] = 0;
}
#else
inline void ObObjFreeList::set_chunk_item_magic(ObChunkInfo *chunk_info, void *item)
{
  UNUSED(chunk_info);
  UNUSED(item);
}

inline void ObObjFreeList::clear_chunk_item_magic(ObChunkInfo *chunk_info, void *item)
{
  UNUSED(chunk_info);
  UNUSED(item);
}
#endif

inline ObChunkInfo *ObObjFreeList::chunk_create(ObThreadCache *thread_cache)
{
  void *chunk_addr = NULL;
  void *curr = NULL;
  void *next = NULL;
  ObChunkInfo *chunk_info = NULL;

  if (NULL == (chunk_addr = ob_malloc_align(alignment_, chunk_byte_size_, mod_id_))) {
    OB_LOG(EDIAG, "failed to allocate chunk", K_(chunk_byte_size));
  } else {
    chunk_info = new (reinterpret_cast<char *>(chunk_addr) + type_size_ * obj_count_per_chunk_) ObChunkInfo();
#ifdef DOUBLE_FREE_CHECK
    memset(chunk_info->item_magic_, 0, chunk_byte_size_ - type_size_ * obj_count_per_chunk_ - sizeof(ObChunkInfo));
#endif
    chunk_info->tid_ = pthread_self();
    chunk_info->head_ = chunk_addr;
    chunk_info->type_size_ = type_size_;
    chunk_info->obj_count_ = obj_count_per_chunk_;
    chunk_info->length_ = chunk_byte_size_;
    chunk_info->allocated_ = 0;
    chunk_info->thread_cache_ = thread_cache;
    chunk_info->link_ = Link<ObChunkInfo>();

    curr = chunk_info->head_;
    chunk_info->inner_free_list_ = curr;
    for (int64_t i = 1; i < obj_count_per_chunk_; i++) {
      next = reinterpret_cast<void *>(reinterpret_cast<char *>(curr) + type_size_);
      *reinterpret_cast<void **>(curr) = next;
      curr = next;
    }
    *reinterpret_cast<void **>(curr) = NULL;

    (void)ATOMIC_FAA(&allocated_, obj_count_per_chunk_);
    (void)ATOMIC_FAA(&ObObjFreeListList::get_freelists().mem_total_, chunk_byte_size_);

    thread_cache->free_chunk_list_.push(chunk_info);
    thread_cache->nr_free_chunks_++;
  }

  return chunk_info;
}

inline void ObObjFreeList::chunk_delete(ObThreadCache *thread_cache, ObChunkInfo *chunk_info)
{
  void *chunk_addr = chunk_info->head_;
  if (OB_UNLIKELY(0 != chunk_info->allocated_)) {
    OB_LOG(EDIAG, "the chunk allocated size isn't 0 when it deleting", K(chunk_info->allocated_));
  }
  thread_cache->free_chunk_list_.remove(chunk_info);
  thread_cache->nr_free_chunks_--;
  ob_free_align(chunk_addr);
  (void)ATOMIC_FAA(&allocated_, -obj_count_per_chunk_);
  (void)ATOMIC_FAA(&ObObjFreeListList::get_freelists().mem_total_, -chunk_byte_size_);
}

inline void *ObObjFreeList::malloc_whole_chunk(ObThreadCache *thread_cache, ObChunkInfo *chunk_info)
{
  void *next = NULL;
  void *item = NULL;

  if (OB_UNLIKELY(0 != chunk_info->allocated_)) {
    OB_LOG(EDIAG, "the allocated size of free chunk isn't 0", K(chunk_info->allocated_));
  }
  item = chunk_info->head_;
  get_chunk_info(item) = chunk_info;
  for (int64_t i = 1; i < obj_count_per_chunk_; i++) {
    next = reinterpret_cast<void *>(reinterpret_cast<char *>(item) + i * type_size_);
    get_chunk_info(next) = chunk_info;
    (void)ATOMIC_FAA(&thread_cache->nr_free_, 1);
    thread_cache->outer_free_list_.push(next);
  }

  chunk_info->allocated_ += obj_count_per_chunk_;
  chunk_info->inner_free_list_ = NULL;
  thread_cache->nr_total_ += obj_count_per_chunk_;

  return item;
}

inline void *ObObjFreeList::malloc_from_chunk(ObThreadCache *thread_cache, ObChunkInfo *chunk_info)
{
  void *item = NULL;

  if (NULL != (item = chunk_info->inner_free_list_)) {
    chunk_info->inner_free_list_ = *reinterpret_cast<void **>(item);
    get_chunk_info(item) = chunk_info;
    chunk_info->allocated_++;
    thread_cache->nr_total_++;
  }

  return item;
}

inline void ObObjFreeList::free_to_chunk(ObThreadCache *thread_cache, void *item)
{
  ObChunkInfo *chunk_info = NULL;

  chunk_info = get_chunk_info(item);
  chunk_info->allocated_--;
  thread_cache->nr_total_--;

  *reinterpret_cast<void **>(item) = chunk_info->inner_free_list_;
  chunk_info->inner_free_list_ = item;

  if (0 == chunk_info->allocated_) {
    chunk_delete(thread_cache, chunk_info);
  }
}

inline void *ObObjFreeList::malloc_from_cache(ObThreadCache *thread_cache, const int64_t nr)
{
  void *item = NULL;
  ObChunkInfo *chunk_info = NULL;
  int64_t malloc_count = nr;

  chunk_info = thread_cache->free_chunk_list_.head_;
  while (NULL != chunk_info && malloc_count > 0) {
    while (NULL != (item = malloc_from_chunk(thread_cache, chunk_info)) && --malloc_count > 0) {
      (void)ATOMIC_FAA(&thread_cache->nr_free_, 1);
      thread_cache->outer_free_list_.push(item);
    }
    chunk_info = chunk_info->link_.next_;
  }

  if (malloc_count > 0 && NULL != (chunk_info = chunk_create(thread_cache))) {
    if (malloc_count == obj_count_per_chunk_) {
      item = malloc_whole_chunk(thread_cache, chunk_info);
    } else {
      while (NULL != (item = malloc_from_chunk(thread_cache, chunk_info)) && --malloc_count > 0) {
        (void)ATOMIC_FAA(&thread_cache->nr_free_, 1);
        thread_cache->outer_free_list_.push(item);
      }
    }
  }

  return item;
}

inline void ObObjFreeList::free_to_cache(ObThreadCache *thread_cache, void *item, const int64_t nr)
{
  int64_t n = nr;

  if (NULL != item) {
    free_to_chunk(thread_cache, item);
  }

  while (0 != n && NULL != (item = thread_cache->outer_free_list_.pop())) {
    free_to_chunk(thread_cache, item);
    n--;
  }
  (void)ATOMIC_FAA(&thread_cache->nr_free_, -(nr - n));
}

inline void ObObjFreeList::refresh_average_info(ObThreadCache *thread_cache)
{
  int64_t nr_free = thread_cache->nr_free_;
  double nr_average = thread_cache->nr_average_;
  double reclaim_factor = static_cast<double>(reclaim_opt_.reclaim_factor_) / 100.0;

  thread_cache->nr_average_ = static_cast<double>((nr_average * (1.0 - reclaim_factor)) +
                       (static_cast<double>(nr_free) * reclaim_factor));
}

inline bool ObObjFreeList::need_to_reclaim(ObThreadCache *thread_cache)
{
  bool ret = false;
  if (reclaim_opt_.enable_reclaim_) {
    if(thread_cache->nr_free_ >= static_cast<int64_t>(thread_cache->nr_average_)
       && thread_cache->nr_free_chunks_ > chunk_count_
       && thread_cache->nr_total_ > std::max(obj_count_base_, obj_count_per_chunk_)) {
      if (thread_cache->nr_overage_++ >= reclaim_opt_.max_overage_) {
        thread_cache->nr_overage_ = 0;
        ret = true;
      }
    } else {
      thread_cache->nr_overage_ = 0;
    }
  }
  return ret;
}

int ObObjFreeList::init(const char *name, const int64_t obj_size,
                        const int64_t obj_count, const int64_t alignment /* = 16 */,
                        const ObMemCacheType cache_type /* = OP_RECLAIM */)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WDIAG, "init twice");
  } else if (OB_ISNULL(name) || OB_UNLIKELY(obj_size <= 0) || OB_UNLIKELY(obj_count <= 0)
      || OB_UNLIKELY(obj_count < 0) || OB_UNLIKELY(alignment <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WDIAG, "invalid argument", K(name), K(obj_size), K(obj_count), K(alignment));
  } else if (OB_FAIL(obj_free_list_.init(name, 0))) {
    OB_LOG(WDIAG, "failed to initialize atomic free list", K(name));
  } else if (OB_FAIL(mutex_init(&lock_))) {
    OB_LOG(WDIAG, "failed to init mutex");
  } else {
    alignment_ = alignment;
    obj_count_base_ = (OP_GLOBAL == cache_type) ? 0 : obj_count;
    type_size_base_ = obj_size;
    only_global_ = (OP_RECLAIM != cache_type);
    name_ = name;

    // Make sure we align *all* the objects in the allocation,
    // not just the first one, each chunk with an extra
    // pointer to store the pointer of chunk info
    if (OB_UNLIKELY(only_global_)) {
      type_size_ = upper_round(obj_size, alignment);
    } else {
      type_size_ = upper_round(obj_size + sizeof(void *), alignment);
    }

    int64_t meta_size = sizeof(ObChunkInfo);
    int64_t real_type_size = type_size_;
#ifdef DOUBLE_FREE_CHECK
    // enlarge type_size to hold a item_magic
    real_type_size = type_size_ + sizeof(uint8_t);
#endif

    int64_t min_chunk_size = upper_round(real_type_size + meta_size, alignment);
    int64_t max_chunk_size = upper_round(real_type_size * obj_count + meta_size, alignment);

    // find fit chunk size
    if (max_chunk_size <= OB_MALLOC_BIG_BLOCK_SIZE) {
      chunk_byte_size_ = upper_round(max_chunk_size, OB_MALLOC_NORMAL_BLOCK_SIZE);
      obj_count_per_chunk_ = (chunk_byte_size_ - meta_size) / real_type_size;
    } else if (min_chunk_size > OB_MALLOC_BIG_BLOCK_SIZE) {
      OB_LOG(WDIAG, "big object pool to initialize", K(obj_size));
      obj_count_per_chunk_ = 1;
      chunk_byte_size_ = min_chunk_size;
    } else {
      // ensure the chunk size is less than 2M,
      // wasted space to at most 8KB
      obj_count_per_chunk_ = (OB_MALLOC_BIG_BLOCK_SIZE - meta_size) / real_type_size;
      chunk_byte_size_ = upper_round(real_type_size * obj_count_per_chunk_ + meta_size,
                                     OB_MALLOC_NORMAL_BLOCK_SIZE);
    }

    chunk_count_ = (obj_count_base_ + obj_count_per_chunk_ - 1) / obj_count_per_chunk_;

    used_ = 0;
    allocated_ = 0;
    allocated_base_ = 0;
    used_base_ = 0;

    thread_cache_ = NULL;
    nr_thread_cache_ = 0;
    is_inited_ = true;
  }

  return ret;
}

void *ObObjFreeList::global_alloc()
{
  void *chunk_addr = NULL;
  void *item = NULL;
  void *next = NULL;
  bool break_loop = false;

  do {
    if (obj_free_list_.empty()) {
      if (NULL == (chunk_addr = ob_malloc_align(alignment_, chunk_byte_size_, mod_id_))) {
        OB_LOG(EDIAG, "failed to allocate chunk", K_(chunk_byte_size));
        break_loop = true;
      } else {
        (void)ATOMIC_FAA(&allocated_, obj_count_per_chunk_);
        (void)ATOMIC_FAA(&ObObjFreeListList::get_freelists().mem_total_, chunk_byte_size_);

        item = chunk_addr;
        break_loop = true;

        // free each of the new elements
        for (int64_t i = 1; i < obj_count_per_chunk_; i++) {
          next = reinterpret_cast<void *>(reinterpret_cast<char *>(item) + i * type_size_);
          obj_free_list_.push(next);
        }
      }
    } else if (NULL != (item = obj_free_list_.pop())) {
      break_loop = true;
    }
  } while (!break_loop);

  return item;
}

void ObObjFreeList::global_free(void *item)
{
  if (OB_LIKELY(NULL != item)) {
    obj_free_list_.push(item);
  }
}

ObThreadCache *ObObjFreeList::init_thread_cache(ObChunkInfo *&chunk_info)
{
  int ret = OB_SUCCESS;
  ObThreadCache *thread_cache = NULL;
  chunk_info = NULL;

  if (NULL == (thread_cache = reinterpret_cast<ObThreadCache *>(tc_allocator_->alloc_void()))) {
    OB_LOG(EDIAG, "failed to allocate memory for thread cache");
  } else {
    thread_cache->f_ = this;
    thread_cache->free_chunk_list_ = DLL<ObChunkInfo>();

    // this lock will only be accessed when initializing
    // thread cache, so it won't damage performance
    if (OB_FAIL(mutex_acquire(&lock_))) {
      OB_LOG(WDIAG, "failed to lock of freelist", K(name_));
    } else if (OB_FAIL(thread_cache->outer_free_list_.init(name_, 0))) {
      OB_LOG(WDIAG, "failed to init outer freelist", K(name_));
      if (OB_UNLIKELY(OB_SUCCESS != mutex_release(&lock_))) {
        OB_LOG(WDIAG, "failed to release mutex");
      }
    } else {
      if (!only_global_ && NULL == (chunk_info = chunk_create(thread_cache))) {
        OB_LOG(EDIAG, "thread cache failed to create chunk info");
        if (OB_UNLIKELY(OB_SUCCESS != mutex_release(&lock_))) {
          OB_LOG(WDIAG, "failed to release mutex");
        }
        tc_allocator_->free_void(thread_cache);
        thread_cache = NULL;
      } else {
        get_thread_cache(thread_cache_idx_) = thread_cache;

        if (NULL != thread_cache_) {
          // we will loop thread_cache.next without lock, following
          // statement's sequence is important for us.
          thread_cache->next_ = thread_cache_;
          thread_cache->prev_ = thread_cache_->prev_;
          thread_cache->next_->prev_ = thread_cache;
          thread_cache->prev_->next_ = thread_cache;
        } else {
          thread_cache->next_ = thread_cache;
          thread_cache->prev_ = thread_cache;
        }

        thread_cache_ = thread_cache;
        nr_thread_cache_++;
        if (OB_UNLIKELY(OB_SUCCESS != mutex_release(&lock_))) {
          OB_LOG(WDIAG, "failed to release mutex");
        }
      }
    }
  }

  return thread_cache;
}

void *ObObjFreeList::alloc()
{
  void *ret = NULL;
  int64_t num_to_move = 0;
  ObChunkInfo *chunk_info = NULL;
  ObThreadCache *thread_cache = NULL;
  ObThreadCache *next_thread_cache = NULL;

  // no thread cache, create it
  if (OB_UNLIKELY(NULL == (thread_cache = get_thread_cache(thread_cache_idx_)))) {
    if (NULL == (thread_cache = init_thread_cache(chunk_info))) {
      OB_LOG(EDIAG, "failed to init object free list thread cache");
      ret = NULL; // allocate failed
    } else {
      if (only_global_) {
        ret = global_alloc();
      } else {
        ret = malloc_whole_chunk(thread_cache, chunk_info);
        set_chunk_item_magic(chunk_info, ret);
      }
      if (NULL != ret) {
        (void)ATOMIC_FAA(&thread_cache->nr_malloc_, 1);
      }
    }
  } else {
    thread_cache->status_ = 0;
    // priority to fetch memory from outer_free_list
    if (NULL != (ret = thread_cache->outer_free_list_.pop())) {
      if (!only_global_) {
        (void)ATOMIC_FAA(&thread_cache->nr_free_, -1);
        (void)ATOMIC_FAA(&thread_cache->nr_malloc_, 1);
        set_chunk_item_magic(NULL, ret);
      } else {
        thread_cache->nr_free_--;
        thread_cache->nr_malloc_++;
      }
    } else if (only_global_) {
      if (NULL != (ret = global_alloc())) {
        thread_cache->nr_malloc_++;
      }
    } else if (chunk_count_ > 0 && thread_cache->nr_free_chunks_ < chunk_count_) {
      // allocate one chunk if chunk count isn't bigger than expected chunk count
      ret = malloc_from_cache(thread_cache, obj_count_per_chunk_);
      refresh_average_info(thread_cache);
      (void)ATOMIC_FAA(&thread_cache->nr_malloc_, 1);
      set_chunk_item_magic(NULL, ret);
    } else {

      // try to steal memory from other thread's outer_free_list
      int64_t steal_thread_count = reclaim_opt_.steal_thread_count_;
      next_thread_cache = thread_cache->next_;
      while (NULL == ret && next_thread_cache != thread_cache && steal_thread_count-- > 0) {
        if (NULL != (ret = next_thread_cache->outer_free_list_.pop())) {
          (void)ATOMIC_FAA(&next_thread_cache->nr_free_, -1);
          (void)ATOMIC_FAA(&next_thread_cache->nr_malloc_, 1);
          set_chunk_item_magic(NULL, ret);
        } else {
          next_thread_cache = next_thread_cache->next_;
        }
      }

      if (NULL == ret) {
        // try to reclaim memory from all caches in the same thread
        for (int64_t i = 0; i < ObObjFreeListList::get_freelists().nr_freelists_
             && i < ObObjFreeListList::MAX_NUM_FREELIST; i++) {
          if (NULL != (next_thread_cache = get_thread_cache(i))
              && next_thread_cache->f_->need_to_reclaim(next_thread_cache)) {
            if (reclaim_opt_.debug_filter_ & 0x1) {
              POOL_LOG("F", next_thread_cache->f_, next_thread_cache);
            }

            num_to_move = static_cast<int64_t>(std::min(next_thread_cache->nr_average_,
                                   static_cast<double>(next_thread_cache->nr_free_)));
            next_thread_cache->f_->free_to_cache(next_thread_cache, NULL, num_to_move);

            if (reclaim_opt_.debug_filter_ & 0x1) {
              POOL_LOG("-", next_thread_cache->f_, next_thread_cache);
            }

            refresh_average_info(next_thread_cache);
          }
        }

        // finally, fetch from thread local cache
        if (reclaim_opt_.debug_filter_ & 0x2) {
          POOL_LOG("M", this, thread_cache);
        }
        ret = malloc_from_cache(thread_cache, obj_count_per_chunk_);
        if (reclaim_opt_.debug_filter_ & 0x2) {
          POOL_LOG("+", this, thread_cache);
        }

        refresh_average_info(thread_cache);
        (void)ATOMIC_FAA(&thread_cache->nr_malloc_, 1);
        set_chunk_item_magic(NULL, ret);
      }
    }
  }

  return ret;
}

void ObObjFreeList::free(void *item)
{
  ObChunkInfo *chunk_info = NULL;
  ObThreadCache *thread_cache = NULL;

  if (NULL != item) {
    if (only_global_) {
      if (OB_UNLIKELY(NULL == (thread_cache = get_thread_cache(thread_cache_idx_)))) {
        if (OB_UNLIKELY(NULL == (thread_cache = init_thread_cache(chunk_info)))) {
          OB_LOG(EDIAG, "failed to init object free list thread cache");
        }
      }

      if (NULL != thread_cache && obj_count_base_ > 0) {
        // free all thread cache obj upto global free list if it's overflow
        if (OB_UNLIKELY(thread_cache->nr_free_ >= obj_count_base_)) {
          void *next = NULL;
          obj_free_list_.push(item);
          // keep half of obj_count_base_
          int64_t low_watermark = obj_count_base_ / 2;
          while (thread_cache->nr_free_ > low_watermark
                 && NULL != (next = thread_cache->outer_free_list_.pop())) {
            obj_free_list_.push(next);
            thread_cache->nr_free_--;
          }
        } else {
          thread_cache->outer_free_list_.push(item);
          thread_cache->nr_free_++;
        }
      } else {
        global_free(item);
      }

      /**
       * For global allocate mode, maybe thread A allocates memory and thread B frees it.
       * So when thread B frees, B's thread cache maybe NULL. The thread_cache->nr_malloc_
       * isn't the actual malloc number of this thread, maybe it's negtive.
       */
      if (NULL != thread_cache) {
        thread_cache->nr_malloc_--;
      }
    } else {
      chunk_info = get_chunk_info(item);
      clear_chunk_item_magic(chunk_info, item);
      thread_cache = chunk_info->thread_cache_;
      (void)ATOMIC_FAA(&thread_cache->nr_free_, 1);
      (void)ATOMIC_FAA(&thread_cache->nr_malloc_, -1);
      thread_cache->outer_free_list_.push(item);

      if (reclaim_opt_.enable_reclaim_ && ATOMIC_BCAS(&thread_cache->status_, 0L, 1L)) {
        refresh_average_info(thread_cache);
        /**
         * We must ensure the thread owned the thread cache can do reclaim thread cache
         */
        if (thread_cache == get_thread_cache(thread_cache_idx_)
            && thread_cache->f_->need_to_reclaim(thread_cache)) {
          if (reclaim_opt_.debug_filter_ & 0x1) {
            POOL_LOG("F", thread_cache->f_, thread_cache);
          }

          int64_t num_to_move = static_cast<int64_t>(std::min(thread_cache->nr_average_,
                                static_cast<double>(thread_cache->nr_free_)));
          thread_cache->f_->free_to_cache(thread_cache, NULL, num_to_move);

          if (reclaim_opt_.debug_filter_ & 0x1) {
            POOL_LOG("-", thread_cache->f_, thread_cache);
          }

          refresh_average_info(thread_cache);
        }
      }
    }
  }
}

inline void ObObjFreeList::update_used()
{
  int64_t used = 0;
  ObThreadCache *next_thread_cache = NULL;

  if (OB_LIKELY(NULL != thread_cache_)) {
    next_thread_cache = thread_cache_->next_;
    used += thread_cache_->nr_malloc_;
    while (next_thread_cache != thread_cache_) {
      used += next_thread_cache->nr_malloc_;
      next_thread_cache = next_thread_cache->next_;
    }
    used_ = used;
  }
}

ObObjFreeListList::ObObjFreeListList()
  : tc_meta_allocator_(ObModIds::OB_CONCURRENCY_OBJ_POOL)
{
  mem_total_ = 0;
  nr_freelists_ = 0;
  is_inited_ = false;
}

int ObObjFreeListList::create_freelist(
    ObObjFreeList *&freelist, const char *name,
    const int64_t obj_size, const int64_t obj_count,
    const int64_t alignment /* = 16 */,
    const ObMemCacheType cache_type /* = OP_RECLAIM */,
    const bool is_meta /* = false */)
{
  int ret = OB_SUCCESS;
  freelist = NULL;

  if (OB_ISNULL(name) || OB_UNLIKELY(obj_size <= 0)
      || OB_UNLIKELY(obj_count < 0) || OB_UNLIKELY(alignment <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WDIAG, "invalid argument", K(name), K(obj_size), K(obj_count), K(alignment));
  } else if (!is_meta && OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WDIAG, "global object freelist list is not initialized success");
  } else if (nr_freelists_ >= ObObjFreeListList::MAX_NUM_FREELIST) {
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(EDIAG, "can't allocate object freelist, freelist is full",
           "max_freelist_num", ObObjFreeListList::MAX_NUM_FREELIST, K(ret));
  } else {
    if (OB_UNLIKELY(is_meta)) {
      freelist = new (std::nothrow) ObObjFreeList();
    } else {
      freelist = fl_allocator_.alloc();
    }
    if (OB_ISNULL(freelist)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(EDIAG, "failed to allocate memory for freelist", K(ret));
    } else {
      freelist->thread_cache_idx_ = get_next_free_slot();
      if (freelist->thread_cache_idx_ >= ObObjFreeListList::MAX_NUM_FREELIST) {
        ret = OB_SIZE_OVERFLOW;
        OB_LOG(EDIAG, "can't allocate object freelist, freelist is full",
                "max_freelist_num", ObObjFreeListList::MAX_NUM_FREELIST, K(ret));
        if (OB_UNLIKELY(is_meta)) {
          delete freelist;
        } else {
          fl_allocator_.free(freelist);
        }
        freelist = NULL;
      } else {
        freelist->mod_id_ = ObModIds::OB_CONCURRENCY_OBJ_POOL;
        freelist->freelists_ = &freelists_;
        freelists_.push(freelist);
        if (OB_UNLIKELY(is_meta)) {
          freelist->tc_allocator_ = &tc_meta_allocator_;
        } else {
          freelist->tc_allocator_ = &tc_allocator_;
        }
        if (OB_FAIL(freelist->init(name, obj_size, obj_count, alignment, cache_type))) {
          OB_LOG(WDIAG, "failed to initialize object freelist",
                 K(name), K(obj_size), K(obj_count), K(alignment));
        }
      }
    }
  }

  return ret;
}

void ObObjFreeListList::snap_baseline()
{
  ObAtomicSLL<ObObjFreeList> &fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList *fl = fll.head();
  while (NULL != fl) {
    fl->allocated_base_ = fl->allocated_;
    fl->used_base_ = fl->used_;
    fl = fll.next(fl);
  }
}

void ObObjFreeListList::dump_baselinerel()
{
  int ret = OB_SUCCESS;
  ObAtomicSLL<ObObjFreeList> &fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList *fl = fll.head();
  int64_t pos = 0;
  int64_t len = OB_MALLOC_NORMAL_BLOCK_SIZE;
  char *buf = reinterpret_cast<char *>(ob_malloc(len, ObModIds::OB_CONCURRENCY_OBJ_POOL));
  int64_t alloc_size = 0;

  if (NULL != buf) {
    databuff_printf(buf, len, pos, "\n     allocated      |       in-use       |  count  | type size  |   free list name\n");
    databuff_printf(buf, len, pos, "  relative to base  |  relative to base  |         |            |                 \n");
    databuff_printf(buf, len, pos, "--------------------|--------------------|---------|------------|----------------------------------\n");

    while (NULL != fl && OB_SUCCESS == ret) {
      alloc_size = fl->allocated_ - fl->allocated_base_;
      if (0 != alloc_size) {
        fl->update_used();
        ret = databuff_printf(buf, len, pos, " %18ld | %18ld | %7ld | %10ld | memory/%s\n",
                (fl->allocated_ - fl->allocated_base_) * fl->type_size_,
                (fl->used_ - fl->used_base_) * fl->type_size_,
                fl->used_ - fl->used_base_, fl->type_size_,
                fl->name_ ? fl->name_ : "<unknown>");
      }
      fl = fll.next(fl);
    }
    _OB_LOG(INFO, "dump object freelist baseline relative:%s", buf);
    ob_free(buf);
  }
}

int ObObjFreeListList::get_info(ObVector<ObObjFreeList *> &flls)
{
  int ret = OB_SUCCESS;
  ObAtomicSLL<ObObjFreeList> &fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList *fl = fll.head();
  while (OB_SUCC(ret) && NULL != fl) {
    fl->update_used();
    if (OB_FAIL(flls.push_back(fl))) {
      OB_LOG(WDIAG, "failed to push back fl", K(ret));
    } else {
      fl = fll.next(fl);
    }
  }
  return ret;
}

void ObObjFreeListList::dump()
{
  int64_t len = 1 << 16; // 64k
  ObMemAttr attr = default_memattr;
  attr.mod_id_ = ObModIds::OB_CONCURRENCY_OBJ_POOL;
  char *buf = reinterpret_cast<char *>(ob_malloc(len, attr));

  if (NULL != buf) {
    to_string(buf, len);
    _OB_LOG(INFO, "dump object freelist statistic:%s", buf);
    ob_free(buf);
  }
}

int64_t ObObjFreeListList::to_string(char *buf, const int64_t len) const
{
  int ret = OB_SUCCESS;
  ObAtomicSLL<ObObjFreeList> &fll = ObObjFreeListList::get_freelists().freelists_;
  ObObjFreeList *fl = fll.head();
  int64_t pos = 0;

  databuff_printf(buf, len, pos, "\n     allocated      |        in-use      | type size  | cache type |   free list name\n");
  databuff_printf(buf, len, pos,   "--------------------|--------------------|------------|------------|----------------------------------\n");
  while (NULL != fl && OB_SUCC(ret)) {
    fl->update_used();
    ret = databuff_printf(buf, len, pos, " %'18ld | %'18ld | %'10ld | %10s | %s\n",
            fl->allocated_ * fl->type_size_,
            fl->used_ * fl->type_size_, fl->type_size_,
            fl->only_global_ ? (fl->obj_count_base_ > 0 ? "tc" : "global") : "reclaim",
            fl->name_ ? fl->name_ : "<unknown>");
    fl = fll.next(fl);
  }
  return pos;
}

} // end of namespace common
} // end of namespace oceanbase
