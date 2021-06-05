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

#ifndef OB_LIB_CONCURRENCY_OBJPOOL_H_
#define OB_LIB_CONCURRENCY_OBJPOOL_H_

#include <typeinfo>
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/list/ob_atomic_list.h"
#include "lib/list/ob_intrusive_list.h"
#include "lib/lock/ob_mutex.h"
#include "lib/container/ob_vector.h"

DEFINE_HAS_MEMBER(OP_LOCAL_NUM);

namespace oceanbase
{
namespace common
{
struct ObThreadCache;
class ObObjFreeList;
class ObObjFreeListList;
class ObFixedMemAllocator;

enum ObMemCacheType
{
  OP_GLOBAL,
  OP_TC,
  OP_RECLAIM
};

enum ObReclaimOptType
{
  ENABLE_RECLAIM,
  STEAL_THREAD_COUNT,
  RECLAIM_FACTOR,
  MAX_OVERAGE,
  DEBUG_FILTER
};

struct ObReclaimOpt
{
  ObReclaimOpt()
      : enable_reclaim_(true), steal_thread_count_(0), reclaim_factor_(30),
        max_overage_(10), debug_filter_(0)
  { }
  ~ObReclaimOpt() { }

  bool enable_reclaim_;
  int64_t steal_thread_count_; // when steal memory from neighbor threads, max traverse thread cache count
  int64_t reclaim_factor_; // percentage, * 100
  int64_t max_overage_;
  // Debug filter bit mask:
  // bit 0: reclaim in thread_cache_free,
  // reclaim memory from all caches in the same thread
  // bit 1: reclaim in thread_cache_alloc, print
  // current object thread cache reclaim info
  int64_t debug_filter_;
};

struct ObChunkInfo
{
  ObChunkInfo()
  {
    memset(this, 0, sizeof(ObChunkInfo));
  }

  ~ObChunkInfo() { }

  pthread_t tid_;

  int64_t type_size_;
  int64_t obj_count_;
  int64_t allocated_;
  int64_t length_;

  // inner free list will only be
  // accessed by creator-thread
  void *inner_free_list_;
  void *head_;

  ObThreadCache *thread_cache_;

  LINK(ObChunkInfo, link_);

#ifdef DOUBLE_FREE_CHECK
  // magic code for each item,
  // it's used to check double-free issue.
  uint8_t item_magic_[0];
#endif
};

struct ObThreadCache
{
  ObThreadCache()
  {
    memset(this, 0, sizeof(ObThreadCache));
  }

  ~ObThreadCache() { }

  // the address must be aligned with 16 bytes if it uses cas 128
  //
  // outer free list will be accessed by:
  // - creator-thread, as a producer-thread
  // - consumer-thread
  // - neighbor-thread
  ObAtomicList outer_free_list_;

  ObObjFreeList *f_;

  // using for memory reclaim algorithm
  double nr_average_;
  int64_t nr_total_;
  int64_t nr_free_;
  int64_t nr_overage_;
  int64_t nr_malloc_;

  // represent the status(state) of allocator: Malloc-ing(0) or Free-ing(1),
  // I use it as an simple state machine - calculating the minimum of free
  // memory only when the status change from Malloc-ing to Free-ing.
  volatile int64_t status_;

  int64_t nr_free_chunks_;
  DLL<ObChunkInfo> free_chunk_list_;

  ObThreadCache *prev_;
  ObThreadCache *next_;
};

class ObObjFreeList
{
  friend class ObObjFreeListList;
  friend struct ObThreadCache;
public:
  ObObjFreeList();
  ~ObObjFreeList() { (void)mutex_destroy(&lock_); }

  // alignment must be a power of 2
  int init(const char *name, const int64_t type_size, const int64_t obj_count,
           const int64_t alignment = 16, const ObMemCacheType cache_type = OP_RECLAIM);
  void *alloc();
  void free(void *item);
  ObReclaimOpt &get_reclaim_opt() { return reclaim_opt_; }
  const char *get_name() const { return name_; }
  int64_t get_allocated() const { return allocated_; }
  int64_t get_allocated_base() const { return allocated_base_; }
  int64_t get_type_size() const { return type_size_; }
  int64_t get_used() const { return used_; }
  int64_t get_used_base() const { return used_base_; }
  int64_t get_chunk_count() const { return chunk_count_; }
  int64_t get_chunk_byte_size() const { return chunk_byte_size_; }
  int64_t get_base_obj_size() const { return type_size_base_; }

private:
  void show_info(const char *file, const int line,
                 const char *tag, ObThreadCache *thread_cache);
  ObChunkInfo *&get_chunk_info(void *item);
#ifdef DOUBLE_FREE_CHECK
  int64_t get_chunk_item_magic_idx(void *item, ObChunkInfo **chunk_info, const bool do_check  = false);
#endif
  void set_chunk_item_magic(ObChunkInfo *chunk_info, void *item);
  void clear_chunk_item_magic(ObChunkInfo *chunk_info, void *item);
  ObChunkInfo *chunk_create(ObThreadCache *thread_cache);
  void chunk_delete(ObThreadCache *thread_cache, ObChunkInfo *chunk_info);
  void *malloc_whole_chunk(ObThreadCache *thread_cache, ObChunkInfo *chunk_info);
  void *malloc_from_chunk(ObThreadCache *thread_cache, ObChunkInfo *chunk_info);
  void free_to_chunk(ObThreadCache *thread_cache, void *item);
  void *malloc_from_cache(ObThreadCache *thread_cache, const int64_t nr);
  void free_to_cache(ObThreadCache *thread_cache, void *item, const int64_t nr);
  void refresh_average_info(ObThreadCache *thread_cache);
  bool need_to_reclaim(ObThreadCache *thread_cache);

  void *global_alloc();
  void global_free(void *item);
  ObThreadCache *init_thread_cache(ObChunkInfo *&chunk_info);

  void update_used();

private:
  ObAtomicList obj_free_list_;
  SLINK(ObObjFreeList, link_);
  ObAtomicSLL<ObObjFreeList> *freelists_; // global free list list

  ObFixedMemAllocator *tc_allocator_;
  ObReclaimOpt reclaim_opt_;

  bool is_inited_;
  bool only_global_;
  int64_t mod_id_;
  int64_t thread_cache_idx_;
  const char *name_;

  // actual size of each object, each object in one chunk, each chunk includes
  // an extra void* to store instance of ObChunkInfo.
  int64_t type_size_;
  // user specified size of object
  int64_t type_size_base_;
  int64_t alignment_;

  // user specified number of elements
  int64_t obj_count_base_;
  // number of elements of each chunk
  int64_t obj_count_per_chunk_;

  // byte size of one block allocated from ob_malloc()
  int64_t chunk_byte_size_;
  // memory block count, blocks are allocated from ob_malloc()
  int64_t chunk_count_;

  int64_t used_;
  int64_t allocated_;
  int64_t allocated_base_;
  int64_t used_base_;

  // number of thread cache in one object freelist
  int64_t nr_thread_cache_;
  ObThreadCache *thread_cache_;
  ObMutex lock_;

  DISALLOW_COPY_AND_ASSIGN(ObObjFreeList);
};

// Allocator for fixed size memory blocks.
class ObFixedMemAllocator
{
public:
  ObFixedMemAllocator() { fl_ = NULL; }
  virtual ~ObFixedMemAllocator() { }

  // Creates a new global allocator.
  // @param name identification tag used for mem tracking .
  // @param obj_size size of memory blocks to be allocated.
  // @param obj_count number of units to be allocated if free pool is
  //        empty.
  // @param alignment of objects must be a power of 2.
  // initialize the parameters of the allocator.
  int init(ObObjFreeListList &fll, const char *name, const int64_t obj_size,
           const int64_t obj_count, const int64_t alignment,
           const ObMemCacheType cache_type = OP_RECLAIM, const bool is_meta = false);

  int init(const char *name, const int64_t obj_size,
           const int64_t obj_count, const int64_t alignment,
           const ObMemCacheType cache_type = OP_RECLAIM, const bool is_meta = false);

  // Allocate a block of memory (size specified during construction
  // of Allocator.
  virtual void *alloc_void()
  {
    void *ret = NULL;
    if (OB_LIKELY(NULL != fl_)) {
      ret = fl_->alloc();
    }
    return ret;
  }

  // Deallocate a block of memory allocated by the Allocator.
  virtual void free_void(void *ptr)
  {
    if (OB_LIKELY(NULL != fl_) && OB_LIKELY(NULL != ptr)) {
      fl_->free(ptr);
      ptr = NULL;
    }
  }

  void set_reclaim_opt(const ObReclaimOptType opt_type, const int64_t value)
  {
    if (OB_LIKELY(NULL != fl_)) {
      switch (opt_type) {
        case ENABLE_RECLAIM:
          fl_->get_reclaim_opt().enable_reclaim_ = (0 != value);
          break;

        case STEAL_THREAD_COUNT:
          fl_->get_reclaim_opt().steal_thread_count_ = (value > 0) ? value : 0;
          break;

        case RECLAIM_FACTOR:
          fl_->get_reclaim_opt().reclaim_factor_ = (value >= 0 && value <= 100) ? value : 30;
          break;

        case MAX_OVERAGE:
          fl_->get_reclaim_opt().max_overage_ = (value >= 0) ? value : 10;
          break;

        case DEBUG_FILTER:
          fl_->get_reclaim_opt().debug_filter_ = (value >= 0 && value <= 3) ? value : 0;
          break;

        default:
          break;
      }
    }
  }

  int64_t get_obj_size() { return ((NULL == fl_) ? 0 : fl_->get_base_obj_size()); }

protected:
  ObObjFreeList *fl_;

  DISALLOW_COPY_AND_ASSIGN(ObFixedMemAllocator);
};

#if defined(GCC_52)
template <class T = void> struct __is_default_constructible__;

template <> struct __is_default_constructible__<void>
{
protected:
    // Put base typedefs here to avoid pollution
    struct twoc { char a, b; };
    template <bool> struct test { typedef char type; };
public:
    static bool const value = false;
};

template <> struct __is_default_constructible__<>::test<true> { typedef twoc type; };

template <class T> struct __is_default_constructible__ : __is_default_constructible__<>
{
private:
    template <class U> static typename test<!!sizeof(::new U())>::type sfinae(U*);
    template <class U> static char sfinae(...);
public:
    static bool const value = sizeof(sfinae<T>(0)) > 1;
};
#else
template <typename T>
struct __is_default_constructible__
{
  typedef char yes[1];
  typedef char no [2];

  template <typename Type>
  static yes &chk(__typeof__(&Type()));

  template <typename>
  static no &chk(...);

  static bool const value = sizeof(chk<T>(0)) == sizeof(yes);
};
#endif

template <class T>
struct ObClassConstructor
{
  template <class Type, bool Cond = true>
  struct Constructor
  {
    Type *operator ()(void *ptr)
    {
      return new (ptr) Type();
    }
  };

  template <class Type>
  struct Constructor<Type, false>
  {
    Type *operator ()(void *ptr)
    {
      return reinterpret_cast<Type *>(ptr);
    }
  };

  T *operator ()(void *ptr)
  {
    Constructor<T, __is_default_constructible__<T>::value> construct;
    return construct(ptr);
  }
};

#define RND16(x) (((x) + 15) & ~15)

// Allocator for Class objects. It uses a prototype object to do
// fast initialization. Prototype of the template class is created
// when the fast allocator is created. This is instantiated with
// default (no argument) constructor. Constructor is not called for
// the allocated objects. Instead, the prototype is just memory
// copied onto the new objects. This is done for performance reasons.
template<class T>
class ObClassAllocator : public ObFixedMemAllocator
{
public:
  ObClassAllocator() { fl_ = NULL; }
  virtual ~ObClassAllocator() { }

  // Create a new global class specific ClassAllocator.
  // @param name some identifying name, used for mem tracking purposes.
  // @param obj_count number of units to be allocated if free pool is
  //        empty.
  // @param alignment of objects must be a power of 2.
  // @param is_meta is allocator for meta
  static ObClassAllocator<T> *get(const int64_t obj_count = 64,
                                  const ObMemCacheType cache_type = OP_RECLAIM,
                                  const int64_t alignment = 16,
                                  const bool is_meta = false)
  {
    ObClassAllocator<T> *instance = NULL;
    while (OB_UNLIKELY(once_ < 2)) {
      if (ATOMIC_BCAS(&once_, 0, 1)) {
        instance = new (std::nothrow) ObClassAllocator<T>();
        if (OB_LIKELY(NULL != instance)) {
          if (common::OB_SUCCESS != instance->init(typeid(T).name(), RND16(sizeof(T)),
                                                   obj_count, RND16(alignment), cache_type, is_meta)) {
            _OB_LOG(ERROR, "failed to init class allocator %s", typeid(T).name());
            delete instance;
            instance = NULL;
            ATOMIC_BCAS(&once_, 1, 0);
          } else {
            if (instance_ != NULL) {
              delete instance_;
            }
            instance_ = instance;
            (void)ATOMIC_BCAS(&once_, 1, 2);
          }
        } else {
          (void)ATOMIC_BCAS(&once_, 1, 0);
        }
      }
    }
    return (ObClassAllocator<T> *)instance_;
  }

  // Allocates objects of the templated type.
  virtual T *alloc()
  {
    T *ret = NULL;
    void *ptr = NULL;
    if (OB_LIKELY(NULL != fl_) && OB_LIKELY(NULL != (ptr = fl_->alloc()))) {
      ObClassConstructor<T> construct;
      ret = construct(ptr);
    }
    return ret;
  }

  // Deallocates objects of the templated type.
  // @param ptr pointer to be freed.
  virtual void free(T *ptr)
  {
    if (OB_LIKELY(NULL != fl_) && OB_LIKELY(NULL != ptr)) {
      ptr->~T();
      fl_->free(ptr);
      ptr = NULL;
    }
  }

  // Allocate objects of the templated type via the inherited interface
  // using void pointers.
  virtual void *alloc_void() { return reinterpret_cast<void *>(alloc()); }

  // Deallocate objects of the templated type via the inherited
  // interface using void pointers.
  // @param ptr pointer to be freed.
  virtual void free_void(void *ptr) { free(reinterpret_cast<T *>(ptr)); }

protected:
  static volatile int64_t once_; // for creating singleton instance
  static volatile ObClassAllocator<T> *instance_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObClassAllocator);
};

// Allocator for meta of ObObjFreeList, ObChunkInfo, ObThreadCache. The other
// objects need created object pools with these three objects. But these three
// objects are also allocated form their object pools. The object pool is
// implied with these three objects. So the object pools of these three objects
// need an especial allocator to allocate themself.
template<class T>
class ObMetaAllocator : public ObFixedMemAllocator
{
public:
  // Create a new class specific ClassAllocator.
  // @param mod allocate mode.
  explicit ObMetaAllocator(const int64_t mod)
  {
    free_list_ = NULL;
    allocator_.set_mod_id(mod);
    (void)mutex_init(&allocator_lock_);
  }

  virtual ~ObMetaAllocator() { (void)mutex_destroy(&allocator_lock_); }

  // Allocates objects of the templated type.
  T *alloc()
  {
    int ret = OB_SUCCESS;
    T *obj = NULL;
    void *ptr = NULL;
    if (OB_SUCC(mutex_acquire(&allocator_lock_))) {
      if (OB_LIKELY(NULL != free_list_)) {
        ptr = free_list_;
        free_list_ = *(reinterpret_cast<void **>(ptr));
      } else {
        ptr = allocator_.alloc_aligned(sizeof(T), 16);
      }
      mutex_release(&allocator_lock_);
    }
    if (OB_LIKELY(NULL != ptr)) {
      obj = new (ptr) T();
    }
    return obj;
  }

  // Deallocates objects of the templated type.
  // @param ptr pointer to be freed.
  void free(T *ptr)
  {
    int ret = OB_SUCCESS;
    if (OB_LIKELY(NULL != ptr)) {
      ptr->~T();
      if (OB_SUCC(mutex_acquire(&allocator_lock_))) {
        *(reinterpret_cast<void **>(ptr)) = free_list_;
        free_list_ = ptr;
        mutex_release(&allocator_lock_);
      }
    }
  }

  // Allocate objects of the templated type via the inherited interface
  // using void pointers.
  virtual void *alloc_void() { return reinterpret_cast<void *>(alloc()); }

  // Deallocate objects of the templated type via the inherited
  // interface using void pointers.
  // @param ptr pointer to be freed.
  virtual void free_void(void *ptr) { free(reinterpret_cast<T *>(ptr)); }

private:
  void *free_list_;
  PageArena<char> allocator_;
  ObMutex allocator_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObMetaAllocator);
};

// Allocator for space class, a class with a lot of uninitialized
// space/members. It uses an instantiate fucntion do initialization
// of objects. This is particulary useful if most of the space in
// the objects does not need to be intialized. The inifunction passed
// can be used to intialize a few fields selectively. Using
// ObClassAllocator for space objects would unnecessarily initialized
// all of the members.
template<class T>
class ObSparseClassAllocator : public ObClassAllocator<T>
{
public:
  ObSparseClassAllocator() : instantiate_(NULL), proto_() { }
  virtual ~ObSparseClassAllocator() { }

  // Create a new class specific ObSparseClassAllocator.
  // @param name some identifying name, used for mem tracking purposes.
  // @param obj_count number of units to be allocated if free pool is empty.
  // @param alignment of objects must be a power of 2.
  // @param instantiate_func
  static ObSparseClassAllocator<T> *get(void (*instantiate_func) (T &proto, T &instance) = NULL,
                                        const int64_t obj_count = 64)
  {
    ObSparseClassAllocator<T> *instance = NULL;
    while (OB_UNLIKELY(once_ < 2)) {
      if (ATOMIC_BCAS(&once_, 0, 1)) {
        instance = new (std::nothrow) ObSparseClassAllocator<T>();
        if (OB_LIKELY(NULL != instance)) {
          instance->instantiate_ = instantiate_func;
          if (common::OB_SUCCESS != instance->init(typeid(T).name(), RND16(sizeof(T)),
                                                   obj_count, 16, OP_RECLAIM, false)) {
            _OB_LOG(ERROR, "failed to init class sparse allocator %s", typeid(T).name());
            delete instance;
            instance = NULL;
            ATOMIC_BCAS(&once_, 1, 0);
          } else {
            instance_ = instance;
            (void)ATOMIC_BCAS(&once_, 1, 2);
          }
        } else {
          (void)ATOMIC_BCAS(&once_, 1, 0);
        }
      }
    }
    return (ObSparseClassAllocator<T> *)instance_;
  }

  // Allocates objects of the templated type.
  virtual T *alloc()
  {
    T *ret = NULL;
    void *ptr = NULL;
    if (OB_LIKELY(NULL != this->fl_) && OB_LIKELY(NULL != (ptr = this->fl_->alloc()))) {
      if (OB_UNLIKELY(NULL == instantiate_)) {
        ret = new (ptr) T();
      } else {
        (*instantiate_) (proto_.type_object_, *reinterpret_cast<T *>(ptr));
        ret = reinterpret_cast<T *>(ptr);
      }
    }
    return ret;
  }

  void (*instantiate_) (T &proto, T &instance);

  struct
  {
    T type_object_;
    int64_t space_holder_;
  } proto_;

private:
  static volatile int64_t once_; // for creating singleton instance
  static volatile ObSparseClassAllocator<T> *instance_;

  DISALLOW_COPY_AND_ASSIGN(ObSparseClassAllocator);
};

class ObObjFreeListList
{
  friend class ObObjFreeList;
public:
  static const int64_t MAX_NUM_FREELIST;

  ~ObObjFreeListList() { }

  int create_freelist(ObObjFreeList *&free_list, const char *name,
                      const int64_t obj_size, const int64_t obj_count,
                      const int64_t alignment = 16, const ObMemCacheType cache_type = OP_RECLAIM,
                      const bool is_meta = false);

  static ObObjFreeListList &get_freelists()
  {
    static ObObjFreeListList g_freelists;
    g_freelists.once_init();

    return g_freelists;
  }

  int64_t get_next_free_slot() { return ATOMIC_FAA(&nr_freelists_, 1); }
  int get_info(ObVector<ObObjFreeList *> &flls);
  void dump();
  void dump_baselinerel();
  void snap_baseline();
  int64_t to_string(char *buf, const int64_t len) const;

private:
  void once_init()
  {
    int ret = OB_SUCCESS;
    while (OB_UNLIKELY(once_ < 2) && OB_SUCC(ret)) {
      if (ATOMIC_BCAS(&once_, 0, 1)) {
        if (OB_FAIL(fl_allocator_.init(*this, "ObjFreeList", sizeof(ObObjFreeList), 64, 16, OP_GLOBAL, true))) {
          OB_LOG(WARN, "failed to init ObjFreeList allocator", K(ret));
          ATOMIC_BCAS(&once_, 1, 0);
        } else if (OB_FAIL(tc_allocator_.init(*this, "ObThreadCache", sizeof(ObThreadCache), 64, 16, OP_RECLAIM, true))) {
          OB_LOG(WARN, "failed to init ObThreadCache allocator", K(ret));
          ATOMIC_BCAS(&once_, 1, 0);
        } else {
          is_inited_ = true;
          (void)ATOMIC_BCAS(&once_, 1, 2);
        }
      }
    }
  }

private:
  static volatile int64_t once_; // for creating singleton instance

  ObObjFreeListList();

  ObAtomicSLL<ObObjFreeList> freelists_; // global free list list
  volatile int64_t nr_freelists_;
  volatile int64_t mem_total_;

  bool is_inited_;
  ObMetaAllocator<ObThreadCache> tc_meta_allocator_;

  ObClassAllocator<ObObjFreeList> fl_allocator_;
  ObClassAllocator<ObThreadCache> tc_allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObObjFreeListList);
};

inline int ObFixedMemAllocator::init(
    ObObjFreeListList &fll,
    const char *name, const int64_t obj_size,
    const int64_t obj_count, const int64_t alignment,
    const ObMemCacheType cache_type /* = OP_RECLAIM */,
    const bool is_meta /* = false */)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(fll.create_freelist(fl_, name, obj_size, obj_count,
                                  alignment, cache_type, is_meta))) {
    // do nothing
  } else if (OB_ISNULL(fl_)) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

inline int ObFixedMemAllocator::init(
    const char *name, const int64_t obj_size,
    const int64_t obj_count, const int64_t alignment,
    const ObMemCacheType cache_type /* = OP_RECLAIM */,
    const bool is_meta /* = false */)
{
  return init(ObObjFreeListList::get_freelists(), name, obj_size, obj_count,
              alignment, cache_type, is_meta);
}

struct ObThreadFreeList
{
  ObThreadFreeList() : reserved_size_(64), allocated_(0), freelist_(NULL) { }
  ~ObThreadFreeList() { }

  int64_t reserved_size_;
  int64_t allocated_;
  void *freelist_;
};

template<class T>
volatile int64_t ObClassAllocator<T>::once_ = 0;
template<class T>
volatile ObClassAllocator<T> *ObClassAllocator<T>::instance_ = NULL;

template<class T>
volatile int64_t ObSparseClassAllocator<T>::once_ = 0;
template<class T>
volatile ObSparseClassAllocator<T> *ObSparseClassAllocator<T>::instance_ = NULL;

template <class T>
struct OPNum
{
  template <class Type, bool Cond = true>
  struct GetOPLocalNum
  {
    static const int64_t v = Type::OP_LOCAL_NUM;
  };

  template <class Type>
  struct GetOPLocalNum<Type, false>
  {
    static const int64_t v = 64;
  };

  static const int64_t LOCAL_NUM = GetOPLocalNum<T, HAS_MEMBER(T, OP_LOCAL_NUM)>::v;
};

// object pool user interface
// Warning: 1. the cached memory can't be freed even through the thread is dead.
//          2. double free is very terriable.
//          3. because object pool uses singleton, please only use one of global,
//             tc or reclaim interfaces for each object type in the whole procject.

// global pool allocator interface
#define op_alloc_args(type, args...) \
  ({ \
    type *ret = NULL; \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM, common::OP_GLOBAL); \
    if (OB_LIKELY(NULL != instance)) { \
      type *tmp = instance->alloc(); \
      if (OB_LIKELY(NULL != tmp)) { \
        ret = new (tmp) type(args); \
      } \
    } \
    ret; \
  })

#define op_alloc(type) \
  ({ \
    type *ret = NULL; \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM, common::OP_GLOBAL); \
    if (OB_LIKELY(NULL != instance)) { \
      ret = instance->alloc(); \
    } \
    ret; \
  })

#define op_free(ptr) \
  ({ \
    common::ObClassAllocator<__typeof__(*ptr)> *instance = common::ObClassAllocator<__typeof__(*ptr)>::get(common::OPNum<__typeof__(*ptr)>::LOCAL_NUM, common::OP_GLOBAL); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->free(ptr); \
    } \
  })

// thread cache pool allocator interface
#define op_tc_alloc_args(type, args...) \
  ({ \
    type *ret = NULL; \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM, common::OP_TC); \
    if (OB_LIKELY(NULL != instance)) { \
      type *tmp = instance->alloc(); \
      if (OB_LIKELY(NULL != tmp)) { \
        ret = new (tmp) type(args); \
      } \
    } \
    ret; \
  })

#define op_tc_alloc(type) \
  ({ \
    type *ret = NULL; \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM, common::OP_TC); \
    if (OB_LIKELY(NULL != instance)) { \
      ret = instance->alloc(); \
    } \
    ret; \
  })

#define op_tc_free(ptr) \
  ({ \
    common::ObClassAllocator<__typeof__(*ptr)> *instance = common::ObClassAllocator<__typeof__(*ptr)>::get(common::OPNum<__typeof__(*ptr)>::LOCAL_NUM, common::OP_TC); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->free(ptr); \
    } \
  })

// thread cache pool and reclaim allocator interface
#define op_reclaim_alloc_args(type, args...) \
  ({ \
    type *ret = NULL; \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      type *tmp = instance->alloc(); \
      if (OB_LIKELY(NULL != tmp)) { \
        ret = new (tmp) type(args); \
      } \
    } \
    ret; \
  })

#define op_reclaim_alloc(type) \
  ({ \
    type *ret = NULL; \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      ret = instance->alloc(); \
    } \
    ret; \
  })

#define op_reclaim_free(ptr) \
  ({ \
    common::ObClassAllocator<__typeof__(*ptr)> *instance = common::ObClassAllocator<__typeof__(*ptr)>::get(common::OPNum<__typeof__(*ptr)>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->free(ptr); \
    } \
  })

#define op_reclaim_opt(type, reclaim_type, value) \
  ({ \
    common::ObClassAllocator<type> *instance = common::ObClassAllocator<type>::get(common::OPNum<type>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->set_reclaim_opt(reclaim_type, value); \
    } \
  })

// thread cache pool and reclaim sparse allocator interface
#define op_reclaim_sparse_alloc(type, init_func) \
  ({ \
    type *ret = NULL; \
    common::ObSparseClassAllocator<type> *instance = common::ObSparseClassAllocator<type>::get(init_func, common::OPNum<type>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      ret = instance->alloc(); \
    } \
    ret; \
  })

#define op_reclaim_sparse_free(ptr) \
  ({ \
    common::ObSparseClassAllocator<__typeof__(*ptr)> *instance = common::ObSparseClassAllocator<__typeof__(*ptr)>::get(NULL, common::OPNum<__typeof__(*ptr)>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->free(ptr); \
    } \
  })

#define op_reclaim_sparse_opt(type, init_func, reclaim_type, value) \
  ({ \
    common::ObSparseClassAllocator<type> *instance = common::ObSparseClassAllocator<type>::get(init_func, common::OPNum<type>::LOCAL_NUM); \
    if (OB_LIKELY(NULL != instance)) { \
      instance->set_reclaim_opt(reclaim_type, value); \
    } \
  })

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_CONCURRENCY_OBJPOOL_H_
