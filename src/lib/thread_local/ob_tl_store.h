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

#ifndef OCEANBASE_LIB_THREAD_LOCAL_OB_TL_STORE_
#define OCEANBASE_LIB_THREAD_LOCAL_OB_TL_STORE_

#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <vector>
#include <algorithm>
#include "lib/lock/Mutex.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
inline int get_tc_tid()
{
  static __thread int tid = -1;
  if (tid == -1) {
    tid = static_cast<int>(syscall(__NR_gettid));
  }
  return tid;
}

class DefaultThreadStoreAlloc
{
public:
  inline void *alloc(const int64_t sz) { return ::malloc(sz); }
  inline void free(void *p) { ::free(p); p = NULL; }
};

template <class Type>
class DefaultInitializer
{
public:
  void operator()(void *ptr)
  {
    new(ptr) Type();
  }
};

template <class Type, class Initializer = DefaultInitializer<Type>,
          class Alloc = DefaultThreadStoreAlloc>
class ObTlStore
{
public:
  typedef ObTlStore<Type, Initializer, Alloc> TSelf;
public:
  struct Item
  {
    TSelf  *self;
    int     thread_id;
    Type    obj;
  };

  class SyncVector
  {
  public:
    typedef std::vector<Item *>        PtrArray;
  public:
    inline int      push_back(Item *ptr);

    template<class Function>
    inline int      for_each(Function &f) const;
    inline void     destroy();
  private:
    PtrArray ptr_array_;
    tbutil::Mutex mutex_;
  };

  template<class Function>
  class ObjPtrAdapter
  {
  public:
    ObjPtrAdapter(Function &f)
        : f_(f)
    {
    }
    void operator()(const Item *item)
    {
      if (NULL != item) {
        f_(&item->obj);
      }
    }
    void operator()(Item *item)
    {
      if (NULL != item) {
        f_(&item->obj);
      }
    }
  protected:
    Function &f_;
  };

public:
  static const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
public:
  ObTlStore(Alloc &alloc);

  ObTlStore(Initializer &initializer, Alloc &alloc);

  ObTlStore(Initializer &initializer);

  ObTlStore();

  virtual ~ObTlStore();

  static void destroy_object(Item *item);

  int32_t  init();

  void     destroy();

  Type    *get();

  template<class Function>
  int      for_each_obj_ptr(Function &f) const;

  template<class Function>
  int      for_each_item_ptr(Function &f) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTlStore);
private:
  pthread_key_t key_;
  DefaultInitializer<Type> default_initializer_;
  DefaultThreadStoreAlloc default_alloc_;
  Initializer &initializer_;
  SyncVector ptr_array_;
  Alloc &alloc_;
  bool is_inited_;
};

template <class Type, class Initializer, class Alloc>
int ObTlStore<Type, Initializer, Alloc>::SyncVector::push_back(Item *ptr)
{
  int ret = OB_SUCCESS;
  if (NULL == ptr) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WDIAG, "invalid argument", K(ptr), K(ret));
  } else {
    mutex_.lock();
    try {
      ptr_array_.push_back(ptr);
    } catch (std::bad_alloc) {
      LIB_LOG(EDIAG, "memory is not enough when push_back");
      ret = OB_ERR_UNEXPECTED;
    }
    mutex_.unlock();
  }
  return ret;
}

template <class Type, class Initializer, class Alloc>
template<class Function>
int ObTlStore<Type, Initializer, Alloc>::SyncVector::for_each(Function &f) const
{
  int ret = OB_SUCCESS;
  mutex_.lock();
  std::for_each(ptr_array_.begin(), ptr_array_.end(), f);
  mutex_.unlock();
  return ret;
}

template <class Type, class Initializer, class Alloc>
void ObTlStore<Type, Initializer, Alloc>::SyncVector::destroy()
{
  mutex_.lock();
  ptr_array_.clear();
  mutex_.unlock();
}

template <class Type, class Initializer, class Alloc>
ObTlStore<Type, Initializer, Alloc>::ObTlStore(Alloc &alloc)
    : key_(INVALID_THREAD_KEY),
      default_initializer_(),
      default_alloc_(),
      initializer_(default_initializer_),
      ptr_array_(),
      alloc_(alloc),
      is_inited_(false)
{
}

template <class Type, class Initializer, class Alloc>
ObTlStore<Type, Initializer, Alloc>::ObTlStore(
    Initializer &initializer, Alloc &alloc)
    : key_(INVALID_THREAD_KEY),
      default_initializer_(),
      default_alloc_(),
      initializer_(initializer),
      ptr_array_(),
      alloc_(alloc),
      is_inited_(false)
{
}

template <class Type, class Initializer, class Alloc>
ObTlStore<Type, Initializer, Alloc>::ObTlStore(
    Initializer &initializer)
    : key_(INVALID_THREAD_KEY),
      default_initializer_(),
      default_alloc_(),
      initializer_(initializer),
      ptr_array_(),
      alloc_(default_alloc_),
      is_inited_(false)
{
}

template <class Type, class Initializer, class Alloc>
ObTlStore<Type, Initializer, Alloc>::ObTlStore()
    : key_(INVALID_THREAD_KEY),
      default_initializer_(),
      default_alloc_(),
      initializer_(default_initializer_),
      ptr_array_(),
      alloc_(default_alloc_),
      is_inited_(false)
{
}

template <class Type, class Initializer, class Alloc>
ObTlStore<Type, Initializer, Alloc>::~ObTlStore()
{
  destroy();
}

template <class Type, class Initializer, class Alloc>
void ObTlStore<Type, Initializer, Alloc>::destroy_object(Item *item)
{
  if (NULL != item) {
    item->obj.~Type();
    item->self->alloc_.free(item);
    item = NULL;
  }
}

template <class Type, class Initializer, class Alloc>
int32_t ObTlStore<Type, Initializer, Alloc>::init()
{
  int32_t ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LIB_LOG(EDIAG, "ObTlStore has already initialized.", K(ret));
  } else {
    if (INVALID_THREAD_KEY == key_) {
      int err = pthread_key_create(&key_, NULL);
      if (0 != err) {
        LIB_LOG(EDIAG, "pthread_key_create error", KERRNOMSGS(err));
        if (errno == ENOMEM) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        is_inited_ = true;
      }
    } else {
      LIB_LOG(EDIAG, "key_ should be INVALID_THREAD_KEY");
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

template <class Type, class Initializer, class Alloc>
void ObTlStore<Type, Initializer, Alloc>::destroy()
{
  if (is_inited_) {
    if (INVALID_THREAD_KEY != key_) {
      //void* mem = pthread_getspecific(key_);
      //if (NULL != mem) destroy_object(mem);
      int err = pthread_key_delete(key_);
      if (err != 0) {
        LIB_LOG(EDIAG, "pthread_key_delete error", KERRNOMSGS(err));
      }
      key_ = INVALID_THREAD_KEY;
    }
    for_each_item_ptr(destroy_object);
    ptr_array_.destroy();
    is_inited_ = false;
  }
}

template <class Type, class Initializer, class Alloc>
Type *ObTlStore<Type, Initializer, Alloc>::get()
{
  Type *ret = NULL;
  if (!is_inited_) {
    LIB_LOG(EDIAG, "ObTlStore has not been initialized");
  } else if (OB_UNLIKELY(INVALID_THREAD_KEY == key_)) {
    LIB_LOG(EDIAG, "ObTlStore thread key is invalid");
  } else {
    Item *item = reinterpret_cast<Item *>(pthread_getspecific(key_));
    if (OB_UNLIKELY(NULL == item)) {
      item = reinterpret_cast<Item *>(alloc_.alloc(sizeof(Item)));
      if (NULL != item) {
        if (0 != pthread_setspecific(key_, item)) {
          alloc_.free(item);
          item = NULL;
        } else {
          initializer_(&item->obj);
          item->self = this;
          item->thread_id = get_tc_tid();
          ptr_array_.push_back(item);
        }
      }
    }
    if (OB_UNLIKELY(NULL != item)) {
      ret = &item->obj;
    }
  }
  return ret;
}

template <class Type, class Initializer, class Alloc>
template<class Function>
int ObTlStore<Type, Initializer, Alloc>::for_each_obj_ptr(Function &f) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LIB_LOG(EDIAG, "ObTlStore has not been initialized");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObjPtrAdapter<Function> opa(f);
    ret = ptr_array_.for_each(opa);
  }
  return ret;
}

template <class Type, class Initializer, class Alloc>
template<class Function>
int ObTlStore<Type, Initializer, Alloc>::for_each_item_ptr(Function &f) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    LIB_LOG(EDIAG, "ObTlStore has not been initialized");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = ptr_array_.for_each(f);
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_LIB_THREAD_LOCAL_OB_TL_STORE_
