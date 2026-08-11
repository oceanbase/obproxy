/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * *************************************************************
 *
 * Note: it would have been nice to have one 'ObPtr' class, but the
 * templating system on some compilers is so broken that it cannot
 * correctly compile ObPtr without downcasting the ptr_ object to
 * a ObRefCountObj.
 */

#ifndef OB_LIB_PTR_H_
#define OB_LIB_PTR_H_

#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_mem_leak_checker.h"

namespace oceanbase
{
namespace common
{
#define DEC_SHARED_REF(ref_ptr) \
  do {  \
    if (OB_NOT_NULL(ref_ptr)) { \
      ref_ptr->dec_ref(); \
      ref_ptr = NULL; \
    } \
  } while(0)

#define DEC_AND_INC_SHARED_REF(des_ref_ptr, src_ref_ptr) \
  do {  \
    if (OB_NOT_NULL(des_ref_ptr)) { \
      des_ref_ptr->dec_ref(); \
      des_ref_ptr = NULL; \
    } \
    if (OB_NOT_NULL(src_ref_ptr)) { \
      des_ref_ptr = src_ref_ptr;  \
      des_ref_ptr->inc_ref(); \
    } \
  } while(0)

class ObSharedRefCount
{
public:
  ObSharedRefCount() : ref_count_(0) {}
  virtual ~ObSharedRefCount() {}

  virtual void free() = 0;

  inline void inc_ref()
  {
    ATOMIC_FAA(&ref_count_, 1);
  }

  inline void dec_ref()
  {
    if (1 == ATOMIC_FAA(&ref_count_, -1)) {
      free();
    }
  }

  inline int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }

  volatile int64_t ref_count_;
};

class ObForceVFPTToTop
{
public:
  ObForceVFPTToTop() { }
  virtual ~ObForceVFPTToTop() { }
};

// class ObRefCountObj
// prototypical class for reference counting
class ObRefCountObj : public ObForceVFPTToTop
{
public:
  ObRefCountObj() : refcount_(0) { }
  virtual ~ObRefCountObj() { }

  ObRefCountObj(const ObRefCountObj &s) : refcount_(0)
  {
    UNUSED(s);
  }

  ObRefCountObj &operator=(const ObRefCountObj &s)
  {
    UNUSED(s);
    return (*this);
  }

  int64_t refcount_inc();
  int64_t refcount_dec();
  int64_t refcount() const;

  virtual void free() { delete this; }

  volatile int64_t refcount_;
};

// Increment the reference count, returning the new count.
inline int64_t ObRefCountObj::refcount_inc()
{
  return ATOMIC_FAA(&refcount_, 1) + 1;
}

// Decrement the reference count, returning the new count.
inline int64_t ObRefCountObj::refcount_dec()
{
  return ATOMIC_FAA(&refcount_, -1) - 1;
}

inline int64_t ObRefCountObj::refcount() const
{
  return refcount_;
}

// ObSharedRefCountHelper and ObRefCountObjHelper is used to find obj reference leak
// useage:
// - if class A is found with reference leak
// - set class A derived from ObSharedRefCountHelper ( or ObRefCountObjHelper)
// - use show proxymemory to get all inf_ref() stack and dec_ref() stack
// - checkout the difference betwen inc_ref() stack and dec_ref() stack, it`s probably the memory leak position
// example:
// 1. class A : public ObSharedRefCountHelper
// 2. reproduce use cases
// 3. show proxymemory
class ObSharedRefCountHelper : public ObSharedRefCount
{
public:
  ObSharedRefCountHelper() : ObSharedRefCount() {}
  virtual ~ObSharedRefCountHelper() {}

  inline void inc_ref()
  {
    get_global_ref_leak_checker().on_inc();
    ATOMIC_FAA(&ref_count_, 1);
  }

  inline void dec_ref()
  {
    get_global_ref_leak_checker().on_dec();
    if (1 == ATOMIC_FAA(&ref_count_, -1)) {
      free();
    }
  }
};

class ObRefCountObjHelper : public ObRefCountObj
{
public:
  ObRefCountObjHelper() : ObRefCountObj() { }
  virtual ~ObRefCountObjHelper() { }

  virtual int64_t refcount_inc();
  virtual int64_t refcount_dec();
};

// Increment the reference count, returning the new count.
inline int64_t ObRefCountObjHelper::refcount_inc()
{
  get_global_ref_leak_checker().on_inc();
  return ATOMIC_FAA(&refcount_, 1) + 1;
}

// Decrement the reference count, returning the new count.
inline int64_t ObRefCountObjHelper::refcount_dec()
{
  get_global_ref_leak_checker().on_dec();
  return ATOMIC_FAA(&refcount_, -1) - 1;
}

// class ObPtr
template<class T>
class ObPtr
{
public:
  explicit ObPtr(T *p = NULL);
  ObPtr(const ObPtr<T> &);
  ~ObPtr();

  void release();
  ObPtr<T> &operator=(const ObPtr<T>&);
  ObPtr<T> &operator=(T*);

  operator T *() const { return (ptr_); }
  T *operator->() const { return (ptr_); }
  T &operator*() const { return (*ptr_); }
  bool operator==(const T *p) { return (ptr_ == p); }
  bool operator==(const ObPtr<T> &p) { return (ptr_ == p.ptr_); }
  bool operator!=(const T *p) { return (ptr_ != p); }
  bool operator!=(const ObPtr<T> &p) { return (ptr_ != p.ptr_); }
  ObRefCountObj *get_ptr() { return reinterpret_cast<ObRefCountObj *>(ptr_); }

  T *ptr_;
};

template<typename T>
ObPtr<T> make_ptr(T *p)
{
  return ObPtr<T>(p);
}

template<class T>
inline ObPtr<T>::ObPtr(T *ptr /* = NULL */) : ptr_(ptr)
{
  if (NULL != ptr_) {
    get_ptr()->refcount_inc();
  }
}

template<class T>
inline ObPtr<T>::ObPtr(const ObPtr<T> &src) : ptr_(src.ptr_)
{
  if (NULL != ptr_) {
    get_ptr()->refcount_inc();
  }
}

template<class T> 
inline ObPtr<T>::~ObPtr()
{
  if ((NULL != ptr_) && 0 == get_ptr()->refcount_dec()) {
    get_ptr()->free();
  }
  return;
}

template<class T>
inline ObPtr<T>& ObPtr<T>::operator = (T *p)
{
  T *temp_ptr = ptr_;

  if (ptr_ != p) {
    ptr_ = p;

    if (NULL != ptr_) {
      get_ptr()->refcount_inc();
    }

    if ((NULL != temp_ptr) && 0 == (reinterpret_cast<ObRefCountObj *>(temp_ptr))->refcount_dec()) {
      (reinterpret_cast<ObRefCountObj *>(temp_ptr))->free();
    }
  }

  return (*this);
}

template<class T>
inline void ObPtr<T>::release()
{
  if (NULL != ptr_) {
    if (0 == (reinterpret_cast<ObRefCountObj *>(ptr_))->refcount_dec()) {
      (reinterpret_cast<ObRefCountObj *>(ptr_))->free();
    }
    ptr_ = NULL;
  }
}

template<class T>
inline ObPtr<T>& ObPtr<T>::operator = (const ObPtr<T> &src)
{
  return (operator =(src.ptr_));
}

} // end of namespace common
} // end of namespace oceanbase

#endif // OB_LIB_PTR_H_
