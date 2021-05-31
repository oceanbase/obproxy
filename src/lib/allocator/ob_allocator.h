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

#ifndef  OCEANBASE_COMMON_ALLOCATOR_H_
#define  OCEANBASE_COMMON_ALLOCATOR_H_

#include "lib/ob_define.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace lib
{
class ObMemAttr;
} // end of namespace lib

namespace common
{
using lib::ObMemAttr;
extern ObMemAttr default_memattr;

class ObIAllocator
{
public:
  virtual ~ObIAllocator() {};
public:
  /************************************************************************/
  /*                     New Interface (Under construction)               */
  /************************************************************************/
  // Use attr passed in by set_attr().
  virtual void *alloc(const int64_t size)
  { UNUSED(size); return NULL; }

  virtual void* alloc(const int64_t size, const ObMemAttr &attr)
  { UNUSED(size); UNUSED(attr); return NULL; }

  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr)
  { UNUSED(ptr); UNUSED(size); UNUSED(attr); return NULL; }

  virtual void free(void *ptr)
  { UNUSED(ptr); }

  virtual int64_t total() const
  {
    return 0;
  }
  virtual void reset() {}

  virtual void set_attr(const ObMemAttr &attr) { UNUSED(attr); }
};

extern ObIAllocator *global_default_allocator;

class ObWrapperAllocator: public ObIAllocator
{
public:
  explicit ObWrapperAllocator(ObIAllocator *alloc): alloc_(alloc) {};
  explicit ObWrapperAllocator(int64_t mod_id): alloc_(NULL) {UNUSED(mod_id);};
  ObWrapperAllocator(): alloc_(NULL) {};
  virtual ~ObWrapperAllocator() {};
  void *alloc(const int64_t sz) { return NULL == alloc_ ? NULL : alloc_->alloc(sz); };
  void free(void *ptr) {if (NULL != alloc_) {alloc_->free(ptr); ptr = NULL; }};
  void set_alloc(ObIAllocator *alloc) {alloc_ = alloc;};
private:
  // data members
  ObIAllocator *alloc_;
};
}
}

#endif //OCEANBASE_COMMON_ALLOCATOR_H_
