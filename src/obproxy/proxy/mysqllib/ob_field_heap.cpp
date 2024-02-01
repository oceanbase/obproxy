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

#define USING_LOG_PREFIX PROXY

#include "proxy/mysqllib/ob_field_heap.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::common::number;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
int64_t ObFieldHeapUtils::get_alloc_size(const int64_t size)
{
  int64_t alloc_size = size;
  if (size <= STR_HEAP_DEFAULT_SIZE) {
    alloc_size = STR_HEAP_DEFAULT_SIZE;
  } else if (size <= DEFAULT_MAX_BUFFER_SIZE) {
    alloc_size = next_pow2(size);
  }
  return alloc_size;
}

int ObFieldHeapUtils::new_field_heap(const int64_t size, ObFieldHeap *&heap)
{
  int ret = OB_SUCCESS;
  heap = NULL;
  void *new_space = NULL;
  int64_t alloc_size = 0;
  if (OB_UNLIKELY(size <= ObFieldHeap::get_hdr_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("size is too small", K(size), K(ObFieldHeap::get_hdr_size()), K(ret));
  } else {
    alloc_size = get_alloc_size(size);
    new_space = op_fixed_mem_alloc(alloc_size);
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(new_space)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_EDIAG("fail to alloc memory for ObFieldHeap", K(ret));
    } else if (OB_ISNULL(heap = new (new_space) ObFieldHeap(alloc_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to new ObFieldHeap", K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != new_space)) {
    op_fixed_mem_free(new_space, alloc_size);
    new_space = NULL;
  }

  return ret;
}

int ObFieldHeapUtils::new_field_str_heap(const int64_t requested_size, ObFieldStrHeap *&str_heap)
{
  // The callee asks for a string heap that can allocate at least requested_size bytes,
  // so we need to add the size of the string heap header in our calculations.
  int ret = OB_SUCCESS;
  str_heap = NULL;
  void *new_space = NULL;
  int64_t alloc_size = 0;
  if (OB_UNLIKELY(requested_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(requested_size), K(ret));
  } else {
    alloc_size = get_alloc_size(requested_size + sizeof(ObFieldStrHeap));
    new_space = op_fixed_mem_alloc(alloc_size);

    if (OB_ISNULL(new_space)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem for ObFieldStrHeap", K(alloc_size), K(requested_size), K(ret));
    } else if (OB_ISNULL(str_heap = new (new_space) ObFieldStrHeap(alloc_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to new ObFieldHeap", K(ret));
    }
  }
  if (OB_FAIL(ret) && OB_LIKELY(NULL != new_space)) {
    op_fixed_mem_free(new_space, alloc_size);
    new_space = NULL;
  }
  return ret;
}

int ObFieldHeapUtils::str_heap_move_str(ObFieldStrHeap &heap, const char *&str, int64_t str_len)
{
  int ret = OB_SUCCESS;
  char *new_str = NULL;
  if (OB_ISNULL(str) || OB_UNLIKELY(str_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid str", K(str), K(str_len), K(ret));
  } else if (OB_ISNULL(new_str = heap.allocate(str_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to allocate memory", K(str_len), K(ret));
  } else {
    MEMCPY(new_str, str, str_len);
    str = new_str;
  }
  return ret;
}

int ObFieldHeapUtils::str_heap_move_obj(ObFieldStrHeap &heap, ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObObj old_obj = obj;
  int64_t deep_copy_size = obj.get_deep_copy_size();
  char *buf = heap.allocate(deep_copy_size);
  int64_t pos = 0;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to allocate memory", K(obj), K(ret));
  } else if (OB_FAIL(obj.deep_copy(old_obj, buf, deep_copy_size, pos))) {
    LOG_WDIAG("fail to deep copy obj", K(obj), K(ret));
  }
  return ret;
}

DEF_TO_STRING(ObFieldHeader)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), K_(length));
  J_OBJ_END();
  return pos;
}

ObFieldStrHeap::ObFieldStrHeap(const int64_t size)
{
  heap_size_ = size;
  free_size_ = size - sizeof(ObFieldStrHeap);
  free_start_ = reinterpret_cast<char *>(this) + sizeof(ObFieldStrHeap);
}

void ObFieldStrHeap::free()
{
  op_fixed_mem_free(this, heap_size_);
}

// allocates nbytes from the str heap, Return NULL on allocation failure
char *ObFieldStrHeap::allocate(const int64_t nbytes)
{
  char *new_space = NULL;
  if (free_size_ >= nbytes) {
    new_space = free_start_;
    free_start_ += nbytes;
    free_size_ -= nbytes;
  }
  return new_space;
}

ObFieldHeap::ObFieldHeap(const int64_t size)
{
  size_ = size;
  free_size_ = size_ - get_hdr_size();
  if (free_size_ <= 0) {
    LOG_EDIAG("size is invalid", K_(size), K(get_hdr_size()));
    writeable_ = false;
  } else {
    data_start_ = (reinterpret_cast<char *>(this)) + get_hdr_size();
    free_start_ = data_start_;
    writeable_ = true;
    next_ = NULL;
    read_write_heap_ = NULL;
    for (int64_t i = 0; i < READ_ONLY_STR_HEAPS; ++i) {
      ronly_heap_[i].reset();
    }
    dirty_string_space_ = 0;
    total_size_ = size;
  }
}

void ObFieldHeap::destroy()
{
  if (NULL != read_write_heap_) {
    read_write_heap_->free();
    read_write_heap_ = NULL;
  }
  for (int64_t i = 0; i < READ_ONLY_STR_HEAPS; ++i) {
    ronly_heap_[i].reset();
  }

  total_size_ = 0;
  op_fixed_mem_free(this, size_);
}

inline int ObFieldHeap::alloc_field_heap(const int64_t size, ObFieldHeap *&heap)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFieldHeapUtils::new_field_heap(size, heap))) {
    LOG_EDIAG("fail to alloc field heap", K(size), K(heap), K(ret));
  } else {
    total_size_ += (NULL == heap ? 0 : heap->size_);
    LOG_DEBUG("alloc field_heap succ", KP(this), K(size), KP(heap), K_(total_size));
  }
  return ret;
}

inline int ObFieldHeap::alloc_str_heap(const int64_t size, ObFieldStrHeap *&str_heap)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFieldHeapUtils::new_field_str_heap(size, str_heap))) {
    LOG_EDIAG("fail to alloc str heap", K(size), K(str_heap), K(ret));
  } else {
    total_size_ += (NULL == str_heap ? 0 : str_heap->get_heap_size());
    LOG_DEBUG("alloc str_heap succ", KP(this), K(size), KP(str_heap));
  }
  return ret;
}

inline void ObFieldHeap::free_str_heap()
{
  int64_t free_size = 0;
  if (NULL != read_write_heap_) {
    free_size += read_write_heap_->get_heap_size();
    read_write_heap_->free();
    read_write_heap_ = NULL;
  }
  for (int64_t i = 0; i < READ_ONLY_STR_HEAPS; ++i) {
    if (NULL != ronly_heap_[i].str_heap_ptr_) {
      free_size += ronly_heap_[i].str_heap_ptr_->get_heap_size();
      ronly_heap_[i].reset();
    }
  }
  total_size_ -= free_size;
  if (OB_UNLIKELY(total_size_ < 0)) {
    LOG_EDIAG("total_size should not less than 0, unexpected!!", K_(total_size));
  }
  dirty_string_space_ = 0;
  LOG_DEBUG("free str_heap succ", KP(this), K_(total_size), K(free_size));
}

int ObFieldHeap::allocate_block(int64_t nbytes, void *&obj)
{
  int ret = OB_SUCCESS;
  obj = NULL;

  nbytes = round(nbytes, HEAP_PRT_SIZE);
  if (OB_UNLIKELY(!writeable_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("field heap is not inited, not writeable so far", K_(writeable),  K(ret));
  } else if (nbytes > get_max_alloc_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("requested size is too big", K(nbytes), K(get_max_alloc_size()), K(ret));
  } else {
    ObFieldHeap *heap = this;
    bool found = false;
    while (NULL != heap && OB_SUCC(ret) && !found) {
      if (nbytes <= heap->free_size_) {
        obj = heap->free_start_;
        heap->free_start_ += nbytes;
        heap->free_size_ -= nbytes;
        found = true;
      } else if (NULL == heap->next_) {
        // allocate our next pointer heap twice as large as this one so
        // number of pointer heaps is O(log n)  with regard to number of bytes allocated
        if (OB_FAIL(alloc_field_heap(heap->size_ * 2, heap->next_))) {
          LOG_EDIAG("fail to new ObFieldHeap", K(ret));
        }
      }
      heap = heap->next_;
    }
  }
  return ret;
}

int ObFieldHeap::deallocate_block(ObFieldHeader *obj)
{
  int ret = OB_SUCCESS;
  if (!writeable_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("field heap is not inited, not writable so far", K(ret));
  } else if (OB_ISNULL(obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(obj), K(ret));
  } else {
    obj->type_ = HEAP_OBJ_EMPTY;
  }
  return ret;
}

// Allocates a new string and copies the old data.
// Returns the new string pointer.
int ObFieldHeap::duplicate_str(const char *in_str, const uint16_t in_len,
                               const char *&out_str, uint16_t &out_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in_str) || OB_UNLIKELY(0 == in_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument fail to allocate str", K(in_str), K(in_len), K(ret));
  } else {
    char *new_str = NULL;
    if (OB_FAIL(allocate_str(static_cast<int64_t>(in_len), new_str))) {
      LOG_WDIAG("fail to allocate str", K(in_str), K(in_len), K(ret));
    } else {
      MEMCPY(new_str, in_str, in_len);
      out_str = new_str;
      out_len = in_len;
    }
  }
  return ret;
}

int ObFieldHeap::duplicate_obj(const ObObj &src_obj, ObObj &dest_obj)
{
  int ret = OB_SUCCESS;
  int64_t deep_copy_size = src_obj.get_deep_copy_size();
  char *buf = NULL;
  if (deep_copy_size > 0) {
    if (OB_FAIL(allocate_str(deep_copy_size, buf))) {
      LOG_WDIAG("fail to allocate str", K(deep_copy_size), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int64_t pos = 0;
    if (OB_FAIL(dest_obj.deep_copy(src_obj, buf, deep_copy_size, pos))) {
      LOG_WDIAG("fail to deep copy obj", K(src_obj), K(dest_obj), K(ret));
    }
  }
  return ret;
}

int ObFieldHeap::duplicate_str_and_obj(const char *in_str, const uint16_t in_len,
                                       const char *&out_str, uint16_t &out_len,
                                       const ObObj &src_obj, ObObj &dest_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(in_str) || OB_UNLIKELY(0 == in_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument fail to allocate str", K(in_str), K(in_len), K(ret));
  } else {
    int64_t deep_copy_size = src_obj.get_deep_copy_size();
    int64_t alloc_len = deep_copy_size + in_len;
    char *buf = NULL;
    if (OB_FAIL(allocate_str(alloc_len, buf))) {
      LOG_WDIAG("fail to allocate str", K(alloc_len), K(ret));
    } else {
      // copy str
      MEMCPY(buf, in_str, in_len);
      out_str = buf;
      out_len = in_len;

      // copy obj
      buf += in_len;
      int64_t pos = 0;
      if (OB_FAIL(dest_obj.deep_copy(src_obj, buf, deep_copy_size, pos))) {
        LOG_WDIAG("fail to deep copy obj", K(src_obj), K(dest_obj), K(ret));
      }
    }
  }
  return ret;
}

void ObFieldHeap::free_string(const char *s, const int64_t len)
{
  if (NULL != s && len > 0) {
    dirty_string_space_ += len;
  }
}

void ObFieldHeap::free_obj(common::ObObj &obj)
{
  ObObjType type = obj.get_type();
  if (ObVarcharType == type) {
    ObString var;
    obj.get_varchar(var);
    free_string(var.ptr(), var.length());
  } else if (ObNumberType == type) {
    ObNumber value = obj.get_number();
    LOG_DEBUG("number_value", K(value));
    free_string(reinterpret_cast<char *>(value.get_digits()),
                value.len_ * ITEM_SIZE(value.get_digits()));
  }
}

int ObFieldHeap::allocate_str(const int64_t requested_size, char *&buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(requested_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == read_write_heap_) {
    //read_write_heap_ is NULL when first allocate str
    ObFieldStrHeap *heap_ptr = NULL;
    if (OB_FAIL(alloc_str_heap(requested_size, heap_ptr))) {
      LOG_EDIAG("fail to new ObFieldStrHeap", K(ret));
    } else {
      read_write_heap_ = heap_ptr;
    }
  } else if (dirty_string_space_ >= MAX_FREED_STR_SPACE) {
    //coalesce string heap, do defragmentation
    if (OB_FAIL(reform_str_heaps(requested_size))) {
      LOG_WDIAG("fail to coalesce str heaps", K(requested_size), K(ret));
    }
  } else if (NULL == (buf = read_write_heap_->allocate(requested_size))) {
    //the free space of the read_write_heap_ is not enough, first move read_write_heap_ to read_only
    //array
    if (OB_FAIL(demote_rw_str_heap())) {
      //fail to demote,then defragment
      if (OB_FAIL(reform_str_heaps(requested_size))) {
        LOG_WDIAG("fail to coalesce str heaps", K(requested_size), K(ret));
      }
    } else {
      ObFieldStrHeap *heap_ptr = NULL;
      if (OB_FAIL(alloc_str_heap(requested_size, heap_ptr))) {
        LOG_EDIAG("fail to new ObFieldStrHeap", K(ret));
      } else {
        read_write_heap_ = heap_ptr;
      }
    }
  }

  if (OB_SUCC(ret) && NULL == buf) {
    if (OB_ISNULL(buf = read_write_heap_->allocate(requested_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to alloc when should succ", K(requested_size), K(ret));
    }
  }
  return ret;
}

int ObFieldHeap::demote_rw_str_heap()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!writeable_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("field heap is not inited, not writable so far", K(ret));
  } else if (OB_ISNULL(read_write_heap_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("read_write_heap is NULL", K(ret));
  } else {
    // First, see if we have any open slots for read
    // only heaps
    LOG_DEBUG("need to demote rw str heap");
    ret = OB_ENTRY_NOT_EXIST; //init as no open slot, so we do not judge ret in following for loop
    for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < READ_ONLY_STR_HEAPS; ++i) {
      if (NULL == ronly_heap_[i].str_heap_ptr_) {
        // We've found a slot
        ronly_heap_[i].str_heap_ptr_ = read_write_heap_;
        read_write_heap_ = NULL;
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObFieldHeap::reform_str_heaps(int64_t incoming_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!writeable_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("field heap is not inited, not writable so far", K(ret));
  } else if (OB_UNLIKELY(incoming_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("incoming_size should not smaller than 0", K(incoming_size), K(ret));
  } else {
    int64_t new_heap_size = incoming_size;
    LOG_DEBUG("need to reform rw str heap", K(incoming_size));

    ObFieldStrHeap *new_heap = NULL;
    if (OB_FAIL(required_space_for_evacuation(new_heap_size))) {
      LOG_WDIAG("fail to compute space needed for evacuation", K(ret));
    } else if (OB_FAIL(alloc_str_heap(new_heap_size, new_heap))) {
      LOG_WDIAG("fail to new ObFieldStrHeap", K(ret));
    } else if (OB_FAIL(evacuate_from_str_heaps(new_heap))) {
      //will not come into this, the space is enough
      LOG_EDIAG("fail to evacuate_from_str_heaps", K(ret));
    } else {
      free_str_heap(); // free old str_heap
      read_write_heap_ = new_heap; // assign new str_heap
    }
  }
  return ret;
}

int ObFieldHeap::evacuate_from_str_heaps(ObFieldStrHeap *new_heap)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!writeable_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("field heap is not inited, not writable so far", K(ret));
  } else if (OB_ISNULL(new_heap)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("new_heap should not be NULL", K(ret));
  } else {
    // Loop over the objects in heap and call the evacuation function in each one
    ObFieldHeap *heap = this;
    char *data = NULL;
    ObFieldHeader *obj = NULL;
    ObSysVarFieldBlock *sys_var = NULL;
    ObUserVarFieldBlock *user_var = NULL;
    ObStrFieldBlock *str = NULL;
    while ((OB_SUCC(ret)) && heap) {
      data = heap->data_start_;
      while ((OB_SUCC(ret)) && (data < heap->free_start_)) {
        obj = reinterpret_cast<ObFieldHeader *>(data);
        if (OB_ISNULL(obj)) {
          ret = OB_BAD_NULL_ERROR;
          LOG_WDIAG("data_start should not be NULL", K(ret));
        } else {
          switch (obj->type_) {
            case HEAP_OBJ_SYS_VAR_BLOCK : {
              sys_var = reinterpret_cast<ObSysVarFieldBlock *>(obj);
              if (OB_FAIL(sys_var->move_strings(new_heap))) {
                LOG_EDIAG("fail to move strings", K(*sys_var), K(ret));
              }
              break;
            }
            case HEAP_OBJ_USER_VAR_BLOCK : {
              user_var = reinterpret_cast<ObUserVarFieldBlock *>(obj);
              if (OB_FAIL(user_var->move_strings(new_heap))) {
                LOG_EDIAG("fail to move strings", K(*user_var), K(ret));
              }
              break;
            }
            case HEAP_OBJ_STR_BLOCK : {
              str = reinterpret_cast<ObStrFieldBlock *>(obj);
              if (OB_FAIL(str->move_strings(new_heap))) {
                LOG_EDIAG("fail to move strings", K(*str), K(ret));
              }
              break;
            }
            case HEAP_OBJ_EMPTY:
              // Nothing to do
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_EDIAG("invalid ObFieldHeader", K(obj), K(ret));
          }
          data += obj->length_;
        }
      }
      heap = heap->next_;
    }
  }
  return ret;
}

int ObFieldHeap::required_space_for_evacuation(int64_t &require_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!writeable_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("field heap is not inited, not writable so far", K(ret));
  } else {
    ObFieldHeap *heap = this;
    int64_t initial_size = require_size;
    char *data = NULL;
    ObFieldHeader *obj = NULL;
    ObSysVarFieldBlock *sys_var = NULL;
    ObUserVarFieldBlock *user_var = NULL;
    ObStrFieldBlock *str = NULL;
    while ((OB_SUCC(ret)) && heap) {
      data = heap->data_start_;
      while ((OB_SUCC(ret)) && (data < heap->free_start_)) {
        obj = reinterpret_cast<ObFieldHeader *>(data);
        switch (obj->type_) {
          case HEAP_OBJ_SYS_VAR_BLOCK: {
            sys_var = reinterpret_cast<ObSysVarFieldBlock *>(obj);
            require_size += sys_var->strings_length();
            break;
          }
          case HEAP_OBJ_USER_VAR_BLOCK: {
            user_var = reinterpret_cast<ObUserVarFieldBlock *>(obj);
            require_size += user_var->strings_length();
            break;
          }
          case HEAP_OBJ_STR_BLOCK: {
            str = reinterpret_cast<ObStrFieldBlock *>(obj);
            require_size += str->strings_length();
            break;
          }
          case HEAP_OBJ_EMPTY:
            // Nothing to do
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid ObFieldHeader type", K(obj), K(ret));
        }
        data += obj->length_;
      }
      heap = heap->next_;
    }
    LOG_INFO("after compute", K(initial_size), K(require_size));
  }
  return ret;
}

}
}
