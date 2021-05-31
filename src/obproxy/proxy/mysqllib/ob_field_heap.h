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

#ifndef OBPROXY_FIELD_HEAP_H
#define OBPROXY_FIELD_HEAP_H

#include "lib/utility/ob_print_utils.h"
#include "common/ob_object.h"
#include "utils/ob_proxy_utils.h"

namespace oceanbase
{
namespace obproxy
{
enum HeapObjType
{
  HEAP_OBJ_EMPTY = 0,
  HEAP_OBJ_SYS_VAR_BLOCK = 1,
  HEAP_OBJ_USER_VAR_BLOCK = 2,
  HEAP_OBJ_STR_BLOCK = 3,
  HEAP_OBJ_MAX = 0x0FEEB1E0
};

struct ObFieldHeader
{
  ObFieldHeader() { init(HEAP_OBJ_EMPTY, 0); }
  void init(const int32_t type, const int64_t nbytes);
  DECLARE_TO_STRING;

  uint32_t type_:8;
  uint32_t length_:24;
};

inline void ObFieldHeader::init(const int32_t type, const int64_t nbytes)
{
  type_ = type & 0xff;
  length_ = nbytes & 0xffffff;
}

class ObFieldStrHeap
{
public:
  ObFieldStrHeap() : heap_size_(0), free_start_(NULL), free_size_(0) { }
  //size must > sizeof(ObFieldStrHeap), don't invoke this constructor ,
  //invoke new_field_str_heap() instead
  explicit ObFieldStrHeap(const int64_t size);
  virtual ~ObFieldStrHeap() { }
  virtual void free();
  char *allocate(const int64_t nbytes);
  int64_t get_heap_size() const { return heap_size_; }

private:
  int64_t heap_size_;//from the very beginning
  char *free_start_;
  int64_t free_size_;
  DISALLOW_COPY_AND_ASSIGN(ObFieldStrHeap);
};

struct StrHeapDesc
{
  StrHeapDesc() : str_heap_ptr_(NULL) { }
  void reset();
  ObFieldStrHeap *str_heap_ptr_;
};

inline void StrHeapDesc::reset()
{
  if (NULL != str_heap_ptr_) {
    str_heap_ptr_->free();
    str_heap_ptr_ = NULL;
  }
}

class ObFieldHeap
{
public:
  static const int64_t HEAP_PRT_SIZE = sizeof(uint64_t);
  static const int64_t FIELD_HEAP_DEFAULT_SIZE = 2048;
  static const int64_t MAX_FREED_STR_SPACE = 1024;
  static const int64_t READ_ONLY_STR_HEAPS = 3;

  // common function
  // size must > sizeof(ObFieldHeap), don't invoke this constructor,
  // invoke new_field_heap() instead
  explicit ObFieldHeap(const int64_t size);
  ~ObFieldHeap() { }
  void destroy();

  static int64_t get_hdr_size();
  static int64_t get_max_alloc_size();

  ObFieldHeap *get_next() const { return next_; }
  void set_size(const int64_t size) { size_ = size; };
  // PtrHeap allocation
  int allocate_block(int64_t nbytes, void *&block_obj);
  int deallocate_block(ObFieldHeader *obj);

  // StrHeap allocation
  int duplicate_str(const char *in_str, const uint16_t in_len, const char *&out_str, uint16_t &out_len);
  int duplicate_obj(const common::ObObj &src_obj, common::ObObj &dest_obj);
  int duplicate_str_and_obj(const char *in_str, const uint16_t in_len,
                            const char *&out_str, uint16_t &out_len,
                            const common::ObObj &src_obj, common::ObObj &dest_obj);
  void free_string(const char *s, const int64_t len);
  void free_obj(common::ObObj &obj);

  int64_t get_memory_size() const { return total_size_; }
private:
  // these function should only be called in the first block
  int alloc_field_heap(const int64_t size, ObFieldHeap *&heap);
  int alloc_str_heap(const int64_t size, ObFieldStrHeap *&str_heap);
  void free_str_heap();

  int allocate_str(const int64_t requested_size, char *&buf);
  int demote_rw_str_heap();
  int reform_str_heaps(const int64_t incoming_size = 0);
  int evacuate_from_str_heaps(ObFieldStrHeap *new_heap);
  int required_space_for_evacuation(int64_t &require_size);

private:
  bool writeable_;//like init
  char *free_start_;
  char *data_start_;
  int64_t size_; // current heap size
  int64_t free_size_;
  int64_t total_size_; // sum of all linked-heap size, only valid in first block
  // Overflow block ptr: Overflow blocks are necessary because we can
  // run out of space in the header heap and the heap is not rellocatable
  //Overflow blocks have the ObFieldHeap full structure header on them,
  //although only first block can point to string heaps
  ObFieldHeap *next_;
  ObFieldStrHeap *read_write_heap_; // String Heap access
  int64_t dirty_string_space_;//that not useful anymore
  StrHeapDesc ronly_heap_[READ_ONLY_STR_HEAPS]; //read only strings
  DISALLOW_COPY_AND_ASSIGN(ObFieldHeap);
};

class ObFieldHeapUtils
{
public:
  static const int64_t STR_HEAP_DEFAULT_SIZE = 2048;

  ObFieldHeapUtils() { }
  ~ObFieldHeapUtils() { }
  static int new_field_heap(const int64_t size, ObFieldHeap *&heap);
  static int new_field_str_heap(const int64_t requested_size, ObFieldStrHeap *&str_heap);
  static int str_heap_move_str(ObFieldStrHeap &heap, const char *&str, int64_t str_len);
  static int str_heap_move_obj(ObFieldStrHeap &heap, common::ObObj &obj);

private:
  static int64_t get_alloc_size(const int64_t size);
  DISALLOW_COPY_AND_ASSIGN(ObFieldHeapUtils);
};

inline int64_t ObFieldHeap::get_hdr_size()
{
  return round(sizeof(ObFieldHeap), HEAP_PRT_SIZE);
}

inline int64_t ObFieldHeap::get_max_alloc_size()
{
  return FIELD_HEAP_DEFAULT_SIZE - get_hdr_size();
}
}//end of obproxy
}//end of namespace oceanbase

#endif
