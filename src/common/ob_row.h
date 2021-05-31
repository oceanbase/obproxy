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

#ifndef OCEANBASE_COMMON_OB_ROW_
#define OCEANBASE_COMMON_OB_ROW_

#include "lib/container/ob_array.h"
#include "common/ob_raw_row.h"
#include "common/ob_row_desc.h"
#include "common/ob_rowkey.h"

namespace commontest
{
class ObRowTest_reset_test_Test;
}

namespace oceanbase
{
namespace common
{

class ObColumnInfo
{
public:
  int64_t index_;
  common::ObCollationType cs_type_;
  ObColumnInfo() : index_(common::OB_INVALID_INDEX), cs_type_(common::CS_TYPE_INVALID) {}
  virtual ~ObColumnInfo() {}
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV2(N_INDEX_ID, index_,
                N_COLLATION_TYPE, common::ObCharset::collation_name(cs_type_));
};

/**
 * @brief: wrap row pointer(an ObObj array) and row size
 */
class ObNewRow
{
public:
  ObNewRow()
      :cells_(NULL),
       count_(0),
       projector_size_(0),
       projector_(NULL)
  {}
  virtual ~ObNewRow() {}

  void reset();
  OB_INLINE bool is_invalid() const
  {
    return NULL == cells_ || count_ <= 0 || (projector_size_ > 0 && NULL == projector_);
  }

  OB_INLINE bool is_valid() const
  {
    return !is_invalid();
  }

  OB_INLINE void assign(ObObj *ptr, const int64_t count)
  {
    cells_ = ptr;
    count_ = count;
  }
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObNewRow &src, char *buf, int64_t len, int64_t &pos);
  NEED_SERIALIZE_AND_DESERIALIZE;

  bool operator==(const ObNewRow &other);
  OB_INLINE int64_t get_count() const { return projector_size_ > 0 ? projector_size_ : count_; }
  const ObObj &get_cell(int64_t index) const
  {
    int64_t real_idx = index;
    if (projector_size_ > 0) {
      if (OB_ISNULL(projector_) || index >= projector_size_) {
        COMMON_LOG(ERROR, "index is invalid", K(index), K_(projector_size), K_(projector));
      } else {
        real_idx = projector_[index];
      }
    }
    if (real_idx >= count_) {
      COMMON_LOG(ERROR, "real_idx is invalid", K_(count), K(real_idx));
    }
    return cells_[real_idx];
  }

  ObObj &get_cell(int64_t index)
  {
    int64_t real_idx = index;
    if (projector_size_ > 0) {
      if (OB_ISNULL(projector_) || index >= projector_size_) {
        COMMON_LOG(ERROR, "index is invalid", K(index), K_(projector_size), K_(projector));
        right_to_die_or_duty_to_live();
      } else {
        real_idx = projector_[index];
      }
    }
    if (real_idx >= count_) {
      COMMON_LOG(ERROR, "real_idx is invalid", K_(count), K(real_idx));
      right_to_die_or_duty_to_live();
    }
    return cells_[real_idx];
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_NAME(N_ROW);
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, cells_, count_);
    J_COMMA();
    J_NAME(N_PROJECTOR);
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, projector_, projector_size_);
    J_OBJ_END();
    return pos;
  }
public:
  ObObj *cells_;
  int64_t count_;  // cells count
  int64_t projector_size_;
  int32_t *projector_;  // if projector is not NULL, the caller should use this projector to access cells_
};

template<typename AllocatorT>
int ob_write_row(AllocatorT &allocator, const ObNewRow &src, ObNewRow &dst)
{
  int ret = OB_SUCCESS;
  void *ptr1 = NULL;
  void *ptr2 = NULL;
  if (src.count_ <= 0) {
    dst.count_ = src.count_;
    dst.cells_ = NULL;
    dst.projector_size_ = 0;
    dst.projector_ = NULL;
  } else if (OB_ISNULL(ptr1 = allocator.alloc(sizeof(ObObj) * src.count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(WARN, "out of memory");
  } else if (NULL != src.projector_
      && OB_ISNULL(ptr2 = allocator.alloc(sizeof(int32_t) * src.projector_size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    _OB_LOG(WARN, "out of memory");
  } else {
    if (NULL != src.projector_) {
      MEMCPY(ptr2, src.projector_, sizeof(int32_t) * src.projector_size_);
    }
    ObObj *objs = new(ptr1) ObObj[src.count_]();
    for (int64_t i = 0; OB_SUCC(ret) && i < src.count_; ++i) {
      if (OB_FAIL(ob_write_obj(allocator, src.cells_[i], objs[i]))) {
        _OB_LOG(WARN, "copy ObObj error, row=%s, i=%ld, ret=%d",
            to_cstring(src.cells_[i]), i, ret);
      }
    }
    if (OB_SUCC(ret)) {
      dst.count_ = src.count_;
      dst.cells_ = objs;
      dst.projector_size_ = src.projector_size_;
      dst.projector_ = (NULL != src.projector_) ? static_cast<int32_t *>(ptr2) : NULL;
    }
  }
  if (OB_FAIL(ret)) {
    if (NULL != ptr1) {
      allocator.free(ptr1);
      ptr1 = NULL;
    }
    if (NULL != ptr2) {
      allocator.free(ptr2);
      ptr2 = NULL;
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif /* OCEANBASE_COMMON_OB_ROW_ */
