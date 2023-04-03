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

#include "common/ob_obj_cast.h"
#include "share/part/ob_part_desc_list.h"
#include "obproxy/proxy/route/obproxy_expr_calculator.h"


namespace oceanbase
{
namespace common
{

ObPartDescList::ObPartDescList() : part_array_ (NULL)
                                   , part_array_size_(0)
                                   , default_part_array_idx_(OB_INVALID_INDEX)
{
}

ObPartDescList::~ObPartDescList()
{
  // the memory is hold by allocator, will free if the allcator is reset
}

int64_t ObPartDescList::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  J_KV("part_type", "list",
       K_(part_array_size));
  J_OBJ_END();
  return pos;
}

ListPartition::ListPartition() : part_id_(0), rows_()
{
}

int ObPartDescList::get_part(ObNewRange &range,
                             ObIAllocator &allocator,
                             ObIArray<int64_t> &part_ids,
                             ObPartDescCtx &ctx,
                             ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObNewRow src_row;
  // for list partition, row in start_key_
  src_row.assign(const_cast<ObObj*>(range.start_key_.get_obj_ptr()), range.start_key_.get_obj_cnt());
  if (OB_ISNULL(part_array_)
      || OB_UNLIKELY(part_array_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", K_(part_array), K_(part_array_size), K(ret));
    // use the fisrt range as the type to cast
  } else {
    bool found = false;
    bool casted = false;
    // cast src_row and compare with part array
    for (int64_t i = 0; i < part_array_size_ && !found; i++) {
      if (i == default_part_array_idx_) {
        continue;
      }
      for (int64_t j = 0; j < part_array_[i].rows_.count() && !found; j++) {
        if (part_array_[i].rows_[j].get_count() == 0) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "no cells in the row", K(part_array_[i].rows_[j]), K(ret));
        } else {
          // if not cast, cast first
          if (!casted && OB_FAIL(cast_row(src_row, part_array_[i].rows_.at(j), allocator, ctx))) {
            COMMON_LOG(WARN, "fail to cast row");
            continue;
          } else {
            casted = true;
          }
          // if casted, then compare
          if (casted && src_row == part_array_[i].rows_.at(j)) {
            found = true;
            if (OB_FAIL(part_ids.push_back(part_array_[i].part_id_))) {
              COMMON_LOG(WARN, "fail to push part id", K(ret));
            } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[i]))) {
              COMMON_LOG(WARN, "fail to push tablet id", K(ret));
            }
          } else {}
        }
      } // end for rows
    } // end for part_array

    if (!found && OB_INVALID_INDEX != default_part_array_idx_) {
      // if no row matches, use default partition
      COMMON_LOG(DEBUG, "will use default partition id", K(src_row), K(ret));
      if (OB_FAIL(part_ids.push_back(part_array_[default_part_array_idx_].part_id_))) {
        COMMON_LOG(WARN, "fail to push part id", K(ret));
      } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[default_part_array_idx_]))) {
        COMMON_LOG(WARN, "fail to push tablet id", K(ret));
      }
    }
  }
  return ret;
}

int ObPartDescList::get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids, common::ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = num % part_array_size_;
  if (OB_FAIL(part_ids.push_back(part_array_[part_idx].part_id_))) {
    COMMON_LOG(WARN, "fail to push part_id", K(ret));
  } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
    COMMON_LOG(WARN, "fal to push tablet id", K(ret));
  }
  return ret;
}

int ObPartDescList::cast_row(ObNewRow &src_row,
                         ObNewRow &target_row,
                         ObIAllocator &allocator,
                         ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t min_col_cnt = std::min(src_row.count_, target_row.count_);
  for (int64_t i = 0; i < min_col_cnt && OB_SUCC(ret); ++i) {
    if (src_row.get_cell(i).is_max_value() ||
        src_row.get_cell(i).is_min_value()) {
      COMMON_LOG(DEBUG, "skip min/max obj");
      continue;
    }
    if (OB_FAIL(cast_obj(src_row.get_cell(i),
                         target_row.get_cell(i),
                         allocator,
                         ctx,
                         accuracies_.at(i)))) {
      COMMON_LOG(INFO, "fail to cast obj", K(i), K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

} // end of common
} // end of oceanbase
