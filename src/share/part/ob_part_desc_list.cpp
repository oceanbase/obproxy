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

  J_KV("part_type", "list", K_(part_array_size), "part_func_type", share::schema::get_partition_func_type_str(part_func_type_));
  J_COMMA();
  BUF_PRINTO("part_array");
  J_COLON();
  J_ARRAY_START();
  for (int i = 0; i < part_array_size_; i++) {
    if (part_array_ + i != NULL) {
      BUF_PRINTO(part_array_[i]);
      if (i + 1 < part_array_size_) {
        J_COMMA();
      }
    }
  }
  J_ARRAY_END();
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
                             ObIArray<int64_t> &tablet_ids,
                             int64_t &part_idx)
{
  int ret = OB_SUCCESS;
  ObNewRow src_row;
  // for list partition, row in start_key_
  src_row.assign(const_cast<ObObj*>(range.start_key_.get_obj_ptr()), range.start_key_.get_obj_cnt());
  if (OB_ISNULL(part_array_)
      || OB_UNLIKELY(part_array_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(DEBUG, "invalid argument, ", K_(part_array), K_(part_array_size), K(ret));
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
          COMMON_LOG(DEBUG, "no cells in the row", K(part_array_[i].rows_[j]), K(ret));
        } else {
          // if not cast, cast first
          if (!casted && OB_FAIL(cast_row(src_row, part_array_[i].rows_.at(j), allocator, ctx))) {
            COMMON_LOG(DEBUG, "fail to cast row");
            continue;
          } else {
            casted = true;
          }
          // if casted, then compare
          if (casted && src_row == part_array_[i].rows_.at(j)) {
            found = true;
            part_idx = i;
            if (OB_FAIL(part_ids.push_back(part_array_[i].part_id_))) {
              COMMON_LOG(WDIAG, "fail to push part id", K(ret));
            } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[i]))) {
              COMMON_LOG(WDIAG, "fail to push tablet id", K(ret));
            }
          } else {}
        }
      } // end for rows
    } // end for part_array

    if (!found && OB_INVALID_INDEX != default_part_array_idx_) {
      // if no row matches, use default partition
      part_idx = default_part_array_idx_;
      COMMON_LOG(DEBUG, "will use default partition id", K(src_row), K(ret));
      if (OB_FAIL(part_ids.push_back(part_array_[default_part_array_idx_].part_id_))) {
        COMMON_LOG(WDIAG, "fail to push part id", K(ret));
      } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[default_part_array_idx_]))) {
        COMMON_LOG(WDIAG, "fail to push tablet id", K(ret));
      }
    }
  }
  return ret;
}

int ObPartDescList::get_all_part_id_for_obkv(ObIArray<int64_t> &part_ids,
                                             ObIArray<int64_t> &tablet_ids,
                                             ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;

  for(int64_t part_idx = 0; part_idx < part_array_size_; ++part_idx) {
    if (OB_FAIL(get_part_by_num(part_idx, part_ids, tablet_ids))) {
      COMMON_LOG(WDIAG, "fail to call get_part_by_num", K(ret));
    } else if (OB_FAIL(get_ls_id_by_num(part_idx, ls_ids))) {
      COMMON_LOG(WDIAG, "fail to call get_ls_id_by_num", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    COMMON_LOG(DEBUG, "obkv get all hash part ids", K(part_ids));
  }

  return ret;
}

int ObPartDescList::get_part_for_obkv(ObNewRange &range,
                                      ObIAllocator &allocator,
                                      ObIArray<int64_t> &part_ids,
                                      ObPartDescCtx &ctx,
                                      ObIArray<int64_t> &tablet_ids,
                                      ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObNewRow src_row;
  // for list partition, row in start_key_
  src_row.assign(const_cast<ObObj*>(range.start_key_.get_obj_ptr()), range.start_key_.get_obj_cnt());

  if (OB_ISNULL(part_array_)
      || OB_UNLIKELY(part_array_size_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "invalid argument, ", K_(part_array), K_(part_array_size), K(ret));
    // use the fisrt range as the type to cast
  } else if (range.is_whole_range() || ctx.calc_first_partition() || ctx.calc_last_partition() || ctx.need_get_whole_range()) {
    if (OB_FAIL(get_all_part_id_for_obkv(part_ids, tablet_ids, ls_ids))) {
      COMMON_LOG(WDIAG, "fail to call get_all_part_id_for_obkv", K(ret));
    }
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
          COMMON_LOG(WDIAG, "no cells in the row", K(part_array_[i].rows_[j]), K(ret));
        } else {
          // if not cast, cast first
          if (!casted && OB_FAIL(cast_row_for_obkv(src_row, part_array_[i].rows_.at(j), allocator, ctx))) {
            COMMON_LOG(WDIAG, "fail to cast row");
            continue;
          } else {
            casted = true;
          }
          // if casted, then compare
          if (casted && src_row == part_array_[i].rows_.at(j)) {
            found = true;
            if (OB_FAIL(part_ids.push_back(part_array_[i].part_id_))) {
              COMMON_LOG(WDIAG, "fail to push part id", K(ret));
            } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[i]))) {
              COMMON_LOG(WDIAG, "fail to push tablet id", K(ret));
            } else if (NULL != ls_id_array_ && OB_FAIL(ls_ids.push_back(ls_id_array_[i]))) {
              COMMON_LOG(WDIAG, "fail to push ls id", K(ret));
            }
          } else {}
        }
      } // end for rows
    } // end for part_array

    if (!found && OB_INVALID_INDEX != default_part_array_idx_) {
      // if no row matches, use default partition
      COMMON_LOG(DEBUG, "will use default partition id", K(src_row), K(ret));
      if (OB_FAIL(part_ids.push_back(part_array_[default_part_array_idx_].part_id_))) {
        COMMON_LOG(WDIAG, "fail to push part id", K(ret));
      } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[default_part_array_idx_]))) {
        COMMON_LOG(WDIAG, "fail to push tablet id", K(ret));
      } else if (NULL != ls_id_array_ && OB_FAIL(ls_ids.push_back(ls_id_array_[default_part_array_idx_]))) {
        COMMON_LOG(WDIAG, "fail to push ls id", K(ret));
      }
    }
  }

  return ret;
}

int ObPartDescList::get_part_by_num(const int64_t num, ObIArray<int64_t> &part_ids, ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = num % part_array_size_;
  if (OB_FAIL(part_ids.push_back(part_array_[part_idx].part_id_))) {
    COMMON_LOG(WDIAG, "fail to push part_id", K(ret));
  } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
    COMMON_LOG(WDIAG, "fal to push tablet id", K(ret));
  }
  return ret;
}

int ObPartDescList::get_ls_id_by_num(const int64_t num, ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = num % part_array_size_;
  if (NULL != ls_id_array_ && OB_FAIL(ls_ids.push_back(ls_id_array_[part_idx]))) {
    COMMON_LOG(WDIAG, "fail to push ls id", K(ret));
  }
  return  ret;
}

int ObPartDescList::cast_row(ObNewRow &src_row,
                         ObNewRow &target_row,
                         ObIAllocator &allocator,
                         ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t min_col_cnt = std::min(src_row.count_, target_row.count_);
  for (int64_t i = 0; i < min_col_cnt && OB_SUCC(ret); ++i) {
    ObObj *src_obj = src_row.get_cell(i);
    ObObj *target_obj = target_row.get_cell(i);
    if (OB_ISNULL(src_obj)) {
      COMMON_LOG(DEBUG, "skip null obj from src row", K(i), K(min_col_cnt));
    } else if (OB_ISNULL(target_obj)) {
      COMMON_LOG(DEBUG, "skip null obj from target row", K(i), K(min_col_cnt));
    } else if (src_obj->is_max_value() || src_obj->is_min_value()){
      COMMON_LOG(DEBUG, "skip min or max obj from src row", K(i), K(min_col_cnt));
    } else if (OB_FAIL(cast_obj(*src_obj,
                                *target_obj,
                                allocator,
                                ctx,
                                accuracies_.at(i)))) {
      COMMON_LOG(DEBUG, "fail to cast obj", K(i), K(ret));
    } else {
      /* succ */
    }
  }

  return ret;
}

int ObPartDescList::cast_row_for_obkv(ObNewRow &src_row,
                                      ObNewRow &target_row,
                                      ObIAllocator &allocator,
                                      ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t min_col_cnt = std::min(src_row.count_, target_row.count_);
  for (int64_t i = 0; i < min_col_cnt && OB_SUCC(ret); ++i) {
    ObObj *src_obj = src_row.get_cell(i);
    ObObj *target_obj = target_row.get_cell(i);
    if (OB_ISNULL(src_obj)) {
      COMMON_LOG(DEBUG, "skip null obj from src row", K(i), K(min_col_cnt));
    } else if (OB_ISNULL(target_obj)) {
      COMMON_LOG(DEBUG, "skip null obj from target row", K(i), K(min_col_cnt));
    } else if (src_obj->is_max_value() || src_obj->is_min_value()){
      COMMON_LOG(DEBUG, "skip min or max obj from src row", K(i), K(min_col_cnt));
    } else if (OB_FAIL(cast_obj_for_obkv(*src_obj,
                                         *target_obj,
                                         allocator,
                                         ctx,
                                         accuracies_.at(i)))) {
      COMMON_LOG(INFO, "fail to cast obj for obkv", K(i), K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int64_t ObPartDescList::to_plain_string(char* buf, const int64_t buf_len) const {
  // "partition by list (P0[{"BIGINT":11111},{"BIGINT":111111}), ("BIGINT":22222, "BIGINT":22222)], P1[("BIGINT":11111,"BIGINT":111111), ("BIGINT":22222, "BIGINT":22222)]....)"
  int64_t pos = 0;
  if (part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
    BUF_PRINTF("sub");
  }
  BUF_PRINTF("partition by list ");
  BUF_PRINTF("(");
  for (int i = 0; i < part_array_size_; i++) {
    if (OB_ISNULL(part_array_ + i)) {
      /* do nothing */
    } else {
      if (i != 0) {
        J_COMMA();
      }
      BUF_PRINTF("P%ld", part_array_[i].part_id_);
      J_ARRAY_START();
      for (int j = 0; j < part_array_[i].rows_.count(); j++) {
        if (j != 0) {
          J_COMMA();
        }
        BUF_PRINTF("(");
        for (int k = 0; k < part_array_[i].rows_[j].get_count(); k++) {
          if (k != 0) {
            J_COMMA();
          }
          if (OB_ISNULL(part_array_[i].rows_[j].get_cell(k))) {
            /* do nothing */
          } else {
            const ObObj *obj = part_array_[i].rows_[j].get_cell(k);
            BUF_PRINTF("%s:", ob_obj_type_str(obj->get_type()));
            obj->print_plain_str_literal(buf, buf_len, pos);
            if (obj->is_ext()) {
              BUF_PRINTF("maxvalue");
            }
            if (obj->is_string_type()) {
              BUF_PRINTF("<%s>", ObCharset::collation_name(obj->get_collation_type()));
            }
          }
        }
        BUF_PRINTF(")");
      }
      J_ARRAY_END();
    }
  }
  BUF_PRINTF(")");
  return pos;
}

} // end of common
} // end of oceanbase
