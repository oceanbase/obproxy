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
#include "share/part/ob_part_desc_range.h"
#include "obproxy/proxy/route/obproxy_expr_calculator.h"


namespace oceanbase
{
namespace common
{
bool RangePartition::less_than(const RangePartition &a, const RangePartition &b)
{
  bool ret = false;
  if (a.is_max_value_) {
    ret = false;
  } else if (b.is_max_value_) {
    ret = true;
  } else {
    ret = a.high_bound_val_ < b.high_bound_val_;
  }
  return ret;
}

ObPartDescRange::ObPartDescRange() : part_array_ (NULL)
                                   , part_array_size_(0)
{
}

ObPartDescRange::~ObPartDescRange()
{
  // the memory is hold by allocator, will free if the allcator is reset
}

int64_t ObPartDescRange::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();

  J_KV("part_type", "range",
       K_(part_array_size));

  J_OBJ_END();
  return pos;
}


int64_t ObPartDescRange::get_start(const RangePartition *part_array,
                                   const int64_t size,
                                   const ObNewRange &range)
{
  int64_t ret = 0;
  RangePartition tmp;
  if (range.start_key_.is_min_row()) {
    ret = 0;
  } else {
    tmp.high_bound_val_ = range.start_key_;
    const RangePartition *result = std::upper_bound(part_array,
                                                    part_array + size,
                                                    tmp,
                                                    RangePartition::less_than);
    ret = result - part_array;
  }
  return ret;
}

int64_t ObPartDescRange::get_end(const RangePartition *part_array,
                                 const int64_t size,
                                 const ObNewRange &range)
{
  int64_t ret = 0;
  RangePartition tmp;
  if (range.end_key_.is_max_row()) {
    ret = size - 1;
  } else {
    tmp.high_bound_val_ = range.end_key_;
    const RangePartition *result = std::lower_bound(part_array,
                                                    part_array + size,
                                                    tmp,
                                                    RangePartition::less_than);
    int64_t pos = result - part_array;
    if (pos >= size) {
      ret = size - 1;
    } else if (result->is_max_value_){
      ret = pos;
    } else if (result->high_bound_val_ == range.end_key_) {
      if (pos == size - 1) {
        ret = size - 1;
      } else {
        if (range.border_flag_.inclusive_end()) {
          ret = pos + 1;
        } else {
          ret = pos;
        }
      }
    } else {
      if (pos == size - 1) {
        ret = size - 1;
      } else {
        ret = pos;
      }
    }
  }
  if (ret >= size) {
    ret = size - 1;
  }
  return ret;
}

RangePartition::RangePartition() : is_max_value_(false)
                                 , part_id_(0)
                                 , first_part_id_(0)
{
}

int ObPartDescRange::get_part(ObNewRange &range,
                              ObIAllocator &allocator,
                              ObIArray<int64_t> &part_ids,
                              ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  part_ids.reset();

  if (OB_ISNULL(part_array_)
      || OB_UNLIKELY(part_array_size_ <= 0)
      || OB_UNLIKELY(part_array_[0].is_max_value_)
      || (!range.start_key_.is_valid() && !range.end_key_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K_(part_array), K_(part_array_size), K(range), K(ret));
    // use the fisrt range as the type to cast
  } else if (OB_FAIL(cast_key(range.start_key_, part_array_[0].high_bound_val_, allocator, ctx))) {
    COMMON_LOG(INFO, "fail to cast start key ",
                     K(range), K(part_array_[0].high_bound_val_), K(ret));
  } else if (OB_FAIL(cast_key(range.end_key_, part_array_[0].high_bound_val_, allocator, ctx))) {
    COMMON_LOG(INFO, "fail to cast end key",
                     K(range), K(part_array_[0].high_bound_val_), K(ret));
  } else {
    int64_t start = get_start(part_array_, part_array_size_, range);
    int64_t end = get_end(part_array_, part_array_size_, range);

    for (int64_t i = start; OB_SUCC(ret) && i <= end; i ++) {
      if (OB_FAIL(part_ids.push_back(part_array_[i].part_id_))) {
        COMMON_LOG(WARN, "fail to push part id", K(ret));
      }
    }
  }
  return ret;
}

int ObPartDescRange::get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = num % part_array_size_;
  if (OB_FAIL(part_ids.push_back(part_array_[part_idx].part_id_))) {
    COMMON_LOG(WARN, "fail to push part id", K(ret));
  }
  return ret;
}

int ObPartDescRange::cast_key(ObRowkey &src_key,
                              ObRowkey &target_key,
                              ObIAllocator &allocator,
                              ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t min_col_cnt = std::min(src_key.get_obj_cnt(), target_key.get_obj_cnt());
  for (int64_t i = 0; i < min_col_cnt && OB_SUCC(ret); ++i) {
    if (OB_FAIL(cast_obj(const_cast<ObObj &>(src_key.get_obj_ptr()[i]),
                         const_cast<ObObj &>(target_key.get_obj_ptr()[i]),
                         allocator,
                         ctx))) {
      COMMON_LOG(INFO, "fail to cast obj", K(i), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

inline int ObPartDescRange::cast_obj(ObObj &src_obj,
                                     ObObj &target_obj,
                                     ObIAllocator &allocator,
                                     ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  COMMON_LOG(DEBUG, "begin to cast obj for range", K(src_obj), K(target_obj));

  ObTimeZoneInfo tz_info;
  ObDataTypeCastParams dtc_params;
  ObObjType obj_type = target_obj.get_type();

  if (OB_FAIL(obproxy::proxy::ObExprCalcTool::build_dtc_params_with_tz_info(ctx.get_session_info(),
                                                                            obj_type, tz_info, dtc_params))) {
    COMMON_LOG(WARN, "fail to build dtc params with ctx session", K(ret), K(obj_type));
  } else {
    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NULL_ON_WARN, target_obj.get_collation_type());
    ObAccuracy accuracy(accuracy_.length_, accuracy_.precision_, accuracy_.scale_);
    const ObObj *res_obj = &src_obj;
    
    // use src_obj as buf_obj
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type, target_obj.get_collation_type(), cast_ctx, src_obj, src_obj))) {
      COMMON_LOG(WARN, "failed to cast obj", K(ret), K(src_obj), K(target_obj));
    } else if (ctx.need_accurate()
               && OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, target_obj.get_collation_type(), *res_obj, src_obj, res_obj))) {
      COMMON_LOG(WARN, "fail to obj accuracy check", K(ret), K(src_obj));
    } else {
      COMMON_LOG(DEBUG, "end to cast obj for range", K(src_obj), K(target_obj));
    }
  }
    
  return ret;
}

} // end of common
} // end of oceanbase
