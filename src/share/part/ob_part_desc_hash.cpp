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

#include <cmath>
#include "common/ob_obj_cast.h"
#include "share/part/ob_part_desc_hash.h"
#include "lib/charset/ob_mysql_global.h"
#include "obproxy/proxy/route/obproxy_expr_calculator.h"

namespace oceanbase
{
namespace common
{
ObPartDescHash::ObPartDescHash() : is_oracle_mode_(false)
                                 , part_num_(0)
                                 , part_space_(0)
                                 , part_array_(NULL)
{
}

/*
  GET PARTITION ID

  If input is not single int value, get all partition ids; otherwise, get the particular one.
 */
int ObPartDescHash::get_part(ObNewRange &range,
                             ObIAllocator &allocator,
                             ObIArray<int64_t> &part_ids,
                             ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (1 != range.get_start_key().get_obj_cnt()) { // single value
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(DEBUG, "hash part should be single key",
                      "obj_cnt", range.get_start_key().get_obj_cnt(), K(ret));
  } else {
    int64_t part_idx = -1;
    ObObj &src_obj = const_cast<ObObj &>(range.get_start_key().get_obj_ptr()[0]);
    if (is_oracle_mode_) {
      ret = calc_value_for_oracle(src_obj, allocator, part_idx, ctx);
    } else {
      ret = calc_value_for_mysql(src_obj, allocator, part_idx);
    }

    if (OB_SUCC(ret)) {
      int64_t part_id = -1;
      if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
        COMMON_LOG(WARN, "fail to get part hash id", K(ret));
      } else if (OB_FAIL(part_ids.push_back(part_id))) {
        COMMON_LOG(WARN, "fail to push part_id", K(ret));
      }
    }
  }

  return ret;
}

int ObPartDescHash::get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_id = -1;
  int64_t part_idx = num % part_num_;
  if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
    COMMON_LOG(WARN, "fail to get part hash id", K(num), K(ret));
  } else if (OB_FAIL(part_ids.push_back(part_id))) {
    COMMON_LOG(WARN, "fail to push part_id", K(ret));
  }
  return ret;
}

bool ObPartDescHash::is_oracle_supported_type(const ObObjType type)
{
  bool supported = false;
  switch (type) {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObNumberType:
    case ObDateTimeType:
    case ObCharType:
    case ObVarcharType:
    case ObTimestampTZType:
    case ObTimestampLTZType:
    case ObTimestampNanoType:
    case ObRawType:
    /*  
    case ObIntervalYMType:
    case ObIntervalDSType:
    */
    case ObNumberFloatType:
    case ObNCharType:
    case ObNVarchar2Type:
    {
      supported = true;
      break;
    }
    default: {
      supported = false;
    }
  }
  return supported;
}

uint64_t ObPartDescHash::calc_hash_value_with_seed(const ObObj &obj, int64_t seed)
{
  uint64 hval = 0;
  ObObjType type = obj.get_type();

  if (ObCharType == type || ObNCharType == type) {
    ObObj obj_trimmed;
    int32_t val_len = obj.get_val_len();
    const char* obj_str = obj.get_string_ptr();
    while (val_len > 1) {
      if (OB_PADDING_CHAR == *(obj_str + val_len - 1)) {
        --val_len;
      } else {
        break;
      }
    }
    obj_trimmed.set_collation_type(obj.get_collation_type());
    obj_trimmed.set_string(type, obj.get_string_ptr(), val_len);
    if (share::schema::PARTITION_FUNC_TYPE_HASH_V2 == part_func_type_) {
      hval = obj_trimmed.hash_murmur(seed);
    } else {
      hval = obj_trimmed.hash(seed);
    }
  } else {
    if (share::schema::PARTITION_FUNC_TYPE_HASH_V2 == part_func_type_) {
      hval = obj.hash_murmur(seed);
    } else {
      hval = obj.hash(seed);
    }
  }

  return hval;
}

int ObPartDescHash::calc_value_for_oracle(ObObj &src_obj,
                                          ObIAllocator &allocator,
                                          int64_t &part_idx,
                                          ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;

  ObTimeZoneInfo tz_info;
  ObDataTypeCastParams dtc_params;

  if (OB_FAIL(obproxy::proxy::ObExprCalcTool::build_dtc_params_with_tz_info(ctx.get_session_info(),
                                                                            obj_type_, tz_info, dtc_params))) {
    COMMON_LOG(WARN, "fail to build dtc params with tz info", K(ret));
  } else {
    lib::set_oracle_mode(true);

    uint64_t hash_val = 0;
    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NULL_ON_WARN, cs_type_);
    const ObObj *res_obj = &src_obj;

    ObAccuracy accuracy(accuracy_.valid_, accuracy_.length_, accuracy_.precision_, accuracy_.scale_);

    // use src_obj as buf_obj
    COMMON_LOG(DEBUG, "begin to cast value for hash oracle", K(src_obj), K(cs_type_));
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type_, cs_type_, cast_ctx, src_obj, src_obj))) {
      COMMON_LOG(WARN, "failed to cast obj", K(ret), K(src_obj), K(obj_type_), K(cs_type_));
    } else if (ctx.need_accurate()
               && OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, cs_type_, *res_obj, src_obj, res_obj))) {
      COMMON_LOG(WARN, "fail to obj accuracy check", K(ret), K(src_obj), K(obj_type_));
    } else {
      COMMON_LOG(DEBUG, "finish to cast and accuracy values for hash oracle", K(src_obj), K(obj_type_), K(cs_type_));
      // calculate hash value
      const ObObjType type = src_obj.get_type();
      if (ObNullType == type) {
        //do nothing, hash_code not changed
      } else if (!is_oracle_supported_type(type)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "type is wrong", K(ret), K(src_obj), K(type));
      } else {
        hash_val = calc_hash_value_with_seed(src_obj, hash_val);
      }
    }

    lib::set_oracle_mode(false);

    // calculate logic partition
    if (OB_SUCC(ret)) {
      int64_t N = 0;
      int64_t powN = 0;
      const static int64_t max_part_num_log2 = 64;

      int64_t result_num = static_cast<int64_t>(hash_val);
      result_num = result_num < 0 ? -result_num : result_num;

      N = static_cast<int64_t>(std::log(part_num_) / std::log(2));
      if (N >= max_part_num_log2) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "result is too big", K(N), K(part_num_), K(result_num));
      } else {
        powN = (1ULL << N);
        part_idx = result_num % powN;
        if (part_idx + powN < part_num_ && (result_num & powN) == powN) {
          part_idx += powN;
        }
      }
      COMMON_LOG(DEBUG, "get hash part idx for oracle mode",
        K(ret), K(result_num), K(part_num_), K(N), K(powN), K(part_idx));
    }
  }

  return ret;
}

int ObPartDescHash::calc_value_for_mysql(ObObj &src_obj, ObIAllocator &allocator, int64_t &part_idx)
{
  int ret = OB_SUCCESS;

  ObCastCtx cast_ctx(&allocator, NULL, CM_NULL_ON_WARN, CS_TYPE_INVALID);
  ObObjType target_type = ObIntType;
  if (OB_FAIL(ObObjCasterV2::to_type(target_type, cs_type_, cast_ctx, src_obj, src_obj))) {
    COMMON_LOG(INFO, "failed to cast to target type", K(target_type), K(src_obj), K(ret));
  } else {
    int64_t val = 0;
    if (OB_FAIL(src_obj.get_int(val))) {
      COMMON_LOG(WARN, "fail to get int", K(src_obj), K(ret));
    } else {
      if (OB_UNLIKELY(INT64_MIN == val)) {
        val = INT64_MAX;
      } else {
        val = val < 0 ? -val : val;
      }
      part_idx = val % part_num_;
      COMMON_LOG(DEBUG, "get hash part idx for mysql mode", K(ret), K(val), K(part_num_), K(part_idx));
    }
  }

  return ret;
}

int ObPartDescHash::get_part_hash_idx(const int64_t part_idx, int64_t &part_id)
{
  int ret =OB_SUCCESS;

  part_id = part_idx;
  if (share::schema::PARTITION_LEVEL_ONE == part_level_) {
    if (part_idx >= 0 && NULL != part_array_) {
      part_id = part_array_[part_idx];
    }
  }
  if (OB_SUCC(ret)) {
    part_id = part_space_ << OB_PART_IDS_BITNUM | part_id;
    COMMON_LOG(DEBUG, "get hash part id", K(ret), K(part_num_), K(part_id));
  }
  return ret;
}
} // end of common
} // end of oceanbase
