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

/**
  GET PARTITION ID
  Hash partition only route like 'by hash(c1,c2) ... where c1=xx and c2=xx',
  only use range.start_key_ to calc, because when condition is like 'c1=xx', 
  start_key_[i] == end_key_[i] 
  @param range (xxx,yyy,min,min ; xxx,yyy,max,max)
  @param allocator
  @param part_ids[out]
  @param ctx
 */
int ObPartDescHash::get_part(ObNewRange &range,
                             ObIAllocator &allocator,
                             ObIArray<int64_t> &part_ids,
                             ObPartDescCtx &ctx,
                             ObIArray<int64_t> &tablet_ids,
                             int64_t &part_idx)
{
  int ret = OB_SUCCESS; 
  // 1. Cast obj
  // provide all objs are valid. 
  // "valid" means the obj not min or max
  int64_t valid_obj_cnt = range.start_key_.get_obj_cnt();
  for (int64_t i = 0; OB_SUCC(ret) && i < range.start_key_.get_obj_cnt(); i++) {
    // if obj is min or max, means all valid obj has been casted
    if (range.start_key_.get_obj_ptr()[i].is_max_value() || 
        range.start_key_.get_obj_ptr()[i].is_min_value()) {
      // minus the number of invalid obj
      valid_obj_cnt = valid_obj_cnt - (range.start_key_.get_obj_cnt() - i);
      // not need to cast any more and break loop
      if (valid_obj_cnt == 0) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(DEBUG, "not support hash partition calc with range", K(range));
      }
      break;
    } else {
      ObObj *src_obj = const_cast<ObObj*>(&range.start_key_.get_obj_ptr()[i]);
      if (OB_ISNULL(src_obj)) {
        // here src_obj shouldn't be null
        ret = OB_ERR_NULL_VALUE;
        COMMON_LOG(DEBUG, "unexpected null pointer src_obj");
      } else if (is_oracle_mode_) {
        if(OB_FAIL(cast_obj(*src_obj, obj_types_[i], cs_types_[i], allocator, ctx, accuracies_.at(i)))) {
          COMMON_LOG(DEBUG, "cast obj failed", K(src_obj), "obj_type", obj_types_[i], "cs_type", cs_types_[i]);
          // TODO: handle failure
        }
      // mysql mode only cast first valid obj to int type then break loop
      } else /*if (is_mysql_mode_) */ {
        ObCastCtx cast_ctx(&allocator, NULL, CM_NULL_ON_WARN, CS_TYPE_INVALID);
        if (OB_FAIL(ObObjCasterV2::to_type(ObIntType, cs_types_[i], cast_ctx, *src_obj, *src_obj))) {
          COMMON_LOG(DEBUG, "failed to cast to ObIntType", K(src_obj), K(ret));
        }
        break;
      }
      if (OB_SUCC(ret)) {
        COMMON_LOG(DEBUG, "succ to cast target obj", K(src_obj));
      }
    }
  }

  // 2. Calc partition id
  if (OB_SUCC(ret)) {
    // hash val
    int64_t result = 0;
    if (is_oracle_mode_) {
      // oracle mode: use obj to calc hash val
      ret = calc_value_for_oracle(range.start_key_.get_obj_ptr(), valid_obj_cnt, result, ctx);
    } else {
      // mysql mode: use single obj to calc hash val
      ret = calc_value_for_mysql(range.start_key_.get_obj_ptr(), result);
    }
    int64_t part_id = -1;
    if (OB_SUCC(ret) && OB_FAIL(calc_hash_part_idx(result, part_num_, part_idx))) {
      COMMON_LOG(DEBUG, "fail to cal hash part idx", K(ret), K(result), K(part_num_));
    } else if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
      COMMON_LOG(DEBUG, "fail to get part hash id", K(part_idx), K(ret));
    } else if (OB_FAIL(part_ids.push_back(part_id))) {
      COMMON_LOG(DEBUG, "fail to push part_id", K(ret));
    } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
      COMMON_LOG(DEBUG, "fail to push tablet_id", K(ret));
    } else {}
  }

  return ret;
}

int ObPartDescHash::get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids, common::ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_id = -1;
  int64_t part_idx = num % part_num_;
  if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
    COMMON_LOG(DEBUG, "fail to get part hash id", K(num), K(ret));
  } else if (OB_FAIL(part_ids.push_back(part_id))) {
    COMMON_LOG(DEBUG, "fail to push part_id", K(ret));
  } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
    COMMON_LOG(DEBUG, "fail to push tablet_id", K(ret));
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
    case ObDecimalIntType:
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

uint64_t ObPartDescHash::calc_hash_value_with_seed(const ObObj &obj, const int64_t cluster_version, int64_t seed)
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
    if ((IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)
         && share::schema::PARTITION_FUNC_TYPE_HASH_V2 == part_func_type_)
        || (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version))) {
      hval = obj_trimmed.hash_murmur(seed);
    } else {
      hval = obj_trimmed.hash(seed);
    }
  } else {
    if ((IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)
          && share::schema::PARTITION_FUNC_TYPE_HASH_V2 == part_func_type_)
        || (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version))) {
      hval = obj.hash_murmur(seed);
    } else {
      hval = obj.hash(seed);
    }
  }

  return hval;
}
int ObPartDescHash::calc_value_for_oracle(const ObObj *objs,
                                          int64_t objs_cnt,
                                          int64_t &result,
                                          ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  uint64_t hash_code = 0;
  if (OB_ISNULL(objs) || 0 == objs_cnt) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(DEBUG, "objs_stack is null or number incorrect", K(objs), K(objs_cnt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < objs_cnt; ++i) {
    const ObObj &obj = objs[i];
    const ObObjType type = obj.get_type();
    if (ObNullType == type) {
      //do nothing, hash_code not changed
    } else if (!is_oracle_supported_type(type)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WDIAG, "type is wrong", K(ret), K(obj), K(type));
    } else if (obj.is_decimal_int()) {
      if (OB_FAIL(decimal_int_murmur_hash(obj, hash_code, hash_code))) {
        COMMON_LOG(DEBUG, "fail to calc hash value of decimal int", K(ret), K(obj), K(hash_code));
      }
    } else {
      hash_code = calc_hash_value_with_seed(obj, ctx.get_cluster_version(), hash_code);
    }
  }
  result = static_cast<int64_t>(hash_code);
  result = result < 0 ? -result : result;
  if (OB_SUCC(ret)) {
    COMMON_LOG(DEBUG, "succ to calc hash value with oracle mode", KP(objs), K(objs[0]), K(objs_cnt), K(result), K(ret));
  } else {
    COMMON_LOG(DEBUG, "fail to calc hash value with oracle mode", KP(objs), K(objs_cnt), K(result), K(ret));
  }
  return ret;
}

int ObPartDescHash::calc_value_for_mysql(const ObObj *obj, int64_t &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(obj)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(DEBUG, "unexpected null ptr", K(ret));
  } else if (OB_UNLIKELY(obj->is_null())) {
    result = 0;
  } else {
    int64_t num = 0;
    if (OB_FAIL(obj->get_int(num))) {
      COMMON_LOG(DEBUG, "fail to get int", K(obj), K(ret));
    } else {
      if (OB_UNLIKELY(INT64_MIN == num)) {
        num = INT64_MAX;
      } else {
        num = num < 0 ? -num : num;
      }
      result = num;
    }
  }
  COMMON_LOG(TRACE, "calc hash value with mysql mode", K(ret));
  return ret;
}

int ObPartDescHash::calc_hash_part_idx(const uint64_t val,
                                       const int64_t part_num,
                                       int64_t &partition_idx)
{
  int ret = OB_SUCCESS;
  int64_t N = 0;
  int64_t powN = 0;
  const static int64_t max_part_num_log2 = 64;
  // This function is used by SQL. Should ensure SQL runs in MySQL mode when query sys table.
  if (is_oracle_mode_) {
    // It will not be a negative number, so use forced conversion instead of floor
    N = static_cast<int64_t>(std::log(part_num) / std::log(2));
    if (N >= max_part_num_log2) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(DEBUG, "result is too big", K(N), K(part_num), K(val));
    } else {
      powN = (1ULL << N);
      partition_idx = val & (powN - 1); //pow(2, N));
      if (partition_idx + powN < part_num && (val & powN) == powN) {
        partition_idx += powN;
      }
    }
    COMMON_LOG(DEBUG, "get hash part idx", K(lbt()), K(ret), K(val), K(part_num), K(N), K(powN), K(partition_idx));
  } else {
    partition_idx = val % part_num;
    COMMON_LOG(DEBUG, "get hash part idx", K(lbt()), K(ret), K(val), K(part_num), K(partition_idx));
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

int ObPartDescHash::set_cs_types(int64_t pos, const ObCollationType cs_type)
{
  int ret = OB_SUCCESS;
  if (cs_types_.count() <= pos || pos < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    COMMON_LOG(WDIAG, "set part desc cs types out of range", K(pos), K(ret));
  } else {
    cs_types_.at(pos) = cs_type;
  }

  return ret;
}

int ObPartDescHash::set_obj_types(int64_t pos, const ObObjType obj_type)
{
  int ret = OB_SUCCESS;
  if (obj_types_.count() <= pos || pos < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    COMMON_LOG(WDIAG, "set part desc obj types out of range", K(pos), K(ret));
  } else {
    obj_types_.at(pos) = obj_type;
  }

  return ret;
}


int64_t ObPartDescHash::to_plain_string(char* buf, const int64_t buf_len) const {
    // "partition by <part_type>(<type>(<charset>), <type>(<charset>), <type>(<charset>)) partitions <part_num>"
    int64_t pos = 0;
    if (part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
      BUF_PRINTF("sub");
    }
    BUF_PRINTF("partition by hash");
    BUF_PRINTF("(");
    for (int i = 0; i < obj_types_.count() && i < cs_types_.count(); i++) {
      if (i != 0) {
        J_COMMA();
      }
      BUF_PRINTF("%s<%s>", ob_sql_type_str(obj_types_[i]), ObCharset::collation_name(cs_types_[i]));
    }
    BUF_PRINTF(")");
    BUF_PRINTF(" partitions %ld", part_num_);
    return pos;
}

} // end of common
} // end of oceanbase
