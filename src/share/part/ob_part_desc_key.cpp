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
#include "share/part/ob_part_desc_key.h"
#include "obproxy/proxy/route/obproxy_expr_calculator.h"


namespace oceanbase
{
namespace common
{
ObPartDescKey::ObPartDescKey() : part_num_(0)
                               , part_space_(0)
                               , first_part_id_(0)
                               , part_array_(NULL)
{
}
/**
  GET PARTITION ID
  Hash partition only route like 'by key(c1,c2) ... where c1=xx and c2=xx',
  only use range.start_key_ to calc, because when condition is like 'c1=xx', 
  start_key_[i] == end_key_[i] 
  @param range (xxx,yyy,min,min ; xxx,yyy,max,max)
  @param allocator
  @param part_ids[out]
  @param ctx
 */
int ObPartDescKey::get_part(ObNewRange &range,
                            ObIAllocator &allocator,
                            ObIArray<int64_t> &part_ids,
                            ObPartDescCtx &ctx,
                            ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  // provide all objs are valid. 
  // "valid" means the obj not min or max
  int64_t valid_obj_cnt = range.start_key_.get_obj_cnt();
  // cast objs store in range.start_key_
  for (int64_t i = 0; OB_SUCC(ret) && i < range.start_key_.get_obj_cnt(); i++) {
    // if obj is min or max, means all valid obj has been casted
    if (range.start_key_.get_obj_ptr()[i].is_max_value() || 
        range.start_key_.get_obj_ptr()[i].is_min_value()) {
      // minus the number of invalid obj
      valid_obj_cnt = valid_obj_cnt - (range.start_key_.get_obj_cnt() - i);
      // not need to cast any more and break loop
      if (valid_obj_cnt == 0) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "not support key partition calc with range", K(range));
      }
      break;
    } else {
      ObObj *src_obj = const_cast<ObObj*>(&range.start_key_.get_obj_ptr()[i]);
      // here src_obj shouldn't be null
      if (OB_ISNULL(src_obj)) {
        ret = OB_ERR_NULL_VALUE;
        COMMON_LOG(ERROR, "unexpected null pointer src_obj");
        break;
      } else if (src_obj->is_null()) {
        // here src_obj shouldn't be null type
        ret = OB_OBJ_TYPE_ERROR;
        COMMON_LOG(ERROR, "unexpected null type", K(src_obj));
      } else if(OB_FAIL(cast_obj(*src_obj, obj_types_[i], cs_types_[i], allocator, ctx, accuracies_.at(i)))) {
        COMMON_LOG(WARN, "cast obj failed", K(src_obj), "obj_type", obj_types_[i], "cs_type", cs_types_[i]);
        // TODO: handle failure
      }
    }


  }

  int64_t part_idx = -1;
  if (OB_SUCC(ret)) {
    // hash val
    int64_t result = 0;
    int64_t part_id = -1;
    if (OB_FAIL(calc_value_for_mysql(range.start_key_.get_obj_ptr(), valid_obj_cnt, result, ctx))) {
      COMMON_LOG(WARN, "fail to cal key val", K(ret), K(valid_obj_cnt));
    } else if (OB_FAIL(calc_key_part_idx(result, part_num_, part_idx))) {
      COMMON_LOG(WARN, "fail to cal key part idx", K(ret), K(result), K(part_num_));
    } else if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
      COMMON_LOG(WARN, "fail to get part key id", K(part_idx), K(ret));
    } else if (OB_FAIL(part_ids.push_back(part_id))) {
      COMMON_LOG(WARN, "fail to push part_id", K(ret));
    } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
      COMMON_LOG(WARN, "fail to push tablet id", K(ret));
    } else {}
  }

  return ret;
}


int ObPartDescKey::calc_value_for_mysql(const ObObj *objs,
                                          int64_t objs_cnt,
                                          int64_t &result,
                                          ObPartDescCtx &ctx)
{
  int ret = OB_SUCCESS;
  uint64_t hash_code = 0;
   if (OB_ISNULL(objs) || 0 == objs_cnt) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "objs_stack is null or number incorrect", K(objs), K(objs_cnt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < objs_cnt; ++i) {
    const ObObj &obj = objs[i];
     if (ObNullType == obj.get_type()) {
      //do nothing, hash_code not changed
    } else {
      hash_code = calc_hash_value_with_seed(obj, ctx.get_cluster_version(), hash_code);
    }
  }
  result = static_cast<int64_t>(hash_code);
  result = result < 0 ? -result : result;
  if (OB_SUCC(ret)) {
    COMMON_LOG(TRACE, "succ to calc hash value with oracle mode", KP(objs), K(objs[0]), K(objs_cnt), K(result), K(ret));
  } else {
    COMMON_LOG(WARN, "fail to calc hash value with oracle mode", KP(objs), K(objs_cnt), K(result), K(ret));
  }
  return ret;
}

uint64_t ObPartDescKey::calc_hash_value_with_seed(const ObObj &obj, const int64_t cluster_version, int64_t seed)
{
  int64_t hval = 0;
  if ((IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version)
        && (share::schema::PARTITION_FUNC_TYPE_KEY_V3 == part_func_type_
          || share::schema::PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_func_type_))
      || (!IS_CLUSTER_VERSION_LESS_THAN_V4(cluster_version))) {
    hval = static_cast<int64_t>(obj.hash_murmur(seed));
  } else {
    hval = static_cast<int64_t>(obj.hash(seed));
  }

  return hval;
}

int ObPartDescKey::calc_key_part_idx(const uint64_t val,
                                      const int64_t part_num,
                                      int64_t &partition_idx)
{
  int ret = OB_SUCCESS;
  partition_idx = val % part_num;
  COMMON_LOG(DEBUG, "get hash part idx", K(lbt()), K(ret), K(val), K(part_num), K(partition_idx));
  return ret;
}

int ObPartDescKey::get_part_by_num(const int64_t num, common::ObIArray<int64_t> &part_ids, common::ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_id = -1;
  int64_t part_idx = num % part_num_;
  if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
    COMMON_LOG(WARN, "fail to get part hash id", K(num), K(ret));
  } else if (OB_FAIL(part_ids.push_back(part_id))) {
    COMMON_LOG(WARN, "fail to push part_id", K(ret));
  } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
    COMMON_LOG(WARN, "fail to push tablet id", K(ret));
  }
  return ret;
}

int ObPartDescKey::get_part_hash_idx(const int64_t part_idx, int64_t &part_id)
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
}
