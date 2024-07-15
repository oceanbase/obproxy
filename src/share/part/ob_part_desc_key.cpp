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
#include "obutils/ob_proxy_config.h"


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
                            ObIArray<int64_t> &tablet_ids,
                            int64_t &part_idx)
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
        COMMON_LOG(DEBUG, "not support key partition calc with range", K(range));
      }
      break;
    } else {
      ObObj *src_obj = const_cast<ObObj*>(&range.start_key_.get_obj_ptr()[i]);
      // here src_obj shouldn't be null
      if (OB_ISNULL(src_obj)) {
        ret = OB_ERR_NULL_VALUE;
        COMMON_LOG(EDIAG, "unexpected null pointer src_obj");
        break;
      } else if(OB_FAIL(cast_obj(*src_obj, obj_types_[i], cs_types_[i], allocator, ctx, accuracies_.at(i)))) {
        COMMON_LOG(DEBUG, "cast obj failed", K(src_obj), "obj_type", obj_types_[i], "cs_type", cs_types_[i]);
        // TODO: handle failure
      }
    }
  }

  if (OB_SUCC(ret)) {
    // hash val
    int64_t result = 0;
    int64_t part_id = -1;
    if (OB_FAIL(calc_value_for_mysql(range.start_key_.get_obj_ptr(), valid_obj_cnt, result, ctx))) {
      COMMON_LOG(DEBUG, "fail to cal key val", K(ret), K(valid_obj_cnt));
    } else if (OB_FAIL(calc_key_part_idx(result, part_num_, part_idx))) {
      COMMON_LOG(DEBUG, "fail to cal key part idx", K(ret), K(result), K(part_num_));
    } else if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
      COMMON_LOG(DEBUG, "fail to get part key id", K(part_idx), K(ret));
    } else if (OB_FAIL(part_ids.push_back(part_id))) {
      COMMON_LOG(DEBUG, "fail to push part_id", K(ret));
    } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
      COMMON_LOG(DEBUG, "fail to push tablet id", K(ret));
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
    COMMON_LOG(DEBUG, "objs_stack is null or number incorrect", K(objs), K(objs_cnt), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < objs_cnt; ++i) {
    ObObj obj = objs[i];
    if (obj.is_decimal_int()) {
      if (OB_FAIL(decimal_int_murmur_hash(obj, hash_code, hash_code))) {
        COMMON_LOG(DEBUG, "fail to calc hash val of decimal int", K(obj), K(hash_code));
      }
    } else {
      if (obj.is_null()) {
        obj.set_int(0);
      }
      hash_code = calc_hash_value_with_seed(obj, ctx.get_cluster_version(), hash_code);
    }
  }
  result = static_cast<int64_t>(hash_code);
  result = result < 0 ? -result : result;
  if (OB_SUCC(ret)) {
    COMMON_LOG(TRACE, "succ to calc hash value with mysql mode", KP(objs), K(objs[0]), K(objs_cnt), K(result), K(ret));
  } else {
    COMMON_LOG(DEBUG, "fail to calc hash value with mysql mode", KP(objs), K(objs_cnt), K(result), K(ret));
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

int ObPartDescKey::get_all_part_id_for_obkv(ObIArray<int64_t> &part_ids,
                                            ObIArray<int64_t> &tablet_ids,
                                            ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;

  for(int64_t part_idx = 0; part_idx < part_num_; ++part_idx) {
    if (OB_FAIL(get_part_by_num(part_idx, part_ids, tablet_ids))) {
      COMMON_LOG(WDIAG, "fail to call get_part_by_num", K(ret));
    } else if (OB_FAIL(get_ls_id_by_num(part_idx, ls_ids))) {
      COMMON_LOG(WDIAG, "fail to call get_part_by_num", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    COMMON_LOG(DEBUG, "obkv get all hash part ids", K(part_ids));
  }

  return ret;
}

int ObPartDescKey::get_part_for_obkv(ObNewRange &range,
                                     ObIAllocator &allocator,
                                     ObIArray<int64_t> &part_ids,
                                     ObPartDescCtx &ctx,
                                     ObIArray<int64_t> &tablet_ids,
                                     ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);

  if (range.is_whole_range() || ctx.calc_first_partition() || ctx.calc_last_partition() || ctx.need_get_whole_range()) {
    if (obproxy::obutils::get_global_proxy_config().rpc_support_key_partition_shard_request) {
      if (OB_FAIL(get_all_part_id_for_obkv(part_ids, tablet_ids, ls_ids))) {
        COMMON_LOG(WDIAG, "fail to call get_all_part_id_for_obkv", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      COMMON_LOG(WDIAG, "key partition shard request not support");
    }
  } else {
    // provide all objs are valid. 
    // "valid" means the obj not min or max
    int64_t valid_obj_cnt = range.start_key_.get_obj_cnt();
    bool is_obj_equal = true;
    // cast objs store in range.start_key_
    for (int64_t i = 0; OB_SUCC(ret) && i < range.start_key_.get_obj_cnt(); i++) {
      // if obj is min or max, means all valid obj has been casted
      if (range.start_key_.get_obj_ptr()[i].is_max_value() || 
          range.start_key_.get_obj_ptr()[i].is_min_value()) {
        if ((range.start_key_.get_obj_ptr()[i].is_max_value() && range.end_key_.get_obj_ptr()[i].is_max_value())
          || (range.start_key_.get_obj_ptr()[i].is_min_value() && range.end_key_.get_obj_ptr()[i].is_min_value())) {
          COMMON_LOG(WDIAG, "unable to calculate max/min objest part id");
          ret = OB_ERR_UNEXPECTED;
        } else {
          is_obj_equal = false;
          COMMON_LOG(DEBUG, "start obj is not equal to end obj");
          break;
        }
      } else {
        ObObj *src_obj = const_cast<ObObj*>(&range.start_key_.get_obj_ptr()[i]);
        ObObj &end_obj = const_cast<ObObj &>(range.end_key_.get_obj_ptr()[i]);

        // here src_obj shouldn't be null
        if (OB_ISNULL(src_obj)) {
          ret = OB_ERR_NULL_VALUE;
          COMMON_LOG(EDIAG, "unexpected null pointer src_obj");
          break;
        } else if (!src_obj->is_equal(end_obj, src_obj->get_collation_type())) {
          is_obj_equal = false;
          COMMON_LOG(DEBUG, "start obj is not equal to end obj", "start obj", *src_obj, K(end_obj), "position", i);
          break;
        } else if(OB_FAIL(cast_obj_for_obkv(*src_obj, obj_types_[i], cs_types_[i], allocator, ctx, accuracies_.at(i)))) {
          COMMON_LOG(WDIAG, "cast obj failed", K(src_obj), "obj_type", obj_types_[i], "cs_type", cs_types_[i]);
          // TODO: handle failure
        }
      }
    }

    int64_t part_idx = -1;
    if (OB_SUCC(ret)) {
      if (!is_obj_equal) {
        if (obproxy::obutils::get_global_proxy_config().rpc_support_key_partition_shard_request) {
          if (OB_FAIL(get_all_part_id_for_obkv(part_ids, tablet_ids, ls_ids))) {
            COMMON_LOG(WDIAG, "fail to call get_all_part_id_for_obkv", K(ret));
          }
        } else {
          ret = OB_ERR_KV_KEY_PARTITION_SHARD_REQUEST;
          COMMON_LOG(WDIAG, "start key must equal to end key");
        }
      } else {
        // hash val
        int64_t result = 0;
        int64_t part_id = -1;
        if (OB_FAIL(calc_value_for_mysql(range.start_key_.get_obj_ptr(), valid_obj_cnt, result, ctx))) {
          COMMON_LOG(WDIAG, "fail to cal key val", K(ret), K(valid_obj_cnt));
        } else if (OB_FAIL(calc_key_part_idx(result, part_num_, part_idx))) {
          COMMON_LOG(WDIAG, "fail to cal key part idx", K(ret), K(result), K(part_num_));
        } else if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
          COMMON_LOG(WDIAG, "fail to get part key id", K(part_idx), K(ret));
        } else if (OB_FAIL(part_ids.push_back(part_id))) {
          COMMON_LOG(WDIAG, "fail to push part_id", K(ret));
        } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
          COMMON_LOG(WDIAG, "fail to push tablet id", K(ret));
        } else if (NULL != ls_id_array_ && OB_FAIL(ls_ids.push_back(ls_id_array_[part_idx]))) {
          COMMON_LOG(WDIAG, "fail to push ls id", K(ret));
        } 
      }
    }
  }

  return ret;
}

int ObPartDescKey::get_part_by_num(const int64_t num, ObIArray<int64_t> &part_ids, ObIArray<int64_t> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_id = -1;
  int64_t part_idx = num % part_num_;
  if (OB_FAIL(get_part_hash_idx(part_idx, part_id))) {
    COMMON_LOG(DEBUG, "fail to get part hash id", K(num), K(ret));
  } else if (OB_FAIL(part_ids.push_back(part_id))) {
    COMMON_LOG(DEBUG, "fail to push part_id", K(ret));
  } else if (NULL != tablet_id_array_ && OB_FAIL(tablet_ids.push_back(tablet_id_array_[part_idx]))) {
    COMMON_LOG(DEBUG, "fail to push tablet id", K(ret));
  }
  return ret;
}

int ObPartDescKey::get_ls_id_by_num(const int64_t num, ObIArray<int64_t> &ls_ids)
{
  int ret = OB_SUCCESS;
  int64_t part_idx = num % part_num_;
  if (ls_id_array_ != NULL && OB_FAIL(ls_ids.push_back(ls_id_array_[part_idx]))) {
    COMMON_LOG(DEBUG, "fail to push ls id", K(ret));
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

int ObPartDescKey::set_cs_types(int64_t pos, const ObCollationType cs_type)
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

int ObPartDescKey::set_obj_types(int64_t pos, const ObObjType obj_type)
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

int64_t ObPartDescKey::to_plain_string(char* buf, const int64_t buf_len) const {
  // "partition by <part_type>(<type>(<charset>), <type>(<charset>), <type>(<charset>)) partitions <part_num>"
  int64_t pos = 0;
  if (part_level_ == share::schema::ObPartitionLevel::PARTITION_LEVEL_TWO) {
    BUF_PRINTF("sub");
  }
  BUF_PRINTF("partition by key");
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
}
