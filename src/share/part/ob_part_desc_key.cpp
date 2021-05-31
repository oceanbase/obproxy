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

namespace oceanbase
{
namespace common
{
ObPartDescKey::ObPartDescKey() : part_num_(0)
                               , part_space_(0)
                               , first_part_id_(0)
                               , obj_type_(ObMaxType)
                               , cs_type_(CS_TYPE_INVALID)
                               , part_array_(NULL)
{
}

/*
  GET PARTITION ID

  If input is not single int value, return invalid argument; otherwise, get the particular one.
 */
int ObPartDescKey::get_part(ObNewRange &range,
                            ObIAllocator &allocator,
                            ObIArray<int64_t> &part_ids)
{
  int ret = OB_SUCCESS;

  if (1 != range.get_start_key().get_obj_cnt()) { // single value
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(DEBUG, "key part should be single key",
                      "obj_cnt", range.get_start_key().get_obj_cnt(), K(ret));
  } else {
    ObObj &src_obj = const_cast<ObObj &>(range.get_start_key().get_obj_ptr()[0]);
    src_obj.set_collation_type(cs_type_);
    ObCastCtx cast_ctx(&allocator, NULL, CM_NULL_ON_WARN, cs_type_);
    // use src_obj as buf_obj
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type_, cast_ctx, src_obj, src_obj))) {
      COMMON_LOG(INFO, "failed to cast obj", K(src_obj), K(obj_type_), K(cs_type_), K(ret));
    } else {
      int64_t result_num = 0;
      if (share::schema::PARTITION_FUNC_TYPE_KEY_V3 == part_func_type_
          || share::schema::PARTITION_FUNC_TYPE_KEY_IMPLICIT_V2 == part_func_type_) {
        result_num = static_cast<int64_t>(src_obj.hash_murmur());
      } else {
        result_num = static_cast<int64_t>(src_obj.hash());
      }
      result_num = result_num < 0 ? -result_num : result_num;
      int64_t part_idx = result_num % part_num_;
      int64_t part_id = part_idx;
      if (share::schema::PARTITION_LEVEL_ONE == part_level_ && NULL != part_array_) {
        part_id = part_array_[part_idx];
      }
      part_id = part_space_ << OB_PART_IDS_BITNUM | part_id;
      if (OB_FAIL(part_ids.push_back(part_id))) {
        COMMON_LOG(WARN, "fail to push part_id", K(ret));
      }
    }
  }

  return ret;
}
} // end of common
} // end of oceanbase
