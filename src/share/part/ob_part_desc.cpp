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

#include "share/part/ob_part_desc.h"
#include "common/ob_obj_cast.h"
#include "obproxy/proxy/mysqllib/ob_proxy_session_info.h"
#include "obproxy/proxy/route/obproxy_expr_calculator.h"


namespace oceanbase
{
namespace common
{

int ObPartDesc::get_part(ObNewRange &range,
                         ObIAllocator &allocator,
                         ObIArray<int64_t> &part_ids,
                         ObPartDescCtx &ctx,
                         ObIArray<int64_t> &tablet_ids,
                         int64_t &part_idx)
{
  UNUSED(range);
  UNUSED(allocator);
  UNUSED(part_ids);
  UNUSED(ctx);
  UNUSED(tablet_ids);
  UNUSED(part_idx);
  return OB_NOT_IMPLEMENT;
}

int ObPartDesc::get_all_part_id_for_obkv(ObIArray<int64_t> &part_ids,
                                         ObIArray<int64_t> &tablet_ids,
                                         ObIArray<int64_t> &ls_ids)
{
  UNUSED(part_ids);
  UNUSED(tablet_ids);
  UNUSED(ls_ids);
  return OB_NOT_IMPLEMENT;
}

int ObPartDesc::get_part_for_obkv(ObNewRange &range,
                                  ObIAllocator &allocator,
                                  ObIArray<int64_t> &part_ids,
                                  ObPartDescCtx &ctx,
                                  ObIArray<int64_t> &tablet_ids,
                                  ObIArray<int64_t> &ls_ids)
{
  UNUSED(range);
  UNUSED(allocator);
  UNUSED(part_ids);
  UNUSED(ctx);
  UNUSED(tablet_ids);
  UNUSED(ls_ids);
  return OB_NOT_IMPLEMENT;
}

int ObPartDesc::get_part_by_num(const int64_t num,
                                ObIArray<int64_t> &part_ids,
                                ObIArray<int64_t> &tablet_ids)
{
  UNUSED(num);
  UNUSED(part_ids);
  UNUSED(tablet_ids);
  return OB_NOT_IMPLEMENT;
}

int ObPartDesc::get_ls_id_by_num(const int64_t num, ObIArray<int64_t> &ls_ids)
{
  UNUSED(num);
  UNUSED(ls_ids);
  return OB_NOT_IMPLEMENT;
}

/*
 * in order to build ObDataTypeCastParams, get sys var value from session, according to obj type
 */
int ObPartDesc::build_dtc_params(obproxy::proxy::ObClientSessionInfo *session_info,
                                 ObObjType obj_type,
                                 ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(session_info)) {
    ObString sys_key_name;
    switch (obj_type) {
      case ObDateTimeType:
        sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_NLS_DATE_FORMAT);
        break;
      case ObTimestampNanoType:
      case ObTimestampLTZType:
        sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_NLS_TIMESTAMP_FORMAT);
        break;
      case ObTimestampTZType:
        sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_NLS_TIMESTAMP_TZ_FORMAT);
        break;
      default:
        break;
    }

    if (!sys_key_name.empty()) {
      ObObj value_obj;
      int sub_ret = OB_SUCCESS;
      if (OB_SUCCESS != (sub_ret = session_info->get_sys_variable_value(sys_key_name, value_obj))) {
        COMMON_LOG(DEBUG, "fail to get sys var from session, use standard nls format", K(sub_ret), K(sys_key_name));
      } else {
        ObString value_str = value_obj.get_string();
        if (OB_FAIL(dtc_params.set_nls_format_by_type(obj_type, value_str))) {
          COMMON_LOG(DEBUG, "fail to set nls format by type", K(ret), K(obj_type), K(value_str));
        } else {
          COMMON_LOG(DEBUG, "succ to set nls format by type", K(obj_type), K(value_str));
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(DEBUG, "fail to build dtc params due to null session", K(ret));
  }

  return ret;
}

int ObPartDesc::set_accuracies(int64_t pos, const ObAccuracy &accuracy)
{
  int ret = OB_SUCCESS;
  if (accuracies_.count() <= pos || pos < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    COMMON_LOG(WDIAG, "set part desc accuracy out of range", K(pos), K(ret));
  } else {
    accuracies_.at(pos) = accuracy;
  }

  return ret;
}

int ObPartDesc::cast_obj(ObObj &src_obj,
                         ObObj &target_obj,
                         ObIAllocator &allocator,
                         ObPartDescCtx &ctx,
                         ObAccuracy &accuracy)
{
  return cast_obj(src_obj, target_obj.get_type(), target_obj.get_collation_type(), allocator, ctx, accuracy);
}

int ObPartDesc::cast_obj(ObObj &src_obj,
                         ObObjType obj_type,
                         ObCollationType cs_type,
                         ObIAllocator &allocator,
                         ObPartDescCtx &ctx,
                         ObAccuracy &accuracy)
{
  int ret = OB_SUCCESS;
  COMMON_LOG(DEBUG, "begin to cast obj", K(src_obj), K(obj_type), K(cs_type));

  ObTimeZoneInfo tz_info;
  ObDataTypeCastParams dtc_params;

  if (OB_FAIL(obproxy::proxy::ObExprCalcTool::build_dtc_params_with_tz_info(ctx.get_session_info(),
                                                                            obj_type, tz_info, dtc_params))) {
    COMMON_LOG(DEBUG, "fail to build dtc params with ctx session", K(ret), K(obj_type));
  } else {
    ObCastMode cm = CM_NULL_ON_WARN;
    // for insert stmt, use column_conv
    if (ctx.need_accurate()) {
      cm = cm | CM_COLUMN_CONVERT;
    // for other stmt, use cast
    } else {
      cm = cm | CM_CONST_TO_DECIMAL_INT_EQ;
    }
    ObCastCtx cast_ctx(&allocator, &dtc_params, cm, cs_type, &accuracy);
    const ObObj *res_obj = &src_obj;

    // use src_obj as buf_obj
    if (OB_FAIL(ObObjCasterV2::to_type(obj_type, cs_type, cast_ctx, src_obj, src_obj))) {
      COMMON_LOG(DEBUG, "failed to cast obj", K(ret), K(src_obj), K(obj_type), K(cs_type));
    } else if (ctx.need_accurate()
               && OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, cs_type, *res_obj, src_obj, res_obj))) {
      COMMON_LOG(DEBUG, "fail to obj accuracy check", K(ret), K(src_obj));
    } else {
      COMMON_LOG(DEBUG, "end to cast obj for range", K(src_obj), K(obj_type), K(cs_type));
    }
  }

  return ret;
}

int ObPartDesc::decimal_int_murmur_hash(const ObObj &val, const uint64_t seed, uint64_t &res)
{
  int ret = OB_SUCCESS;
  constexpr static uint32_t SIGN_BIT_MASK = (1 << 31);
  const uint32_t *data = reinterpret_cast<const uint32_t *>(val.get_decimal_int());
  int32_t last = val.get_int_bytes() / sizeof(uint32_t) - 1;
  // find minimum length of uint32_t values to represent `val`:
  // if data[last] ==  UINT32_MAX && data[last - 1]'s highest bit is 1, last--
  // else if data[last] == 0 && data[last - 1]'s highest bit is 0, last--
  //
  // this way, val can be easily recovered appending 0/UINT32_MAX values
  if (last <= 0) {
    // do nothing
  } else if (data[last] == UINT32_MAX) {
    while (last > 0 && data[last] == UINT32_MAX && (data[last - 1] & SIGN_BIT_MASK)) {
      last--;
    }
  } else if (data[last] == 0) {
    while (last > 0 && data[last] == 0 && ((data[last - 1] & SIGN_BIT_MASK) == 0)) {
      last--;
    }
  }
  res = murmurhash(data, (last + 1) * sizeof(uint32_t), seed);
  return ret;
}

int ObPartDesc::cast_obj_for_obkv(ObObj &src_obj,
                                  ObObj &target_obj,
                                  ObIAllocator &allocator,
                                  ObPartDescCtx &ctx,
                                  ObAccuracy &accuracy)
{
  return cast_obj_for_obkv(src_obj, target_obj.get_type(), target_obj.get_collation_type(), allocator, ctx, accuracy);
}

int ObPartDesc::cast_obj_for_obkv(ObObj &src_obj,
                                  ObObjType obj_type,
                                  ObCollationType cs_type,
                                  ObIAllocator &allocator,
                                  ObPartDescCtx &ctx,
                                  ObAccuracy &accuracy)
{
  int ret = OB_SUCCESS;
  COMMON_LOG(DEBUG, "begin to cast obj", K(src_obj), K(obj_type), K(cs_type));

  // Currently obkv does not handle timestamp variables and accurate check
  ObDataTypeCastParams dtc_params;
  ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NULL_ON_WARN, cs_type);
  const ObObj *res_obj = &src_obj;

  // use src_obj as buf_obj
  if (OB_FAIL(ObObjCasterV2::to_type(obj_type, cs_type, cast_ctx, src_obj, src_obj))) {
    COMMON_LOG(WDIAG, "failed to cast obj", K(ret), K(src_obj), K(obj_type), K(cs_type));
  } else if (ctx.need_accurate()
              && OB_FAIL(obj_accuracy_check(cast_ctx, accuracy, cs_type, *res_obj, src_obj, res_obj))) {
    COMMON_LOG(WDIAG, "fail to obj accuracy check", K(ret), K(src_obj));
  } else {
    COMMON_LOG(DEBUG, "end to cast obj for range", K(src_obj), K(obj_type), K(cs_type));
  }

  return ret;
}

} // end of common
} // end of oceanbase
