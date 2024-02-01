/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "common/ob_object.h"
#include "common/ob_obj_cast.h"

namespace oceanbase
{
namespace common
{
class ObDatumCast 
{
public:
  static int common_scale_decimalint(const ObDecimalInt *decint, const int32_t int_bytes,
                                     const ObScale in_scale, const ObScale out_scale,
                                     const ObPrecision out_prec,
                                     const ObCastMode cast_mode, ObDecimalIntBuilder &val);

  static int align_decint_precision_unsafe(const ObDecimalInt *decint, const int32_t int_bytes,
                                           const int32_t expected_int_bytes,
                                           ObDecimalIntBuilder &res);

  inline static bool need_scale_decimalint(const ObScale in_scale,
                                           const ObPrecision in_precision,
                                           const ObScale out_scale,
                                           const ObPrecision out_precision) {
    bool ret = false;
    if (in_scale != out_scale) {
      ret = true;
    } else if (get_decimalint_type(in_precision) !=
               get_decimalint_type(out_precision)) {
      ret = true;
    }
    return !ret;
  }

  inline static bool need_scale_decimalint(const ObScale in_scale,
                                           const int32_t in_bytes,
                                           const ObScale out_scale,
                                           const int32_t out_bytes) {
    bool ret = false;
    if (in_scale != out_scale) {
      ret = true;
    } else if (in_bytes != out_bytes) {
      ret = true;
    }
    return ret;
  }
};

} //end namspace common
} //end namespace oceanbase