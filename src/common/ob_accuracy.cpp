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

#include "common/ob_accuracy.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase
{
namespace common
{

using namespace number;

const ObAccuracy ObAccuracy::DDL_DEFAULT_ACCURACY[ObMaxType] = {
  ObAccuracy(),         // null.
  ObAccuracy(4, 0),     // int8.
  ObAccuracy(6, 0),     // int16.
  ObAccuracy(9, 0),     // int24.
  ObAccuracy(11, 0),    // int32.
  ObAccuracy(20, 0),    // int64.
  ObAccuracy(3, 0),     // uint8.
  ObAccuracy(5, 0),     // uint16.
  ObAccuracy(8, 0),     // uint24.
  ObAccuracy(10, 0),    // uint32.
  ObAccuracy(20, 0),    // uint64.
  ObAccuracy(),         // float.
  ObAccuracy(),         // double.
  ObAccuracy(),         // ufloat.
  ObAccuracy(),         // udouble.
  ObAccuracy(10, 0),    // number.
  ObAccuracy(10, 0),    // unumber.
  ObAccuracy(19, 6),    // datetime.
  ObAccuracy(19, 6),    // timestamp.
  ObAccuracy(10, 0),    // date.
  ObAccuracy(10, 6),    // time. -838:59:59' to '838:59:59
  ObAccuracy(4, 0),     // year.
  ObAccuracy(),         // varchar.
  ObAccuracy(1),        // char.
  ObAccuracy(),         // hex_string.
  ObAccuracy(),         // extend.
  ObAccuracy()          // unknown.
};

const ObAccuracy ObAccuracy::MAX_ACCURACY[ObMaxType] = {
  ObAccuracy(),         // null.
  ObAccuracy(4, 0),     // int8.
  ObAccuracy(6, 0),     // int16.
  ObAccuracy(9, 0),     // int24.
  ObAccuracy(11, 0),    // int32.
  ObAccuracy(20, 0),    // int64.
  ObAccuracy(3, 0),     // uint8.
  ObAccuracy(5, 0),     // uint16.
  ObAccuracy(8, 0),     // uint24.
  ObAccuracy(10, 0),    // uint32.
  ObAccuracy(20, 0),    // uint64.
  ObAccuracy(255, 30),  // float.
  ObAccuracy(255, 30),  // double.
  ObAccuracy(255, 30),  // ufloat.
  ObAccuracy(255, 30),  // udouble.
  ObAccuracy(OB_MAX_DECIMAL_PRECISION, OB_MAX_DECIMAL_SCALE),    // number.
  ObAccuracy(OB_MAX_DECIMAL_PRECISION, OB_MAX_DECIMAL_SCALE),    // unumber.
  ObAccuracy(19, 6),    // datetime.
  ObAccuracy(19, 6),    // timestamp.
  ObAccuracy(10, 0),    // date.
  ObAccuracy(10, 6),    // time. -838:59:59' to '838:59:59
  ObAccuracy(4, 0),     // year.
  ObAccuracy(),         // varchar.
  ObAccuracy(1),        // char.
  ObAccuracy(),         // hex_string.
  ObAccuracy(),         // extend.
  ObAccuracy()          // unknown.
};

const ObAccuracy ObAccuracy::DML_DEFAULT_ACCURACY[ObMaxType] = {
  ObAccuracy(),         // null.
  ObAccuracy(),         // int8.
  ObAccuracy(),         // int16.
  ObAccuracy(),         // int24.
  ObAccuracy(),         // int32.
  ObAccuracy(),         // int64.
  ObAccuracy(),         // uint8.
  ObAccuracy(),         // uint16.
  ObAccuracy(),         // uint24.
  ObAccuracy(),         // uint32.
  ObAccuracy(),         // uint64.
  ObAccuracy(),         // float.
  ObAccuracy(),         // double.
  ObAccuracy(),         // ufloat.
  ObAccuracy(),         // udouble.
  ObAccuracy(),         // number.
  ObAccuracy(),         // unumber.
  ObAccuracy(0, 6),     // datetime.
  ObAccuracy(0, 6),     // timestamp.
  ObAccuracy(0, 0),     // date.
  ObAccuracy(0, 6),     // time.
  ObAccuracy(4, 0),     // year.
  ObAccuracy(),         // varchar.
  ObAccuracy(),         // char.
  ObAccuracy(),         // hex_string.
  ObAccuracy(),         // extend.
  ObAccuracy()          // unknown.
};

OB_SERIALIZE_MEMBER_SIMPLE(ObAccuracy, accuracy_);

}
}
