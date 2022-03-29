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

#ifndef _OBSM_UTILS_H_
#define _OBSM_UTILS_H_

#include <inttypes.h>
#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/timezone/ob_timezone_info.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_field.h"
#include "common/ob_object.h"
#include "common/ob_accuracy.h"

namespace oceanbase
{
namespace common
{
class ObField;
class ObSMUtils {
public:
  /**
   *
   * @param [in] obj: need to serialize cell
   * @param [in] buf: output buf
   * @param [in] len: length of bug
   * @param [in,out] pos: buf position
   * @param [in] cell index for binary protocol
   *
   * @return if success return OB_SUCCESS, else return oceanbase error code
   */
  static int cell_str(
      char *buf, const int64_t len,
      const ObObj &obj,
      obmysql::MYSQL_PROTOCOL_TYPE type, int64_t &pos,
      int64_t cell_idx, char *bitmap,
      const ObTimeZoneInfo *tz_info,
      const ObField *field);

  static bool update_from_bitmap(ObObj &param, const char *bitmap, int64_t field_index);

  static int get_type_length(ObObjType ob_type, int64_t &length);

  static int get_mysql_type(ObObjType ob_type, obmysql::EMySQLFieldType &mysql_type,
                            uint16_t &flags, ObScale &num_decimals);

  static int get_ob_type(ObObjType &ob_type, obmysql::EMySQLFieldType mysql_type);
  static int to_ob_field(const obmysql::ObMySQLField &field, ObField &mfield);
};

} // end of namespace common
} // end of namespace oceanbase

#endif /* _OBSM_UTILS_H_ */
