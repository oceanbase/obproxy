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

#define USING_LOG_PREFIX RPC_OBMYSQL
#include "ompk_prepare.h"
#include "lib/oblog/ob_log_module.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

namespace oceanbase
{
using namespace common;
namespace obmysql
{

int OMPKPrepare::decode()
{
  int ret = OB_SUCCESS;
  const char *pos = cdata_;
  //OB_ASSERT(NULL != cdata_);
  if (NULL != cdata_) {
    ObMySQLUtil::get_uint1(pos, status_);
    ObMySQLUtil::get_uint4(pos, statement_id_);
    ObMySQLUtil::get_uint2(pos, column_num_);
    ObMySQLUtil::get_uint2(pos, param_num_);
    ObMySQLUtil::get_uint1(pos, reserved_);
    ObMySQLUtil::get_uint2(pos, warning_count_);
    if (hdr_.len_ - (pos - cdata_) >= 5) {
      ObMySQLUtil::get_uint4(pos, extend_flag_);
      ObMySQLUtil::get_uint1(pos, has_result_set_);
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", K(ret), K(cdata_));
  }
  return ret;
}

int OMPKPrepare::serialize(char* buffer, int64_t length, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (NULL == buffer || 0 >= length || pos < 0 || length - pos < get_serialize_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buffer), K(length), K(pos), "need_size", get_serialize_size());
  } else {
    if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buffer, length, status_, pos))) {
      LOG_WARN("store failed", K(buffer), K(length), K_(status), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(buffer, length, statement_id_, pos))) {
      LOG_WARN("store failed", K(buffer), K(length), K_(statement_id), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, length, column_num_, pos))) {
      LOG_WARN("store failed", K(buffer), K(length), K_(column_num), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, length, param_num_, pos))) {
      LOG_WARN("store failed", K(buffer), K(length), K_(param_num), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, reserved_, pos))) {
      LOG_WARN("store failed", K(buffer), K(length), K_(reserved), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, length, warning_count_, pos))) {
      LOG_WARN("store failed", K(buffer), K(length), K_(warning_count), K(pos));
    }
  }
  return ret;
}

int64_t OMPKPrepare::get_serialize_size() const
{
  int64_t len = 0;
  len += 1;                 // status
  len += 4;                 // statement id
  len += 2;                 // column num
  len += 2;                 // param num
  len += 1;                 // reserved
  len += 2;                 // warning count
  return len;
}




} //end of obmysql
} //end of oceanbase
