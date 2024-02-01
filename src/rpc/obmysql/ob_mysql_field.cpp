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

#include "rpc/obmysql/ob_mysql_field.h"

#include "rpc/obmysql/ob_mysql_util.h"

namespace oceanbase
{
namespace obmysql
{
ObMySQLField::ObMySQLField()
    : catalog_("def"),
      type_(OB_MYSQL_TYPE_NOT_DEFINED),
      flags_(0),
      default_value_(OB_MYSQL_TYPE_NOT_DEFINED),
      charsetnr_(0),
      length_(0)
{
}

int ObMySQLField::serialize_pro41(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  uint8_t num_decimals = static_cast<uint8_t>(accuracy_.get_scale());  //decimals_;

  if (OB_FAIL(ObMySQLUtil::store_str(buf, len, catalog_, pos))) {

    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize catalog failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, dname_.ptr(), dname_.length(), pos))) {

    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize db failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, tname_.ptr(), tname_.length(),
                                                           pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize tname failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, org_tname_.ptr(),
                                                           org_tname_.length(), pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize org_tname failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, cname_.ptr(), cname_.length(),
                                                           pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize cname failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, org_cname_.ptr(),
                                                           org_cname_.length(), pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize org_cname failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, 0xc, pos))) { // length of belows
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize 0xc failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int2(buf, len, charsetnr_, pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize charsetnr failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int4(buf, len, length_, pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize length failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, type_, pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize type failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int2(buf, len, flags_, pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize flags failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int1(buf, len, num_decimals, pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize num_decimals failed", K(ret));
    }
  } else if (OB_FAIL(ObMySQLUtil::store_int2(buf, len, 0, pos))) {
    if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret)) {
      LOG_WDIAG("serialize 0 failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_MYSQL_TYPE_COMPLEX == type_) {
    if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, type_info_.relation_name_.ptr(), type_info_.relation_name_.length(), pos))) {
      LOG_WDIAG("serialize relation_name_ failed", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_str_v(buf, len, type_info_.type_name_.ptr(), type_info_.type_name_.length(),pos))) {
      LOG_WDIAG("serialize type_name_ failed", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_length(buf, len, type_info_.version_, pos))) {
      LOG_WDIAG("serialize type_name_ failed", K(ret));
    } else { /* succ to write complex type */ }
  }

  return ret;
}

} // end of namespace obmysql
} // end of namespace oceanbase
