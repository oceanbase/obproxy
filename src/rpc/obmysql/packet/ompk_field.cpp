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
#include "ompk_field.h"
#include "lib/oblog/ob_log_module.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::obmysql;

OMPKField::OMPKField(ObMySQLField &field)
    : field_(field)
{}

int OMPKField::decode()
{
  int ret = OB_SUCCESS;
  //OB_ASSERT(NULL != cdata_);
  if (NULL != cdata_) {
    uint64_t len = 0;
    const char *pos = cdata_;
    const char *end = cdata_ + hdr_.len_;
    ObString catalog;
    if (OB_FAIL(assign_string(catalog, pos))) {
      LOG_WDIAG("fail to assign_string(catalog)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field_.dname_, pos))) {
      LOG_WDIAG("fail to assign_string(db)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field_.tname_, pos))) {
      LOG_WDIAG("fail to assign_string(table)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field_.org_tname_, pos))) {
      LOG_WDIAG("fail to assign_string(org_table)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field_.cname_, pos))) {
      LOG_WDIAG("fail to assign_string(name)", KP(pos), K(ret));
    } else if (OB_FAIL(assign_string(field_.org_cname_, pos))) {
      LOG_WDIAG("fail to assign_string(org_name)", KP(pos), K(ret));
    } else if (OB_FAIL(ObMySQLUtil::get_length(pos, len))) {
      LOG_WDIAG("fail to get length", K(ret));
    } else {
      uint16_t value = 0;
      ObMySQLUtil::get_uint2(pos, value);
      field_.charsetnr_ = value;

      uint32_t length_value = 0;
      ObMySQLUtil::get_uint4(pos, length_value);
      field_.length_ = length_value;

      uint8_t type_value = 0;
      ObMySQLUtil::get_uint1(pos, type_value);
      field_.type_ = (EMySQLFieldType)(type_value);

      value = 0;
      ObMySQLUtil::get_uint2(pos, value);
      field_.flags_ = value;

      uint8_t decimals_value = 0;
      ObMySQLUtil::get_uint1(pos, decimals_value);
      field_.accuracy_.set_scale(static_cast<ObScale>(decimals_value));
    }

    if (OB_SUCC(ret)) {
      if (pos > end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("pos must be <= end", KP(pos), KP(end), K(cdata_), K(ret));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("null input", K(ret), K(cdata_));
  }

  return ret;
}

int OMPKField::assign_string(ObString &str, const char *&pos)
{
  uint64_t len = 0;
  int ret = OB_SUCCESS;
  if (NULL == pos) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", KP(pos), K(ret));
  } else if (OB_FAIL(ObMySQLUtil::get_length(pos, len))) {
    LOG_WDIAG("fail to get length", K(ret));
  } else {
    str.assign_ptr(pos, static_cast<uint32_t>(len));
    pos += len;
  }

  return ret;
}

int OMPKField::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  return field_.serialize(buffer, len, pos);
}
