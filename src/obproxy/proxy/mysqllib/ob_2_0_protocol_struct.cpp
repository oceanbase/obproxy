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

#define USING_LOG_PREFIX PROXY

#include "proxy/mysqllib/ob_2_0_protocol_struct.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{


int Ob20ExtraInfo::get_next_sess_info(common::ObString &sess_info)
{
  sess_info.reset();
  int ret = common::OB_SUCCESS;
  uint64_t length = sess_info_length_.at(sess_info_cur_idx_);
  uint64_t buf_length = sess_info_buf_.length(); 
  if (sess_info_offset_ + length > buf_length) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected length of sess info buffer", K(ret), K(sess_info_offset_), K(length));
  } else {
    sess_info.assign_ptr(sess_info_buf_.ptr() + sess_info_offset_, static_cast<int32_t>(length));
    sess_info_offset_ += length;
    sess_info_cur_idx_ ++;
  }
  return ret;
}

int Ob20ExtraInfo::add_sess_info_buf(const char *value, const int64_t len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == value || 0 >= len)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(value), K(len), K(ret));
  } else if (OB_FAIL(sess_info_buf_.append(value, len))) {
    LOG_WDIAG("fail to append sess info buf", K(value), K(len), K(ret));
  } else if (OB_FAIL(sess_info_length_.push_back(len))) {
    LOG_WDIAG("fail to record sess info buf length", K(ret));
  } else {
    is_exist_sess_info_ = true;
    sess_info_count_++;
  }
  return ret;
}

int Ob20ExtraInfo::decode_feedback_proxy_info(const char * buf, const int64_t len)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == buf || 0 >= len)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(buf), K(len), K(ret));
  } else {
    int64_t pos = 0;
    uint16_t sub_type = 0;
    uint32_t value_len = 0;
    ObString value;
    while (OB_SUCC(ret) && pos < len) {
      if (len - pos < ObProto20Utils::OB_20_PROTOCOL_TYPE_LEN + ObProto20Utils::OB_20_PROTOCOL_VAL_LENGTH_LEN) {
        ret = OB_SIZE_OVERFLOW;
        LOG_EDIAG("size overflow", K(pos), K(len), K(value_len), K(ret));
      } else {
        const char *tmp_buf = buf + pos;
        obmysql::ObMySQLUtil::get_uint2(tmp_buf, sub_type);
        obmysql::ObMySQLUtil::get_uint4(tmp_buf, value_len);
        pos += ObProto20Utils::OB_20_PROTOCOL_TYPE_LEN + ObProto20Utils::OB_20_PROTOCOL_VAL_LENGTH_LEN;
        if (len - pos < value_len) {
          ret = OB_SIZE_OVERFLOW;
          LOG_EDIAG("size overflow", K(pos), K(len), K(value_len), K(ret));
        } else {
          value.assign_ptr(buf + pos, static_cast<int32_t>(value_len));
          LOG_DEBUG("get feedback proxy info", K(sub_type), K(value));
          switch (sub_type) {
            case IS_LOCK_SESSION : {
              feedback_proxy_info_.is_lock_session_ = (0 == value.compare("1"));
            }
            default :
              break; // maybe newer observer, ignore
          }
          pos += value_len;
        }
      }
    }

    if (pos != len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_EDIAG("size overflow", K(pos), K(len), K(value_len), K(ret));
    }
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
