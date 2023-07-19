/**
 * Copyright (c) 2023 OceanBase
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

#include "rpc/obmysql/ob_mysql_util.h"
#include "lib/utility/ob_2_0_sess_veri.h"
#include "lib/utility/ob_2_0_full_link_trace_util.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

bool SessionInfoVerification::is_valid() const
{
  return (addr_ != NULL
          && addr_len_ > 0
          && server_sess_id_ != 0
          && proxy_sess_id_ != 0);
}

// total serialize max size: 2+4+64 + 2+4+4 + 2+4+8 = 94
int SessionInfoVerification::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sess info veri", K(ret), K(addr_len_), K(server_sess_id_), K(proxy_sess_id_));
  } else {
    if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_str(buf, len, pos,
                                                      addr_, static_cast<int32_t>(addr_len_),
                                                      SESS_INFO_VERI_ADDR))) {
      LOG_WARN("fail to store addr str", K(ret), K(addr_));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int4(buf, len, pos,
                                                              static_cast<int32_t>(server_sess_id_),
                                                              SESS_INFO_VERI_SERVER_SESS_ID))) {
      LOG_WARN("fail to store server sess id", K(ret));
    } else if (OB_FAIL(Ob20FullLinkTraceTransUtil::store_int8(buf, len, pos,
                                                              static_cast<int64_t>(proxy_sess_id_),
                                                              SESS_INFO_VERI_PROXY_SESS_ID))) {
      LOG_WARN("fail to store proxy sess id");
    } else {
      LOG_DEBUG("succ to seri sess info veri", K(pos), K(len));
    }
  }

  return ret;
}


} // proxy
} // obproxy
} // oceanbase

