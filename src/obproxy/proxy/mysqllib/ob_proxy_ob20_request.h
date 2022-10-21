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

#ifndef OBPROXY_MYSQL_OB20_REQUEST_H
#define OBPROXY_MYSQL_OB20_REQUEST_H

#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObProxyObProto20Request {
public:
  ObProxyObProto20Request()
    : remain_payload_len_(0), ob20_request_received_done_(false)
  {
    ob20_header_.reset();
  }
  ~ObProxyObProto20Request() {}
  void reset()
  {
    remain_payload_len_ = 0;
    ob20_request_received_done_ = false;
    ob20_header_.reset();
  }

  // ob20 payload could contain more than one mysql packet, the total mysql packet len stored here
  int64_t remain_payload_len_;
  bool ob20_request_received_done_;
  Ob20ProtocolHeader ob20_header_;

  TO_STRING_KV(K_(remain_payload_len), K_(ob20_request_received_done), K_(ob20_header));
};

} // proxy
} // obproxy
} // oceanbase

#endif
