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

#ifndef OBPROXY_OB20_SESS_VERI_H
#define OBPROXY_OB20_SESS_VERI_H

#include "proxy/mysqllib/ob_mysql_common_define.h"
#include "lib/utility/ob_print_utils.h"


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define OB_SESS_INFO_VERI_BUF_MAX (100)       // currently used 94 max

class SessionInfoVerification {
public:
  SessionInfoVerification() : addr_(NULL), addr_len_(0), server_sess_id_(0), proxy_sess_id_(0) {}
  SessionInfoVerification(char *addr, const int64_t addr_len, const uint32_t server_sess_id,
                          const uint64_t proxy_sess_id)
    : addr_(addr), addr_len_(addr_len), server_sess_id_(server_sess_id), proxy_sess_id_(proxy_sess_id) {}
  ~SessionInfoVerification() {}

  bool is_valid() const;
  int serialize(char *buf, const int64_t len, int64_t &pos);
  
private:
  // send string on network only, it is not ended with \0, serialize format is the same as full link trace: 2,4,value
  // the address could be loaded as net::ObIpEndpoint / ObAddr
  char *addr_;
  int64_t addr_len_;
  uint32_t server_sess_id_;
  uint64_t proxy_sess_id_;
};

enum SessionInfoVerificationId {
  SESS_INFO_VERI_ADDR = 1,
  SESS_INFO_VERI_SERVER_SESS_ID = 2,
  SESS_INFO_VERI_PROXY_SESS_ID = 3,
  SESS_INFO_VERI_MAX_ID
};


} // proxy
} // obproxy
} // oceanbase

#endif
