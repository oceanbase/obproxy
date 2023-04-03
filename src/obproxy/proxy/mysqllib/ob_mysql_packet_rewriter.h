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

#ifndef OBPROXY_MYSQL_PACKET_REWRITER_H
#define OBPROXY_MYSQL_PACKET_REWRITER_H
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace obmysql
{
class OMPKOK;
class OMPKHandshakeResponse;
union ObMySQLCapabilityFlags;
}
namespace obproxy
{
namespace event
{
class ObIOBufferReader;
}
namespace proxy
{
class ObMysqlAuthRequest;
class ObClientSessionInfo;

static const int64_t OB_MAX_UINT64_BUF_LEN = 22; // string length of max uint64_t(2**64 - 1)

struct ObHandshakeResponseParam
{
public:
  ObHandshakeResponseParam() { MEMSET(this, 0, sizeof(*this)); }
  ~ObHandshakeResponseParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  bool is_cluster_name_valid() const { return !cluster_name_.empty(); }
  bool is_client_ip_valid() const { return 0 != STRLEN(client_ip_buf_); }
  bool is_proxy_scramble_valid() const { return !proxy_scramble_.empty(); }
  bool is_cluster_id_valid() const { return OB_DEFAULT_CLUSTER_ID != cluster_id_; }
  int write_conn_id_buf(const uint32_t conn_id);
  int write_proxy_version_buf();
  int write_proxy_conn_id_buf(const uint64_t proxy_sessid);
  int write_global_vars_version_buf(const int64_t version);
  int write_capability_buf(const uint64_t cap);
  int write_proxy_scramble(const common::ObString &proxy_scramble,
                           const common::ObString &server_scramble);
  int write_client_addr_buf(const common::ObAddr &addr);
  int write_cluster_id_buf(const int64_t cluster_id);

  static const int64_t OB_MAX_UINT32_BUF_LEN = 11; // string length of max uint32_t(2**32 - 1)
  static const int64_t OB_MAX_VERSION_BUF_LEN = 22; // string length of (xxx.xxx.xxx.xxx.xxx)

  bool is_saved_login_;
  bool use_compress_;
  bool use_ob_protocol_v2_;
  int64_t cluster_id_;
  bool use_ssl_;
  bool enable_client_ip_checkout_;
  common::ObString cluster_name_;
  common::ObString proxy_scramble_;

  char conn_id_buf_[OB_MAX_UINT32_BUF_LEN];
  char proxy_version_buf_[OB_MAX_VERSION_BUF_LEN];
  char proxy_conn_id_buf_[OB_MAX_UINT64_BUF_LEN];
  char global_vars_version_buf_[OB_MAX_UINT64_BUF_LEN];
  char cap_buf_[OB_MAX_UINT64_BUF_LEN];
  char proxy_scramble_buf_[obmysql::OMPKHandshake::SCRAMBLE_TOTAL_SIZE];
  char client_ip_buf_[MAX_IP_ADDR_LENGTH];
  char cluster_id_buf_[OB_MAX_UINT64_BUF_LEN];

private:
   DISALLOW_COPY_AND_ASSIGN(ObHandshakeResponseParam);
};

class ObMysqlPacketRewriter
{
public:
  static int rewrite_ok_packet(const obmysql::OMPKOK &src_ok,
                               const obmysql::ObMySQLCapabilityFlags &des_cap,
                               obmysql::OMPKOK &des_ok,
                               ObClientSessionInfo &client_info,
                               char *cap_buf,
                               const int64_t cap_buf_max_len,
                               const bool is_auth_request);

  static int rewrite_handshake_response_packet(ObMysqlAuthRequest &orig_auth_req,
                                               ObHandshakeResponseParam &param,
                                               obmysql::OMPKHandshakeResponse &tg_hsr);

  static int add_connect_attr(const char *key,
                              const char *value,
                              obmysql::OMPKHandshakeResponse &tg_hsr);

  static int add_connect_attr(const char *key,
                              const common::ObString &value,
                              obmysql::OMPKHandshakeResponse &tg_hsr);
  // add others, like rewrite eof, rewrite error etc.
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OB_MYSQL_PACKET_REWRITER_H */
