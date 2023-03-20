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

#ifndef OBPROXY_RAW_MYSQL_CLIENT_H
#define OBPROXY_RAW_MYSQL_CLIENT_H

#include "lib/container/ob_iarray.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "iocore/net/ob_connection.h"
#include "iocore/eventsystem/ob_io_buffer.h"
#include "proxy/client/ob_client_utils.h"
#include "proxy/route/ob_route_struct.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObRawMysqlClientActor
{
public:
  ObRawMysqlClientActor();
  ~ObRawMysqlClientActor() { destroy(); }

  int init(ObClientRequestInfo &info);
  int sync_raw_execute(const char *sql, const int64_t timeout_ms, ObClientMysqlResp *&resp);
  void reset() { destroy(); }
  bool is_avail() { return is_avail_; }
  int set_addr(const common::ObAddr &addr);

private:
  void destroy();
  int connect(const common::ObAddr &addr, const int64_t timeout_ms);
  int send_handshake_response();
  int send_request(const char *sql);
  int read_response(const obmysql::ObMySQLCmd cmd);
  int write_request(const char *buf, int64_t buf_len);
  int write_request(event::ObIOBufferReader *request_reader);

private:
  bool is_inited_;
  bool is_avail_;
  ObClientRequestInfo *info_;
  ObClientMysqlResp *resp_;
  net::ObConnection con_;
  common::ObAddr addr_;

  event::ObMIOBuffer *request_buf_;
  event::ObIOBufferReader *request_reader_;

  DISALLOW_COPY_AND_ASSIGN(ObRawMysqlClientActor);
};

inline int ObRawMysqlClientActor::set_addr(const common::ObAddr &addr)
{
  int ret = common::OB_SUCCESS;
  if (!addr.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    addr_ = addr;
  }
  return ret;
}

class ObRawMysqlClient
{
public:
  ObRawMysqlClient() :  is_inited_(false), info_(), target_server_(), actor_(), mutex_() {}
  ~ObRawMysqlClient() { destroy(); }

  int init(const common::ObString &user_name,
           const common::ObString &password,
           const common::ObString &database,
           const common::ObString &cluster_name,
           const common::ObString &password1 = "");

  int sync_raw_execute(const char *sql, const int64_t timeout_ms, ObClientMysqlResp *&resp);

  int set_server_addr(const common::ObIArray<common::ObAddr> &addrs);
  int set_target_server(const common::ObIArray<ObProxyReplicaLocation> &replicas);
  ObClientRequestInfo &get_request_info() { return info_; }

  int disconnect();
  void destroy();

  DECLARE_TO_STRING;

private:
  static const int64_t DEFAULT_SERVER_ADDRS_COUNT = 3;
  bool is_inited_;
  ObClientRequestInfo info_;
  ObProxyReplicaLocation target_server_[DEFAULT_SERVER_ADDRS_COUNT];
  ObRawMysqlClientActor actor_;
  ObMutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObRawMysqlClient);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_RAW_MYSQL_CLIENT_H
