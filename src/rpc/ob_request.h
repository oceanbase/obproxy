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

#ifndef _OCEABASE_RPC_OB_REQUEST_H_
#define _OCEABASE_RPC_OB_REQUEST_H_

#include <arpa/inet.h>
#include "easy_io.h"
#include "lib/oblog/ob_log.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/queue/ob_link.h"
#include "rpc/ob_packet.h"
#include "rpc/obrpc/ob_rpc_packet.h"

namespace oceanbase
{
namespace rpc
{

using common::ObAddr;

class ObRequest: public common::ObLink
{
public:
  enum Type { OB_RPC, OB_MYSQL, OB_TASK };

public:
  explicit ObRequest(Type type)
      : type_(type), pkt_(NULL), ez_req_(NULL),
        auth_pkt_(false), recv_timestamp_(0), enqueue_timestamp_(0)
  {}
  virtual ~ObRequest() {}

  Type get_type() const { return type_; }
  void set_type(const Type &type) { type_ = type; }

  inline void set_packet(const ObPacket *pkt);
  inline const ObPacket &get_packet() const;

  inline easy_request_t *get_request() const;
  inline void set_request(easy_request_t *r);
  inline void set_request_rtcode(int8_t rt) const;

  inline int64_t get_send_timestamp() const;
  inline int64_t get_receive_timestamp() const;
  inline void set_receive_timestamp(int64_t recv_timestamp);
  inline void set_enqueue_timestamp(int64_t enqueue_timestamp);
  inline int64_t get_enqueue_timestamp() const;
  inline ObAddr get_peer() const;

  inline void set_auth_pkt();
  inline bool is_auth_pkt() const;

  inline void set_session(void *session);
  inline void *get_session() const;

  inline void disconnect() const;
  inline easy_request_t *get_ez_req() const;
  char *easy_alloc(int64_t size) const;

  VIRTUAL_TO_STRING_KV("packet", pkt_, "type", type_, "is_auth", auth_pkt_);

protected:
  Type type_;
  const ObPacket *pkt_; // set in rpc handler
  easy_request_t *ez_req_; // set in ObRequest new
  bool auth_pkt_;
  int64_t recv_timestamp_;
  int64_t enqueue_timestamp_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRequest);
}; // end of class ObRequest

void ObRequest::set_packet(const ObPacket *pkt)
{
  pkt_ = pkt;
}

const ObPacket &ObRequest::get_packet() const
{
  return *pkt_;
}

easy_request_t *ObRequest::get_request() const
{
  return ez_req_;
}

void ObRequest::set_request(easy_request_t *r)
{
  ez_req_ = r;
}

void ObRequest::set_request_rtcode(int8_t rt) const
{
  if (OB_ISNULL(ez_req_)) {
    RPC_LOG(EDIAG, "invalid argument", K(ez_req_));
  } else {
    ez_req_->retcode = rt;
  }
}

int64_t ObRequest::get_receive_timestamp() const
{
  return recv_timestamp_;
}

void ObRequest::set_receive_timestamp(int64_t recv_timestamp)
{
  recv_timestamp_ = recv_timestamp;
}

int64_t ObRequest::get_enqueue_timestamp() const
{
  return enqueue_timestamp_;
}

void ObRequest::set_enqueue_timestamp(int64_t enqueue_timestamp)
{
  enqueue_timestamp_ = enqueue_timestamp;
}

ObAddr ObRequest::get_peer() const
{
  ObAddr addr;
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms)
      || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(EDIAG, "invalid argument", K(ez_req_));
  } else {
    easy_addr_t &ez = ez_req_->ms->c->addr;
    addr.set_ipv4_addr(ntohl(ez.u.addr), (ntohs)(ez.port));
  }
  return addr;
}

void ObRequest::set_auth_pkt()
{
  auth_pkt_ = true;
}

bool ObRequest::is_auth_pkt() const
{
  return auth_pkt_;
}

void ObRequest::set_session(void *session)
{
  if (OB_ISNULL(session)
      || OB_ISNULL(ez_req_)
      || OB_ISNULL(ez_req_->ms)
      || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(EDIAG, "invalid argument", K(ez_req_));
  } else {
    ez_req_->ms->c->user_data = session;
  }
}

void *ObRequest::get_session() const
{
  void *session = NULL;
  if (OB_ISNULL(ez_req_)
      || OB_ISNULL(ez_req_->ms)
      || OB_ISNULL(ez_req_->ms->c)) {
    RPC_LOG(EDIAG, "invalid argument", K(ez_req_));
  } else {
    session = ez_req_->ms->c->user_data;
  }
  return session;
}

int64_t ObRequest::get_send_timestamp() const
{
  int64_t ts = 0;
  if (type_ == OB_RPC && NULL != pkt_) {
    ts = reinterpret_cast<const obrpc::ObRpcPacket*>(pkt_)->get_timestamp();
  }
  return ts;
}

void ObRequest::disconnect() const
{
  if (OB_ISNULL(ez_req_) || OB_ISNULL(ez_req_->ms)) {
    RPC_LOG(EDIAG, "invalid argument", K(ez_req_));
  } else {
    easy_connection_destroy_dispatch(ez_req_->ms->c);
  }
}

easy_request_t *ObRequest::get_ez_req() const
{
  return ez_req_;
}

} // end of namespace rp
} // end of namespace oceanbase


#endif /* _OCEABASE_RPC_OB_REQUEST_H_ */
