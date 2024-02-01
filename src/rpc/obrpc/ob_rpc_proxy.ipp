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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_IPP_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_IPP_

#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/serialization.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_trace_log.h"
#include "common/ob_debug_sync.h"
#include "rpc/obrpc/ob_rpc_stat.h"

namespace oceanbase
{
namespace obrpc
{

template <ObRpcPacketCode pcode>
void ObRpcProxy::AsyncCB<pcode>::do_first()
{
  using namespace oceanbase::common;
  rpc::RpcStatPiece piece;
  piece.async_ = true;
  piece.size_ = payload_;
  piece.time_ = ObTimeUtility::current_time() - send_ts_;
  if (NULL != req_) {
    if (NULL == req_->ipacket) {
      piece.is_timeout_ = true;
      piece.failed_ = true;
    }
  }
  RPC_STAT(pcode, piece);
}

template <ObRpcPacketCode pcode>
bool SSHandle<pcode>::has_more() const
{
  return has_more_;
}

template <ObRpcPacketCode pcode>
int SSHandle<pcode>::get_more(typename ObRpcProxy::ObRpc<pcode>::Response &result)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  static const int64_t PAYLOAD_SIZE = 0;

  int ret = common::OB_SUCCESS;

  ObReqTransport::Request<ObRpcPacket>  req;
  ObReqTransport::Result<ObRpcPacket>   r;

  if (OB_ISNULL(transport_)) {
    ret = OB_NOT_INIT;
    RPC_OBRPC_LOG(WDIAG, "transport_ is NULL", K(ret));
  } else if (OB_FAIL(transport_->create_request(
      req, dst_, PAYLOAD_SIZE, proxy_.timeout()))) {
    RPC_OBRPC_LOG(WDIAG, "create request fail", K(ret));
  } else if (NULL == req.pkt() || NULL == req.buf()) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WDIAG, "request packet is NULL", K(ret));
  } else if (OB_FAIL(proxy_.init_pkt(req.pkt(), pcode, opts_))) {
    RPC_OBRPC_LOG(WDIAG, "Init packet error", K(ret));
  } else {
    req.pkt()->set_stream_next();
    req.pkt()->set_session_id(sessid_);

    if (OB_FAIL(transport_->send(req, r))) {
      RPC_OBRPC_LOG(WDIAG, "send request fail", K(ret));
    } else if (OB_ISNULL(r.pkt()->get_cdata())) {
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(WDIAG, "cdata should not be NULL", K(ret));
    } else {
      int64_t       pos = 0;
      const char   *buf = r.pkt()->get_cdata();
      const int64_t len = r.pkt()->get_clen();

      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WDIAG, "deserialize result code fail", K(ret));
      } else if (rcode_.rcode_ != common::OB_SUCCESS) {
        ret = rcode_.rcode_;
        RPC_OBRPC_LOG(WDIAG, "execute rpc fail", K(ret));
      } else if (OB_FAIL(common::serialization::decode(buf, len, pos, result))) {
        RPC_OBRPC_LOG(WDIAG, "deserialize result fail", K(ret));
      } else {
        has_more_ = r.pkt()->is_stream_next();
      }
    }
  }

  return ret;
}

template <ObRpcPacketCode pcode>
int SSHandle<pcode>::abort()
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  static const int64_t PAYLOAD_SIZE = 0;

  int ret = common::OB_SUCCESS;

  ObReqTransport::Request<ObRpcPacket>  req;
  ObReqTransport::Result<ObRpcPacket>   r;

  if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(EDIAG, "transport_ should not be NULL", K(ret));
  } else if (OB_FAIL(transport_->create_request(
      req, dst_, PAYLOAD_SIZE, proxy_.timeout()))) {
    RPC_OBRPC_LOG(WDIAG, "create request fail", K(ret));
  } else if (NULL == req.pkt() || NULL == req.buf()) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WDIAG, "request packet is NULL", K(ret));
  } else if (OB_FAIL(proxy_.init_pkt(req.pkt(), pcode, opts_))) {
    RPC_OBRPC_LOG(WDIAG, "Fail to init packet", K(ret));
  } else {
    req.pkt()->set_stream_last();
    req.pkt()->set_session_id(sessid_);

    if (OB_FAIL(transport_->send(req, r))) {
      RPC_OBRPC_LOG(WDIAG, "send request fail", K(ret));
    } else if (OB_ISNULL(r.pkt()->get_cdata())) {
      ret = OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(WDIAG, "cdata should not be NULL", K(ret));
    } else {
      int64_t       pos = 0;
      const char   *buf = r.pkt()->get_cdata();
      const int64_t len = r.pkt()->get_clen();

      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WDIAG, "deserialize result code fail", K(ret));
      } else if (rcode_.rcode_ != common::OB_SUCCESS) {
        ret = rcode_.rcode_;
        RPC_OBRPC_LOG(WDIAG, "execute rpc fail", K(ret));
      } else {
        //do nothing
      }
      has_more_ = false;
    }
  }

  return ret;
}

template <ObRpcPacketCode pcode>
const ObRpcResultCode &SSHandle<pcode>::get_result_code() const
{
  return rcode_;
}

template <ObRpcPacketCode pcode>
int ObRpcProxy::AsyncCB<pcode>::decode(void *pkt)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int           ret   = common::OB_SUCCESS;

  if (OB_ISNULL(pkt)) {
    ret = OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(WDIAG, "pkt should not be NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    ObRpcPacket  *rpkt  = reinterpret_cast<ObRpcPacket*>(pkt);
    const char   *buf   = rpkt->get_cdata();
    const int64_t len   = rpkt->get_clen();
    int64_t       pos   = 0;

    if (OB_FAIL(rpkt->verify_checksum())) {
      RPC_OBRPC_LOG(EDIAG, "verify checksum fail", K(*rpkt), K(ret));
    } else if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
      RPC_OBRPC_LOG(WDIAG, "decode result code fail", K(*rpkt), K(ret));
    } else if (rcode_.rcode_ != common::OB_SUCCESS) {
      // RPC_OBRPC_LOG(WDIAG, "execute rpc fail", K_(rcode));
    } else if (OB_FAIL(result_.deserialize(buf, len, pos))) {
      RPC_OBRPC_LOG(WDIAG, "decode packet fail", K(ret));
    } else {
      //do nothing
    }
  }

  return ret;
}

template <typename Input, typename Out>
int ObRpcProxy::rpc_call(ObRpcPacketCode pcode, const Input &args,
                         Out &result, Handle *handle, const ObRpcOpts &opts)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int ret = common::OB_SUCCESS;

  const int64_t start_ts = ObTimeUtility::current_time();
  rpc::RpcStatPiece piece;

  if (!init_) {
    ret = common::OB_NOT_INIT;
    RPC_OBRPC_LOG(WDIAG, "Rpc proxy not inited", K(ret));
  } else if (!active_) {
    ret = common::OB_INACTIVE_RPC_PROXY;
    RPC_OBRPC_LOG(WDIAG, "Rpc proxy inactive", K(ret));
  } else {
    //do nothing
  }

  int64_t pos = 0;
  const int64_t payload = common::serialization::encoded_length(args)
      + GDS.rpc_spread_actions().get_serialize_size();
  ObReqTransport::Request<ObRpcPacket> req;

  if (OB_FAIL(ret)) {
  } else if (payload > OB_MAX_RPC_PACKET_LENGTH) {
    ret = common::OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(WDIAG, "obrpc packet payload execced its limit",
            K(payload), "limit", OB_MAX_RPC_PACKET_LENGTH,
            K(ret));
  } else if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WDIAG, "transport_ should not be NULL", K(ret));
  } else if (OB_FAIL(transport_->create_request(
                     req, dst_, payload, timeout_))) {
    RPC_OBRPC_LOG(WDIAG, "create request fail", K(ret));
  } else if (NULL == req.pkt() || NULL == req.buf()) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WDIAG, "request packet is NULL", K(req), K(ret));
  } else if (OB_FAIL(common::serialization::encode(
                     req.buf(), payload, pos, args))) {
    RPC_OBRPC_LOG(WDIAG, "serialize argument fail", K(pos), K(payload), K(ret));
  } else if (OB_FAIL(common::serialization::encode(
      req.buf(), payload, pos, GDS.rpc_spread_actions()))) {
    RPC_OBRPC_LOG(WDIAG, "serialize debug sync actions fail", K(pos), K(payload), K(ret));
  } else if (OB_FAIL(init_pkt(req.pkt(), pcode, opts))) {
    RPC_OBRPC_LOG(WDIAG, "Init packet error", K(ret));
  } else {
    ObReqTransport::Result<ObRpcPacket> r;
    if (OB_FAIL(send_request(req, r))) {
      RPC_OBRPC_LOG(WDIAG, "send rpc request fail fail", K(pcode));
    } else {
      const char *buf = r.pkt()->get_cdata();
      int64_t     len = r.pkt()->get_clen();
      int64_t     pos = 0;

      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WDIAG, "deserialize result code fail", K(ret));
      } else {
        int wb_ret = common::OB_SUCCESS;
        if (rcode_.rcode_ != common::OB_SUCCESS) {
          ret = rcode_.rcode_;
          RPC_OBRPC_LOG(WDIAG, "execute rpc fail", K(ret));
        } else if (OB_FAIL(common::serialization::decode(buf, len, pos, result))) {
          RPC_OBRPC_LOG(WDIAG, "deserialize result fail", K(ret));
        } else {
          ret = rcode_.rcode_;
        }

        if (common::OB_SUCCESS == ret && NULL != handle) {
          handle->has_more_  = r.pkt()->is_stream_next();
          handle->dst_       = dst_;
          handle->sessid_    = r.pkt()->get_session_id();
          handle->opts_      = opts;
          handle->transport_ = transport_;
          handle->proxy_     = *this;
        }
        if (common::OB_SUCCESS != (wb_ret = log_user_error_and_warn(rcode_))) {
          RPC_OBRPC_LOG(WDIAG, "fail to log user error and warn", K(ret), K(wb_ret), K((rcode_)));
        }
      }
    }
  }

  piece.size_ = payload;
  piece.time_ = ObTimeUtility::current_time() - start_ts;
  if (OB_FAIL(ret)) {
    piece.failed_ = true;
    if (common::OB_TIMEOUT == ret) {
      piece.is_timeout_ = true;
    }
  }
  RPC_STAT(pcode, piece);

  return ret;
}

template <typename Input>
int ObRpcProxy::rpc_call(ObRpcPacketCode pcode, const Input &args,
                         Handle *handle, const ObRpcOpts &opts)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int ret = common::OB_SUCCESS;

  const int64_t start_ts = ObTimeUtility::current_time();
  rpc::RpcStatPiece piece;

 if (!init_) {
    ret = common::OB_NOT_INIT;
  } else if (!active_) {
    ret = common::OB_INACTIVE_RPC_PROXY;
  }

  int64_t pos = 0;
  const int64_t payload = args.get_serialize_size()
      + GDS.rpc_spread_actions().get_serialize_size();
  ObReqTransport::Request<ObRpcPacket> req;

  if (OB_FAIL(ret)) {
  } else if (payload > OB_MAX_RPC_PACKET_LENGTH) {
    ret = common::OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(WDIAG, "obrpc packet payload execced its limit",
                  K(ret), K(payload), "limit", OB_MAX_RPC_PACKET_LENGTH);
  } else if (OB_FAIL(transport_->create_request(req, dst_, payload, timeout_))) {
    RPC_OBRPC_LOG(WDIAG, "create request fail", K(ret));
  } else if (NULL == req.pkt()) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WDIAG, "request packet is NULL", K(ret));
  } else if (OB_FAIL(args.serialize(req.buf(), payload, pos))) {
    RPC_OBRPC_LOG(WDIAG, "serialize argument fail", K(ret));
  } else if (OB_FAIL(common::serialization::encode(
      req.buf(), payload, pos, GDS.rpc_spread_actions()))) {
    RPC_OBRPC_LOG(WDIAG, "serialize debug sync actions fail", K(ret), K(pos), K(payload));
  } else if (OB_FAIL(init_pkt(req.pkt(), pcode, opts))) {
    RPC_OBRPC_LOG(WDIAG, "Init packet error", K(ret));
  } else {
    ObReqTransport::Result<ObRpcPacket> r;
    if (OB_FAIL(send_request(req, r))) {
      RPC_OBRPC_LOG(WDIAG, "send rpc request fail fail", K(ret), K(pcode));
    } else {
      const char *buf = r.pkt()->get_cdata();
      int64_t     len = r.pkt()->get_clen();
      int64_t     pos = 0;

      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WDIAG, "deserialize result code fail", K(ret));
      } else {
        int wb_ret = common::OB_SUCCESS;
        ret = rcode_.rcode_;
        if (common::OB_SUCCESS == ret && NULL != handle) {
          handle->has_more_  = r.pkt()->is_stream_next();
          handle->dst_       = dst_;
          handle->sessid_    = r.pkt()->get_session_id();
          handle->opts_      = opts;
          handle->transport_ = transport_;
          handle->proxy_     = *this;
        }
        if (common::OB_SUCCESS != (wb_ret = log_user_error_and_warn(rcode_))) {
          RPC_OBRPC_LOG(WDIAG, "fail to log user error and warn", K(ret), K(wb_ret), K((rcode_)));
        }
      }
    }
  }

  piece.size_ = payload;
  piece.time_ = ObTimeUtility::current_time() - start_ts;
  if (OB_FAIL(ret)) {
    piece.failed_ = true;
    if (common::OB_TIMEOUT == ret) {
      piece.is_timeout_ = true;
    }
  }
  RPC_STAT(pcode, piece);

  return ret;
}

template <typename Output>
int ObRpcProxy::rpc_call(ObRpcPacketCode pcode, Output &result,
                         Handle *handle, const ObRpcOpts &opts)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  static const int64_t PAYLOAD_SIZE = 0;
  int ret = common::OB_SUCCESS;

  const int64_t start_ts = ObTimeUtility::current_time();
  rpc::RpcStatPiece piece;

  if (!init_) {
    ret = common::OB_NOT_INIT;
  } else if (!active_) {
    ret = common::OB_INACTIVE_RPC_PROXY;
  } else {
    //do nothing
  }

  ObReqTransport::Request<ObRpcPacket> req;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(transport_->create_request(
                        req, dst_, PAYLOAD_SIZE, timeout_))) {
    RPC_OBRPC_LOG(WDIAG, "create request fail", K(ret));
  } else if (NULL == req.pkt()) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    RPC_OBRPC_LOG(WDIAG, "request packet is NULL", K(ret));
  } else if (OB_FAIL(init_pkt(req.pkt(), pcode, opts))) {
    RPC_OBRPC_LOG(WDIAG, "Init packet error", K(ret));
  } else {
    ObReqTransport::Result<ObRpcPacket> r;
    if (OB_FAIL(send_request(req, r))) {
      RPC_OBRPC_LOG(WDIAG, "send rpc request fail fail", K(pcode));
    } else {
      rpc::RpcStatPiece piece;
      piece.size_ = 0;
      piece.time_ = ObTimeUtility::current_time() - req.pkt()->get_timestamp();
      RPC_STAT(pcode, piece);

      const char *buf = r.pkt()->get_cdata();
      int64_t len     = r.pkt()->get_clen();
      int64_t pos     = 0;

      if (OB_FAIL(rcode_.deserialize(buf, len, pos))) {
        RPC_OBRPC_LOG(WDIAG, "deserialize result code fail", K(ret));
      } else {
        int wb_ret = common::OB_SUCCESS;
        if (rcode_.rcode_ != common::OB_SUCCESS) {
          ret = rcode_.rcode_;
          RPC_OBRPC_LOG(WDIAG, "execute rpc fail", K(ret));
        } else if (OB_FAIL(common::serialization::decode(buf, len, pos, result))) {
          RPC_OBRPC_LOG(WDIAG, "deserialize result fail", K(ret));
        } else {
          ret = rcode_.rcode_;
        }

        if (OB_SUCC(ret) && NULL != handle) {
          handle->has_more_  = r.pkt()->is_stream_next();
          handle->dst_       = dst_;
          handle->sessid_    = r.pkt()->get_session_id();
          handle->opts_      = opts;
          handle->transport_ = transport_;
          handle->proxy_     = *this;
        }
        if (common::OB_SUCCESS != (wb_ret = log_user_error_and_warn(rcode_))) {
          RPC_OBRPC_LOG(WDIAG, "fail to log user error and warn", K(ret), K(wb_ret), K((rcode_)));
        }
      }
    }
  }

  piece.size_ = PAYLOAD_SIZE;
  piece.time_ = ObTimeUtility::current_time() - start_ts;
  if (OB_FAIL(ret)) {
    piece.failed_ = true;
    if (common::OB_TIMEOUT == ret) {
      piece.is_timeout_ = true;
    }
  }
  RPC_STAT(pcode, piece);

  return ret;
}

template <ObRpcPacketCode pcode>
int ObRpcProxy::rpc_post(const typename ObRpc<pcode>::Request &args,
                         AsyncCB<pcode> *cb, const ObRpcOpts &opts)
{
  using namespace oceanbase::common;
  using namespace rpc::frame;
  int ret = common::OB_SUCCESS;

  common::ObTimeGuard timeguard("rpc post", 10 * 1000);
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!init_) {
    ret = common::OB_NOT_INIT;
    RPC_OBRPC_LOG(EDIAG, "rpc not inited", K(ret));
  } else if (!active_) {
    ret = common::OB_INACTIVE_RPC_PROXY;
    RPC_OBRPC_LOG(EDIAG, "rpc is inactive", K(ret));
  }

  ObReqTransport::Request<ObRpcPacket> req;
  int64_t pos = 0;
  const int64_t payload = args.get_serialize_size()
      + GDS.rpc_spread_actions().get_serialize_size();

  if (OB_FAIL(ret)) {
  } else if (payload > OB_MAX_RPC_PACKET_LENGTH) {
    ret = common::OB_RPC_PACKET_TOO_LONG;
    RPC_OBRPC_LOG(WDIAG, "obrpc packet payload execced its limit",
                  K(ret), K(payload), "limit", OB_MAX_RPC_PACKET_LENGTH);
  } else if (OB_ISNULL(transport_)) {
    ret = OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(EDIAG, "transport_ should not be NULL", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(transport_->create_request(
                       req, dst_, payload, timeout_, cb))) {
      RPC_OBRPC_LOG(WDIAG, "create request fail", K(ret));
    } else if (NULL == req.pkt()) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      RPC_OBRPC_LOG(WDIAG, "request packet is NULL", K(ret));
    }
    timeguard.click();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(args.serialize(req.buf(), payload, pos))) {
      RPC_OBRPC_LOG(WDIAG, "serialize argument fail", K(ret));
    } else if (OB_FAIL(common::serialization::encode(
        req.buf(), payload, pos, GDS.rpc_spread_actions()))) {
      RPC_OBRPC_LOG(WDIAG, "serialize debug sync actions fail", K(pos), K(payload), K(ret));
    }
    timeguard.click();
  }

  if (OB_SUCC(ret)) {
    AsyncCB<pcode> *newcb = reinterpret_cast<AsyncCB<pcode>*>(req.cb());
    if (newcb) {
      newcb->set_args(args);
      newcb->set_dst(dst_);
      newcb->set_tenant_id(tenant_id_);
      newcb->set_timeout(timeout_);
      newcb->set_send_ts(start_ts);
      newcb->set_payload(payload);
    }
    req.set_async();
    if (OB_FAIL(init_pkt(req.pkt(), pcode, opts))) {
      RPC_OBRPC_LOG(WDIAG, "Init packet error", K(ret));
    }
    timeguard.click();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(transport_->post(req))) {
      RPC_OBRPC_LOG(WDIAG, "post packet fail", K(req), K(ret));
      req.destroy();
    } else {
      //do nothing
    }
    timeguard.click();
  }

  NG_TRACE_EXT(post_packet, Y(ret), Y(pcode), ID(addr), dst_);
  return ret;
}

} // end of namespace rpc
} // end of namespace oceanbase


#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_IPP_
