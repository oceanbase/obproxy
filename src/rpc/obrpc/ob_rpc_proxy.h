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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/net/ob_addr.h"
#include "rpc/frame/ob_req_transport.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
// macros of ObRpcProxy
#include "rpc/obrpc/ob_rpc_proxy_macros.h"

namespace oceanbase
{

namespace obrpc
{
class Handle;

struct ObRpcOpts
{
  uint64_t tenant_id_;
  ObRpcPriority pr_;    // priority of this RPC packet
  mutable bool is_stream_; // is this RPC packet a stream packet?
  mutable bool is_stream_last_; // is this RPC packet the last packet in stream?

  ObRpcOpts()
      : tenant_id_(common::OB_INVALID_ID),
        pr_(ORPR_UNDEF),
        is_stream_(false),
        is_stream_last_(false)
  {}
};

class ObRpcProxy {
public:
  static const int64_t MAX_RPC_TIMEOUT = 9000 * 1000;
  static common::ObAddr myaddr_;
  struct NoneT
  {
    int serialize(SERIAL_PARAMS) const
    {
      UNF_UNUSED_SER;
      return common::OB_SUCCESS;
    }
    int deserialize(DESERIAL_PARAMS)
    {
      UNF_UNUSED_DES;
      return common::OB_SUCCESS;
    }
    int64_t get_serialize_size() const
    {
      return 0;
    }
    TO_STRING_EMPTY();
  };

public:
  template <ObRpcPacketCode pcode, typename IGNORE=void> struct

  ObRpc {};

  // asynchronous callback
  template <ObRpcPacketCode pcode>
  class AsyncCB
      : public rpc::frame::ObReqTransport::AsyncCB
  {
  public:
    int decode(void *pkt);

    virtual void do_first();
    virtual void set_args(const typename ObRpc<pcode>::Request &arg) = 0;
    virtual void destroy() {}

  protected:
    typename ObRpc<pcode>::Response result_;
    ObRpcResultCode rcode_;
  };

public:
  ObRpcProxy();

  int init(const rpc::frame::ObReqTransport *transport,
           const common::ObAddr &dst = common::ObAddr());
  void destroy() { init_ = false; }
  bool is_inited() const { return init_; }
  void set_timeout(int64_t timeout) { timeout_ = timeout; }
  void set_tenant(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_priv_tenant(uint64_t tenant_id) { priv_tenant_id_ = tenant_id; }
  void set_server(const common::ObAddr &dst) { dst_ = dst; }
  const common::ObAddr &get_server() const { return dst_; }

  // when active is set as false, all RPC calls will simply return OB_INACTIVE_RPC_PROXY.
  void active(const bool active) { active_ = active; }

  int64_t timeout() const { return timeout_; }

  const ObRpcResultCode &get_result_code() const;

  int init_pkt(ObRpcPacket *pkt,
               ObRpcPacketCode pcode,
               const ObRpcOpts &opts) const;


  //// example:
  //// without argument and result
  //
  // RPC_S(@PR5 rpc_name, pcode);
  //
  //// with argument but without result
  //
  // RPC_S(@PR5 rpc_name, pcode, (args));
  //
  //// without argument but with result
  //
  // RPC_S(@PR5 rpc_name, pcode, retult);
  //
  //// with both argument and result
  //
  // RPC_S(@PR5 rpc_name, pcode, (args), retult);
  //
  // Make sure 'args' and 'result' are serializable and deserializable.

protected:
  // we can definitely judge input or output argument by their
  // constant specifier since they're only called by our wrapper
  // function where input argument is always const-qualified whereas
  // output result is not.
  template <typename Input, typename Out>
  int rpc_call(ObRpcPacketCode pcode,
               const Input &args,
               Out &result,
               Handle *handle,
               const ObRpcOpts &opts);
  template <typename Input>
  int rpc_call(ObRpcPacketCode pcode,
               const Input &args,
               Handle *handle,
               const ObRpcOpts &opts);
  template <typename Output>
  int rpc_call(ObRpcPacketCode pcode,
               Output &result,
               Handle *handle,
               const ObRpcOpts &opts);
  int rpc_call(ObRpcPacketCode pcode, Handle *handle, const ObRpcOpts &opts);

  template <ObRpcPacketCode pcode>
  int rpc_post(const typename ObRpc<pcode>::Request &args,
               AsyncCB<pcode> *cb,
               const ObRpcOpts &opts);
  int rpc_post(ObRpcPacketCode pcode,
               rpc::frame::ObReqTransport::AsyncCB *cb,
               const ObRpcOpts &opts);

private:
  int send_request(
      const rpc::frame::ObReqTransport::Request<ObRpcPacket> &req,
      rpc::frame::ObReqTransport::Result<ObRpcPacket> &result) const;

  int log_user_error_and_warn(const ObRpcResultCode &rcode) const;

protected:
  const rpc::frame::ObReqTransport *transport_;
  common::ObAddr dst_;
  int64_t timeout_;
  uint64_t tenant_id_;
  uint64_t priv_tenant_id_;
  bool init_;
  bool active_;
  ObRpcResultCode rcode_;
};

// common handle
class Handle {
  friend class ObRpcProxy;

public:
  Handle();
  const common::ObAddr &get_dst_addr() const { return dst_; }

protected:
  bool has_more_;
  common::ObAddr dst_;
  int64_t sessid_;
  ObRpcOpts opts_;
  const rpc::frame::ObReqTransport *transport_;
  ObRpcProxy proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(Handle);
};

// stream rpc handle
template <ObRpcPacketCode pcode>
class SSHandle
    : public Handle {
  friend class ObRpcProxy;

public:
  bool has_more() const;
  int get_more(typename ObRpcProxy::ObRpc<pcode>::Response &result);
  int abort();
  const ObRpcResultCode &get_result_code() const;

protected:
  ObRpcResultCode rcode_;
};

// asynchronous store result rpc handle
template <ObRpcPacketCode pcode>
class AsyncSHandle
    : public SSHandle<pcode>
{
public:
};

} // end of namespace common
} // end of namespace oceanbase

#include "rpc/obrpc/ob_rpc_proxy.ipp"

#define DEFINE_TO(CLS, ...)                                               \
  inline CLS to(const common::ObAddr &dst = common::ObAddr())  const      \
  {                                                                       \
    CLS proxy(*this);                                                     \
    proxy.set_server(dst);                                                \
    return proxy;                                                         \
  }                                                                       \
  inline CLS& timeout(int64_t timeout)                                    \
  {                                                                       \
    set_timeout(timeout);                                                 \
    return *this;                                                         \
  }                                                                       \
  inline CLS& by(uint64_t tenant_id)                                      \
  {                                                                       \
    set_tenant(tenant_id);                                                \
    return *this;                                                         \
  }                                                                       \
  inline CLS& as(uint64_t tenant_id)                                      \
  {                                                                       \
    set_priv_tenant(tenant_id);                                           \
    return *this;                                                         \
  }                                                                       \
  explicit CLS(CLS *mock_proxy=NULL)                                      \
      : mock_proxy_(mock_proxy)                                           \
  { __VA_ARGS__; }                                                        \
  CLS *mock_proxy_

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROXY_
