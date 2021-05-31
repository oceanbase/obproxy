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

#ifndef OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_
#define OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_

#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/frame/ob_req_processor.h"

namespace oceanbase
{
namespace obrpc
{

class ObRpcStreamCond;
template <class T>
class ObRpcProcessor : public rpc::frame::ObReqProcessor
{
public:
  enum { PCODE = T::PCODE };

  static const int64_t DEFAULT_WAIT_NEXT_PACKET_TIMEOUT = 30 * 1000 * 1000L;

public:
  ObRpcProcessor()
      : rpc_pkt_(NULL), sc_(NULL), is_stream_(false),
        is_stream_end_(false), preserve_recv_data_(false), preserved_buf_(NULL),
        using_buffer_(NULL), send_timestamp_(0), pkt_size_(0)
  {}

  virtual ~ObRpcProcessor();


  void set_ob_request(rpc::ObRequest &req)
  {
    rpc::frame::ObReqProcessor::set_ob_request(req);
    rpc_pkt_ = &reinterpret_cast<const ObRpcPacket&>(req.get_packet());
    pkt_size_ = rpc_pkt_->get_clen();
    send_timestamp_ = req.get_send_timestamp();
  }

  void set_stream_cond(ObRpcStreamCond &sc) { sc_ = &sc; }
  // timestamp of the packet
  int64_t get_send_timestamp() const
  { return send_timestamp_; }


protected:
  struct Response {
    Response(int64_t sessid,
             bool is_stream,
             bool is_stream_last,
             ObRpcPacket *pkt)
        : sessid_(sessid),
          is_stream_(is_stream),
          is_stream_last_(is_stream_last),
          pkt_(pkt)
    { }

    // for stream options
    int64_t sessid_;
    bool is_stream_;
    bool is_stream_last_;
    ObRpcPacket *pkt_;

    TO_STRING_KV(K(sessid_), K(is_stream_), K(is_stream_last_));
  };

  void reuse();
  int deserialize();
  int serialize();
  int response(const int retcode)
  { return part_response(retcode, true); }

  int flush(int64_t wait_timeout = DEFAULT_WAIT_NEXT_PACKET_TIMEOUT);

  virtual int process() = 0;
  virtual void cleanup();

  void set_preserve_recv_data()
  { preserve_recv_data_ = true; }

  private:
  int part_response(const int retcode, bool is_last);
  int do_response(const Response &rsp);

protected:
  typename T::Request arg_;
  typename T::Response result_;

  const ObRpcPacket *rpc_pkt_;
  ObRpcStreamCond *sc_;

  // mark if current request is in a stream.
  bool is_stream_;
  // If this request is a stream request, this mark means the stream
  // is end so that no need to response any packet back. When wait
  // client's next packet timeout, the req of this processor is
  // invalid, so the stream is end.
  bool is_stream_end_;

  // The flag marks received data must copy out from `easy buffer'
  // before we response packet back. Typical case is when we use
  // shadow copy when deserialize the argument but response before
  // process this argument.
  bool preserve_recv_data_;
  char *preserved_buf_;

  common::ObDataBuffer *using_buffer_;

  int64_t send_timestamp_;
  int64_t pkt_size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcProcessor);
}; // end of class ObRpcProcessor

} // end of namespace observer
} // end of namespace oceanbase

#include "rpc/obrpc/ob_rpc_processor.ipp"

#endif //OCEANBASE_RPC_OBRPC_OB_RPC_PROCESSOR_
