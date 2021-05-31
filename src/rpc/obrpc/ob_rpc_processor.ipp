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

#include "easy_io.h"
#include "lib/ob_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/oblog/ob_warning_buffer.h"
#include "rpc/obrpc/ob_rpc_req_context.h"
#include "rpc/obrpc/ob_rpc_stream_cond.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "common/ob_debug_sync.h"

namespace oceanbase
{

namespace obrpc
{

template <class T>
void ObRpcProcessor<T>::reuse()
{
  rpc::frame::ObReqProcessor::reuse();
  rpc_pkt_ = NULL;
}

template <class T>
ObRpcProcessor<T>::~ObRpcProcessor()
{
  reuse();
  if (NULL != sc_) {
    sc_->destroy();
    sc_ = NULL;
  }
}

template <class T>
int ObRpcProcessor<T>::deserialize()
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(rpc_pkt_)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "rpc_pkt_ should not be NULL", K(ret));
  } else {
    const int64_t len = rpc_pkt_->get_clen();
    const char *ez_buf = rpc_pkt_->get_cdata();
    int64_t pos = 0;
    if (OB_ISNULL(ez_buf)) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(WARN, "ez buf should not be NULL", K(ret));
    } else if (OB_FAIL(rpc_pkt_->verify_checksum())) {
      RPC_OBRPC_LOG(ERROR, "verify packet checksum fail", K(*rpc_pkt_), K(ret));
    } else {
      //do nothing
    }

    if (OB_SUCC(ret)) {
      if (preserve_recv_data_) {
        char *new_buf = static_cast<char*>(
            common::ob_malloc(len, common::ObModIds::OB_RPC_PROCESSOR));
        if (!new_buf) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          RPC_OBRPC_LOG(WARN, "Allocate memory error", K(ret));
        } else {
          MEMCPY(new_buf, ez_buf, len);
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(common::serialization::decode(new_buf, len, pos, arg_))) {
          int pcode = PCODE;
          RPC_OBRPC_LOG(WARN, "decode argument fail", K(pcode), K(ret));
          common::ob_free(new_buf);
        } else {
          if (len > pos) {
            if (OB_FAIL(GDS.rpc_spread_actions().deserialize(new_buf, len, pos))) {
              RPC_OBRPC_LOG(WARN, "decode debug sync actions fail", K(ret));
              common::ob_free(new_buf);
            }
          }
          if (OB_SUCC(ret)) {
            preserved_buf_ = new_buf;
          }
        }
      } else {
        ret = common::serialization::decode(ez_buf, len, pos, arg_);
        if (OB_SUCC(ret)) {
          if (len > pos) {
            if (OB_FAIL(GDS.rpc_spread_actions().deserialize(ez_buf, len, pos))) {
              RPC_OBRPC_LOG(WARN, "decode debug sync actions fail", K(ret));
            }
          }
        } else {
          RPC_OBRPC_LOG(WARN, "Decode error", K(ret));
        }
      }
    }
  }

  return ret;
}

template <class T>
int ObRpcProcessor<T>::serialize()
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(using_buffer_)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "using_buffer_ should not be NULL", K(ret));
  } else if (OB_FAIL(common::serialization::encode(
        using_buffer_->get_data(), using_buffer_->get_capacity(),
        using_buffer_->get_position(), result_))) {
    RPC_OBRPC_LOG(WARN, "encode data error", K(ret));
  } else {
    //do nothing
  }
  return ret;
}

template <class T>
int ObRpcProcessor<T>::do_response(const Response &rsp)
{
  int ret = common::OB_SUCCESS;
  if (is_stream_end_) {
    RPC_OBRPC_LOG(DEBUG, "stream rpc end before");
  } else if (OB_ISNULL(req_)) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else if (OB_ISNULL(req_->get_request())) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "req is NULL", K(ret));
  } else if (OB_ISNULL(rpc_pkt_)) {
    ret = common::OB_ERR_NULL_VALUE;
    RPC_OBRPC_LOG(WARN, "rpc pkt is NULL", K(ret));
  } else {
    // TODO: make force_destroy_second as a configure item
    // static const int64_t RESPONSE_RESERVED_US = 20 * 1000 * 1000;
    // int64_t rts = static_cast<int64_t>(req_->get_request()->start_time) * 1000 * 1000;
    // todo: get 'force destroy second' from eio?
    // if (rts > 0 && eio_->force_destroy_second > 0
    //     && ::oceanbase::common::ObTimeUtility::current_time() - rts + RESPONSE_RESERVED_US > eio_->force_destroy_second * 1000000) {
    //   _OB_LOG(ERROR, "pkt process too long time: pkt_receive_ts=%ld, pkt_code=%d", rts, pcode);
    // }
    //copy packet into req buffer
    ObRpcPacketCode pcode = rpc_pkt_->get_pcode();
    if (OB_SUCC(ret)) {
      ObRpcPacket *packet = rsp.pkt_;
      packet->set_pcode(pcode);
      packet->set_chid(rpc_pkt_->get_chid());
      packet->set_session_id(rsp.sessid_);
      packet->set_trace_id(common::ObCurTraceId::get());
      packet->set_resp();
      if (rsp.is_stream_) {
        if (!rsp.is_stream_last_) {
          packet->set_stream_next();
        } else {
          packet->set_stream_last();
        }
      }
      packet->calc_checksum();
      req_->get_request()->opacket = packet;
    }
    //just set request retcode, wakeup in ObSingleServer::handlePacketQueue()
    req_->set_request_rtcode(EASY_OK);
    wakeup_request();
  }
  return ret;
}

template <class T>
int ObRpcProcessor<T>::part_response(const int retcode, bool is_last)
{
  int ret = common::OB_SUCCESS;
  if (req_has_wokenup_ || OB_ISNULL(req_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_OBRPC_LOG(WARN, "invalid req, maybe stream rpc timeout", K(ret), K(retcode),
        K(is_last), K(req_has_wokenup_), KP_(req));
  } else {
    ObRpcResultCode rcode;
    rcode.rcode_ = retcode;

    // add warning buffer into result code buffer if rpc fails.
    common::ObWarningBuffer *wb = common::ob_get_tsi_warning_buffer();
    if (wb) {
      if (retcode != common::OB_SUCCESS) {
        (void)snprintf(rcode.msg_, common::OB_MAX_ERROR_MSG_LEN, "%s", wb->get_err_msg());
      }
      //always add warning buffer
      bool not_null = true;
      for (uint32_t idx = 0; OB_SUCC(ret) && not_null && idx < wb->get_readable_warning_count(); idx++) {
        const common::ObWarningBuffer::WarningItem *item = wb->get_warning_item(idx);
        if (item != NULL) {
          if (OB_FAIL(rcode.warnings_.push_back(*item))) {
            RPC_OBRPC_LOG(WARN, "Failed to add warning", K(ret));
          }
        } else {
          not_null = false;
        }
      }
    }

    int64_t content_size = common::serialization::encoded_length(result_) +
        common::serialization::encoded_length(rcode);

    char *buf = NULL;
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (content_size > common::OB_MAX_PACKET_LENGTH) {
      ret = common::OB_RPC_PACKET_TOO_LONG;
      RPC_OBRPC_LOG(WARN, "response content size bigger than OB_MAX_PACKET_LENGTH", K(ret));
    } else {
      //allocate memory from easy
      //[ ObRpcPacket ... ObDatabuffer ... serilized content ...]
      int64_t size = (content_size) + sizeof (common::ObDataBuffer) + sizeof(ObRpcPacket);
      buf = static_cast<char*>(easy_alloc(size));
      if (NULL == buf) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        RPC_OBRPC_LOG(WARN, "allocate rpc data buffer fail", K(ret), K(size));
      } else {
        using_buffer_ = new (buf + sizeof(ObRpcPacket)) common::ObDataBuffer();
        if (!(using_buffer_->set_data(buf + sizeof(ObRpcPacket) + sizeof (*using_buffer_),
            content_size))) {
          ret = common::OB_INVALID_ARGUMENT;
          RPC_OBRPC_LOG(WARN, "invalid parameters", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_ISNULL(using_buffer_)) {
      ret = common::OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(ERROR, "using_buffer_ is NULL", K(ret));
    } else if (OB_FAIL(rcode.serialize(using_buffer_->get_data(),
        using_buffer_->get_capacity(),
        using_buffer_->get_position()))) {
      RPC_OBRPC_LOG(WARN, "serialize result code fail", K(ret));
    } else {
      // also send result if process successfully.
      if (common::OB_SUCCESS == retcode) {
        if (OB_FAIL(serialize())) {
          RPC_OBRPC_LOG(WARN, "serialize result fail", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t sessid = sc_ ? sc_->sessid() : 0;
      ObRpcPacket *pkt = new (buf) ObRpcPacket();
      Response rsp(sessid, is_stream_, is_last, pkt);
      pkt->set_content(using_buffer_->get_data(), using_buffer_->get_position());
      if (OB_FAIL(do_response(rsp))) {
        RPC_OBRPC_LOG(WARN, "response data fail", K(ret));
      }
    }

    using_buffer_ = NULL;
  }
  return ret;
}

template <class T>
int ObRpcProcessor<T>::flush(int64_t wait_timeout)
{
  int ret = common::OB_SUCCESS;
  is_stream_ = true;
  rpc::ObRequest *req = NULL;

  if (OB_ISNULL(sc_)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "sc is NULL", K(ret));
  } else if (OB_ISNULL(rpc_pkt_) || is_stream_end_) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(WARN, "request is NULL, maybe wait timeout",
        K(ret), K(rpc_pkt_), K(is_stream_end_));
  } else if (rpc_pkt_ && rpc_pkt_->is_stream_last()) {
    ret = common::OB_ITER_END;
    RPC_OBRPC_LOG(WARN, "stream is end", K(ret), K(*rpc_pkt_));
  } else if (OB_FAIL(sc_->prepare())) {
    RPC_OBRPC_LOG(WARN, "prepare stream session fail", K(ret));
  } else if (OB_FAIL(part_response(common::OB_SUCCESS, false))) {
    RPC_OBRPC_LOG(WARN, "response part result to peer fail", K(ret));
  } else if (OB_FAIL(sc_->wait(req, wait_timeout))) {
    req_ = NULL; //wait fail, invalid req_
    reuse();
    is_stream_end_ = true;
    RPC_OBRPC_LOG(WARN, "wait next packet fail, set req_ to null", K(ret), K(wait_timeout));
  } else if (OB_ISNULL(req)) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_OBRPC_LOG(ERROR, "Req should not be NULL", K(ret));
  } else {
    reuse();
    set_ob_request(*req);
    if (!rpc_pkt_) {
      wakeup_request();
      is_stream_end_ = true;
      ret = common::OB_ERR_UNEXPECTED;
      RPC_OBRPC_LOG(ERROR, "rpc packet is NULL in stream", K(ret));
    } else if (rpc_pkt_->is_stream_last()) {
      ret = common::OB_ITER_END;
    } else {
      //do nothing
    }
  }

  // here we don't care what exactly the packet is.

  return ret;
}

template <class T>
void ObRpcProcessor<T>::cleanup()
{
  if (preserve_recv_data_) {
    if (preserved_buf_) {
      common::ob_free(preserved_buf_);
    } else {
      RPC_OBRPC_LOG(WARN, "preserved buffer is NULL, maybe alloc fail");
    }
  }

  // record
  // TODO: support streaming interface
  if (!is_stream_) {
    rpc::RpcStatPiece piece;
    piece.is_server_ = true;
    piece.size_ = pkt_size_;
    piece.net_time_ = get_receive_timestamp() - get_send_timestamp();
    piece.wait_time_ = get_enqueue_timestamp() - get_receive_timestamp();
    piece.queue_time_ = get_run_timestamp() - get_enqueue_timestamp();
    piece.process_time_ = common::ObTimeUtility::current_time() - get_run_timestamp();
    RPC_STAT(static_cast<ObRpcPacketCode>(PCODE), piece);
  }
}

} // end of namespace obrpc
} // end of namespace oceanbase
