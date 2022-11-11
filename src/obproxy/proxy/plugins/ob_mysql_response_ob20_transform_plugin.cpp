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
#define USING_LOG_PREFIX PROXY

#include "lib/oblog/ob_log.h"
#include "ob_mysql_response_ob20_transform_plugin.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/mysqllib/ob_2_0_protocol_utils.h"
#include "proxy/mysqllib/ob_2_0_protocol_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;


namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObMysqlResponseOb20ProtocolTransformPlugin *ObMysqlResponseOb20ProtocolTransformPlugin::alloc(ObApiTransaction &transaction)
{
  return op_reclaim_alloc_args(ObMysqlResponseOb20ProtocolTransformPlugin, transaction);
}

ObMysqlResponseOb20ProtocolTransformPlugin::ObMysqlResponseOb20ProtocolTransformPlugin(ObApiTransaction &transaction)
  : ObTransformationPlugin(transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION),
    tail_crc_(0), target_payload_len_(0), handled_payload_len_(0), state_(OB20_PLUGIN_INIT_STATE),
    local_reader_(NULL), out_buffer_(NULL), out_buffer_reader_(NULL)
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseOb20ProtocolTransformPlugin born", K(this));
}

void ObMysqlResponseOb20ProtocolTransformPlugin::destroy()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseOb20ProtocolTransformPlugin destroy");
  ObTransformationPlugin::destroy();
  op_reclaim_free(this);
}

// process the packet while handling the io buffer reader
int ObMysqlResponseOb20ProtocolTransformPlugin::consume(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;
  bool is_resp_finished = sm_->trans_state_.trans_info_.server_response_.get_analyze_result().is_resp_completed();

  PROXY_API_LOG(DEBUG, "ObMysqlResponseOb20ProtocolTransformPlugin::consume begin", K(is_resp_finished), K(state_));
  
  if (state_ == OB20_PLUGIN_INIT_STATE) {
    if (OB_FAIL(alloc_iobuffer_inner(reader))) {
      LOG_WARN("fail to alloc iobuffer inner", K(ret));
    } else {
      int64_t total_avail = local_reader_->read_avail();
      target_payload_len_ = total_avail;
      LOG_DEBUG("set total payload len in ob20 resp", K(target_payload_len_));
    }
  }

  if (OB_SUCC(ret) && state_ == OB20_PLUGIN_INIT_STATE && target_payload_len_ > 0) {
    ObSEArray<ObObJKV, 1> extra_info;
    char client_extra_info_buf[CLIENT_EXTRA_INFO_BUF_MAX_LEN] = "\0";
      if (OB_FAIL(ObProxyTraceUtils::build_extra_info_for_client(sm_, client_extra_info_buf,
                                                           CLIENT_EXTRA_INFO_BUF_MAX_LEN, extra_info))) {
      LOG_WARN("fail to build extra info for client", K(ret));
    } else if (OB_FAIL(build_ob20_head_and_extra(out_buffer_, extra_info, is_resp_finished))) {
      LOG_WARN("fail to build ob20 head and extra", K(ret));
    } else {
      int64_t local_reader_avail = local_reader_->read_avail();
      int64_t could_write_max = MIN(local_reader_avail, target_payload_len_);  // more than MYSQL_NET_HEADER_LENGTH
      if (OB_FAIL(build_ob20_body(local_reader_, local_reader_avail, out_buffer_, could_write_max, tail_crc_))) {
        LOG_WARN("fail to build ob20 body", K(ret));
      } else if (OB_UNLIKELY(handled_payload_len_ > target_payload_len_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err, handled len > target len", K(ret), K(handled_payload_len_), K(target_payload_len_));
      } else if (handled_payload_len_ == target_payload_len_) {
        state_ = OB20_PLUGIN_FIN_STATE;
      } else {
        state_ = OB20_PLUGIN_CONT_STATE;  // handled mysql len < target mysql len, continue
      }
    }
  }

  if (OB_SUCC(ret) && state_ == OB20_PLUGIN_CONT_STATE) {
    // build body as much as possible
    int64_t local_reader_avail = local_reader_->read_avail();
    int64_t could_write_max = MIN(local_reader_avail, target_payload_len_ - handled_payload_len_);
    if (OB_FAIL(build_ob20_body(local_reader_, local_reader_avail, out_buffer_, could_write_max, tail_crc_))) {
      LOG_WARN("fail to build ob20 body", K(ret));
    } else if (OB_UNLIKELY(handled_payload_len_ > target_payload_len_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected handle > target len", K(ret), K(handled_payload_len_), K(target_payload_len_));
    } else if (handled_payload_len_ == target_payload_len_) {
      state_ = OB20_PLUGIN_FIN_STATE;
    } else {
      // handled len < target len, continue
    }
  }

  if (OB_SUCC(ret) && state_ == OB20_PLUGIN_FIN_STATE) {
    if (OB_FAIL(ObProto20Utils::fill_proto20_tailer(out_buffer_, tail_crc_))) {
      LOG_WARN("fail to build ob20 tail crc", K(ret));
    }
  }

  if (OB_SUCC(ret) && state_ != OB20_PLUGIN_INIT_STATE) {
    int64_t avail_size = out_buffer_reader_->read_avail();
    int64_t produce_size = 0;
    if (avail_size != (produce_size = produce(out_buffer_reader_, avail_size))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to produce to VIO", K(ret), K(avail_size), K(produce_size));
    } else if (OB_FAIL(out_buffer_reader_->consume(avail_size))) {
      LOG_WARN("fail to consume out buffer reader", K(ret), K(avail_size));
    } else if (state_ == OB20_PLUGIN_FIN_STATE) {
      state_ = OB20_PLUGIN_INIT_STATE;
      reset();
      LOG_DEBUG("finish this ob20 response back to client");
    }
  }

  if (OB_FAIL(ret)) {
    reset();
    sm_->trans_state_.inner_errcode_ = ret;
    sm_->trans_state_.current_.state_ = ObMysqlTransact::INTERNAL_ERROR;
    LOG_WARN("fail to consume in ob20 response transform plugin", K(ret));
  }

  return ret;
}

int ObMysqlResponseOb20ProtocolTransformPlugin::build_ob20_head_and_extra(ObMIOBuffer *write_buf,
                                                                          ObIArray<ObObJKV> &extra_info,
                                                                          bool is_last_packet)
{
  int ret = OB_SUCCESS;

  // total mysql len in ob20 response payload
  int64_t payload_len = target_payload_len_;   // msyql + (4 + extra_info_len() if exist)
  char *head_start = NULL;
  if (OB_FAIL(ObProto20Utils::reserve_proto20_hdr(write_buf, head_start))) {
    LOG_WARN("fail to reserve ob20 head", K(ret));
  } else {
    ObMysqlClientSession *client_session = sm_->get_client_session();
    Ob20ProtocolHeader &ob20_head = client_session->get_session_info().ob20_request_.ob20_header_;
    uint8_t last_compressed_seq = static_cast<uint8_t>(client_session->get_compressed_seq() + 1);
    const bool is_new_extra_info = client_session->get_session_info().is_client_support_new_extra_info();
    bool is_extra_info_exist = false;
    
    if (OB_FAIL(ObProto20Utils::fill_proto20_extra_info(write_buf, &extra_info, is_new_extra_info, 
                                                        payload_len, tail_crc_, is_extra_info_exist))) {
      LOG_WARN("fail to fill ob20 extra info", K(ret));
    } else if (OB_FAIL(ObProto20Utils::fill_proto20_header(head_start,
                                                           payload_len,
                                                           last_compressed_seq,
                                                           last_compressed_seq,
                                                           ob20_head.request_id_,
                                                           client_session->get_cs_id(),
                                                           is_last_packet,
                                                           false,
                                                           is_extra_info_exist,
                                                           is_new_extra_info))) {
      LOG_WARN("fail to fill ob20 head", K(ret));
    } else {
      client_session->set_compressed_seq(last_compressed_seq);
    }
  }

  return ret;
}

int ObMysqlResponseOb20ProtocolTransformPlugin::build_ob20_body(ObIOBufferReader *reader,
                                                                int64_t reader_avail,
                                                                ObMIOBuffer *write_buf,
                                                                int64_t write_len,
                                                                uint64_t &crc)
{
  int ret = OB_SUCCESS;

  if (reader_avail == 0 || write_len == 0) {
    LOG_DEBUG("arg is 0, continue...", K(reader_avail), K(write_len));
  } else if (reader_avail < 0 || write_len < 0 || reader_avail < write_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected argument check, plz check", K(ret), K(reader_avail), K(write_len));
  } else {
    int64_t payload_len = 0;
    if (OB_FAIL(ObProto20Utils::fill_proto20_payload(reader, write_buf, write_len, payload_len, crc))) {
      LOG_WARN("fail to fill ob20 body in ob20 plugin", K(ret), K(write_len), K(reader_avail));
    } else {
      handled_payload_len_ += write_len;
    }
  }
  
  return ret;
}

void ObMysqlResponseOb20ProtocolTransformPlugin::handle_input_complete()
{
  PROXY_API_LOG(DEBUG, "ObMysqlResponseOb20ProtocolTransformPlugin::handle_input_complete happen", K(lbt()));

  if (NULL != local_reader_) {
    LOG_DEBUG("free local reader in handle input complete");
    local_reader_->dealloc();
    local_reader_ = NULL;
  }

  if (NULL != out_buffer_) {
    free_miobuffer(out_buffer_);
    out_buffer_ = NULL;
    out_buffer_reader_ = NULL;
  }
  
  set_output_complete();
}

int ObMysqlResponseOb20ProtocolTransformPlugin::alloc_iobuffer_inner(event::ObIOBufferReader *reader)
{
  int ret = OB_SUCCESS;

  if (NULL == local_reader_ && OB_ISNULL(local_reader_ = reader->clone())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to clone reader to local", K(ret));
  }
  
  if (OB_SUCC(ret) && NULL == out_buffer_) {
    if (OB_ISNULL(out_buffer_ = new_miobuffer(MYSQL_BUFFER_SIZE))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new miobuffer", K(ret));
    } else if (NULL == (out_buffer_reader_ = out_buffer_->alloc_reader())) {
      LOG_WARN("fail to alloc local reader", K(ret));
    } else {
      LOG_DEBUG("alloc iobuffer inner finish");
    }
  }
  
  return ret;
}

void ObMysqlResponseOb20ProtocolTransformPlugin::reset()
{
  tail_crc_ = 0;
  handled_payload_len_ = 0;
  target_payload_len_= 0;
  state_ = OB20_PLUGIN_INIT_STATE;
}

} // proxy
} // obproxy
} // oceanbase

