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

#ifndef OBPROXY_TRIM_OKPACKET_TRANSFORM_PLUGIN_H
#define OBPROXY_TRIM_OKPACKET_TRANSFORM_PLUGIN_H

#include "ob_global_plugin.h"
#include "ob_transformation_plugin.h"
#include "ob_proxy_session_info_handler.h"
#include "ob_resultset_stream_analyzer.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObTrimOkPakcetTransformPlugin : public ObTransformationPlugin
{
public:
  static ObTrimOkPakcetTransformPlugin *alloc(ObApiTransaction &transaction)
  {
    return op_reclaim_alloc_args(ObTrimOkPakcetTransformPlugin, transaction);
  }

  explicit ObTrimOkPakcetTransformPlugin(ObApiTransaction &transaction)
      : ObTransformationPlugin(transaction, ObTransformationPlugin::RESPONSE_TRANSFORMATION),
        local_reader_(NULL)
  {
  }

  virtual void destroy()
  {
    ObTransformationPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void consume(event::ObIOBufferReader *reader)
  {
    int64_t consume_size = 0;
    int64_t produce_size = 0;
    int64_t read_avail = 0;
    int64_t remain_size = 0;
    ObResultsetStreamStatus stream_status = RSS_CONTINUE_STATUS;

    if (NULL == local_reader_) {
      local_reader_ = reader->clone();
    }

    if (NULL != local_reader_) {
      read_avail = local_reader_->read_avail();
      if (common::OB_SUCCESS != resultset_analyzer_.analyze_resultset_stream(
          local_reader_, consume_size, stream_status)) {
        _PROXY_API_LOG(WDIAG, "failed to analyze result set stream, reader=%p", local_reader_);
        // if we analyze failed, we transform the origin result
        consume_size = read_avail;
      }
    }

    if (consume_size > 0) {
      if (consume_size != (produce_size = produce(local_reader_, consume_size))) {
        _PROXY_API_LOG(WDIAG, "failed to produce expected_produce_size=%ld, produce_szie=%ld",
                consume_size, produce_size);
      }

      int ret = OB_SUCCESS;
      if (OB_FAIL(local_reader_->consume(consume_size))) {
        PROXY_API_LOG(WDIAG, "fail to consume ", K(consume_size), K(ret));
      } else {
        remain_size = read_avail - consume_size;
        if (RSS_END_NORMAL_STATUS == stream_status) {
          // ignore return code, even through it failed, we just lose
          // the flag of "is_partition_hit"
          trim_remain_okpacket();
        } else if (remain_size > 0 && remain_size > local_reader_->mbuf_->water_mark_) {
          ObMysqlAnalyzeResult result;
          if (OB_UNLIKELY(OB_SUCCESS != ObProxyParserUtils::analyze_one_packet(*local_reader_, result))) {
            _PROXY_API_LOG(WDIAG, "failed to analyze one packet");
          } else if (ANALYZE_DONE != result.status_
                     && result.meta_.pkt_len_ > local_reader_->mbuf_->water_mark_) {
            // ensure the input buffer can cache the last ok packt
            local_reader_->mbuf_->water_mark_ = result.meta_.pkt_len_;
          } else {
            // ensure the input buffer can cache the last ok packt
            local_reader_->mbuf_->water_mark_ = remain_size;
          }
        }
      }
    }
  }

  virtual void handle_input_complete()
  {
    if (NULL != local_reader_) {
      local_reader_->dealloc();
      local_reader_ = NULL;
    }
    set_output_complete();
  }

  int trim_remain_okpacket()
  {
    int ret = OB_SUCCESS;
    ObMysqlAnalyzeResult result;

    if (ObProxyParserUtils::is_ok_packet(*local_reader_, result)) {
      if (local_reader_->is_in_single_buffer_block()) {
        int64_t content_len = local_reader_->read_avail() - MYSQL_NET_HEADER_LENGTH;
        char *content_start = local_reader_->start() + MYSQL_NET_HEADER_LENGTH;
        ObString content(content_len, content_start);
        if (OB_SUCCESS != (ret = save_changed_session_info(content))) {
          _PROXY_API_LOG(WDIAG, "failed to save changed session info, ret=%d", ret);
        }
      } else {
        // this ok packet is in multi miobuffer
        int64_t read_avail = local_reader_->read_avail();
        char *buf = (char *)op_fixed_mem_alloc(read_avail);
        if (NULL == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          _PROXY_API_LOG(WDIAG, "failed to alloc mem, size = %ld, ret=%d", read_avail, ret);
        } else {
          char *written_pos = local_reader_->copy(buf, read_avail, 0);
          if (written_pos != buf + read_avail) {
            ret = OB_ERR_UNEXPECTED;
            _PROXY_API_LOG(WDIAG, "not copy completely", K(written_pos), K(buf), K(read_avail), K(ret));
          } else {
            ObString content(read_avail - MYSQL_NET_HEADER_LENGTH, buf + MYSQL_NET_HEADER_LENGTH);
            if (OB_SUCCESS != (ret = save_changed_session_info(content))) {
              _PROXY_API_LOG(WDIAG, "failed to save changed session info, ret=%d", ret);
            }
          }
          if (NULL != buf) {
            op_fixed_mem_free(buf, read_avail);
            buf = NULL;
          }
        }
      }
    }

    if (OB_FAIL(local_reader_->consume(local_reader_->read_avail()))) {
      PROXY_API_LOG(WDIAG, "fail to consume ", K(ret));
    }
    return ret;
  }

  int save_changed_session_info(common::ObString &content)
  {
    int ret = OB_SUCCESS;
    obmysql::OMPKOK src_ok;
    int64_t content_len = content.length();
    char *content_start = content.ptr();
    src_ok.set_content(content_start, static_cast<uint32_t>(content_len));

    // maybe the server session is attached to client session after one command complete
    ObMysqlClientSession *client_session = sm_->get_client_session();
    ObMysqlServerSession *server_session = sm_->get_server_session();
    if (NULL == server_session) {
      server_session = client_session->get_server_session();
    }

    if (NULL != client_session) {
      if (NULL != server_session) {
        ObClientSessionInfo &client_info = client_session->get_session_info();
        ObServerSessionInfo &server_info = server_session->get_session_info();
        const obmysql::ObMySQLCapabilityFlags cap = server_info.get_compatible_capability_flags();
        src_ok.set_capability(cap);

        if (OB_SUCCESS != (ret = src_ok.decode())) {
          PROXY_API_LOG(WDIAG, "fail to decode ok packet", K(src_ok), K(ret));
        } else if (OB_SUCCESS != (ret = ObProxySessionInfoHandler::save_changed_session_info(
            client_info, server_info, sm_->trans_state_.is_auth_request_, NULL, src_ok, 
            sm_->trans_state_.trans_info_.resp_result_, sm_->trans_state_.trace_log_))) {
          _PROXY_API_LOG(WDIAG, "fail to save changed session info, is_auth_request=%d, ret=%d",
                         sm_->trans_state_.is_auth_request_, ret);
        }
        PROXY_API_LOG(DEBUG, "analyze last ok packet of result set", K(src_ok));
      } else {
        ret = OB_ERROR;
        _PROXY_API_LOG(WDIAG, "server session is dead, server_session=NULL");
      }
    }

    return ret;
  }

private:
  event::ObIOBufferReader *local_reader_;
  ObResultsetStreamAnalyzer resultset_analyzer_;
};

class ObTrimOkPakcetGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObTrimOkPakcetGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObTrimOkPakcetGlobalPlugin);
  }

  ObTrimOkPakcetGlobalPlugin()
  {
    register_hook(HOOK_READ_RESPONSE);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_read_response(ObApiTransaction &transaction)
  {
    ObTransactionPlugin *plugin = NULL;

    if (need_enable_plugin(transaction.get_sm())) {
      plugin = ObTrimOkPakcetTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        _PROXY_API_LOG(DEBUG, "add ObTrimOkPakcetTransformPlugin=%p", plugin);
      } else {
        _PROXY_API_LOG(EDIAG, "failed to allocate memory for ObTrimOkPakcetTransformPlugin");
      }
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    return (!sm->trans_state_.is_auth_request_
            && !sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
            && sm->trans_state_.trans_info_.resp_result_.is_resultset_resp());
  }
};

void init_trim_okpacket_transform()
{
  _PROXY_API_LOG(DEBUG, "init trim ok pakcet transformation plugin");
  ObTrimOkPakcetGlobalPlugin *trim_transform = ObTrimOkPakcetGlobalPlugin::alloc();
  UNUSED(trim_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_TRIM_OKPACKET_TRANSFORM_PLUGIN_H
