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

#ifndef OBPROXY_MYSQL_RESPONSE_COMPRESS_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_RESPONSE_COMPRESS_TRANSFORM_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysqllib/ob_resultset_stream_analyzer.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObMysqlResponseCompressTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlResponseCompressTransformPlugin *alloc(ObApiTransaction &transaction);

  explicit ObMysqlResponseCompressTransformPlugin(ObApiTransaction &transaction);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  bool is_analyze_compressed_ob20;
  uint8_t req_seq_;
  uint32_t request_id_;
  uint32_t server_sessid_;
  event::ObIOBufferReader *local_reader_;
  event::ObIOBufferReader *local_transfer_reader_;
  ObMysqlCompressAnalyzer compress_analyzer_;
  ObMysqlCompressOB20Analyzer compress_ob20_analyzer_;
  ObMysqlCompressAnalyzer *analyzer_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponseCompressTransformPlugin);
};

class ObMysqlResponseCompressGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlResponseCompressGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlResponseCompressGlobalPlugin);
  }

  ObMysqlResponseCompressGlobalPlugin()
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
      plugin = ObMysqlResponseCompressTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlResponseCompressTransformPlugin", K(plugin));
      } else {
        PROXY_API_LOG(EDIAG, "fail to allocate memory for ObMysqlResponseCompressTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "no need setup ObMysqlResponseCompressTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    return (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && NULL != sm->client_session_
            && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
            && !sm->trans_state_.trans_info_.server_response_.get_analyze_result().is_decompressed()
            && (ObProxyProtocol::PROTOCOL_CHECKSUM == sm->get_server_session_protocol()
                || ObProxyProtocol::PROTOCOL_OB20 == sm->get_server_session_protocol()));
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponseCompressGlobalPlugin);
};

void init_mysql_resposne_compress_transform()
{
  PROXY_API_LOG(INFO, "init mysql response compress transformation plugin");
  ObMysqlResponseCompressGlobalPlugin *compress_transform = ObMysqlResponseCompressGlobalPlugin::alloc();
  UNUSED(compress_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_RESPONSE_COMPRESS_TRANSFORM_PLUGIN_H
