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

#ifndef OBPROXY_MYSQL_RESPONSE_OB20_PROTOCOL_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_RESPONSE_OB20_PROTOCOL_TRANSFORM_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

enum Ob20ProtolTransPluginState {
  OB20_PLUGIN_INIT_STATE,         // init, at least need mysql head(4)
  OB20_PLUGIN_CONT_STATE,         // recevied mysql head done, total mysql packet has not received done yet
  OB20_PLUGIN_FIN_STATE           // mysql packet handled done, need handle tail crc, then reset to init state
};

class ObMysqlResponseOb20ProtocolTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlResponseOb20ProtocolTransformPlugin *alloc(ObApiTransaction &transaction);

  explicit ObMysqlResponseOb20ProtocolTransformPlugin(ObApiTransaction &transaction);

  virtual void destroy();

  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  int alloc_iobuffer_inner(event::ObIOBufferReader *reader);
  void reset();

  int build_ob20_head_and_extra(event::ObMIOBuffer *write_buf, common::ObIArray<ObObJKV> &extra_info,
                                bool is_last_packet);
  int build_ob20_body(event::ObIOBufferReader *reader, int64_t reader_avail, event::ObMIOBuffer *write_buf,
                      int64_t write_len, uint64_t &crc);

private:
  uint64_t tail_crc_;
  int64_t target_payload_len_;
  int64_t handled_payload_len_;
  Ob20ProtolTransPluginState state_;

  event::ObIOBufferReader *local_reader_;
  event::ObMIOBuffer *mysql_decompress_buffer_;                  // save temp result, after produce, consume all
  event::ObIOBufferReader *mysql_decompress_buffer_reader_;  // reader of mysql_decompress_buffer_

  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponseOb20ProtocolTransformPlugin);
};


class ObMysqlResponseOb20ProtocolGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlResponseOb20ProtocolGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlResponseOb20ProtocolGlobalPlugin);
  }

  ObMysqlResponseOb20ProtocolGlobalPlugin()
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
      plugin = ObMysqlResponseOb20ProtocolTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlResponseOb20ProtocolTransformPlugin", K(plugin));
      } else {
        PROXY_API_LOG(EDIAG, "fail to alloc mem for ObMysqlResponseOb20ProtocolTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "no need setup ObMysqlResponseOb20ProtocolTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    return (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
            && NULL != sm->client_session_
            // inner sql will received compeleted, no need plugin
            && !sm->client_session_->is_proxy_mysql_client_
            && ObMysqlTransact::SERVER_SEND_REQUEST == sm->trans_state_.current_.send_action_
            && ObProxyProtocol::PROTOCOL_OB20 == sm->get_client_session_protocol());
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlResponseOb20ProtocolGlobalPlugin);
};


void init_mysql_response_ob20_protocol_transform()
{
  PROXY_API_LOG(INFO, "init mysql response ob20 protocol transform plugin");
  ObMysqlResponseOb20ProtocolGlobalPlugin *ob20_prot_trans = ObMysqlResponseOb20ProtocolGlobalPlugin::alloc();
  UNUSED(ob20_prot_trans);
}

} // proxy
} // obproxy
} // oceanbase

#endif

