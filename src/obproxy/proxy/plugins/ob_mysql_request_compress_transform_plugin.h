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

#ifndef OBPROXY_MYSQL_REQUEST_COMPRESS_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_REQUEST_COMPRESS_TRANSFORM_PLUGIN_H

#include "proxy/api/ob_global_plugin.h"
#include "proxy/api/ob_transformation_plugin.h"
#include "proxy/mysqllib/ob_proxy_session_info_handler.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

// covert standart mysql protocol to compress protocol
class ObMysqlRequestCompressTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlRequestCompressTransformPlugin *alloc(ObApiTransaction &transaction);

  explicit ObMysqlRequestCompressTransformPlugin(ObApiTransaction &transaction);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  int build_compressed_packet(bool is_last_segment);
  int check_last_data_segment(event::ObIOBufferReader &reader, bool &is_last_segment);

private:
  // every when received reqeust data >= MIN_COMPRESS_DATA_SIZE,
  // we will compress it and send to server
  static const int64_t MIN_COMPRESS_DATA_SIZE = 8 * 1024;

  event::ObIOBufferReader *local_reader_;
  event::ObIOBufferReader *local_transfer_reader_;
  event::ObMIOBuffer *mio_buffer_;
  uint8_t compressed_seq_;
  uint32_t request_id_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRequestCompressTransformPlugin);
};

class ObMysqlRequestCompressGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlRequestCompressGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlRequestCompressGlobalPlugin);
  }

  ObMysqlRequestCompressGlobalPlugin()
  {
    register_hook(HOOK_READ_REQUEST);
  }

  virtual void destroy()
  {
    ObGlobalPlugin::destroy();
    op_reclaim_free(this);
  }

  virtual void handle_read_request(ObApiTransaction &transaction)
  {
    ObTransactionPlugin *plugin = NULL;

    if (need_enable_plugin(transaction.get_sm())) {
      plugin = ObMysqlRequestCompressTransformPlugin::alloc(transaction);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlRequestCompressTransformPlugin", K(plugin));
      } else {
        PROXY_API_LOG(ERROR, "fail to allocate memory for ObMysqlRequestCompressTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "handle_read_request, no need setup ObMysqlRequestCompressTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    PROXY_API_LOG(DEBUG, "need_enable_plugin",
                  "request_content_length", sm->trans_state_.trans_info_.request_content_length_,
                  "enable_compression_protocol", sm->trans_state_.mysql_config_params_->enable_compression_protocol_,
                  "enable_ob_protocol_v2", sm->is_enable_ob_protocol_v2());
    return (sm->trans_state_.trans_info_.request_content_length_ > 0
            && (sm->trans_state_.mysql_config_params_->enable_compression_protocol_
                || sm->is_enable_ob_protocol_v2()));
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRequestCompressGlobalPlugin);
};

void init_mysql_request_compress_transform()
{
  PROXY_API_LOG(INFO, "init mysql request compress transformation plugin");
  ObMysqlRequestCompressGlobalPlugin *compress_transform = ObMysqlRequestCompressGlobalPlugin::alloc();
  UNUSED(compress_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_REQUEST_COMPRESS_TRANSFORM_PLUGIN_H
