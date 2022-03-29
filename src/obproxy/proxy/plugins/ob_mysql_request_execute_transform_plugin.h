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

#ifndef OBPROXY_MYSQL_REQUEST_EXECUTE_TRANSFORM_PLUGIN_H
#define OBPROXY_MYSQL_REQUEST_EXECUTE_TRANSFORM_PLUGIN_H

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

class ObMysqlRequestExecuteTransformPlugin : public ObTransformationPlugin
{
public:
  static ObMysqlRequestExecuteTransformPlugin *alloc(ObApiTransaction &transaction, int64_t ps_pkt_len);

  ObMysqlRequestExecuteTransformPlugin(ObApiTransaction &transaction, int64_t ps_pkt_len);

  virtual void destroy();

  // this func can not consume the reader, super class will do it
  virtual int consume(event::ObIOBufferReader *reader);

  virtual void handle_input_complete();

private:
  int produce_data(event::ObIOBufferReader *reader, const int64_t produce_size);

  int analyze_without_flag(const int64_t local_read_avail);
  int analyze_with_flag(const int64_t local_read_avail);
  int analyze_ps_execute_request(const int64_t local_read_avail);

  int rewrite_pkt_length_and_flag();
  int produce_param_type();

private:
  int64_t ps_pkt_len_;
  bool is_handle_finished_;
  int64_t param_type_pos_; // The starting position of param type in the package
  int64_t param_num_;
  int64_t param_offset_;
  int8_t new_param_bound_flag_;
  ObSqlString param_type_buf_;
  ObPsIdEntry *ps_id_entry_;
  event::ObIOBufferReader *local_reader_;
  bool is_first_analyze_with_flag_;
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRequestExecuteTransformPlugin);
};

class ObMysqlRequestExecuteGlobalPlugin : public ObGlobalPlugin
{
public:
  static ObMysqlRequestExecuteGlobalPlugin *alloc()
  {
    return op_reclaim_alloc(ObMysqlRequestExecuteGlobalPlugin);
  }

  ObMysqlRequestExecuteGlobalPlugin()
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
      // Contains the size of the header
      int64_t req_pkt_len = transaction.get_sm()->trans_state_.trans_info_.request_content_length_;
      plugin = ObMysqlRequestExecuteTransformPlugin::alloc(transaction, req_pkt_len);
      if (NULL != plugin) {
        transaction.add_plugin(plugin);
        PROXY_API_LOG(DEBUG, "add ObMysqlRequestExecuteTransformPlugin", K(plugin), K(req_pkt_len));
      } else {
        PROXY_API_LOG(ERROR, "fail to allocate memory for ObMysqlRequestExecuteTransformPlugin");
      }
    } else {
      PROXY_API_LOG(DEBUG, "handle_read_request, no need setup ObMysqlRequestExecuteTransformPlugin");
    }

    transaction.resume();
  }

  inline bool need_enable_plugin(ObMysqlSM *sm) const
  {
    PROXY_API_LOG(DEBUG, "need_enable_plugin",
                  "request_content_length", sm->trans_state_.trans_info_.request_content_length_,
                  "mysql_cmd", get_mysql_cmd_str(sm->trans_state_.trans_info_.sql_cmd_));

    bool bret = false;

    if (!sm->trans_state_.trans_info_.client_request_.is_internal_cmd()
        && sm->trans_state_.trans_info_.request_content_length_ > 0
        && obmysql::OB_MYSQL_COM_STMT_EXECUTE == sm->trans_state_.trans_info_.sql_cmd_) {
      ObClientSessionInfo &cs_info = sm->get_client_session()->get_session_info();
      ObPsIdEntry* ps_id_entry = cs_info.get_ps_id_entry();
      if (OB_ISNULL(ps_id_entry)) {
        PROXY_API_LOG(WARN, "client ps id entry is null");
      } else {
        int64_t param_num = ps_id_entry->get_param_count();
        if (param_num > 0) {
          bret = true;
        }
      }
    }

    return bret;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMysqlRequestExecuteGlobalPlugin);
};

void init_mysql_request_execute_transform()
{
  PROXY_API_LOG(INFO, "init mysql request execute transformation plugin");
  ObMysqlRequestExecuteGlobalPlugin *execute_transform = ObMysqlRequestExecuteGlobalPlugin::alloc();
  UNUSED(execute_transform);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_REQUEST_EXECUTE_TRANSFORM_PLUGIN_H
