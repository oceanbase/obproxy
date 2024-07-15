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

#include "proxy/api/ob_api_transaction.h"
#include "proxy/mysql/ob_mysql_sm.h"
#include "proxy/api/ob_api_utils_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;
using namespace net;

ObApiTransaction::ObApiTransaction(ObMysqlSM *sm)
    : sm_(NULL), client_request_(NULL), server_request_(NULL), server_resp_result_(NULL),
      client_resp_result_(NULL)
{
  if (NULL == sm) {
    WARN_API("invalid argument, sm=%p", sm);
  } else {
    DEBUG_API("ObApiTransaction mysqlsm=%p constructing ObApiTransaction object %p", sm, this);
    sm_ = sm;
    client_request_ = &sm_->trans_state_.trans_info_.client_request_;
    server_request_ = &sm_->trans_state_.trans_info_.client_request_;
    server_resp_result_ = &sm_->trans_state_.trans_info_.resp_result_;
    client_resp_result_ = &sm_->trans_state_.trans_info_.resp_result_;
  }
}

void ObApiTransaction::reset()
{
  DEBUG_API("ObApiTransaction mysqlsm=%p destroying ObApiTransaction object %p", sm_, this);
  ObTransactionPlugin *tmp_plugin = NULL;
  for (ObTransactionPlugin *plugin = plugins_.head_; NULL != plugin;) {
    DEBUG_API("Locking TransacitonPlugin mutex to delete transaction plugin at %p", plugin);
    MUTEX_LOCK(lock, plugin->get_mutex(), this_ethread());
    DEBUG_API("Locked Mutex... Deleting transaction plugin at %p", plugin);

    // get next first, then destroy current plugin
    tmp_plugin = plugin;
    plugin = plugins_.next(plugin);
    tmp_plugin->destroy();
  }
  plugins_.reset();
}

void ObApiTransaction::destroy()
{
  reset();
  op_reclaim_free(this);
}

void ObApiTransaction::resume()
{
  ObApiUtilsInternal::mysqlsm_reenable(sm_, OB_EVENT_MYSQL_CONTINUE);
}

void ObApiTransaction::error()
{
  DEBUG_API("ObApiTransaction mysqlsm=%p reenabling to error state", sm_);
  ObApiUtilsInternal::mysqlsm_reenable(sm_, OB_EVENT_MYSQL_ERROR);
}

void ObApiTransaction::error(ObMIOBuffer *error_packet)
{
  set_error_body(error_packet);
  error(); // finally, reenable with MYSQL_ERROR
}

void ObApiTransaction::set_error_body(ObMIOBuffer *error_packet)
{
  UNUSED(error_packet);
  DEBUG_API("ObApiTransaction mysqlsm=%p setting error packet", sm_);
  // TODO: set the internal_msg_buffer
}

bool ObApiTransaction::is_internal_cmd() const
{
  // TODO: read the flag from client request
  return client_request_->is_internal_cmd();
}

void ObApiTransaction::add_plugin(ObTransactionPlugin *plugin)
{
  DEBUG_API("ObApiTransaction mysqlsm=%p registering new ObTransactionPlugin %p.", sm_, plugin);
  plugins_.push(plugin);
}

//const sockaddr *ObApiTransaction::get_incoming_address() const
//{
//  const sockaddr *ret = NULL;
//  ObMysqlClientSession *client_session = sm_->client_session_;
//  ObNetVConnection *vc = NULL;
//  if (NULL != client_session) {
//    vc = client_session->get_netvc();
//    if (NULL != vc) {
//      ret = vc->get_local_addr();
//    }
//  }
//  return ret;
//}
//
//const sockaddr *ObApiTransaction::get_client_address() const
//{
//  const sockaddr *ret = NULL;
//  ObMysqlClientSession *client_session = sm_->client_session_;
//  ObNetVConnection *vc = NULL;
//  if (NULL != client_session) {
//    vc = client_session->get_netvc();
//    if (NULL != vc) {
//      ret = vc->get_remote_addr();
//    }
//  }
//  return ret;
//}
//
//const sockaddr *ObApiTransaction::get_server_address() const
//{
//  return &sm_->trans_state_.server_info_.addr_.sa_;
//}
//
//bool ObApiTransaction::set_server_address(const sockaddr &addr)
//{
//  int ret = OB_SUCCESS;
//  // ops_ip_copy will copy both ip and port
//  if (ops_ip_copy(sm_->trans_state_.server_info_.addr_.sa_, addr)) {
//    sm_->trans_state_.api_server_addr_set_ = true;
//    ret = OB_SUCCESS;
//  } else {
//    ret = OB_ERROR;
//  }
//  return ret;
//}

bool ObApiTransaction::set_incoming_port(uint16_t port)
{
  ops_ip_port_cast(sm_->trans_state_.server_info_.addr_.sa_) = (htons)(port);
  return true;
}

void ObApiTransaction::set_timeout(ObApiTransaction::ObTimeoutType type, int32_t time_ms)
{
  switch (type) {
    case TIMEOUT_CONNECT:
      sm_->trans_state_.api_txn_connect_timeout_value_ = time_ms;
      break;

    case TIMEOUT_NO_ACTIVITY:
      sm_->trans_state_.api_txn_no_activity_timeout_value_ = time_ms;
      break;

    case TIMEOUT_ACTIVE:
      sm_->trans_state_.api_txn_active_timeout_value_ = time_ms;
      break;

    default:
      break;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
