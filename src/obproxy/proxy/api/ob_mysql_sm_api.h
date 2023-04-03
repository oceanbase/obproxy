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

#ifndef OBPROXY_MYSQL_SM_API_H
#define OBPROXY_MYSQL_SM_API_H

#include "utils/ob_proxy_lib.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/mysql/ob_mysql_transact.h"
#include "proxy/mysql/ob_mysql_vctable.h"
#include "proxy/mysql/ob_mysql_tunnel.h"
#include "proxy/mysql/ob_mysql_client_session.h"
#include "proxy/api/ob_api_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define MYSQL_API_CONTINUE   (API_EVENT_EVENTS_START + 0)
#define MYSQL_API_ERROR      (API_EVENT_EVENTS_START + 1)

class ObMysqlSM;

struct ObMysqlTransformInfo
{
  ObMysqlTransformInfo() : entry_(NULL), vc_(NULL) { }
  ~ObMysqlTransformInfo() { }

  ObMysqlVCTableEntry *entry_;
  event::ObVConnection *vc_;
};

enum ObMysqlSMRequestTransferStatusType
{
  MYSQL_SM_REQUEST_TRANSFER_UNKNOWN = 0,
  MYSQL_SM_REQUEST_TRANSFER_CLIENT_FAIL = 1,
  MYSQL_SM_REQUEST_TRANSFER_SERVER_FAIL = 2,
  MYSQL_SM_REQUEST_TRANSFER_SUCCESS = 3,
  MYSQL_SM_REQUEST_TRANSFER_TRANSFORM_FAIL = 4
};

enum ObMysqlSMTransformStatusType
{
  MYSQL_SM_TRANSFORM_OPEN = 0,
  MYSQL_SM_TRANSFORM_CLOSED = 1,
  MYSQL_SM_TRANSFORM_FAIL = 2
};

enum ObMysqlApiStateType
{
  MYSQL_API_NO_CALLOUT,
  MYSQL_API_IN_CALLOUT,
  MYSQL_API_DEFERED_CLOSE,
  MYSQL_API_DEFERED_SERVER_ERROR
};

enum ObMysqlPluginTunnelType
{
  MYSQL_NO_PLUGIN_TUNNEL = 0,
  MYSQL_PLUGIN_AS_SERVER,
  MYSQL_PLUGIN_AS_INTERCEPT
};

class ObPluginVCCore;
class ObMysqlSMApi
{
  friend class ObMysqlSM;
public:
  ObMysqlSMApi();
  ~ObMysqlSMApi() { }

  void destroy();
  void reset();
  void transform_cleanup(const ObMysqlHookID hook, ObMysqlTransformInfo &info);

  int state_api_callout(int event, void *data);
  void do_api_callout_internal();

  // Called by transact. Synchronous.
  event::ObVConnection *do_response_transform_open();
  event::ObVConnection *do_request_transform_open();

  int state_request_wait_for_transform_read(int event, void *data);
  int state_response_wait_for_transform_read(int event, void *data);
  int state_common_wait_for_transform_read(ObMysqlTransformInfo &t_info,
                                           MysqlSMHandler tunnel_handler,
                                           int event, void *data);

  int tunnel_handler_transform_write(int event, ObMysqlTunnelConsumer &c);
  int tunnel_handler_transform_read(int event, ObMysqlTunnelProducer &p);
  int tunnel_handler_plugin_client(int event, ObMysqlTunnelConsumer &c);

  int setup_transform_to_server_transfer();
  int setup_server_transfer_to_transform();
  int setup_transfer_from_transform();

  int setup_plugin_clients(ObMysqlTunnelProducer &p);
  void plugin_clients_cleanup();

  void set_mysql_schedule(event::ObContinuation *cont);
  int get_mysql_schedule(int event, void *data);

  // Functions for manipulating api hooks
  void txn_hook_append(const ObMysqlHookID id, ObContInternal *cont);
  void txn_hook_prepend(const ObMysqlHookID id, ObContInternal *cont);
  ObAPIHook *txn_hook_get(const ObMysqlHookID id);
  void txn_destroy_hook(const ObMysqlHookID id);

public:
  // Tunneling request to plugin
  ObMysqlPluginTunnelType plugin_tunnel_type_;
  ObPluginVCCore *plugin_tunnel_;

  // The next two enable plugins to tag the state machine for
  // the purposes of logging so the instances can be correlated
  // with the source plugin.
  char const *plugin_tag_;
  int64_t plugin_id_;

private:
  ObMysqlSM *sm_;
  ObMysqlTransformInfo response_transform_info_;
  ObMysqlTransformInfo request_transform_info_;

  event::ObContinuation *schedule_cont_;

  // Set if plugin client are active.
  // Need primarily for cleanup.
  bool has_active_plugin_clients_;

  ObMysqlHookID cur_hook_id_;
  ObAPIHook *cur_hook_;

  int32_t cur_hook_count_;
  ObMysqlApiStateType callout_state_;

  // api_hooks must not be changed directly Use
  // txn_hook_{ap,pre}pend so hooks_set is updated
  ObMysqlAPIHooks api_hooks_;
};

inline void ObMysqlSMApi::transform_cleanup(const ObMysqlHookID hook, ObMysqlTransformInfo &info)
{
  ObAPIHook *t_hook = api_hooks_.get(hook);

  if (NULL != t_hook && NULL == info.vc_) {
    do {
      event::ObVConnection *t_vcon = t_hook->cont_;

      t_vcon->do_io_close();
      t_hook = t_hook->link_.next_;
    } while (NULL != t_hook);
  }
}


inline void ObMysqlSMApi::txn_hook_append(const ObMysqlHookID id, ObContInternal *cont)
{
  api_hooks_.append(id, cont);
}

inline void ObMysqlSMApi::txn_hook_prepend(const ObMysqlHookID id, ObContInternal *cont)
{
  api_hooks_.prepend(id, cont);
}

inline ObAPIHook *ObMysqlSMApi::txn_hook_get(const ObMysqlHookID id)
{
  return api_hooks_.get(id);
}

inline void ObMysqlSMApi::txn_destroy_hook(const ObMysqlHookID id)
{
  api_hooks_.destroy(id);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_SM_API_H
