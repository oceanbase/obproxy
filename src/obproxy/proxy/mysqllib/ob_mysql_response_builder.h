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

#ifndef OBPROXY_MYSQL_RESPONSE_BUILDER_H
#define OBPROXY_MYSQL_RESPONSE_BUILDER_H
#include "utils/ob_proxy_lib.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObMIOBuffer;
}
namespace proxy
{
class ObMysqlClientSession;
class ObClientSessionInfo;
class ObProxyMysqlRequest;
class ObMysqlResponseBuilder
{
public:
  static const common::ObString OBPROXY_ROUTE_ADDR_NAME;
  static const common::ObString OBPROXY_PROXY_VERSION_NAME;

  static int build_ok_resp(event::ObMIOBuffer &mio_buf, ObProxyMysqlRequest &client_request,
                           ObMysqlClientSession &client_session, const bool is_in_trans, const bool is_state_changed);

  static int build_start_trans_resp(event::ObMIOBuffer &mio_buf, ObProxyMysqlRequest &client_request,
                                    ObMysqlClientSession &client_session);

  static int build_select_tx_ro_resp(event::ObMIOBuffer &mio_buf,
                                     ObProxyMysqlRequest &client_request,
                                     ObMysqlClientSession &client_session,
                                     const bool is_in_trans);

  static int build_ok_resq_with_state_changed(event::ObMIOBuffer &mio_buf,
                                              ObProxyMysqlRequest &client_request,
                                              ObMysqlClientSession &client_session,
                                              const bool is_in_trans);

  static int build_select_route_addr_resp(event::ObMIOBuffer &mio_buf, ObProxyMysqlRequest &client_request,
                                          ObMysqlClientSession &client_session, const bool is_in_trans, int64_t addr);

  static int build_set_route_addr_resp(event::ObMIOBuffer &mio_buf,
                                       ObProxyMysqlRequest &client_request,
                                       ObMysqlClientSession &client_session,
                                       const bool is_in_trans);

  static int build_select_proxy_version_resp(event::ObMIOBuffer &mio_buf,
                                             ObProxyMysqlRequest &client_request,
                                             ObMysqlClientSession &client_session,
                                             const bool is_in_trans);
};

inline int ObMysqlResponseBuilder::build_start_trans_resp(event::ObMIOBuffer &mio_buf,
                                                          ObProxyMysqlRequest &client_request,
                                                          ObMysqlClientSession &client_session)
{
  static const bool is_in_trans = true;
  static const bool is_state_changed = false;
  return build_ok_resp(mio_buf, client_request, client_session, is_in_trans, is_state_changed);
}

inline int ObMysqlResponseBuilder::build_ok_resq_with_state_changed(event::ObMIOBuffer &mio_buf,
                                                                    ObProxyMysqlRequest &client_request,
                                                                    ObMysqlClientSession &client_session,
                                                                    const bool is_in_trans)
{
  static const bool is_state_changed = true;
  return build_ok_resp(mio_buf, client_request, client_session, is_in_trans, is_state_changed);
}

inline int ObMysqlResponseBuilder::build_set_route_addr_resp(event::ObMIOBuffer &mio_buf,
                                                             ObProxyMysqlRequest &client_request,
                                                             ObMysqlClientSession &client_session,
                                                             const bool is_in_trans)
{
  static const bool is_state_changed = false;
  return build_ok_resp(mio_buf, client_request, client_session, is_in_trans, is_state_changed);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_RESPONSE_BUILDER_H
