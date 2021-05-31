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

#include "proxy/api/ob_api_plugin.h"
#include "proxy/api/ob_api_utils_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

const char *HOOK_TYPE_STRINGS[] = {
  "HOOK_SSN_START",
  "HOOK_TXN_START",
  "HOOK_READ_REQUEST",
  "HOOK_SEND_REQUEST",
  "HOOK_READ_RESPONSE",
  "HOOK_SEND_RESPONSE",
  "HOOK_OBSERVER_PL",
  "HOOK_CMD_COMPLETE",
  "HOOK_TXN_CLOSE",
  "HOOK_SSN_CLOSE"
};

void ObApiPlugin::handle_ssn_start(ObProxyClientSession *ssn)
{
  ObApiUtilsInternal::ssn_reenable(ssn, OB_EVENT_MYSQL_CONTINUE);
}

void ObApiPlugin::handle_txn_start(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_read_request(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_send_request(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_read_response(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_send_response(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_observer_partition_location(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_cmd_complete(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_txn_close(ObApiTransaction &transaction)
{
  transaction.resume();
}

void ObApiPlugin::handle_ssn_close(ObProxyClientSession *ssn)
{
  ObApiUtilsInternal::ssn_reenable(ssn, OB_EVENT_MYSQL_CONTINUE);
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
