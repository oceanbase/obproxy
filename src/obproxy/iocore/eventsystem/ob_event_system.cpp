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

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
int init_event_system(ObModuleVersion version)
{
  int ret = OB_SUCCESS;
  op_reclaim_opt(ObMIOBuffer, ENABLE_RECLAIM, 1);
  op_reclaim_opt(ObIOBufferBlock, ENABLE_RECLAIM, 1);
  op_reclaim_opt(ObIOBufferData, ENABLE_RECLAIM, 1);
  op_reclaim_opt(ObProxyMutex, ENABLE_RECLAIM, 1);
  if (OB_FAIL(check_module_version(version, EVENT_SYSTEM_MODULE_VERSION))) {
    LOG_WARN("failed to check module version", K(version), K(ret));
  }
  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
