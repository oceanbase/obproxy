/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBPROXY_OB_API_H
#define OBPROXY_OB_API_H

#include "proxy/api/ob_api_defs.h"
#include "proxy/api/ob_iocore_api.h"
#include "proxy/api/ob_api_internal.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

#define DEBUG_API(fmt...) _PROXY_API_LOG(DEBUG, ##fmt)
#define INFO_API(fmt...) _PROXY_API_LOG(INFO, ##fmt)
#define WARN_API(fmt...) _PROXY_API_LOG(WDIAG, ##fmt)
#define ERROR_API(fmt...) _PROXY_API_LOG(EDIAG, ##fmt)

int api_init();

class ObMysqlApiDebugNames
{
public:
  static const char *get_api_hook_name(ObMysqlHookID t);
};

extern ObMysqlAPIHooks *mysql_global_hooks;
extern ObRecRawStatBlock *api_rsb;

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_OB_API_H
