/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY
#include "obutils/ob_proxy_reload_config.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
// use to enable proxy config valid, after config changed
int ObProxyReloadConfig::operator()(ObProxyConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reloader_)) {
     ret = OB_NOT_INIT;
     LOG_WDIAG("ObPrxyReloadConfig is not inited", K_(reloader), K(ret));
  } else {
    if (OB_FAIL(reloader_->do_reload_config(config))) {
      LOG_WDIAG("fail to reload config", K(ret));
    }
    config.print();
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
