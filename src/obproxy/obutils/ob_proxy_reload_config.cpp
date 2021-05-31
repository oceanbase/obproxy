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
     LOG_WARN("ObPrxyReloadConfig is not inited", K_(reloader), K(ret));
  } else {
    if (OB_FAIL(reloader_->do_reload_config(config))) {
      LOG_WARN("fail to reload config", K(ret));
    }
    config.print();
  }

  return ret;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
