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

#include "share/config/ob_reload_config.h"

namespace oceanbase
{
namespace common
{
int ObReloadConfig::reload_ob_logger_set()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(conf_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WDIAG, "server config is null", K(ret));
  } else {
    if (OB_FAIL(OB_LOGGER.parse_set(conf_->syslog_level,
                                    static_cast<int32_t>(STRLEN(conf_->syslog_level)),
                                    (conf_->syslog_level).version()))) {
      OB_LOG(EDIAG, "fail to parse_set syslog_level",
             K(conf_->syslog_level.str()), K((conf_->syslog_level).version()), K(ret));
    } else {
      OB_LOGGER.set_log_warn(conf_->enable_syslog_wf);
      ObKVGlobalCache::get_instance().reload_priority();
    }
  }
  return ret;
}

}//end of common
}//end of oceanbase
