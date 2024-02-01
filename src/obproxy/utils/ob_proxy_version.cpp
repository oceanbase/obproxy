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

#include "utils/ob_proxy_version.h"
#include "utils/ob_proxy_lib.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{

ObAppVersionInfo::ObAppVersionInfo()
{
  defined_ = false;
  pkg_str_[0] = '?';
  pkg_str_[1] = '\0';
  app_str_[0] = '?';
  app_str_[1] = '\0';
  version_str_[0] = '?';
  version_str_[1] = '\0';
  full_version_info_str_[0] = '?';
  full_version_info_str_[1] = '\0';
}

int ObAppVersionInfo::setup(const char *pkg_name, const char *app_name, const char *app_version)
{
  int ret = OB_SUCCESS;
  int64_t pkg_pos = 0;
  int64_t app_pos = 0;
  int64_t version_pos = 0;
  int64_t full_version_pos = 0;

  if (OB_ISNULL(pkg_name) || OB_ISNULL(app_name) || OB_ISNULL(app_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_EDIAG("invalid argument", K(pkg_name), K(app_name), K(app_version), K(ret));
  } else {
    // now construct the version information
    if (OB_FAIL(databuff_printf(pkg_str_, sizeof(pkg_str_), pkg_pos, "%s", pkg_name))) {
      LOG_WDIAG("failed format pakage string", K(ret));
    } else if (OB_FAIL(databuff_printf(app_str_, sizeof(app_str_), app_pos, "%s", app_name))) {
      LOG_WDIAG("failed format app string", K(ret));
    } else if (OB_FAIL(databuff_printf(version_str_, sizeof(version_str_), version_pos, "%s", app_version))) {
      LOG_WDIAG("failed format version string", K(ret));
    }

    // the manager doesn't like empty strings, so prevent them
    if (OB_SUCC(ret)) {
      if ('\0' == pkg_str_[0]) {
        pkg_str_[0] = '?';
        pkg_str_[1] = '\0';
      }

      if ('\0' == app_str_[0]) {
        app_str_[0] = '?';
        app_str_[1] = '\0';
      }

      if ('\0' == version_str_[0]) {
        version_str_[0] = '?';
        version_str_[1] = '\0';
      }

      if ('\0' == full_version_info_str_[0]) {
        full_version_info_str_[0] = '?';
        full_version_info_str_[1] = '\0';
      }

      if (OB_FAIL(databuff_printf(full_version_info_str_, sizeof(full_version_info_str_),
                                  full_version_pos, "%s-%s-%s", app_str_, pkg_str_, version_str_))) {
        LOG_WDIAG("failed format full version info string", K(ret));
      } else {
        defined_ = true;
      }
    }

  }
  return ret;
}

int64_t ObAppVersionInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(defined), K_(pkg_str), K_(app_str), K_(version_str), K_(full_version_info_str));
  J_OBJ_END();
  return pos;
}

} //end of namespace obproxy
} //end of namespace oceanbase
