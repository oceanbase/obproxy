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

#ifndef OBPROXY_VERSION_H
#define OBPROXY_VERSION_H

#include <stdint.h>
#include "lib/ob_errno.h"
#include "common/ob_version.h"

namespace oceanbase
{
namespace obproxy
{

struct ObVersionNumber
{
  ObVersionNumber() {}
  ObVersionNumber(const int16_t major, const int16_t minor) : major_(major), minor_(minor) {}

  int16_t major_;          // incompatible change
  int16_t minor_;          // minor change, not incompatible
};

inline bool operator < (const ObVersionNumber &lhs, const ObVersionNumber &rhs) {
  return ((lhs.major_ < rhs.major_)
    || (lhs.major_ == rhs.major_ && lhs.minor_ < rhs.minor_));
}

enum ObModuleVersion
{
  MODULE_VERSION_MIN = 0,
  MODULE_VERSION_MAX = 2147483647
};

enum ObModuleHeaderType
{
  PUBLIC_MODULE_HEADER,
  PRIVATE_MODULE_HEADER
};

static inline ObModuleVersion make_module_version(
    int32_t major_version,
    int32_t minor_version,
    ObModuleHeaderType module_type)
{
  return (static_cast<ObModuleVersion>((module_type << 24) + (major_version << 16) +
                                       ((static_cast<int32_t>(minor_version)) << 8)));
}

static inline int32_t major_module_version(ObModuleVersion version)
{
  return (((static_cast<int32_t>(version)) >> 16) & 255);
}

static inline int32_t minor_module_version(ObModuleVersion version)
{
  return (((static_cast<int32_t>(version)) >> 8) & 255);
}

static inline int32_t module_version_type(ObModuleVersion version)
{
  return (((static_cast<int32_t>(version)) >> 24) & 127);
}

static inline int check_module_version(const ObModuleVersion userVersion, const ObModuleVersion libVersion)
{
  int ret = common::OB_SUCCESS;
  if (module_version_type(userVersion) == PUBLIC_MODULE_HEADER) {
    if ((major_module_version(userVersion) != major_module_version(libVersion))
        || (minor_module_version(userVersion) > minor_module_version(libVersion))) {
      ret = common::OB_ERROR;
    } else {
      ret = common::OB_SUCCESS;
    }
  } else if (module_version_type(userVersion) == PRIVATE_MODULE_HEADER) {
    if ((major_module_version(userVersion) != major_module_version(libVersion))
        || (minor_module_version(userVersion) != minor_module_version(libVersion))) {
      ret = common::OB_ERROR;
    } else {
      ret = common::OB_SUCCESS;
    }
  } else {
    ret = common::OB_ERROR;
  }
  return ret;
}

class ObAppVersionInfo
{
public:
  ObAppVersionInfo();
  ~ObAppVersionInfo() {};

  int setup(const char *pkg_name, const char *app_name, const char *app_version);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  bool defined_;
  char pkg_str_[128];
  char app_str_[128];
  char version_str_[128];
  char full_version_info_str_[256];
};

} //end of namespace obproxy
} //end of namespace oceanbase

#endif // OBPROXY_VERSION_H
