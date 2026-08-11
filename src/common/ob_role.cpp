/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON

#include "ob_role.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{

static const char *role_strs[] = { "invalid_role", "leader", "follower" };
const char *role2str(ObRole role)
{
  const char *role_str = NULL;
  if (role < INVALID_ROLE || role > FOLLOWER) {
    LOG_EDIAG("fatal error, unknown role", K(role));
  } else {
    role_str = role_strs[role];
  }
  return role_str;
}

int str2role(const char *role_str, ObRole &role)
{
  int ret = OB_SUCCESS;
  role = INVALID_ROLE;
  if (NULL == role_str) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("empty role_str", KP(role_str), K(ret));
  } else {
    for (int64_t i = 0; i <= FOLLOWER; ++i) {
      if (0 == strncasecmp(role_strs[i], role_str, strlen(role_strs[i]))) {
        role = static_cast<ObRole>(i);
      }
    }
  }
  return ret;
}

}//end namespace common
}//end namespace oceanbase

