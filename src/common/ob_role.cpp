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
    LOG_ERROR("fatal error, unknown role", K(role));
  } else {
    role_str = role_strs[role];
  }
  return role_str;
}

ObRole str2role(const char *role_str)
{
  ObRole role = INVALID_ROLE;
  if (NULL == role_str) {
    LOG_ERROR("empty role_str", KP(role_str));
  } else {
    for (int64_t i = 0; i <= FOLLOWER; ++i) {
      if (0 == strncasecmp(role_strs[i], role_str, strlen(role_strs[i]))) {
        role = static_cast<ObRole>(i);
      }
    }
  }
  return role;
}

}//end namespace common
}//end namespace oceanbase

