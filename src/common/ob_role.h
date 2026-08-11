/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_ROLE_H_
#define OCEANBASE_COMMON_OB_ROLE_H_

namespace oceanbase
{
namespace common
{
enum ObRole
{
  INVALID_ROLE = 0,
  LEADER = 1,
  FOLLOWER = 2,
};

const char *role2str(ObRole role);
int str2role(const char *role_str, ObRole &role);

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_ROLE_H_
