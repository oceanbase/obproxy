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
ObRole str2role(const char *role_str);

}//end namespace common
}//end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_ROLE_H_
