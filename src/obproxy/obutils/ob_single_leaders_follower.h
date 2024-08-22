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

#ifndef OB_SINGLE_LEADERS_FOLLOWER_H_
#define OB_SINGLE_LEADERS_FOLLOWER_H_
#include "lib/ob_define.h"
#include "iocore/net/ob_inet.h"
#include "proxy/route/ob_ldc_struct.h"
namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
namespace obutils
{
class ObSingleLeadersFollower
{
public:
  ObSingleLeadersFollower() : addr_(), replica_type_(common::REPLICA_TYPE_FULL) {}
  net::ObIpEndpoint addr_;
  common::ObReplicaType replica_type_;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(addr), "replica_type", ObProxyReplicaLocation::get_replica_type_string(replica_type_));
    J_OBJ_END();
    return pos;
  }

};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif