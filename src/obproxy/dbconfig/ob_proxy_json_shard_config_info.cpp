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

#include "dbconfig/ob_proxy_json_shard_config_info.h"
#include "utils/ob_proxy_utils.h"
#include "lib/number/ob_number_v2.h"
#include "lib/hash_func/murmur_hash.h"

using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

//------ ObProxyShardRuleInfo ------
DEF_TO_STRING(ObProxyShardRuleInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(shard_rule_str));
  J_OBJ_END();
  return pos;
}

ObProxyShardRuleInfo::ObProxyShardRuleInfo()
  : shard_rule_str_(), expr_(NULL)
{
  reset();
}

bool ObProxyShardRuleInfo::is_valid() const
{
  if (NULL != expr_) {
    return true;
  } else {
    return false;
  }
}

}//end of namespace obutils
}//end of namespace obproxy
}//end of namespace oceanbase
