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

#ifndef OBPROXY_JSON_SHARD_CONFIG_INFO_H
#define OBPROXY_JSON_SHARD_CONFIG_INFO_H

#include "obutils/ob_proxy_json_config_info.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "lib/ob_define.h"
#include "lib/hash_func/murmur_hash.h"

namespace oceanbase
{
namespace obproxy
{

namespace opsql
{
class ObProxyExpr;
}

namespace dbconfig
{

class ObProxyShardRuleInfo
{
public:
  ObProxyShardRuleInfo();
  virtual ~ObProxyShardRuleInfo() { }

  void destroy() { op_free(this); }
  bool is_valid() const;

  void reset()
  {
    shard_rule_str_.reset();
    expr_ = NULL;
  }

  int assign(const ObProxyShardRuleInfo &other)
  {
    reset();
    shard_rule_str_.set_value(other.shard_rule_str_);
    expr_ = other.expr_;
    return common::OB_SUCCESS;
  }

  DECLARE_TO_STRING;

public:
  static const int64_t MAX_RULES_COUNT = 16;

  obutils::ObProxyConfigString shard_rule_str_; //save sharding expr
  opsql::ObProxyExpr *expr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyShardRuleInfo);
};

typedef common::ObSEArray<ObProxyShardRuleInfo, ObProxyShardRuleInfo::MAX_RULES_COUNT> ObProxyShardRuleList;

}//end of namespace dbconfig
}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_JSON_CONFIG_INFO_H */
