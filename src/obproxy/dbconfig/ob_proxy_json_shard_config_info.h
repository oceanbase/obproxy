/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

  obutils::ObProxyConfigString shard_rule_str_; //保存配置中的分区表达式
  opsql::ObProxyExpr *expr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyShardRuleInfo);
};

typedef common::ObSEArray<ObProxyShardRuleInfo, ObProxyShardRuleInfo::MAX_RULES_COUNT> ObProxyShardRuleList;

}//end of namespace dbconfig
}//end of namespace obproxy
}//end of namespace oceanbase

#endif /* OBPROXY_JSON_CONFIG_INFO_H */
