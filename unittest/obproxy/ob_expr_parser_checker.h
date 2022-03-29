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

#include "lib/allocator/page_arena.h"
//#include "sql/parser/ob_parser.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"

namespace oceanbase
{
namespace obproxy
{
namespace test
{
class ObExprParserChecker
{
public:
  ObExprParserChecker();
  ~ObExprParserChecker() {}

  // parse sql
  bool run_parse_string(const common::ObString query_str, std::string &extra_str, common::ObCollationType connection_collation);
  bool run_parse_std_string(std::string query_str, std::string extra_str, common::ObCollationType connection_collation);
  bool run_parse_file(const char *filepath, std::string extra_str, common::ObCollationType connection_collation);
  int do_obproxy_parser(const common::ObString &query_str, ObExprParseResult &result, common::ObCollationType connection_collation);
  int do_expr_resolver(opsql::ObExprResolverContext &ctx, opsql::ObExprResolverResult &result);

  void print_stat();

  // variables
  bool is_verbose_;
  bool need_resolve_;
  ObExprParseMode parse_mode_;
  ObProxyBasicStmtType stmt_type_;
  // total/succ count in a file
  int64_t total_count_;
  int64_t succ_count_;
  // parse time during this run
  int64_t parse_time_;
  int64_t resolve_time_;

  const char *result_file_name_;
  FILE *result_file_;
  common::ObArenaAllocator allocator_;
private:
  void build_schema(std::string &extra_str, ObExprParseResult &result);
  void build_ctx(std::string &extra_str, opsql::ObExprResolverContext &ctx);
  ObProxyParseString get_value(std::string &extra_str, const char* key_name, std::size_t &pos);
};

} // end of namespace test
} // end of namespace obproxy
} // end of namespace oceanbase
