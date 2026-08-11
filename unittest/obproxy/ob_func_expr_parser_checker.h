/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/page_arena.h"
//#include "sql/parser/ob_parser.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "opsql/func_expr_parser/ob_func_expr_parser.h"

namespace oceanbase
{
namespace obproxy
{
namespace test
{
class ObFuncExprParserChecker
{
public:
  ObFuncExprParserChecker();
  ~ObFuncExprParserChecker() {}

  // parse sql
  int do_obproxy_parser(const common::ObString &query_str, ObFuncExprParseResult &result);
  int do_obproxy_resolve(const common::ObString &query_str, ObFuncExprParseResult &result);

  void print_stat();

  // variables
  bool is_verbose_;
  bool need_resolve_;
  ObFuncExprParseMode parse_mode_;
  // total/succ count in a file
  int64_t total_count_;
  int64_t succ_count_;
  // parse time during this run
  int64_t parse_time_;
  int64_t resolve_time_;
  bool is_oracle_mode_;
  const char *result_file_name_;
  FILE *result_file_;
  common::ObArenaAllocator allocator_;
private:
  ObProxyParseString get_value(std::string &extra_str, const char* key_name, std::size_t &pos);
};

} // end of namespace test
} // end of namespace obproxy
} // end of namespace oceanbase
