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

#define private public
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

  const char *result_file_name_;
  FILE *result_file_;
  common::ObArenaAllocator allocator_;
private:
  ObProxyParseString get_value(std::string &extra_str, const char* key_name, std::size_t &pos);
};

} // end of namespace test
} // end of namespace obproxy
} // end of namespace oceanbase
