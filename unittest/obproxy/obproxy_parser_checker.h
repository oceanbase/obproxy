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
#include "opsql/parser/ob_proxy_parser.h"

namespace oceanbase
{
namespace obproxy
{
namespace test
{
class ObProxyParseResultWapper
{
public:
  ObProxyParseResultWapper() : expected_parsed_length_(0) { reset(); }
  ~ObProxyParseResultWapper() {}
  int load_result(ObProxyParseResult *orig_result)
  {
    int ret = parse_result_.load_result(*orig_result);
    if (OB_LIKELY(NULL != orig_result->table_info_.table_name_.str_)
        && OB_LIKELY(orig_result->table_info_.table_name_.str_len_ > 0)) {
      if (OB_LIKELY(NULL != orig_result->table_info_.alias_name_.str_)
          && OB_LIKELY(orig_result->table_info_.alias_name_.str_len_ > 0)) {
        expected_parsed_length_ = parse_result_.get_parsed_length();
      } else {
        expected_parsed_length_ = parse_result_.get_parsed_length();
      }
    } else {
      expected_parsed_length_ = parse_result_.get_parsed_length();
    }
    if (expected_parsed_length_ != parse_result_.get_parsed_length()) {
      ret = common::OB_ERR_UNEXPECTED;
    }
    return ret;
  }
  void reset() { parse_result_.reset(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int64_t expected_parsed_length_;
  obutils::ObSqlParseResult parse_result_;
};

class ObProxyParserChecker
{
public:
  ObProxyParserChecker();
  ~ObProxyParserChecker() {}

  // parse sql
  bool run_parse_string(const common::ObString query_str,
                        common::ObCollationType connection_collation);
  bool run_parse_std_string(std::string query_str,
                            common::ObCollationType connection_collation);
  bool run_parse_file(const char *filename,
                      common::ObCollationType connection_collation);
  int do_obproxy_parser(const common::ObString &query_str,
                        ObProxyParseResultWapper &result,
                        common::ObCollationType connection_collation);

  void print_stat();

  // variables
  bool is_verbose_;
  // total/succ count in a file
  int64_t total_count_;
  int64_t succ_count_;
  // proxy/sql parse time during this run
  int64_t proxy_parse_time_;

  const char *result_file_name_;
  FILE *result_file_;
  common::ObArenaAllocator allocator_;
};

} // end of namespace test
} // end of namespace obproxy
} // end of namespace oceanbase
