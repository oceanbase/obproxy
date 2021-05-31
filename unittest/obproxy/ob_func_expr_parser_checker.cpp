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
#include "ob_func_expr_parser_checker.h"
#include "lib/utility/ob_print_utils.h"
#include "opsql/func_expr_parser/ob_func_expr_parser_utils.h"
#include <string>
#include <dirent.h>
#include <sys/types.h>

using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;


namespace oceanbase
{
namespace obproxy
{
namespace test
{

ObFuncExprParserChecker::ObFuncExprParserChecker() : is_verbose_(true),
                                             need_resolve_(false),
                                             parse_mode_(GENERATE_FUNC_PARSE_MODE),
                                             total_count_(0), succ_count_(0),
                                             parse_time_(0),
                                             result_file_name_(""), result_file_(NULL),
                                             allocator_(common::ObModIds::TEST)
{
}

int ObFuncExprParserChecker::do_obproxy_parser(const ObString &query_str, ObFuncExprParseResult &result)
{
  int ret = OB_SUCCESS;

  int64_t t0 = ObTimeUtility::current_time();
  ObFuncExprParser parser(allocator_, parse_mode_);
  if (OB_FAIL(parser.parse(query_str, result))) {
    // do nothing
  } else {
    LOG_INFO("succ to parse function expr", "parse result:", ObFuncExprParseResultPrintWrapper(result));
  }

  // record parse time
  int64_t t1 = ObTimeUtility::current_time();
  parse_time_ += (t1 - t0);

  return ret;
}

ObProxyParseString ObFuncExprParserChecker::get_value(std::string &extra_str, const char* key_name,
                                                      std::size_t &pos)
{
  ObProxyParseString ret_str;
  ret_str.str_len_ = 0;
  std::size_t key_index = extra_str.find(key_name, pos);
  if (key_index != std::string::npos) {
    std::size_t value_begin = extra_str.find_first_not_of("\r\f\n\t :", key_index + strlen(key_name));
    if (std::string::npos != value_begin) {
      std::size_t value_end = extra_str.find(",", value_begin);
      value_end = (value_end == std::string::npos) ? extra_str.length() - 1 : value_end - 1;
      ret_str.str_ = const_cast<char *>(extra_str.c_str() + value_begin);
      ret_str.str_len_ = static_cast<int32_t>(value_end - value_begin + 1);
      pos = value_end + 1;
    } else {
      pos = std::string::npos;
    }
  } else {
    pos = std::string::npos;
  }
  return ret_str;
}

void ObFuncExprParserChecker::print_stat()
{
  printf("sql count %ld\n", total_count_);
  printf("parser time %lf (us)\n", (double)(parse_time_) / (double)(total_count_));
  if (need_resolve_) {
    printf("resolve time %lf (us)\n", (double)(resolve_time_) / (double)(total_count_));
  }
}

} // end of namespace test
} // end of namespace obproxy
} // end of namespace oceanbase

extern int obfuncexprdebug;
using namespace oceanbase::obproxy::test;
int main(int argc, char **argv)
{
  int ret = 0;
  ObFuncExprParserChecker checker;
  int c = -1;
  const char *input_str = "";
  while(-1 != (c = getopt(argc, argv, "e:s:n:r:SDRI"))) {
    switch(c) {
      case 's':
        input_str = optarg;
        break;
      case 'S':
        checker.is_verbose_ = false;
        break;
      case 'D':
        obfuncexprdebug = 1;
        oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
        OB_LOGGER.set_log_level("DEBUG");
        break;
      case 'r':
        checker.result_file_name_ = optarg;
        break;
    }
  }
  ObFuncExprParseResult result;
  std::string query_str(input_str);
  ObString input_query(query_str.size(), query_str.c_str());
  checker.do_obproxy_parser(input_query, result);

  checker.print_stat();
  return ret;
}
