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

#include <fstream>
#include <string>
#include <dirent.h>
#include <sys/types.h>
#include "lib/utility/ob_print_utils.h"
#include "ob_func_expr_parser_checker.h"
#include "opsql/func_expr_parser/ob_func_expr_parser_utils.h"
#include "opsql/func_expr_resolver/ob_func_expr_resolver.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_factory.h"
#include "lib/oblog/ob_log.h"
#define private public

using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

#define DUMP_RESULT(fmt, args...) if (is_verbose_) fprintf(result_file_ ? result_file_ : stdout, fmt, ##args)

namespace oceanbase
{
namespace obproxy
{
namespace test
{

ObFuncExprParserChecker::ObFuncExprParserChecker() : is_verbose_(true),
                                             need_resolve_(false),
                                             parse_mode_(SHARDING_EXPR_FUNC_PARSE_MODE),
                                             total_count_(0), succ_count_(0),
                                             parse_time_(0), is_oracle_mode_(false),
                                             result_file_name_(""), result_file_(NULL),
                                             allocator_(common::ObModIds::TEST)
{
}

int ObFuncExprParserChecker::do_obproxy_parser(const ObString &query_str, ObFuncExprParseResult &result)
{
  int ret = OB_SUCCESS;
  DUMP_RESULT("MODE:%s \n", is_oracle_mode_ ? "oracle" : "mysql");
  int64_t t0 = ObTimeUtility::current_time();
  ObFuncExprParser parser(allocator_, parse_mode_);
  if (OB_FAIL(parser.parse(query_str, result))) {
    // do nothing
  }

  // record parse time
  int64_t t1 = ObTimeUtility::current_time();
  parse_time_ += (t1 - t0);

  return ret;
}

int ObFuncExprParserChecker::do_obproxy_resolve(const ObString &query_str, ObFuncExprParseResult &result) {
  int ret = OB_SUCCESS;
  ObObj target_obj;
  if (OB_FAIL(ObProxyExprFactory::register_proxy_expr())) {
    printf("fail register proxy expr: %d\n", ret);
  } else {
    ObFuncExprParser parser(allocator_, SHARDING_EXPR_FUNC_PARSE_MODE);
    ObFuncExprParseResult result;
    ObClientSessionInfo client_session_info;
    ObProxyExprFactory factory(allocator_);
    ObFuncExprResolverContext ctx(&allocator_, &factory);
    ObFuncExprResolver resolver(ctx);
    ObProxyExpr *expr;

    if (OB_FAIL(parser.parse(query_str, result))) {
      printf("expr parse failed: %d\n", ret);
    } else if (OB_FAIL(resolver.resolve(result.param_node_, expr))) {
      printf("expr resolve failed: %d\n", ret);
    } else {
      ObSEArray<ObObj, 4> result_array;
      SqlFieldResult sql_field_result;
      ObProxyExprCalcItem calc_item(&sql_field_result);
      ObProxyExprCtx expr_ctx(0, dbconfig::TESTLOAD_NON, false, &allocator_, &client_session_info);
      expr_ctx.is_oracle_mode = is_oracle_mode_;
      if (OB_FAIL(expr->calc(expr_ctx, calc_item, result_array))) {
        printf("calc expr result failed: %d\n", ret);
      } else if (OB_FAIL(result_array.at(0, target_obj))) {
        printf("get result failed: %d\n", ret);
      }
    }
  }
  DUMP_RESULT("SQL   : %.*s\n", query_str.length(), query_str.ptr());
  if (OB_SUCC(ret)) {
    static const int64_t MAX_STR_LEN = 4096;
    static char result_buf[MAX_STR_LEN];
    static char obj_buf[MAX_STR_LEN];
    ObFuncExprParseResultPrintWrapper wrapper(result);
    DUMP_RESULT("PARSE RESULT:\n%.*s", static_cast<int32_t>(wrapper.to_string(result_buf, MAX_STR_LEN)), result_buf);;
    DUMP_RESULT("CALC RESULT:%.*s\n", static_cast<int32_t>(target_obj.to_string(obj_buf, MAX_STR_LEN)), obj_buf);
  } else {
    DUMP_RESULT("TOKEN CALC FAILED\n");
  }
  DUMP_RESULT("--------------------\n");
  return ret;
}

ObProxyParseString ObFuncExprParserChecker::get_value(std::string &extra_str, const char* key_name,
                                                      std::size_t &pos)
{
  ObProxyParseString ret_str;
  memset(&ret_str, 0, sizeof(ObProxyParseString));
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

int obfuncexprdebug;
using namespace oceanbase::obproxy::test;
int main(int argc, char **argv)
{
  int ret = 0;
  ObFuncExprParserChecker checker;
  int c = -1;
  const char *input_str = "";
  while(-1 != (c = getopt(argc, argv, "e:s:n:r:m:SDRI"))) {
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
      case 'm':
        checker.is_oracle_mode_ = (STRCMP(optarg, "oracle") == 0);
        break;
    }
  }

  if (NULL == checker.result_file_ && strlen(checker.result_file_name_) > 0) {
    checker.result_file_ = fopen(checker.result_file_name_, "a+");
  }
  ObFuncExprParseResult result;
  std::string query_str(input_str);
  ObString input_query(query_str.size(), query_str.c_str());
  checker.do_obproxy_parser(input_query, result);
  checker.do_obproxy_resolve(input_query, result);

  if (OB_NOT_NULL(checker.result_file_)) {
    fclose(checker.result_file_);
  }
  return ret;
}
