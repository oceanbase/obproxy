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

#include <fstream>
#include <iterator>
#include <vector>
#include <string>
#include <dirent.h>
#include <sys/types.h>
#include "ob_expr_parser_checker.h"
#define private public
#include "lib/utility/ob_print_utils.h"
#include "obproxy/opsql/expr_parser/ob_expr_parser_utils.h"
#include "obutils/ob_proxy_config_processor.h"
#include "obproxy/proxy/route/obproxy_part_info.h"

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
ObExprParserChecker::ObExprParserChecker() : is_verbose_(true),
                                             need_resolve_(false),
                                             parse_mode_(SELECT_STMT_PARSE_MODE),
                                             stmt_type_(OBPROXY_T_INVALID),
                                             total_count_(0), succ_count_(0),
                                             parse_time_(0),
                                             resolve_time_(0),
                                             result_file_name_(""), result_file_(NULL),
                                             allocator_(common::ObModIds::TEST)
{
}

int ObExprParserChecker::do_obproxy_parser(const ObString &query_str, ObExprParseResult &result, ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;

  int64_t t0 = ObTimeUtility::current_time();
  ObExprParser parser(allocator_, parse_mode_);
  if (OB_FAIL(parser.parse(query_str, result, connection_collation))) {
    // do nothing
  }

  // record parse time
  int64_t t1 = ObTimeUtility::current_time();
  parse_time_ += (t1 - t0);

  return ret;
}

int ObExprParserChecker::do_expr_resolver(ObExprResolverContext &ctx, ObExprResolverResult &result)
{
  int ret = OB_SUCCESS;

  int64_t t0 = ObTimeUtility::current_time();
  ObExprResolver resolver(allocator_);
  if (OB_FAIL(resolver.resolve(ctx, result))) {
    // do nothing
  }

  // record parse time
  int64_t t1 = ObTimeUtility::current_time();
  resolve_time_ += (t1 - t0);

  return ret;
}

ObProxyParseString ObExprParserChecker::get_value(std::string &extra_str, const char* key_name,
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

void ObExprParserChecker::build_schema(std::string &extra_str, ObExprParseResult &result)
{
  std::size_t pos = 0;
  result.table_info_.database_name_ = get_value(extra_str, "db_name", pos);
  pos = 0;
  result.table_info_.table_name_ = get_value(extra_str, "table_name", pos);
  pos = 0;
  result.table_info_.alias_name_ = get_value(extra_str, "alias_name", pos);
  pos = 0;
  ObProxyParseString tmp_str = get_value(extra_str, "oracle_mode", pos);
  pos = 0;
  if (NULL != tmp_str.str_ && strncasecmp(tmp_str.str_, "true", 4) == 0) {
    result.is_oracle_mode_ = true;
  } else {
    result.is_oracle_mode_ = false;
  }

  result.part_key_info_.key_num_ = 0;
  result.target_mask_ = 0;
  while (true) {
    ObProxyParseString part_key_name = get_value(extra_str, "part_key", pos);
    ObProxyParseString part_key_level = get_value(extra_str, "part_level", pos);
    std::size_t tmp_pos = pos;
    ObProxyParseString part_key_idx = get_value(extra_str, "part_idx", pos);
    if (part_key_name.str_len_ == 0 || part_key_level.str_len_ == 0) {
      break;
    } else {
      ObProxyPartKeyLevel level = static_cast<ObProxyPartKeyLevel>(part_key_level.str_[0] - '0');
      result.part_key_info_.part_keys_[result.part_key_info_.key_num_].name_ = part_key_name;
      result.part_key_info_.part_keys_[result.part_key_info_.key_num_].level_ = level;
      if (part_key_idx.str_len_ == 0) {
        result.part_key_info_.part_keys_[result.part_key_info_.key_num_].idx_ = -1;
        pos = tmp_pos;
      } else {
        result.part_key_info_.part_keys_[result.part_key_info_.key_num_].idx_ = part_key_idx.str_[0] - '0';
      }
      ++result.part_key_info_.key_num_;
      if (PART_KEY_LEVEL_ONE == level) {
        result.target_mask_ |= BOTH_BOUND_FLAG;
      } else {
        result.target_mask_ |= (BOTH_BOUND_FLAG << 2);
      }
    }
  }
}

bool ObExprParserChecker::run_parse_string(const ObString query_str, std::string &extra_str, ObCollationType connection_collation)
{
  bool bret = false;

  static const int64_t MAX_STR_LEN = 65535;
  static char buf[MAX_STR_LEN];
  ObExprParseResult result;

  if (NULL == result_file_ && strlen(result_file_name_) > 0) {
    result_file_ = fopen(result_file_name_, "a+");
  }

  build_schema(extra_str, result);

  DUMP_RESULT("SQL   : %.*s\n", query_str.length(), query_str.ptr());
  if (OB_SUCCESS == do_obproxy_parser(query_str, result, connection_collation)) {
    ObExprParseResultPrintWrapper wrapper(result);
    DUMP_RESULT("RESULT:\n%.*s\n", static_cast<int32_t>(wrapper.to_string(buf, MAX_STR_LEN)), buf);

    if (need_resolve_) {
      ObProxyPartInfo part_info;
      ObExprResolverContext ctx;
      ObExprResolverResult expr_result;
      ctx.relation_info_ = &result.relation_info_;
      ctx.part_info_ = &part_info;
      if (OB_SUCCESS == do_expr_resolver(ctx, expr_result)) {
        DUMP_RESULT("RANGES: %.*s\n",
                     static_cast<int32_t>(expr_result.to_string(buf, MAX_STR_LEN)), buf);
      } else {
        DUMP_RESULT("RANGES: RESOLVE FAILED\n");
      }
    }
  } else {
    DUMP_RESULT("RESULT: PARSE FAILED\n");
  }
  DUMP_RESULT("\n");

  total_count_++;
  allocator_.reuse();
  return bret;
}

bool ObExprParserChecker::run_parse_std_string(std::string query_str, std::string extra_str, ObCollationType connection_collation)
{
  query_str += "  ";
  ObString input_query(query_str.size(), query_str.c_str());
  input_query.ptr()[input_query.length() - 2] = 0;
  input_query.ptr()[input_query.length() - 1] = 0;

  const char *expr_sql_str = input_query.ptr();
  const char *pos = NULL;
  if (OB_LIKELY(NULL != expr_sql_str)) {
    if (SELECT_STMT_PARSE_MODE == parse_mode_) {
      if (NULL != (pos = strcasestr(expr_sql_str, "JOIN"))) {
        // pos = JOIN
      } else if (NULL != (pos = strcasestr(expr_sql_str, "WHERE"))) {
        // pos = WHERE
      }
    } else if (OBPROXY_T_UPDATE == stmt_type_) {
      if (NULL != (pos = strcasestr(expr_sql_str, "SET"))) {
        // pos = SET
      }
    } else if (OBPROXY_T_MERGE == stmt_type_) {
      if (NULL != (pos = strcasestr(expr_sql_str, "ON"))) {
        // pos = ON
      }
    }

    if ((NULL != pos) && ((pos - expr_sql_str) <= 1 || (pos - expr_sql_str) >= input_query.length())) {
      pos = NULL;
    }

    if (NULL != pos) {
      const int32_t len_before_where = static_cast<int32_t>(pos - expr_sql_str);
      input_query += (len_before_where - 1);
    }
  }
  return run_parse_string(input_query, extra_str, connection_collation);
}

bool ObExprParserChecker::run_parse_file(const char *filepath, std::string extra_str, ObCollationType connection_collation)
{
  bool bret = false;
  std::ifstream input_file(filepath);
  std::string line_str;
  std::string query_str;

  if (!input_file.is_open()) {
    fprintf(stderr, "file is not open, filepath:%s\n", filepath);
    bret = false;
  } else {
    // open result file
    if (NULL == result_file_ && strlen(result_file_name_) > 0) {
      result_file_ = fopen(result_file_name_, "w+");
    }

    // walk through and do parser string
    while (std::getline(input_file, line_str)) {
      if (query_str == "") {
        query_str = line_str;
      } else {
        query_str += "\n" + line_str;
      }
      std::size_t begin = query_str.find_first_not_of("\r\f\n\t ");
      begin = (begin == std::string::npos) ? 0 : begin;
      if (query_str.size() <= 0 || query_str.at(begin) == '#' || query_str.at(begin) == '-' ) {
        query_str = "";
        continue;
      } else {
        std::size_t end = -1;
        if (std::string::npos !=  (end = query_str.find_last_of(';'))) {
          query_str = query_str.substr(begin, end - begin + 1);
          run_parse_std_string(query_str, extra_str, connection_collation);
          query_str = "";
        } else {
          // not contains ';'
        }
      }
    }
    input_file.close();

    // close if need
    if (NULL != result_file_) {
      fclose(result_file_);
    }
  }
  return bret;
}

void ObExprParserChecker::print_stat()
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

int ob_expr_parser_utf8_yydebug;
using namespace oceanbase::obproxy::test;
int main(int argc, char **argv)
{
  int ret = 0;
  ObExprParserChecker checker;
  int c = -1;
  const char *filepath = "";
  const char *input_str = "";
  const char *extra_str = "";
  int loop_count = 1;
  ObCollationType connection_collation = CS_TYPE_INVALID;;
  while(-1 != (c = getopt(argc, argv, "f:e:s:n:r:t:c:SDRI"))) {
    switch(c) {
      case 'f':
        filepath = optarg;
        break;
      case 's':
        input_str = optarg;
        break;
      case 'S':
        checker.is_verbose_ = false;
        break;
      case 'D':
        ob_expr_parser_utf8_yydebug = 1;
        oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
        OB_LOGGER.set_log_level("DEBUG");
        break;
      case 'n':
        loop_count = atoi(optarg);
        break;
      case 'r':
        checker.result_file_name_ = optarg;
        break;
      case 'e':
        extra_str = optarg;
        break;
      case 'R':
        checker.need_resolve_ = true;
        break;
      case 'I':
        checker.parse_mode_ = INSERT_STMT_PARSE_MODE;
        break;
      case 'c':
        if (strcmp(optarg, "utb8") == 0) {
          connection_collation = CS_TYPE_UTF8MB4_GENERAL_CI;
        } else if (strcmp(optarg, "gb18030") == 0) {
          connection_collation = CS_TYPE_GB18030_CHINESE_CI;
        }
        break;
      case 't':
        ObString stmt_type(strlen(optarg), optarg);
        checker.stmt_type_ = get_stmt_type_by_name(stmt_type);
        break;
    }
  }

  while (loop_count-- > 0) {
    if (strlen(filepath) > 0 && strlen(extra_str)) {
      checker.run_parse_file(filepath, extra_str, connection_collation);
    } else if (strlen(input_str) > 0 && strlen(extra_str)) {
      checker.run_parse_std_string(input_str, extra_str, connection_collation);
    } else {
      printf("must spec input_str(-i)/file_name(-f) and extra_str(-e)");
    }
  }

  checker.print_stat();
  return ret;
}
