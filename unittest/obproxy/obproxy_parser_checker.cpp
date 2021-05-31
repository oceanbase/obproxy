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

#include "obproxy_parser_checker.h"
#include "lib/utility/ob_print_utils.h"
#include <fstream>
#include <iterator>
#include <vector>
#include <string>
#include <dirent.h>
#include <sys/types.h>

using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;

#define DUMP_RESULT(fmt, args...) if (is_verbose_) fprintf(result_file_ ? result_file_ : stdout, fmt, ##args)

#define YYDEBUG 1

namespace oceanbase
{
namespace obproxy
{
namespace test
{
int64_t ObProxyParseResultWapper::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_KV(K_(parse_result),
       K_(expected_parsed_length));
  return pos;
}

ObProxyParserChecker::ObProxyParserChecker() : is_verbose_(true),
                                               total_count_(0), succ_count_(0),
                                               proxy_parse_time_(0),
                                               result_file_name_(""), result_file_(NULL),
                                               allocator_(common::ObModIds::TEST)
{
}

int ObProxyParserChecker::do_obproxy_parser(const ObString &query_str, ObProxyParseResultWapper &result)
{
  int ret = OB_SUCCESS;

  int64_t t0 = ObTimeUtility::current_time();
  ObProxyParser parser(allocator_, NORMAL_PARSE_MODE);
  ObProxyParseResult obproxy_parse_result;
  if (OB_FAIL(parser.parse(query_str, obproxy_parse_result))) {
    // do nothing
  } else if (OB_FAIL(result.load_result(&obproxy_parse_result))) {
    // do nothing
  }

  // record parse time
  int64_t t1 = ObTimeUtility::current_time();
  proxy_parse_time_ += (t1 - t0);

  return ret;
}


bool ObProxyParserChecker::run_parse_string(const ObString query_str)
{
  bool bret = false;

  static const int64_t MAX_STR_LEN = 65535;
  static char parse_string[MAX_STR_LEN];
  ObProxyParseResultWapper result;

  DUMP_RESULT("SQL   : %.*s\n", query_str.length(), query_str.ptr());
  if (OB_SUCCESS == do_obproxy_parser(query_str, result)) {
    DUMP_RESULT("RESULT: %s\n",
                (result.to_string(parse_string, MAX_STR_LEN), parse_string));
  } else {
    DUMP_RESULT("RESULT: PARSE FAILED\n");
  }
  DUMP_RESULT("\n");

  total_count_++;
  allocator_.reuse();
  return bret;
}

bool ObProxyParserChecker::run_parse_std_string(std::string query_str)
{
  query_str += "  ";
  ObString input_query(query_str.size(), query_str.c_str());
  input_query.ptr()[input_query.length() - 2] = 0;
  input_query.ptr()[input_query.length() - 1] = 0;
  return run_parse_string(input_query);
}

bool ObProxyParserChecker::run_parse_file(const char *filepath)
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
          run_parse_std_string(query_str);
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

void ObProxyParserChecker::print_stat()
{
  printf("sql count %ld\n", total_count_);
  printf("parser time %lf (us)\n", (double)(proxy_parse_time_) / (double)(total_count_));
}

} // end of namespace test
} // end of namespace obproxy
} // end of namespace oceanbase

#define YYDEBUG 1
#if YYDEBUG
extern int obproxydebug;
#endif
using namespace oceanbase::obproxy::test;
int main(int argc, char **argv)
{
  int ret = 0;
  ObProxyParserChecker checker;
  int c = -1;
  const char *filepath = "";
  const char *input_str = "";
  int loop_count = 1;
  while(-1 != (c = getopt(argc, argv, "f:s:n:r:SD"))) {
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
#if YYDEBUG
        obproxydebug = 1;
#endif
        break;
      case 'n':
        loop_count = atoi(optarg);
        break;
      case 'r':
        checker.result_file_name_ = optarg;
    }
  }

  while (loop_count-- > 0) {
    if (strlen(filepath) > 0) {
      checker.run_parse_file(filepath);
    } else if (strlen(input_str) > 0) {
      checker.run_parse_std_string(input_str);
    } else {
      checker.run_parse_file("parser/mysqltest.sql");
    }
  }

  checker.print_stat();
  return ret;
}
