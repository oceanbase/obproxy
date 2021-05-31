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

#include "sql/parser/ob_parser.h"
#include "opsql/ob_proxy_parser.h"
#include <gtest/gtest.h>
#include "lib/allocator/page_arena.h"
#include <fstream>
#include <iterator>
#include <vector>
#include <string>
#include <iostream>

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::obproxy::opsql;
namespace test
{
static char *parse_file = NULL;
static int LOOP_COUNT = 1;
static bool PRINT_STAT = false;
static bool IS_FP = false;
static bool IS_COMPARE = false;

class TestParserPerf
{
public:
  TestParserPerf();
  virtual ~TestParserPerf();
  void do_parse(const char* query_str);
  void do_obproxy_parse(const char* query_str);
private:
  DISALLOW_COPY_AND_ASSIGN(TestParserPerf);
public:
  ObArenaAllocator allocator_;
  ObArenaAllocator proxy_allocator_;
  int64_t total_t_;
  int64_t total_cnt_;
  int64_t succ_cnt_;
};

TestParserPerf::TestParserPerf()
    : allocator_(ObModIds::TEST),
      proxy_allocator_(ObModIds::TEST),
      total_t_(0),
      total_cnt_(0),
      succ_cnt_(0)
{
}

TestParserPerf::~TestParserPerf()
{
}

void TestParserPerf::do_parse(const char* query_str)
{
  int64_t t0 = 0, t1 = 0;
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator_, mode);
  ParseResult parse_result;
  parse_result.is_fp_ = IS_FP;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  t0 = ObTimeUtility::current_time();
  ret = parser.parse(query, parse_result, IS_FP ? FP_MODE : STD_MODE);
  t1 = ObTimeUtility::current_time();
  if (ret == OB_SUCCESS) {
    succ_cnt_ ++;
  }
  total_t_ += t1 - t0;
  total_cnt_++;

  if (PRINT_STAT) {
    printf("==%s\n",query_str);
    printf("==%s\n", parse_result.no_param_sql_);
    printf("time:%ld, len:%d, ",t1 - t0 , parse_result.no_param_sql_len_);
    printf("param_node_num:%d\n", parse_result.param_node_num_);
    ParamList *param = parse_result.param_nodes_;
    for (int32_t i = 0; OB_SUCC(ret) && i < parse_result.param_node_num_ && NULL != param; i ++) {
      printf("    param_%d: type:%d; value:%ld, str_value:%s\n",
                       i, param->node_->type_,
                       param->node_->value_,
                       param->node_->str_value_);
      param = param->next_;
    }
  }
  parser.free_result(parse_result);
  allocator_.reuse();
}

void TestParserPerf::do_obproxy_parse(const char* query_str)
{
  int64_t t0 = 0, t1 = 0;
  ObProxyParser parser(proxy_allocator_, NORMAL_PARSE_MODE);
  ObProxyParseResult parse_result;
  ObString query = ObString::make_string(query_str);
  int ret = OB_SUCCESS;
  t0 = ObTimeUtility::current_time();
  ret = parser.parse(query, parse_result);
  t1 = ObTimeUtility::current_time();
  if (ret == OB_SUCCESS) {
    succ_cnt_ ++;
  }
  total_t_ += t1 - t0;
  total_cnt_++;

  if (PRINT_STAT) {
    printf("==%s\n",query_str);
    printf("stmt type: %s\n", get_obproxy_stmt_name(parse_result.stmt_type_));
    printf("database name: %.*s\n", static_cast<int32_t>(parse_result.table_info_.database_name_.str_len_),
                                    parse_result.table_info_.database_name_.str_);
    printf("table name: %.*s\n", static_cast<int32_t>(parse_result.table_info_.table_name_.str_len_),
                                 parse_result.table_info_.table_name_.str_);
    printf("alias name: %.*s\n", static_cast<int32_t>(parse_result.table_info_.alias_name_.str_len_),
                                 parse_result.table_info_.alias_name_.str_);
    //printf("query_timeout: %ld\n", parse_result.query_timeout_);
    //printf("has_found_rows: %d\n", parse_result.has_found_rows_);
    //printf("has_row_count: %d\n", parse_result.has_row_count_);
    //printf("has_last_insert_id: %d\n", parse_result.has_last_insert_id_);
  }
  parser.free_result(parse_result);
  proxy_allocator_.reuse();
}



int load_sql(const char *test_file, std::vector<std::string> &sql_array)
{
  int ret = OB_SUCCESS;
  std::ifstream if_tests(test_file);
  if (!if_tests.is_open()) {
    SQL_PC_LOG(ERROR, "maybe reach max file open");
    ret = OB_ERROR;
  }
  std::string line;
  std::string total_line;
  ;
  while (std::getline(if_tests, line)) {
    // allow human readable formatting
    if (line.size() <= 0) continue;
    std::size_t begin = line.find_first_not_of('\t');
    if (line.at(begin) == '#') continue;
    std::size_t end = line.find_last_not_of('\t');
    std::string exact_line = line.substr(begin, end - begin + 1);
    total_line += exact_line;
    total_line += " ";
    if (exact_line.at(exact_line.length() - 1) != ';') continue;
    else {
      sql_array.push_back(total_line);
      total_line = "";
    }
  }
  if_tests.close();
  return ret;
}

void run() {
  std::vector<std::string> test_sql_array;
  load_sql("./test_parser.sql", test_sql_array);
  TestParserPerf pp;


  std::cout << "input sql list:" << std::endl;
  for (int i = 0; i < static_cast<int>(test_sql_array.size()); ++i) {
    std::cout << test_sql_array[i] << std::endl;
  }

  // for debug
  // if ((int)test_sql_array.size() > 0) {
  //   pp.do_obproxy_parse(test_sql_array.at(0).c_str());
  // }

  if (test::IS_COMPARE) {
    pp.succ_cnt_ = 0;
    pp.total_cnt_ = 0;
    pp.total_t_ = 0;
    for(int i = 0; i < LOOP_COUNT; i++) {
      for (int j = 0; j < (int)test_sql_array.size(); j++) {
        pp.do_parse(test_sql_array.at(j).c_str());
      }
    }
    std::cout << "====" << "succ_cnt:" << pp.succ_cnt_ << std::endl;
    std::cout << "====" << "total_cnt:" << pp.total_cnt_ << std::endl;
    std::cout << "====" << "avg_time:" << (double)(pp.total_t_)/(double)(pp.total_cnt_) << std::endl;
  }

  pp.succ_cnt_ = 0;
  pp.total_cnt_ = 0;
  pp.total_t_ = 0;
  std::cout << "proxy parser::" << std::endl;
  for(int i = 0; i < LOOP_COUNT; i++) {
    for (int j = 0; j < (int)test_sql_array.size(); j++) {
      pp.do_obproxy_parse(test_sql_array.at(j).c_str());
    }
  }
  std::cout << "====" << "succ_cnt:" << pp.succ_cnt_ << std::endl;
  std::cout << "====" << "total_cnt:" << pp.total_cnt_ << std::endl;
  std::cout << "====" << "avg_time:" << (double)(pp.total_t_)/(double)(pp.total_cnt_) << std::endl;

}
}

extern int obproxydebug;
int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("ERROR");
  OB_LOGGER.set_file_name("test_parser.log", true);
  int c = 0;
  while(-1 != (c = getopt(argc, argv, "q:cdpfl:n:"))) {
    switch(c) {
      case 'q':
        test::parse_file = optarg;
        break;
      case 'n':
        test::LOOP_COUNT = atoi(optarg);
        break;
      case 'f':
        test::IS_FP = true;
        break;
      case 'c':
        test::IS_COMPARE = true;
        break;
      case 'l':
        if (NULL != optarg) {
          OB_LOGGER.set_log_level(optarg);
        }
        break;
      case 'd':
        obproxydebug = 1;
        break;
      case 'p':
        test::PRINT_STAT = true;
        break;
      default:
        printf("usage:");
        break;
    }
  }
  ::test::run();
  return 0;
}
