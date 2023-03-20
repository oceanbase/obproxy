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
#include "lib/utility/ob_print_utils.h"
#include <fstream>
#include <iterator>
#include <vector>
#include <string>
#include <dirent.h>
#include <sys/types.h>
#include "obutils/ob_proxy_sql_parser.h"
#include "opsql/parser/ob_proxy_parser.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include "opsql/expr_parser/ob_expr_parser_utils.h"
#include "obproxy/obutils/ob_proxy_sql_parser.h"
#include "lib/allocator/page_arena.h"
#include "obutils/ob_proxy_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

const int buffer_size = 4096;
void do_expr_parser_test(std::string query_str, std::string extra_str, bool is_select);
int parse_sql_fileds(ObProxyMysqlRequest &client_request, ObString expr_sql);
void extract_local_fileds(const ObExprParseResult& result, ObProxyMysqlRequest &client_request);
std::string function_type_to_string(ObProxyFunctionType type);
std::string token_type_to_string(ObProxyTokenType type);
void show_result(std::string query_str, ObSqlParseResult& parser_result) {

  static const int64_t MAX_STR_LEN = 65535;
  static char parse_string[MAX_STR_LEN];

  printf("===============parser_result is============\n sql:%s, result :%s\n", query_str.c_str(), (parser_result.to_string(parse_string, MAX_STR_LEN), parse_string));
  printf("===============parser_result end============\n");
}

void show_sql_result(SqlFieldResult& sql_result) {
  fprintf(stdout, "========== sql_result is ===========\n");
  for(int i = 0; i < sql_result.field_num_; i++) {
    char buf[256];
    printf("column_name:length:%d\t", sql_result.fields_[i]->column_name_.config_string_.length());
    snprintf(buf, 256, "column_name:%.*s",
      sql_result.fields_[i]->column_name_.config_string_.length(),
      sql_result.fields_[i]->column_name_.config_string_.ptr());
    printf("%s\t", buf);
    //printf(" %s ", function_type_to_string(sql_result.fields_[i].type_).c_str());
    printf(" (%s) ", token_type_to_string(sql_result.fields_[i]->value_type_).c_str());
    if(sql_result.fields_[i]->value_type_ == TOKEN_INT_VAL) {
      snprintf(buf, 256, "column_value:%ld", sql_result.fields_[i]->column_int_value_);
    } else if (sql_result.fields_[i]->value_type_ == TOKEN_STR_VAL){
     snprintf(buf, sql_result.fields_[i]->column_value_.config_string_.length()+1, "column_value:%.*s",
      sql_result.fields_[i]->column_value_.config_string_.length(),
      sql_result.fields_[i]->column_value_.config_string_.ptr());
   }
   printf("%s\n", buf);
  //   fprintf(stdout, "%s %s %s\n", sql_result.fields_[i].column_name_,
  //           function_type_to_string(sql_result.fields_[i].type_).c_str(),
  //           sql_result.fields_[i].column_value_);
 }
 fprintf(stdout, "========== sql_result end ===========\n");
}

void do_parser_test(ObProxyMysqlRequest& client_request, std::string query_str) {
  fprintf(stdout, "sql is %s\n", query_str.c_str());
  int ret = OB_SUCCESS;
    //std::string query_str = "select * from test;";
  query_str += "  ";
  ObString input_query(query_str.size(), query_str.c_str());
  input_query.ptr()[input_query.length() - 2 ] = 0;
  input_query.ptr()[input_query.length() - 1 ] = 0;
  oceanbase::common::ObArenaAllocator alloc(oceanbase::common::ObModIds::TEST);

  int64_t t0 = ObTimeUtility::current_time();
  ObProxySqlParser sql_parser;
  ObSqlParseResult& sql_result = client_request.get_parse_result();

  if (OB_FAIL(sql_parser.parse_sql(input_query, NORMAL_PARSE_MODE, sql_result, true, CS_TYPE_UTF8MB4_GENERAL_CI, false, false))) {
    fprintf(stderr, "parse sql failed");
  } else if (sql_result.get_proxy_stmt() != NULL) {
    ObSqlString sql_string;
    sql_result.get_proxy_stmt()->to_sql_string(sql_string);
  }
  int64_t t1 = ObTimeUtility::current_time();
  LOG_DEBUG("parse success. using time in usec", K(t1 - t0));
  show_result(query_str, sql_result);
  ObString expr_sql = ObProxyMysqlRequest::get_expr_sql(input_query, sql_result.get_parsed_length());
  LOG_DEBUG("expr_sql is", K(expr_sql));
  if (OB_FAIL(parse_sql_fileds(client_request, expr_sql))) {
    LOG_WARN("parse sql_fields  failed", K(expr_sql));
  }
  return;
}

ObProxyParseString get_value(std::string &extra_str, const char* key_name,
  std::size_t &pos) {
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
void build_schema(std::string &extra_str, ObExprParseResult &result)
{
  std::size_t pos = 0;
  result.table_info_.database_name_ = get_value(extra_str, "db_name", pos);
  pos = 0;
  result.table_info_.table_name_ = get_value(extra_str, "table_name", pos);
  pos = 0;
  result.table_info_.alias_name_ = get_value(extra_str, "alias_name", pos);
  pos = 0;

  result.part_key_info_.key_num_ = 0;
  while (true) {
    ObProxyParseString part_key_name = get_value(extra_str, "part_key", pos);
    ObProxyParseString part_key_level = get_value(extra_str, "part_level", pos);
    std::size_t tmp_pos = pos;
    ObProxyParseString part_key_idx = get_value(extra_str, "part_idx", pos);
    
    if (part_key_name.str_len_ == 0 || part_key_level.str_len_ == 0) {
      break;
    } else {
      std::string key_name(part_key_name.str_, part_key_name.str_len_);
      std::string key_level(part_key_level.str_, part_key_level.str_len_);
      std::string key_idx(part_key_idx.str_, part_key_idx.str_len_);
      fprintf(stdout, "part_key_idx:%s, part_key_name:%s, part_key_level:%s\n", 
        key_idx.c_str(), key_name.c_str(), key_level.c_str());
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
    }
  }
}

std::string token_type_to_string(ObProxyTokenType type) {
  switch(type) {
    case TOKEN_STR_VAL: return "TOKEN_STR_VAL";
    case TOKEN_INT_VAL: return "TOKEN_INT_VAL";
    case TOKEN_FUNC: return "TOKEN_FUNC";
    default: return "unknown";
  }
}
std::string  function_type_to_string(ObProxyFunctionType type) {
  switch (type) {
    case F_NONE: return "F_NONE";
    case F_OP_AND : return "F_OP_AND";
    case F_OP_OR : return "F_OP_OR";
    case F_COMP_START : return "F_COMP_START";
    case F_COMP_EQ : return "F_COMP_EQ";
    case F_COMP_NSEQ: return "F_COMP_NSEQ";
    case F_COMP_GE: return "F_COMP_GE";
    case F_COMP_GT: return "F_COMP_GT";
    case F_COMP_LE: return "F_COMP_LE";
    case F_COMP_LT: return "F_COMP_LT";
    case F_COMP_NE: return "F_COMP_NE";
    default: return "unknown";
  }
}


void extract_local_fileds(const ObExprParseResult& result, ObProxyMysqlRequest &client_request) {
  SqlFieldResult& sql_result = client_request.get_parse_result().get_sql_filed_result();
  sql_result.field_num_ = 0;
  int64_t total_num = result.all_relation_info_.relation_num_;
  for(int64_t i = 0;i < total_num; i ++) {
    ObProxyRelationExpr* relation_expr = result.all_relation_info_.relations_[i];
    if(relation_expr == NULL) {
      printf("Got an empty relation_expr");
      continue;
    }
    SqlField *field = NULL;
    SqlField::alloc_sql_field(field);
    //sql_result.fields_[sql_result.field_num_].type_ = relation_expr->type_;
    if(relation_expr->left_value_ != NULL 
      && relation_expr->left_value_->head_ != NULL
      && relation_expr->left_value_->head_->type_ == TOKEN_COLUMN) {
      if(relation_expr->left_value_->head_->column_name_.str_ != NULL) {
        ObString tmp_column(relation_expr->left_value_->head_->column_name_.str_len_,
          relation_expr->left_value_->head_->column_name_.str_);
        LOG_DEBUG("column_name is ", K(tmp_column));
        field->column_name_.set_value(tmp_column);

        LOG_DEBUG("field->column", K(field->column_name_));
        // strncpy(sql_result.fields_[sql_result.field_num_].column_name_,
        //           relation_expr->left_value_->head_->column_name_.str_,
        //           relation_expr->left_value_->head_->column_name_.str_len_);
            // std::string(relation_expr->left_value_->head_->column_name_.str_,
            //             relation_expr->left_value_->head_->column_name_.str_len_);
      } else {
        LOG_WARN("get an empty column_name_");
      }
    } else {
      LOG_WARN("left value is null");
    }
    if(relation_expr->right_value_ != NULL
      && relation_expr->right_value_->head_ != NULL) {
      if(relation_expr->right_value_->head_->type_ == TOKEN_INT_VAL) {
        field->value_type_ = TOKEN_INT_VAL;
        field->column_int_value_ = relation_expr->right_value_->head_->int_value_;
        LOG_DEBUG("field->value", K(field->column_int_value_));
          // snprintf(sql_result.fields_[sql_result.field_num_].column_value_,
          //            32, "%ld", relation_expr->right_value_->head_->int_value_);
          // sql_result.fields_[sql_result.field_num_].column_value_ = "";
          // sql_result.fields_[sql_result.field_num_].column_value_ = std::string(buf);
      } else if(relation_expr->right_value_->head_->type_ == TOKEN_STR_VAL) {
       field->value_type_ = TOKEN_STR_VAL;
       field->column_value_.config_string_.assign_ptr(
        relation_expr->right_value_->head_->str_value_.str_,
        relation_expr->right_value_->head_->str_value_.str_len_);
        LOG_DEBUG("field->column_value", K(field->column_value_));

          // strncpy(sql_result.fields_[sql_result.field_num_].column_value_,
          //           relation_expr->right_value_->head_->str_value_.str_,
          //           relation_expr->right_value_->head_->str_value_.str_len_);
        // sql_result.fields_[sql_result.field_num_].column_value_ = "";
            // std::string(relation_expr->right_value_->head_->str_value_.str_,
            //   relation_expr->right_value_->head_->str_value_.str_len_);
     } else {             
      printf("invalid type :");
    }
  } else {
    printf("right value is null\n");
  }
  sql_result.fields_.push_back(field);
  sql_result.field_num_++;
  if(sql_result.field_num_ >= 100) {
    printf("too much fileds");
    break;
  }
}
}


int parse_sql_fileds(ObProxyMysqlRequest &client_request, ObString expr_sql) {
  int ret = OB_SUCCESS;
  // expr_sql.ptr()[expr_sql.length() - 2 ] = 0;
  // expr_sql.ptr()[expr_sql.length() - 1 ] = 0;
  ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
  ObArenaAllocator *allocator = NULL;
  ObExprParseResult expr_result;
  if(OB_FAIL(ObProxySqlParser::get_parse_allocator(allocator))) {
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ObExprParseMode parse_mode = INVALID_PARSE_MODE;
    if (sql_parse_result.is_select_stmt() || sql_parse_result.is_delete_stmt()) {
    // we treat delete as select
      parse_mode = SELECT_STMT_PARSE_MODE;
    } else if (sql_parse_result.is_insert_stmt() || sql_parse_result.is_replace_stmt()
     || sql_parse_result.is_update_stmt()) {
      parse_mode = INSERT_STMT_PARSE_MODE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      fprintf(stderr, "invalid type\n");
      return ret;
    }
    int64_t t0 = ObTimeUtility::current_time();
    ObExprParser expr_parser(*allocator, parse_mode);
    // ObString sql = client_request.get_parse_sql();
    // // init expr result
    // expr_result.table_info_.table_name_.str_ = const_cast<char *>(sql_parse_result.get_table_name().ptr());
    // expr_result.table_info_.table_name_.str_len_ = sql_parse_result.get_table_name().length();
    // expr_result.table_info_.database_name_.str_ = const_cast<char *>(sql_parse_result.get_database_name().ptr());
    // expr_result.table_info_.database_name_.str_len_ = sql_parse_result.get_database_name().length();
    // expr_result.table_info_.alias_name_.str_ = const_cast<char *>(sql_parse_result.get_alias_name().ptr());
    // expr_result.table_info_.alias_name_.str_len_ = sql_parse_result.get_alias_name().length();
    expr_result.part_key_info_.key_num_ = 0;
    // ObString expr_sql = ObProxyMysqlRequest::get_expr_sql(sql, sql_parse_result.get_parsed_length());
    if (SELECT_STMT_PARSE_MODE == parse_mode) {
      const char *expr_sql_str = expr_sql.ptr();
      const char *pos = NULL;
      if (OB_LIKELY(NULL != expr_sql_str)
        && NULL != (pos = strcasestr(expr_sql_str, "WHERE"))
        && (pos - expr_sql_str) > 1
        && OB_LIKELY((pos - expr_sql_str) < expr_sql.length())) {
        const int32_t len_before_where = static_cast<int32_t>(pos - expr_sql_str);
      expr_sql += (len_before_where - 1);
    }
  }
  if(OB_FAIL(expr_parser.parse(expr_sql, expr_result, CS_TYPE_UTF8MB4_GENERAL_CI))) {
    fprintf(stderr, "parse failed %s\n", expr_sql.ptr());
  } else {
    int64_t t1 = ObTimeUtility::current_time();
    fprintf(stdout, "parse fields success. using time %ld usec\n", t1 - t0);
    LOG_DEBUG("succ to do expr parse", "parse_result", ObExprParseResultPrintWrapper(expr_result));
    fprintf(stdout, "parse success:%s, %ld\n", 
      expr_sql.ptr(), expr_result.all_relation_info_.relation_num_);
    extract_local_fileds(expr_result, client_request);
    show_sql_result(client_request.get_parse_result().get_sql_filed_result());
  }
    // extract_fileds(expr_result, client_request);
} 
if(NULL != allocator) {
  allocator->reuse();
}
return ret;
}

// int main(int argc, char** argv) {
//   if (argc < 1) {
//     fprintf(stderr, "invalid argc %d", argc);
//     return -1;
//   }
//   if (argc == 2) {
//     fprintf(stdout, "argv is %s\n", argv[1]);
//   }
//   ObProxyMysqlRequest client_request;
//   ObSqlParseResult &sql_parse_result = client_request.get_parse_result();
//   sql_parse_result.set_stmt_type(OBPROXY_T_SELECT);
//   ObString expr_sql(argv[1]);
//   parse_sql_fileds(client_request, expr_sql);
// }

int main(int argc, char **argv) {
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_log_level("DEBUG");
  if (argc < 1) {
    fprintf(stderr, "invalid argc %d", argc);
    return -1;
  }
  ObProxyMysqlRequest client_request;
  if (argc == 2) {
    fprintf(stdout, "argv is %s", argv[1]);
    do_parser_test(client_request, argv[1]);
  }
  if (argc == 3) {
    const char* file_name = argv[1];
    fprintf(stdout, "file name is %s", argv[1]);
    ssize_t read_len;
    size_t len;
    FILE* fp = fopen(file_name, "r");
    if(fp == NULL) {
      printf("oepn file failed %s\n", file_name);
      return -1;
    }
    char * line = NULL;
    while((read_len = getline(&line, &len, fp)) != -1) {
      line[strlen(line) -1] = 0;
      do_parser_test(client_request, line);
    }
    if(line != NULL) {
      free(line);
    }
  }
    //./obproxy_parser_test "where t1.c1=1 and t1.c2==2;" "table_name:t1, part_key:c1, part_level:1, range_type:int"
    // if (argc == 4) {
    //     fprintf(stdout, "argv[0] is %s, argv[1] is %s,argv[2] is %s argv[3] is %s\n",
    //                 argv[0], argv[1], argv[2], argv[3]);
    //     if (strcmp(argv[3], "S") == 0)  do_expr_parser_test(argv[1], argv[2], true); 
    //     else do_expr_parser_test(argv[1], argv[2], false); 
    // }
    // fprintf(stdout, "argc is %d\n", argc);
  return 0;
}
