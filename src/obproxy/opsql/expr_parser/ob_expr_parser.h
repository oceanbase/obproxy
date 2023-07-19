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

#ifndef OBEXPR_PARSER_H
#define OBEXPR_PARSER_H
#include "lib/ob_define.h"
#include "common/ob_sql_mode.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"
#include "opsql/expr_parser/ob_expr_parser_utils.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "lib/string/ob_string.h"

extern "C" int ob_expr_parse_utf8_sql(ObExprParseResult *p, const char *pszSql, size_t iLen);
extern "C" int ob_expr_parse_gbk_sql(ObExprParseResult *p, const char *pszSql, size_t iLen);

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObString;
}
namespace obproxy
{
namespace opsql
{

class ObExprParser
{
public:
  explicit ObExprParser(common::ObIAllocator &allocator, ObExprParseMode parse_mode);
  // will not be inherited, do not set to virtual
  ~ObExprParser() {}

  int parse(const common::ObString &sql_string, ObExprParseResult &parse_result,
            common::ObCollationType connection_collation);

  int parse_reqsql(const common::ObString &req_sql, int64_t parsed_length,
                   ObExprParseResult &parse_result, ObProxyBasicStmtType stmt_type,
                   common::ObCollationType connection_collation);

  void free_result(ObExprParseResult &parse_result);
private:
  int init_result(ObExprParseResult &parse_result, const char *start_pos);
  // data members
  common::ObIAllocator &allocator_;
  ObExprParseMode parse_mode_;

  DISALLOW_COPY_AND_ASSIGN(ObExprParser);
};

inline ObExprParser::ObExprParser(common::ObIAllocator &allocator, ObExprParseMode parse_mode)
    : allocator_(allocator), parse_mode_(parse_mode)
{
}

inline int ObExprParser::init_result(ObExprParseResult &parse_result, const char *start_pos)
{
  int ret = common::OB_SUCCESS;
  // input
  parse_result.malloc_pool_ = static_cast<void *>(&allocator_);
  parse_result.parse_mode_ = parse_mode_;
  parse_result.start_pos_ = start_pos;

  // scan info
  parse_result.yyscan_info_ = NULL;
  parse_result.tmp_buf_ = NULL;
  parse_result.tmp_len_ = 0;
  parse_result.end_pos_ = NULL;
  parse_result.column_idx_ = 0;
  parse_result.values_list_idx_ = 0;
  parse_result.multi_param_values_ = 0;
  parse_result.placeholder_list_idx_ = 0;
  parse_result.need_parse_token_list_ = true;

  // result argument
  parse_result.relation_info_.relation_num_ = 0;
  parse_result.all_relation_info_.relation_num_ = 0;
  parse_result.all_relation_info_.right_value_num_ = 0;
  for (int64_t i = 0; i < parse_result.part_key_info_.key_num_; ++i) {
    parse_result.part_key_info_.part_keys_[i].is_exist_in_sql_ = false;
  }

  if (INVALID_PARSE_MODE == parse_result.parse_mode_) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(DEBUG, "failed to initialized parser, maybe parse sql for shard user",
              K(parse_result.parse_mode_),
              K(ret));
  }
  return common::OB_SUCCESS;
}

inline void ObExprParser::free_result(ObExprParseResult &parse_result)
{
  parse_result.yyscan_info_ = NULL;
}

inline int ObExprParser::parse(const common::ObString &sql_string,
                               ObExprParseResult &parse_result,
                               common::ObCollationType connection_collation)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_SUCCESS != init_result(parse_result, sql_string.ptr())) {
    ret = common::OB_ERR_PARSER_INIT;
    PROXY_LOG(WARN, "failed to initialized parser", KERRMSGS, K(ret));
  } else {
    switch (connection_collation) {
      //case 28/*CS_TYPE_GBK_CHINESE_CI*/:
      //case 87/*CS_TYPE_GBK_BIN*/:
      case 216/*CS_TYPE_GB18030_2022_BIN*/:
      case 217/*CS_TYPE_GB18030_2022_PINYIN_CI*/:
      case 218/*CS_TYPE_GB18030_2022_PINYIN_CS*/:
      case 219/*CS_TYPE_GB18030_2022_RADICAL_CI*/:
      case 220/*CS_TYPE_GB18030_2022_RADICAL_CS*/:
      case 221/*CS_TYPE_GB18030_2022_STROKE_CI*/:
      case 222/*CS_TYPE_GB18030_2022_STROKE_CS*/:
      case 248/*CS_TYPE_GB18030_CHINESE_CI*/:
      case 249/*CS_TYPE_GB18030_BIN*/:
      case 251/*CS_TYPE_GB18030_CHINESE_CS*/:
        if (common::OB_SUCCESS != ob_expr_parse_gbk_sql(&parse_result,
                                                        sql_string.ptr(),
                                                        static_cast<size_t>(sql_string.length()))) {
          ret = common::OB_ERR_PARSE_SQL;
          PROXY_LOG(WARN, "failed to parser gbk sql", KERRMSGS, K(connection_collation), K(ret));
        }
        break;
      case 45/*CS_TYPE_UTF8MB4_GENERAL_CI*/:
      case 46/*CS_TYPE_UTF8MB4_BIN*/:
      default:
        if (common::OB_SUCCESS != ob_expr_parse_utf8_sql(&parse_result,
                                                         sql_string.ptr(),
                                                         static_cast<size_t>(sql_string.length()))) {
          ret = common::OB_ERR_PARSE_SQL;
          PROXY_LOG(WARN, "failed to parser utf8 sql", KERRMSGS, K(connection_collation), K(ret));
        }
        break;
    }
  }
  return ret;
}

inline int ObExprParser::parse_reqsql(const common::ObString &req_sql, int64_t parsed_length,
                                      ObExprParseResult &expr_result, ObProxyBasicStmtType stmt_type,
                                      common::ObCollationType connection_collation)
{
  int ret = common::OB_SUCCESS;
  common::ObString expr_sql = obproxy::proxy::ObProxyMysqlRequest::get_expr_sql(req_sql, parsed_length);
  const char *expr_sql_str = expr_sql.ptr();
  const char *pos = NULL;
  char* replace_sql_str = NULL;
  int   replace_sql_len = 0;
  if (OB_LIKELY(NULL != expr_sql_str)) {
    if (SELECT_STMT_PARSE_MODE == parse_mode_) {
      if (NULL != (pos = strcasestr(expr_sql_str, "JOIN"))) {
        // pos = JOIN
      } else if (NULL != (pos = strcasestr(expr_sql_str, "WHERE"))) {
        // pos = WHERE
      }
    } else if (OBPROXY_T_UPDATE == stmt_type) {
      if (NULL != (pos = strcasestr(expr_sql_str, "SET"))) {
        // pos = SET
      }
    } else if (OBPROXY_T_MERGE == stmt_type) {
      if (NULL != (pos = strcasestr(expr_sql_str, "ON"))) {
        // pos = ON
      }
    }

    if ((NULL != pos) && ((pos - expr_sql_str) <= 1 || (pos - expr_sql_str) >= expr_sql.length())) {
      pos = NULL;
    }

    if (NULL != pos) {
      expr_sql += static_cast<int32_t>(pos - expr_sql_str);
    }

    if (OBPROXY_T_TEXT_PS_PREPARE == stmt_type || OBPROXY_T_TEXT_PS_EXECUTE == stmt_type) {
      int i, j, index = 0;
      replace_sql_len = expr_sql.length();
      if (OB_ISNULL(replace_sql_str = static_cast<char *>(op_fixed_mem_alloc(replace_sql_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        PROXY_LOG(WARN, "fail to alloc mem", "alloc_size", replace_sql_len, K(ret));
      } else {
        // PREPARE ps_stmt FROM 'select * from test9 where ID = \\'001\\''; remove escape symbols
        for (i = 0; i < expr_sql.length() - 1; ++i) {
          if (expr_sql[i] == 0x5c && (expr_sql[i+1] == 0x27 || expr_sql[i+1] == 0x22)) {
            // do nothing
          } else {
            replace_sql_str[index++] = expr_sql[i];
          }
        }
        replace_sql_str[index++] = expr_sql[i];

        // PREPARE ps_stmt FROM 'select * from test9 where ID = \\'001\\''; remove trailing single quotes
        if (replace_sql_str[index-3] == 0x27 || replace_sql_str[index-3] == 0x22) {
          for (j = index - 3; j < index - 1; ++j) {
            replace_sql_str[j] = replace_sql_str[j+1];
          }
          index -= 1;
        }
        PROXY_LOG(DEBUG, "parse length", K(index), K(replace_sql_len), K(replace_sql_str)); 
        expr_sql.assign_ptr(replace_sql_str, index);
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(parse(expr_sql, expr_result, connection_collation))) {
      PROXY_LOG(DEBUG, "fail to do expr parse", K(expr_sql), K(ret));
    } else {
      PROXY_LOG(DEBUG, "succ to do expr parse", "expr_result", ObExprParseResultPrintWrapper(expr_result), K(expr_sql));
    }
  }
  if (NULL != replace_sql_str) {
    op_fixed_mem_free(replace_sql_str, replace_sql_len);
  }
  return ret;
}

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBEXPR_PARSER_H
