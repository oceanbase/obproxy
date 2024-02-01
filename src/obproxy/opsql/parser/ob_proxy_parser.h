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

#ifndef OBPROXY_PARSER_H
#define OBPROXY_PARSER_H
#include "lib/ob_define.h"
#include "common/ob_sql_mode.h"
#include "opsql/parser/ob_proxy_parse_result.h"
#include "lib/string/ob_string.h"
#include "utils/ob_proxy_lib.h"
#include "lib/charset/ob_charset.h"

#include <ob_sql_parser.h>
#include <parse_malloc.h>
#include <parse_node.h>

extern "C" int obproxy_parse_utf8_sql(ObProxyParseResult *p, const char *pszSql, size_t iLen);
extern "C" int obproxy_parse_gbk_sql(ObProxyParseResult *p, const char *pszSql, size_t iLen);

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
class ObProxyParser
{
public:
  explicit ObProxyParser(common::ObIAllocator &allocator, ObProxyParseMode parse_mode);
  // will not be inherited, do not set to virtual
  ~ObProxyParser() {}

  int parse(const common::ObString &sql_string, ObProxyParseResult &parse_result,
            common::ObCollationType connection_collation);
  void free_result(ObProxyParseResult &parse_result);
  // the following function use ob parser
  int obparse(const common::ObString &sql_string, ParseResult &parse_result);
private:
  int init_result(ObProxyParseResult &parse_result, const char *start_pos);
  int init_ob_result(ParseResult &parse_result, const common::ObString &sql_string);
  // data members
  common::ObIAllocator &allocator_;
  ObProxyParseMode parse_mode_;

  DISALLOW_COPY_AND_ASSIGN(ObProxyParser);
};

inline ObProxyParser::ObProxyParser(common::ObIAllocator &allocator, ObProxyParseMode parse_mode)
    : allocator_(allocator), parse_mode_(parse_mode)
{
}

inline int ObProxyParser::init_result(ObProxyParseResult &parse_result, const char *start_pos)
{
  memset(&parse_result, 0, sizeof(parse_result));
  parse_result.malloc_pool_ = static_cast<void *>(&allocator_);
  parse_result.parse_mode_ = parse_mode_;
  parse_result.start_pos_ = start_pos;
  parse_result.end_pos_ = NULL;
  parse_result.read_consistency_type_ = OBPROXY_READ_CONSISTENCY_INVALID;
  parse_result.table_info_.table_name_.str_len_ = 0;
  parse_result.table_info_.alias_name_.str_len_ = 0;
  parse_result.table_info_.database_name_.str_len_ = 0;

  parse_result.cmd_info_.sub_type_ = OBPROXY_T_SUB_INVALID;
  parse_result.cmd_info_.err_type_ = OBPROXY_T_ERR_INVALID;
  for (int64_t i = 0; i < OBPROXY_ICMD_MAX_VALUE_COUNT; ++i) {
    parse_result.cmd_info_.integer_[i] = -1;
  }
  parse_result.has_simple_route_info_ = false;
  parse_result.placeholder_list_idx_ = 0;
  parse_result.text_ps_name_.str_len_ = 0;

  parse_result.has_shard_comment_ = false;
  parse_result.dbmesh_route_info_.group_idx_str_.str_len_ = 0;
  parse_result.dbmesh_route_info_.tb_idx_str_.str_len_ = 0;
  parse_result.dbmesh_route_info_.es_idx_str_.str_len_ = 0;
  parse_result.dbmesh_route_info_.testload_str_.str_len_ = 0;
  parse_result.dbmesh_route_info_.table_name_str_.str_len_ = 0;
  parse_result.dbmesh_route_info_.disaster_status_str_.str_len_ = 0;
  parse_result.dbmesh_route_info_.node_count_ = 0;
  parse_result.dbmesh_route_info_.head_ = NULL;
  parse_result.dbmesh_route_info_.tail_ = NULL;
  parse_result.dbmesh_route_info_.index_count_ = 0;

  parse_result.set_parse_info_.node_count_ = 0;
  parse_result.set_parse_info_.head_ = NULL;
  parse_result.set_parse_info_.tail_ = NULL;

  parse_result.comment_begin_ = NULL;
  parse_result.comment_end_ = NULL;

  return common::OB_SUCCESS;
}

inline int ObProxyParser::init_ob_result(ParseResult &parse_result, const common::ObString &sql_string)
{
  int ret = common::OB_SUCCESS;
  int new_length = static_cast<int>(sql_string.length());
  memset(&parse_result, 0, sizeof(parse_result));
  parse_result.is_fp_ = false;
  parse_result.is_multi_query_ = false;
  parse_result.malloc_pool_ =  static_cast<void *>(&allocator_);
  parse_result.is_ignore_hint_ = false;
  parse_result.need_parameterize_ = true;
  parse_result.pl_parse_info_.is_pl_parse_ = false;
  parse_result.minus_ctx_.has_minus_ = false;
  parse_result.minus_ctx_.pos_ = -1;
  parse_result.minus_ctx_.raw_sql_offset_ = -1;
  parse_result.is_for_trigger_ = false;
  parse_result.is_dynamic_sql_ = false;
  parse_result.is_batched_multi_enabled_split_ = false;
  parse_result.realloc_cnt_ = 10; //control hint's alloc
  char *buf = (char *)parse_malloc(new_length, parse_result.malloc_pool_);

  parse_result.param_nodes_ = NULL;
  parse_result.tail_param_node_ = NULL;
  parse_result.no_param_sql_ = buf;
  parse_result.no_param_sql_buf_len_ = new_length;
  return ret;
}

inline void ObProxyParser::free_result(ObProxyParseResult &parse_result)
{
  parse_result.yyscan_info_ = NULL;
}

inline int ObProxyParser::obparse(const common::ObString &sql_string,
                                  ParseResult &parse_result)
{
  int ret = common::OB_SUCCESS;
  memset(&parse_result, 0, sizeof(parse_result));
  parse_result.sql_mode_ = DEFAULT_MYSQL_MODE; // DEFAULT_MYSQL_MODE;
  if(sql_string.empty()) {
    ret = common::OB_INVALID_ARGUMENT;
    PROXY_LOG(WDIAG, "sql_string is empty", K(ret));
  } else {
    common::ObString parse_sql_string = sql_string;
    //set ; ob parse need end with ;
    bool add_semicolon = false;
    char origin_byte = parse_sql_string.ptr()[sql_string.length() - 2];
    if (parse_sql_string.ptr()[sql_string.length() - 3] != ';') {
      parse_sql_string.ptr()[sql_string.length() - 2] = ';';
      add_semicolon = true;
    }
    if (0 != init_ob_result(parse_result, parse_sql_string)) {
      ret = common::OB_ERR_PARSER_INIT;
      PROXY_LOG(WDIAG, "failed to initialized parser", KERRMSGS, K(ret));
    } else {
      sql::ObSQLParser sql_parser(*(common::ObIAllocator *)(parse_result.malloc_pool_),
                           parse_result.sql_mode_);
      if (OB_FAIL(sql_parser.parse(parse_sql_string.ptr(),  parse_sql_string.length(), parse_result))) {
        PROXY_LOG(WDIAG, "failed to do obparse", K(sql_string), K(ret));
      } else {
        PROXY_LOG(DEBUG, "obparse succ", K(sql_string), K(ret));
      }
    }
    if (add_semicolon) {
      parse_sql_string.ptr()[sql_string.length() - 2] = origin_byte;
    }
  }
  return ret;
}

inline int ObProxyParser::parse(const common::ObString &sql_string,
                                ObProxyParseResult &parse_result,
                                common::ObCollationType connection_collation)
{
  int ret = common::OB_SUCCESS;
  if (0 != init_result(parse_result, sql_string.ptr())) {
    ret = common::OB_ERR_PARSER_INIT;
    PROXY_LOG(WDIAG, "failed to initialized parser", KERRMSGS, K(ret));
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
        if (common::OB_SUCCESS != obproxy_parse_gbk_sql(&parse_result,
                                                        sql_string.ptr(),
                                                        static_cast<size_t>(sql_string.length()))) {
          ret = common::OB_ERR_PARSE_SQL;
        }
        break;
      case 45/*CS_TYPE_UTF8MB4_GENERAL_CI*/:
      case 46/*CS_TYPE_UTF8MB4_BIN*/:
      default:
        if (common::OB_SUCCESS != obproxy_parse_utf8_sql(&parse_result,
                                                         sql_string.ptr(),
                                                         static_cast<size_t>(sql_string.length()))) {
          ret = common::OB_ERR_PARSE_SQL;
        }
        break;
    }
  }
  return ret;
}

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_PARSER_H
