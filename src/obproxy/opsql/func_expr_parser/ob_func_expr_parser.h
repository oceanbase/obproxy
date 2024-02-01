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

#ifndef OB_FUNC_EXPR_PARSER_H
#define OB_FUNC_EXPR_PARSER_H
#include "lib/ob_define.h"
#include "common/ob_sql_mode.h"
#include "lib/string/ob_string.h"
#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"

extern "C" int ob_func_expr_parse_sql(ObFuncExprParseResult *p, const char *pszSql, size_t iLen);

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
class ObFuncExprParser
{
public:
  explicit ObFuncExprParser(common::ObIAllocator &allocator, ObFuncExprParseMode parse_mode);
  // will not be inherited, do not set to virtual
  ~ObFuncExprParser() {}

  int parse(const common::ObString &sqlFunc_string, ObFuncExprParseResult &parse_result);
  void free_result(ObFuncExprParseResult &parse_result);
private:
  int init_result(ObFuncExprParseResult &parse_result, const char *start_pos);
  // data members
  common::ObIAllocator &allocator_;
  ObFuncExprParseMode parse_mode_;

  DISALLOW_COPY_AND_ASSIGN(ObFuncExprParser);
};

inline ObFuncExprParser::ObFuncExprParser(common::ObIAllocator &allocator, ObFuncExprParseMode parse_mode)
    : allocator_(allocator), parse_mode_(parse_mode)
{
}

inline int ObFuncExprParser::init_result(ObFuncExprParseResult &parse_result, const char *start_pos)
{
  int ret = common::OB_SUCCESS;
  // input
  memset(&parse_result, 0, sizeof(ObFuncExprParseResult));
  parse_result.malloc_pool_ = static_cast<void *>(&allocator_);
  parse_result.parse_mode_ = parse_mode_;
  parse_result.start_pos_ = start_pos;

  parse_result.param_node_ = NULL;

  // scan info
  parse_result.yyscan_info_ = NULL;
  parse_result.tmp_buf_ = NULL;
  parse_result.tmp_len_ = 0;
  parse_result.end_pos_ = NULL;

  return ret;
}

inline void ObFuncExprParser::free_result(ObFuncExprParseResult &parse_result)
{
  parse_result.yyscan_info_ = NULL;
}

inline int ObFuncExprParser::parse(const common::ObString &sql_string,
                                   ObFuncExprParseResult &parse_result)
{
  int ret = common::OB_SUCCESS;
  if (common::OB_SUCCESS != init_result(parse_result, sql_string.ptr())) {
    ret = common::OB_ERR_PARSER_INIT;
    PROXY_LOG(WDIAG, "fail to initialized parser", KERRMSGS, K(ret));
  } else if (common::OB_SUCCESS != ob_func_expr_parse_sql(&parse_result,
                                                          sql_string.ptr(),
                                                          static_cast<size_t>(sql_string.length()))) {
    ret = common::OB_ERR_PARSE_SQL;
    PROXY_LOG(INFO, "ob_func_expr_parse_sql failed", K(ret), K(sql_string));
  }

  return ret;
}

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_FUNC_EXPR_PARSER_H
