/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "qos/ob_proxy_qos_condition.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{
using namespace common;
using namespace proxy;
using namespace obutils;

int64_t ObProxyQosCond::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type));
  J_OBJ_END();
  return pos;
}

int ObProxyQosCondNoWhere::calc(ObProxyMysqlRequest &client_request,
                                ObIAllocator *allocator,
                                bool &is_match)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;

  ObString expr_sql = client_request.get_expr_sql();

  if (OB_LIKELY(!expr_sql.empty())) {
    const char *expr_sql_str = expr_sql.ptr();
    const char *pos = NULL;
    if (NULL != (pos = strcasestr(expr_sql_str, "WHERE"))
        && OB_LIKELY((pos - expr_sql_str) < expr_sql.length())) {
      // 这个条件是 noWhere, 匹配表示有 where, 就不满足条件
      is_match = false;
    } else {
      is_match = true;
    }
  } else {
    // expr_sql 为空, 表示没有 where, 满足条件
    is_match = true;
  }

  return ret;
}

int ObProxyQosCondUseLike::calc(ObProxyMysqlRequest &client_request,
                                ObIAllocator *allocator,
                                bool &is_match)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;

  ObString expr_sql = client_request.get_expr_sql();

  if (OB_LIKELY(!expr_sql.empty())) {
    const char *expr_sql_str = expr_sql.ptr();
    const char *pos = NULL;
    if (NULL != (pos = strcasestr(expr_sql_str, "LIKE"))
        && OB_LIKELY((pos - expr_sql_str) < expr_sql.length())) {
      is_match = true;
    } else {
      is_match = false;
    }
  } else {
    // expr_sql 为空, 表示没有 like, 不满足条件
    is_match = false;
  }

  return ret;
}

int ObProxyQosCondStmtType::add_stmt_type(const ObProxyBasicStmtType stmt_type)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(stmt_type_array_.push_back(stmt_type))) {
    LOG_WDIAG("fail to push back stmt type", K(stmt_type), K(ret));
  }

  return ret;
}

int ObProxyQosCondStmtType::calc(ObProxyMysqlRequest &client_request,
                                 ObIAllocator *allocator,
                                 bool &is_match)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;

  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObProxyBasicStmtType stmt_type = parse_result.get_stmt_type();

  if (OB_PROXY_QOS_COND_STMT_KIND_ALL == stmt_kind_) {
    is_match = true;
  } else if (OB_UNLIKELY(stmt_type_array_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("empty stmt type array", K(ret));
  } else {
    // !stmt_type_array_.empty()
    is_match = false;
    for (int64_t i = 0; i < stmt_type_array_.count(); ++i) {
      if (stmt_type == stmt_type_array_.at(i)) {
        is_match = true;
        break;
      }
    }

  }

  return ret;
}

int ObProxyQosCondTableName::init(const common::ObString &table_name_re, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (table_name_re.empty()) {
    is_param_empty_ = true;
  } else {
    if (OB_FAIL(table_name_re_.init(table_name_re, OB_REG_ICASE, *allocator))) {
      LOG_WDIAG("fail to init table name re", K(table_name_re), K(ret));
    }
  }

  return ret;
}

int ObProxyQosCondTableName::calc(ObProxyMysqlRequest &client_request,
                                  ObIAllocator *allocator,
                                  bool &is_match)
{
  int ret = OB_SUCCESS;

  // 如果参数为空, 表示匹配全部表
  if (is_param_empty_) {
    is_match = true;
  } else {
    ObSqlParseResult &parse_result = client_request.get_parse_result();
    ObString table_name = parse_result.get_table_name();

    if (OB_LIKELY(!table_name.empty())) {
      if (OB_FAIL(table_name_re_.match(table_name, 0, is_match, *allocator))) {
        LOG_WDIAG("fail to match table name", K(table_name), K(ret));
      }
    } else {
      is_match = false;
    }
    LOG_DEBUG("match sql result", K(is_match), K_(table_name_re), K(table_name));
  }

  return ret;
}

int ObProxyQosCondSQLMatch::init(const common::ObString &sql_re, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (sql_re.empty()) {
    is_param_empty_ = true;
  } else {
    if (OB_FAIL(sql_re_.init(sql_re, OB_REG_ICASE, *allocator))) {
      LOG_WDIAG("fail to init sql re", K(sql_re), K(ret));
    }
  }

  return ret;
}

int ObProxyQosCondSQLMatch::calc(ObProxyMysqlRequest &client_request,
                                 ObIAllocator *allocator,
                                 bool &is_match)
{
  int ret = OB_SUCCESS;

  ObString sql = client_request.get_parse_sql();

  // 如果参数为空, 表示匹配所有 SQL
  if (is_param_empty_) {
    is_match = true;
  } else  {
    if (OB_LIKELY(!sql.empty())) {
      if (OB_FAIL(sql_re_.match(sql, 0, is_match, *allocator))) {
        LOG_WDIAG("fail to match sql", K(sql), K(ret));
      }
    } else {
      is_match = false;
    }
    LOG_DEBUG("match sql result", K(is_match), K_(sql_re), K(sql));
  }

  return ret;
}

int ObProxyQosCondTestLoadTableName::calc(ObProxyMysqlRequest &client_request,
                                          ObIAllocator *allocator,
                                          bool &is_match)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;

  is_match = false;
  ObSqlParseResult &parse_result = client_request.get_parse_result();
  ObString table_name = parse_result.get_table_name();

  int32_t table_name_length = table_name.length();
  if (table_name_length > 2) {
    if ((table_name[table_name_length - 1] == 't' || table_name[table_name_length - 1] == 'T')
        && (table_name[table_name_length - 2] == '_')) {
      is_match = true;
    }
  }

  return ret;
}

} // end qos
} // end obproxy
} // end oceanbase
