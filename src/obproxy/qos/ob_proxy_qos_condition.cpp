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
      // this is NoWhere. so if have where, not match
      is_match = false;
    } else {
      is_match = true;
    }
  } else {
    // if expr_sql is empry, no where. match
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
    // if expr_sql is empry, no like. not match
    is_match = false;
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
    // ALL is CURD
    if (OBPROXY_T_SELECT == stmt_type
        || OBPROXY_T_UPDATE == stmt_type
        || OBPROXY_T_INSERT == stmt_type
        || OBPROXY_T_DELETE == stmt_type
        || OBPROXY_T_MERGE == stmt_type) {
      is_match = true;
    } else {
      is_match = false;
    }
  } else {
    is_match = (stmt_type == stmt_type_);
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

  // param empty means match all table
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

  ObString expr_sql = client_request.get_expr_sql();

  // param empty means match all sql
  if (is_param_empty_) {
    is_match = true;
  } else  {
    if (OB_LIKELY(!expr_sql.empty())) {
      if (OB_FAIL(sql_re_.match(expr_sql, 0, is_match, *allocator))) {
        LOG_WDIAG("fail to match sql", K(expr_sql), K(ret));
      }
    } else {
      is_match = false;
    }
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
