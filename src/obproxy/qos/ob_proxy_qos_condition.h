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

#ifndef OB_PROXY_QOS_CONDITION_H
#define OB_PROXY_QOS_CONDITION_H

#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "common/expression/ob_expr_regexp_context.h"

namespace oceanbase
{
namespace obproxy
{
namespace qos
{

typedef enum ObProxyQosCondType
{
  OB_PROXY_QOS_COND_TYPE_NONE = 0,
  OB_PROXY_QOS_COND_TYPE_NOWHERE,
  OB_PROXY_QOS_COND_TYPE_TABLE_NAME,
  OB_PROXY_QOS_COND_TYPE_STMT_TYPE,
  OB_PROXY_QOS_COND_TYPE_SQL_MATCH,
  OB_PROXY_QOS_COND_TYPE_MAX,
} ObProxyQosCondType;

class ObProxyQosCond
{
public:
  explicit ObProxyQosCond() : type_(OB_PROXY_QOS_COND_TYPE_NONE) {}
  ~ObProxyQosCond() {}

  void set_cond_type(const ObProxyQosCondType type) { type_ = type; }
  ObProxyQosCondType get_cond_type() const { return type_; }

  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match) = 0;

  int64_t to_string(char *buf, int64_t buf_len) const;

private:
  ObProxyQosCondType type_;
};

class ObProxyQosCondNoWhere : public ObProxyQosCond
{
public:
  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match);
};

class ObProxyQosCondUseLike : public ObProxyQosCond
{
public:
  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match);
};

typedef enum ObProxyQosCondStmtKind
{
  OB_PROXY_QOS_COND_STMT_KIND_INVALID = 0,
  OB_PROXY_QOS_COND_STMT_KIND_ALL,
  OB_PROXY_QOS_COND_STMT_KIND_DDL,
  OB_PROXY_QOS_COND_STMT_KIND_MAX
} ObProxyQosCondStmtKind;

class ObProxyQosCondStmtType : public ObProxyQosCond
{
public:
  ObProxyQosCondStmtType() :
    stmt_type_(OBPROXY_T_INVALID), stmt_kind_(OB_PROXY_QOS_COND_STMT_KIND_INVALID) {}
  void set_stmt_type(const ObProxyBasicStmtType stmt_type) { stmt_type_ = stmt_type; }
  ObProxyBasicStmtType get_stmt_type() const { return stmt_type_; }

  void set_stmt_kind(const ObProxyQosCondStmtKind stmt_kind) { stmt_kind_ = stmt_kind; }

  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match);

private:
  ObProxyBasicStmtType stmt_type_;
  ObProxyQosCondStmtKind stmt_kind_;
};

class ObProxyQosCondTableName : public ObProxyQosCond
{
public:
  ObProxyQosCondTableName() : is_param_empty_(false) {}
  int init(const common::ObString &table_name_re, common::ObIAllocator *allocator);
  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match);

private:
  common::ObExprRegexContext table_name_re_;
  bool is_param_empty_;
};

class ObProxyQosCondSQLMatch : public ObProxyQosCond
{
public:
  ObProxyQosCondSQLMatch() : is_param_empty_(false) {}
  int init(const common::ObString &sql_re, common::ObIAllocator *allocator);
  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match);

private:
  common::ObExprRegexContext sql_re_;
  bool is_param_empty_;
};

class ObProxyQosCondTestLoadTableName : public ObProxyQosCond
{
public:
  ObProxyQosCondTestLoadTableName() {}
  virtual int calc(proxy::ObProxyMysqlRequest &client_request, common::ObIAllocator *allocator, bool &is_match);
};

} // end qos
} // end obproxy
} // end oceanbase

#endif
