/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_PROXY_FUNC_EXPR_RESOLVER_H
#define OB_PROXY_FUNC_EXPR_RESOLVER_H

#include "opsql/func_expr_parser/ob_func_expr_parse_result.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_factory.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace obproxy
{
namespace opsql
{

struct ObFuncExprResolverContext
{
  ObFuncExprResolverContext(common::ObIAllocator *allocator, ObProxyExprFactory *expr_factory)
   : allocator_(allocator), expr_factory_(expr_factory) {}

public:
  common::ObIAllocator *allocator_;
  ObProxyExprFactory *expr_factory_;
};

class ObFuncExprResolver
{
public:
  explicit ObFuncExprResolver(ObFuncExprResolverContext &ctx) : ctx_(ctx) {}
  ~ObFuncExprResolver() {}

  int resolve(const ObProxyParamNode *node, ObProxyExpr *&expr);

private:
  int recursive_resolve_proxy_expr(const ObProxyParamNode *node, ObProxyExpr *&expr);
  int create_func_expr_by_type(const ObFuncExprNode *node, ObProxyFuncExpr *&func_expr);

private:
  ObFuncExprResolverContext &ctx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFuncExprResolver);
};

} // end opsql
} // end obproxy
} // end oceanbase

#endif