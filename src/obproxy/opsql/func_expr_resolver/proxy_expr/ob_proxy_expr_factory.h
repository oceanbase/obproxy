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

#ifndef OB_FUNC_EXPR_PROXY_EXPR_FACTORY_H
#define OB_FUNC_EXPR_PROXY_EXPR_FACTORY_H

#define USING_LOG_PREFIX PROXY

#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{

class ObProxyExprFactory
{
public:
  explicit ObProxyExprFactory(common::ObIAllocator &allocator) : allocator_(allocator) {}
  ~ObProxyExprFactory() {}

  template<typename T>
  int create_proxy_expr(const ObProxyExprType type, T *&expr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(OB_PROXY_EXPR_TYPE_NONE >= type || OB_PROXY_EXPR_TYPE_MAX <= type)) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      expr = NULL;
      void *ptr = allocator_.alloc(sizeof(T));

      if (OB_UNLIKELY(NULL == ptr)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        expr = new(ptr) T();
        expr->set_expr_type(type);
      }
    }

    return ret;
  }

private:
  common::ObIAllocator &allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObProxyExprFactory);
};

} // end opsql
} // end obproxy
} // end oceanbase

#endif