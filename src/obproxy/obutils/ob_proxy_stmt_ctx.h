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

#ifndef OBPROXY_RESOLVE_CTX_H
#define OBPROXY_RESOLVE_CTX_H

#include "lib/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{

namespace common
{
class ObString;
}
namespace obproxy
{
namespace opsql
{
class ObProxyExpr;
}
namespace obutils
{


class ObProxyStmtCtx
{
public:
  typedef common::hash::ObHashMap<common::ObString, opsql::ObProxyExpr *, common::hash::NoPthreadDefendMode> ExprMap;
  typedef common::ObSEArray<std::pair<common::ObString, opsql::ObProxyExpr *>, 4> ExprArray;
  const int default_bucket_num = 50; // real bucket num is 53 in cal_next_prime
  ObProxyStmtCtx(common::ObIAllocator& allocator);
  ~ObProxyStmtCtx();
  int init(int pre_allocate_level = 0);

public:
  int inc_ctx_level();
  int dec_ctx_level();

  int find_name(const common::ObString& name, opsql::ObProxyExpr*& ret_expr);
  int find_table_name(const common::ObString& name, opsql::ObProxyExpr*& ret_expr);
  int find_alias_name(const common::ObString& name, opsql::ObProxyExpr*& ret_expr);
  int add_table_name(const common::ObString& name, opsql::ObProxyExpr* const table_expr);
  int add_alias_name(const common::ObString& name, opsql::ObProxyExpr* const alias_expr);
  ExprMap& get_all_table_exprs_map() {return all_table_exprs_map_;}
  ExprArray& get_all_table_exprs_array() {return all_table_exprs_array_;}

private:
  int64_t valid_level_;
  common::ObSEArray<ExprMap*, 4> table_exprs_maps_;
  common::ObSEArray<ExprMap*, 4> alias_exprs_maps_;
  ExprMap all_table_exprs_map_;
  ExprArray all_table_exprs_array_;
  common::ObIAllocator& allocator_;
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif //