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

#ifndef OBPROXY_EXPR_CALCULATOR_H
#define OBPROXY_EXPR_CALCULATOR_H
#include "opsql/expr_parser/ob_expr_parse_result.h"
namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObArenaAllocator;
class ObString;
}
namespace obproxy
{
namespace opsql
{
class ObExprResolverResult;
}
namespace obutils
{
class ObSqlParseResult;
}
namespace proxy
{
class ObProxyMysqlRequest;
class ObProxyPartInfo;
class ObClientSessionInfo;
class ObPsEntry;
class ObTextPsEntry;

class ObProxyExprCalculator
{
public:
  ObProxyExprCalculator() {}
  ~ObProxyExprCalculator() {}
  int calculate_partition_id(common::ObArenaAllocator &allocator,
                             const common::ObString &req_sql,
                             const obutils::ObSqlParseResult &parse_result,
                             ObProxyMysqlRequest &client_request,
                             ObClientSessionInfo &client_info,
                             ObProxyPartInfo &part_info,
                             int64_t &partition_id);
private:
  // do parse -> do resolve -> do partition id calc
  int do_expr_parse(const common::ObString &req_sql,
                    const obutils::ObSqlParseResult &parse_result,
                    ObProxyPartInfo &part_info,
                    common::ObIAllocator &allocator,
                    ObExprParseResult &expr_result);
  int do_expr_resolve(ObExprParseResult &expr_result,
                      const ObProxyMysqlRequest &client_request,
                      ObClientSessionInfo *client_info,
                      ObPsEntry *ps_entry,
                      ObTextPsEntry *text_ps_entry,
                      ObProxyPartInfo &part_info,
                      common::ObIAllocator &allocator,
                      opsql::ObExprResolverResult &resolve_result);
  int do_partition_id_calc(opsql::ObExprResolverResult &resolve_result,
                           ObProxyPartInfo &part_info,
                           common::ObIAllocator &allocator,
                           int64_t &partition_id);
  int calc_part_id_with_simple_route_info(common::ObArenaAllocator &allocator,
                                          const obutils::ObSqlParseResult &parse_result,
                                          ObProxyPartInfo &part_info,
                                          int64_t &part_id);
  int do_resolve_with_part_key(const obutils::ObSqlParseResult &parse_result,
                               common::ObIAllocator &allocator,
                               opsql::ObExprResolverResult &resolve_result);
  int calc_partition_id_using_rowid(const ObExprParseResult &parse_result,
                                    ObProxyPartInfo &part_info,
                                    opsql::ObExprResolverResult &resolve_result,
                                    common::ObIAllocator &allocator);
};
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_EXPR_CALCULATOR_H */
