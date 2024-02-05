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
#include "lib/charset/ob_charset.h"
#include "common/ob_obj_type.h"
#include "common/ob_object.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObArenaAllocator;
class ObString;
class ObTimeZoneInfo;
class ObDataTypeCastParams;
}
namespace obproxy
{
namespace opsql
{
class ObExprResolverResult;
struct ObExprResolverContext;
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
class ObPsIdEntry;
class ObTextPsEntry;
class ObServerRoute;
class ObRouteDiagnosis;

class ObProxyExprCalculator
{
public:
  ObProxyExprCalculator() : route_diagnosis_(NULL) {}
  ~ObProxyExprCalculator() { set_route_diagnosis(NULL); }
  int calculate_partition_id(common::ObArenaAllocator &allocator,
                             const common::ObString &req_sql,
                             const obutils::ObSqlParseResult &parse_result,
                             ObProxyMysqlRequest &client_request,
                             ObClientSessionInfo &client_info,
                             ObServerRoute &route,
                             ObProxyPartInfo &part_info,
                             int64_t &partition_id);
  int calc_part_id_by_random_choose_from_exist(ObProxyPartInfo &part_info,
                                               int64_t &first_part_id,
                                               int64_t &sub_part_id,
                                               int64_t &phy_part_id);
  void set_route_diagnosis(ObRouteDiagnosis *route_diagnosis);
private:
  // do parse -> do resolve -> do partition id calc
  int do_expr_parse(const common::ObString &req_sql,
                    const obutils::ObSqlParseResult &parse_result,
                    ObProxyPartInfo &part_info,
                    common::ObIAllocator &allocator,
                    ObExprParseResult &expr_result,
                    common::ObCollationType connection_collation);
  int do_expr_resolve(ObExprParseResult &expr_result,
                      const ObProxyMysqlRequest &client_request,
                      ObClientSessionInfo *client_info,
                      ObPsIdEntry *ps_id_entry,
                      ObTextPsEntry *text_ps_entry,
                      ObProxyPartInfo &part_info,
                      common::ObIAllocator &allocator,
                      opsql::ObExprResolverResult &resolve_result,
                      const obutils::ObSqlParseResult &sql_parse_result,
                      int64_t &partition_id);
  int do_partition_id_calc(opsql::ObExprResolverResult &resolve_result,
                           ObClientSessionInfo &client_info,
                           ObServerRoute &route,
                           ObProxyPartInfo &part_info,
                           const obutils::ObSqlParseResult &parse_result,
                           common::ObIAllocator &allocator,
                           int64_t &partition_id,
                           int64_t &part_idx,
                           int64_t &sub_part_idx);
  int calc_part_id_with_simple_route_info(common::ObArenaAllocator &allocator,
                                          const obutils::ObSqlParseResult &parse_result,
                                          ObClientSessionInfo &client_info,
                                          ObServerRoute &route,
                                          ObProxyPartInfo &part_info,
                                          int64_t &part_id,
                                          int64_t &part_idx,
                                          int64_t &sub_part_idx);
  int do_resolve_with_part_key(const obutils::ObSqlParseResult &parse_result,
                               common::ObIAllocator &allocator,
                               opsql::ObExprResolverResult &resolve_result);
  int calc_partition_id_using_rowid(opsql::ObExprResolverContext &ctx,
                                    opsql::ObExprResolverResult &resolve_result,
                                    common::ObIAllocator &allocator,
                                    int64_t &partition_id);
  int calc_partition_id_with_rowid(ObProxyRelationExpr *relation,
                                   opsql::ObExprResolverContext &ctx,
                                   common::ObIAllocator &allocator,
                                   opsql::ObExprResolverResult &resolve_result,
                                   int64_t &partition_id);
  int calc_partition_id_with_rowid_str(const char *str,
                                       const int64_t str_len,
                                       common::ObIAllocator &allocator,
                                       opsql::ObExprResolverResult &resolve_result,
                                       ObProxyPartInfo &part_info,
                                       int64_t &partition_id,
                                       int32_t &state,
                                       int16_t &version);
  int do_expr_parse_diagnosis(ObExprParseResult &expr_result);
  ObRouteDiagnosis *route_diagnosis_;
};

class ObExprCalcTool {
public:
  static int build_dtc_params_with_tz_info(ObClientSessionInfo *session_info,
                                           common::ObObjType obj_type,
                                           common::ObTimeZoneInfo &tz_info,
                                           common::ObDataTypeCastParams &dtc_params);
  static int build_tz_info(ObClientSessionInfo *session_info,
                           common::ObObjType obj_type,
                           common::ObTimeZoneInfo &tz_info);

  static int build_tz_info_for_all_type(ObClientSessionInfo *session_info,
                                        common::ObTimeZoneInfo &tz_info);
  static int build_dtc_params(ObClientSessionInfo *session_info,
                              common::ObObjType obj_type,
                              common::ObDataTypeCastParams &dtc_params);
  static bool is_contains_null_params(common::ObSEArray<common::ObObj, 4> &param_result);
};


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_EXPR_CALCULATOR_H */
