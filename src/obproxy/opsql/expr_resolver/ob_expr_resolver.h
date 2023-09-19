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

#ifndef OBEXPR_RESOLVER_H
#define OBEXPR_RESOLVER_H
#include "common/ob_object.h"
#include "common/ob_range2.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObNewRange;
}
namespace obproxy
{

namespace obutils
{
struct SqlFieldResult;
}

namespace proxy
{
class ObProxyPartInfo;
class ObProxyMysqlRequest;
class ObPsIdEntry;
class ObTextPsEntry;
class ObClientSessionInfo;
class ObRouteDiagnosis;
}
namespace opsql
{
struct ObExprResolverContext
{
  ObExprResolverContext() : relation_info_(NULL), part_info_(NULL), client_request_(NULL),
                            ps_id_entry_(NULL), text_ps_entry_(NULL), client_info_(NULL),
                            sql_field_result_(NULL), route_diagnosis_(NULL) {}
  // parse result
  ObProxyRelationInfo *relation_info_;
  proxy::ObProxyPartInfo *part_info_;
  proxy::ObProxyMysqlRequest *client_request_;
  // proxy::ObPsEntry *ps_entry_;
  proxy::ObPsIdEntry *ps_id_entry_;
  proxy::ObTextPsEntry *text_ps_entry_;
  proxy::ObClientSessionInfo *client_info_;
  obutils::SqlFieldResult *sql_field_result_;
  ObExprParseResult *parse_result_;
  bool is_insert_stm_;
  proxy::ObRouteDiagnosis *route_diagnosis_;
};

class ObExprResolverResult
{
public:
  ObExprResolverResult() : ranges_() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;

  common::ObNewRange ranges_[OBPROXY_MAX_PART_LEVEL];
};

class ObExprResolver
{
public:
  explicit ObExprResolver(common::ObIAllocator &allocator) : allocator_(allocator)
  { }
  // will not be inherited, do not set to virtual
  ~ObExprResolver() {}

  int resolve(ObExprResolverContext &ctx, ObExprResolverResult &result);
  int resolve_token_list(ObProxyRelationExpr *relation,
                         proxy::ObProxyPartInfo *part_info,
                         proxy::ObProxyMysqlRequest *client_request,
                         proxy::ObClientSessionInfo *client_info,
                         proxy::ObPsIdEntry *ps_entry,
                         proxy::ObTextPsEntry *text_ps_entry,
                         common::ObObj *target_obj,
                         obutils::SqlFieldResult *sql_field_result,
                         const bool has_rowid = false);
private:
  int preprocess_range(common::ObNewRange &range, common::ObIArray<common::ObBorderFlag> &border_flags);
  int place_obj_to_range(ObProxyFunctionType type,
                         int64_t idx_in_part_columns,
                         common::ObObj *target_obj,
                         common::ObNewRange *range,
                         common::ObIArray<common::ObBorderFlag> &border_flags);
  int calc_token_func_obj(ObProxyTokenNode *token,
                          proxy::ObClientSessionInfo *client_session_info,
                          common::ObObj &target_obj,
                          obutils::SqlFieldResult *sql_field_result,
                          const bool is_oracle_mode);
  int calc_token_hex_obj(ObProxyTokenNode *token, common::ObObj &target_obj);
  int calc_generated_key_value(common::ObObj &obj, const ObProxyPartKey &part_key, const bool is_oracle_mode);
  int get_obj_with_param(common::ObObj &target_obj,
                         proxy::ObProxyMysqlRequest *client_request,
                         proxy::ObClientSessionInfo *client_info,
                         proxy::ObProxyPartInfo *part_info,
                         proxy::ObPsIdEntry *ps_entry,
                         const int64_t param_index);
  int convert_token_node_to_param_node(ObProxyTokenNode *token,
                                       ObProxyParamNode *&param);
  int recursive_convert_func_token(ObProxyTokenNode *token,
                                   ObProxyParamNode *param);
  int handle_default_value(ObProxyPartKeyInfo &part_info,
                           proxy::ObClientSessionInfo *client_info,
                           common::ObNewRange range[],
                           obutils::SqlFieldResult *sql_field_result,
                           common::ObIArray<common::ObBorderFlag> &part_border_flags,
                           common::ObIArray<common::ObBorderFlag> &sub_part_border_flags,
                           bool is_oracle_mode);
  int parse_and_resolve_default_value(ObProxyParseString &default_value_expr,
                                      proxy::ObClientSessionInfo *client_session_info,
                                      obutils::SqlFieldResult *sql_field_result,
                                      common::ObObj *target_obj,
                                      bool is_oracle_mode);

  ObProxyExprType get_expr_token_func_type(common::ObString *func);
  common::ObIAllocator &allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObExprResolver);
};

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBEXPR_RESOLVER_H
