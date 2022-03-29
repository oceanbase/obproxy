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
namespace proxy
{
class ObProxyPartInfo;
class ObProxyMysqlRequest;
class ObPsIdEntry;
class ObTextPsEntry;
class ObClientSessionInfo;
}
namespace opsql
{
struct ObExprResolverContext
{
  ObExprResolverContext() : relation_info_(NULL), part_info_(NULL), client_request_(NULL),
                            ps_id_entry_(NULL), text_ps_entry_(NULL), client_info_(NULL) {}
  // parse result
  ObProxyRelationInfo *relation_info_;
  proxy::ObProxyPartInfo *part_info_;
  proxy::ObProxyMysqlRequest *client_request_;
  // proxy::ObPsEntry *ps_entry_;
  proxy::ObPsIdEntry *ps_id_entry_;
  proxy::ObTextPsEntry *text_ps_entry_;
  proxy::ObClientSessionInfo *client_info_;
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
private:
  int resolve_token_list(ObProxyRelationExpr *relation,
                         proxy::ObProxyPartInfo *part_info,
                         proxy::ObProxyMysqlRequest *client_request,
                         proxy::ObClientSessionInfo *client_info,
                         proxy::ObPsIdEntry *ps_entry,
                         proxy::ObTextPsEntry *text_ps_entry,
                         common::ObNewRange &range);
  int calc_token_func_obj(ObProxyTokenNode *token,
                          proxy::ObClientSessionInfo *client_session_info,
                          common::ObObj &target_obj);
  int calc_generated_key_value(common::ObObj &obj, const ObProxyPartKey &part_key, const bool is_oracle_mode);
  int get_obj_with_param(common::ObObj &target_obj,
                         proxy::ObProxyMysqlRequest *client_request,
                         proxy::ObClientSessionInfo *client_info,
                         proxy::ObProxyPartInfo *part_info,
                         proxy::ObPsIdEntry *ps_entry,
                         const int64_t param_index);
  common::ObIAllocator &allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObExprResolver);
};

} // end of namespace opsql
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBEXPR_RESOLVER_H
