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
#include "opsql/func_expr_resolver/ob_func_expr_resolver.h"
#include "opsql/func_expr_parser/ob_func_expr_parser.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr_factory.h"
#include "opsql/func_expr_resolver/proxy_expr/ob_proxy_expr.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "proxy/route/obproxy_part_info.h"
#include "proxy/route/obproxy_expr_calculator.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "utils/ob_proxy_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "common/ob_obj_compare.h"
#include "lib/utility/ob_print_utils.h"
#include "proxy/route/ob_route_diagnosis.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::dbconfig;

namespace oceanbase
{
namespace obproxy
{
namespace opsql
{

int64_t ObExprResolverResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < OBPROXY_MAX_PART_LEVEL; ++i) {
    databuff_printf(buf, buf_len, pos, " ranges_[%ld]:", i);

    if (ranges_[i].border_flag_.inclusive_start()) {
      databuff_printf(buf, buf_len, pos, "[");
    } else {
      databuff_printf(buf, buf_len, pos, "(");
    }
    pos += ranges_[i].start_key_.to_plain_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, " ; ");
    pos += ranges_[i].end_key_.to_plain_string(buf + pos, buf_len - pos);
    if (ranges_[i].border_flag_.inclusive_end()) {
      databuff_printf(buf, buf_len, pos, "]");
    } else {
      databuff_printf(buf, buf_len, pos, ")");
    }

    databuff_printf(buf, buf_len, pos, ",");
  }
  J_OBJ_END();
  return pos;
}

/**
 * @brief The range's border flag is decided by the last valid column.
 *        If use range directly, the range will cover partition which
 *        doesn't contain the data.
 *
 * @param range
 * @param border_flags all columns' border flag
 * @return int
 */
int ObExprResolver::preprocess_range(ObNewRange &range, ObIArray<ObBorderFlag> &border_flags) {
  int ret = OB_SUCCESS;
  ObObj *obj_start = const_cast<ObObj*>(range.start_key_.get_obj_ptr());
  ObObj *obj_end = const_cast<ObObj*>(range.end_key_.get_obj_ptr());
  int64_t invalid_idx = range.start_key_.get_obj_cnt();
  for (int64_t i = 0; i < range.start_key_.get_obj_cnt(); i++) {
    // find the last valid col,
    // use the last valid col's border flag as range's border flag
    range.border_flag_.set_data(border_flags.at(i).get_data());
    if (OB_ISNULL(obj_start) || OB_ISNULL(obj_end)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected null pointer");
    } else {
      ObCompareCtx cmp_ctx(ObMaxType, CS_TYPE_INVALID, true, INVALID_TZ_OFF);
      bool need_cast = false;
      ObObj cmp_result(false);
      if (OB_FAIL(ObObjCmpFuncs::compare(cmp_result, *obj_start, *obj_end, cmp_ctx, ObCmpOp::CO_EQ, need_cast))) {
        LOG_WDIAG("fail to compare", K(ret));
        invalid_idx = i + 1;
        ret = OB_SUCCESS;
        break;
      } else if (!cmp_result.get_bool()) {
        invalid_idx = i + 1;
        break;
      }
    }
  }
  // set the cols after invalid_idx(included) to (max : min)
  for (int64_t i = invalid_idx; i < range.start_key_.get_obj_cnt(); i++) {
    (obj_start + i)->set_max_value();
    (obj_end + i)->set_min_value();
  }
  LOG_DEBUG("succ to simplify range", K(range));
  return ret;
}

int ObExprResolver::resolve(ObExprResolverContext &ctx, ObExprResolverResult &result)
{
  int ret = OB_SUCCESS;
  ObString range, sub_range;
  ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_EXPR, resolve_expr, ret, range, sub_range);
  if (OB_ISNULL(ctx.relation_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid ctx", K(ctx.relation_info_), K(ret));
  } else {
    proxy::ObProxyPartInfo *part_info = ctx.part_info_;

    // to store partition column's border flag as order above like
    // "where c2 > 1 and c1 < 33" => ordered_part_col_border [(exclusive_start, exclusive_end]
    ObSEArray<ObBorderFlag, 2> part_columns_border;
    ObSEArray<ObBorderFlag, 2> sub_part_columns_border;

    // init range and border
    if (part_info->get_part_level() >= share::schema::PARTITION_LEVEL_ONE) {
      if (OB_FAIL(result.ranges_[0].build_row_key(part_info->get_part_columns().count(), allocator_))) {
        LOG_WDIAG("fail to init range", K(ret));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < part_info->get_part_columns().count(); i++) {
          if (OB_FAIL(part_columns_border.push_back(ObBorderFlag()))) {
            LOG_WDIAG("fail to push border flag", K(i), K(ret));
          }
        }
      }
    }
    if (part_info->get_part_level() == share::schema::PARTITION_LEVEL_TWO) {
      if (OB_FAIL(result.ranges_[1].build_row_key(part_info->get_sub_part_columns().count(), allocator_))) {
        LOG_WDIAG("fail to init range", K(ret));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < part_info->get_sub_part_columns().count(); i++) {
          if (OB_FAIL(sub_part_columns_border.push_back(ObBorderFlag()))) {
            LOG_WDIAG("fail to push border flag", K(i), K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObObj *target_obj = NULL;
      void *tmp_buf = NULL;
      // ignore ret in for loop
      // resolve every relation
      for (int64_t i = 0; i < ctx.relation_info_->relation_num_; ++i) {
        if (OB_ISNULL(ctx.relation_info_->relations_[i])) {
          LOG_INFO("relations is not valid here, ignore it",
                   K(ctx.relation_info_->relations_[i]), K(i));
        } else if (OB_UNLIKELY(ctx.relation_info_->relations_[i]->level_ == PART_KEY_LEVEL_ZERO)) {
          LOG_INFO("level is zero, ignore it");
        } else {
          if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObObj)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc new obj", K(ret));
          } else if (OB_ISNULL(target_obj = new (tmp_buf) ObObj())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to do placement new", K(ret));
          } else if (OB_FAIL(resolve_token_list(ctx.relation_info_->relations_[i], ctx.part_info_, ctx.client_request_,
                                                ctx.client_info_, ctx.ps_id_entry_, ctx.text_ps_entry_, target_obj,
                                                ctx.sql_field_result_))) {
            LOG_DEBUG("fail to resolve token list, ignore it", K(ret));
          } else if ((PART_KEY_LEVEL_ONE == ctx.relation_info_->relations_[i]->level_ ||
                      PART_KEY_LEVEL_BOTH == ctx.relation_info_->relations_[i]->level_) &&
                     OB_FAIL(place_obj_to_range(ctx.relation_info_->relations_[i]->type_,
                                                ctx.relation_info_->relations_[i]->first_part_column_idx_,
                                                target_obj, &result.ranges_[0], part_columns_border))) {
            LOG_WDIAG("fail to place obj to range of part level one", K(ret));
          } else if ((PART_KEY_LEVEL_TWO == ctx.relation_info_->relations_[i]->level_ ||
                      PART_KEY_LEVEL_BOTH == ctx.relation_info_->relations_[i]->level_) &&
                     OB_FAIL(place_obj_to_range(ctx.relation_info_->relations_[i]->type_,
                                                ctx.relation_info_->relations_[i]->second_part_column_idx_,
                                                target_obj, &result.ranges_[1], sub_part_columns_border))) {
            LOG_WDIAG("fail to place obj to range of part level two", K(ret));
          } else {}
          if (OB_NOT_NULL(target_obj)) {
            allocator_.free(target_obj);
          }
        }
      }

      if (OB_SUCC(ret) && ctx.is_insert_stm_) {
        if(OB_FAIL(handle_default_value(ctx.parse_result_->part_key_info_,
                                        ctx.client_info_, result.ranges_,
                                        ctx.sql_field_result_,part_columns_border,
                                        sub_part_columns_border, part_info->is_oracle_mode()))){
          LOG_WDIAG("fail to handle default value of part keys", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(preprocess_range(result.ranges_[0], part_columns_border))) {
          LOG_WDIAG("fail to preprocess range, part key level 0", K(ret));
        } else if (OB_FAIL(preprocess_range(result.ranges_[1], sub_part_columns_border))) {
          LOG_WDIAG("fail to preprocess range, part key level 1", K(ret));
        } else {}
      }
    }
  }
  ObDiagnosisResolveExpr *resolve_expr = NULL;
  if (OB_NOT_NULL(route_diagnosis_) && route_diagnosis_->is_diagnostic(RESOLVE_EXPR)) {
    resolve_expr = reinterpret_cast<ObDiagnosisResolveExpr*>(route_diagnosis_->get_last_matched_diagnosis_point(RESOLVE_EXPR));
    if (OB_NOT_NULL(resolve_expr)) {
      char range_buf[RESOLVE_EXPR_MAX_LEN] { 0 };
      char sub_range_buf[RESOLVE_EXPR_MAX_LEN] { 0 };
      if (!result.ranges_[0].empty()) {
        result.ranges_[0].to_plain_string(range_buf, RESOLVE_EXPR_MAX_LEN);
        range.assign_ptr(range_buf, (ObString::obstr_size_t) strlen(range_buf));
      }
      if (!result.ranges_[1].empty()) {
        result.ranges_[1].to_plain_string(sub_range_buf, RESOLVE_EXPR_MAX_LEN);
        sub_range.assign_ptr(sub_range_buf, (ObString::obstr_size_t) strlen(sub_range_buf));
      }
      resolve_expr->ret_ = ret;
      deep_copy_string(resolve_expr->alloc_, range, resolve_expr->part_range_);
      deep_copy_string(resolve_expr->alloc_, sub_range, resolve_expr->sub_part_range_);
    }
  }
  return ret;
}

/**
 * @brief 1. Place the resolved partition obj to range.start_key_/range.end_key_[idx].
 *        The idx is the obj's according column's idx in partition expression.
 *        2. Set the border flag of the partition obj. Use the border flag later in ObExprResolver::preprocess_range.
 *
 * @param relation
 * @param part_info
 * @param target_obj
 * @param range        out
 * @param border_flags out
 * @param part_columns
 * @return int
 */
int ObExprResolver::place_obj_to_range(ObProxyFunctionType type,
                                       int64_t idx_in_part_columns,
                                       ObObj *target_obj,
                                       ObNewRange *range,
                                       ObIArray<ObBorderFlag> &border_flags)
{
  int ret = OB_SUCCESS;
  ObObj *start_obj = const_cast<ObObj*>(range->start_key_.get_obj_ptr()) + idx_in_part_columns;
  ObObj *end_obj = const_cast<ObObj*>(range->end_key_.get_obj_ptr()) + idx_in_part_columns;
  if (OB_ISNULL(start_obj) || OB_ISNULL(end_obj) || OB_ISNULL(target_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected null pointer");
  }

  // 1. deep copy the resolved obj to right position in result.ranges_[x]
  // 2. set the resolved obj's border flag
  if (OB_SUCC(ret)) {
    switch (type) {
     case F_COMP_EQ:
       *start_obj = *target_obj;
       *end_obj = *target_obj;
        border_flags.at(idx_in_part_columns).set_inclusive_start();
        border_flags.at(idx_in_part_columns).set_inclusive_end();
       break;
     case F_COMP_GE:
       *start_obj = *target_obj;
       border_flags.at(idx_in_part_columns).set_inclusive_start();
       break;
     case F_COMP_GT:
       *start_obj = *target_obj;
       break;
     case F_COMP_LE:
       *end_obj = *target_obj;
       border_flags.at(idx_in_part_columns).set_inclusive_end();
       break;
     case F_COMP_LT:
       *end_obj = *target_obj;
       break;
     default:
       LOG_INFO("this func is not useful for range", "func_type",
                 get_obproxy_function_type(type));
       break;
    } // end of switch
  }
  return ret;
}

/*
 * calculate partition key value
 * for normal ps sql, placeholder_idx_ in token node means the pos of '?'
 * for normal pl sql, placeholder_idx_ in token node means the index of call_info.params_
 * for pl sql with ps, placeholder_idx_ in call_info_node_ means the pos of '?'
 * for example: ps sql = call func1(11, ?, 22, ?),
 * the first sql of func1 is select * from t1 where a = :1 and b = :2 and c =:3 and d = :4
 * result:
 * call_info_.params_[1].placeholder_idx_ = 0, call_info_.params_[3].placeholder_idx_ = 1
*/
int ObExprResolver::resolve_token_list(ObProxyRelationExpr *relation,
                                       ObProxyPartInfo *part_info,
                                       ObProxyMysqlRequest *client_request,
                                       ObClientSessionInfo *client_info,
                                       ObPsIdEntry *ps_id_entry,
                                       ObTextPsEntry *text_ps_entry,
                                       ObObj *target_obj,
                                       SqlFieldResult *sql_field_result,
                                       const bool has_rowid)
{
  int ret = OB_SUCCESS;
  ObProxyTokenType token_type = ObProxyTokenType::TOKEN_NONE;
  ObString token_str;
  char int_token_buf[20] { 0 };
  ObProxyExprType expr_type = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
  ObProxyExprType generated_func = ObProxyExprType::OB_PROXY_EXPR_TYPE_NONE;
  UNUSED(text_ps_entry);
  if (OB_ISNULL(target_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("target_obj is null");
  } else if (OB_ISNULL(relation) || OB_ISNULL(part_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("relation or part info is null", K(relation), K(part_info), K(ret));
  } else if (OB_ISNULL(relation->right_value_) || OB_ISNULL(relation->right_value_->head_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("token list or head is null", K(relation->right_value_), K(ret));
  } else {
    ObProxyTokenNode *token = relation->right_value_->head_;
    int64_t col_idx = relation->column_idx_;
    token_type = token->type_;
    if (TOKEN_STR_VAL == token->type_) {
      token_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
      target_obj->set_varchar(token->str_value_.str_, token->str_value_.str_len_);
      target_obj->set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      if (token->str_value_.str_len_ > 2 &&
          token->str_value_.str_[1] == '\'' &&
          token->str_value_.str_[token->str_value_.str_len_ - 1] == '\'' &&
          lib::is_oracle_mode()) {
        // oracle mode u'xxxx' str pattern treated as unicode(CHARSET_BINARY)
        if (token->str_value_.str_[0] == 'u' || token->str_value_.str_[0] == 'U') {
          target_obj->set_collation_type(ObCharset::get_default_collation_oracle(CHARSET_BINARY));
          LOG_DEBUG("succ to parse u'xxx' pattern and set to binary in oracle mode", K(*target_obj));
        } else if (token->str_value_.str_[0] == 'n' || token->str_value_.str_[0] == 'N') {
          LOG_DEBUG("succ to parse n'xxx' pattern in oracle mode", K(*target_obj));
        }
        // n/u'string_val' -> string_val
        target_obj->set_varchar(token->str_value_.str_ + 2, token->str_value_.str_len_ - 3);
      }
    } else if (TOKEN_INT_VAL == token->type_) {
      target_obj->set_int(token->int_value_);
      sprintf(int_token_buf, "%ld",token->int_value_);
      token_str.assign_ptr(int_token_buf, (ObString::obstr_size_t) strlen(int_token_buf));
    } else if (TOKEN_PLACE_HOLDER == token->type_) {
      int64_t param_index = token->placeholder_idx_;
      if (OB_FAIL(get_obj_with_param(*target_obj, client_request, client_info,
                                     part_info, ps_id_entry, param_index))) {
        LOG_DEBUG("fail to get target obj with param", K(ret));
      }
      token_str.assign_ptr(NULL, 0);
    } else if (TOKEN_FUNC == token->type_) {
      if (OB_FAIL(calc_token_func_obj(token, client_info, *target_obj, sql_field_result, part_info->is_oracle_mode(), expr_type))) {
        LOG_WDIAG("fail to calc token func obj", K(ret));
      }
      token_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
    } else if (TOKEN_HEX_VAL == token->type_) {
      if (OB_FAIL(calc_token_hex_obj(token, *target_obj))) {
        LOG_WDIAG("fail to calc token hex obj", K(ret));
      }
      token_str.assign_ptr(token->str_value_.str_, token->str_value_.str_len_);
    } else if (TOKEN_COLUMN == token->type_) {
      token_str.assign_ptr(token->column_name_.str_, token->column_name_.str_len_);
      ret = OB_INVALID_ARGUMENT;
    } else if (TOKEN_NULL == token->type_) {
      target_obj->set_null();
      token_str = "NULL";
    } else {
      token_str.assign_ptr(NULL, 0);
      ret = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCC(ret)
         && !has_rowid
         && part_info->has_generated_key()) {
      int64_t target_idx = -1;
      ObProxyPartKeyInfo &part_key_info = part_info->get_part_key_info();
      if (col_idx >= part_key_info.key_num_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("relation column index is invalid", K(col_idx), K(part_key_info.key_num_), K(ret));
      } else if (part_key_info.part_keys_[col_idx].is_generated_) {
        // do nothing, user sql explicitly contains value for generated key, no need to calculate
      } else if (FALSE_IT(target_idx = part_key_info.part_keys_[col_idx].generated_col_idx_)) {
        // will not come here
      } else if (OB_UNLIKELY(0 > target_idx)) {
        LOG_DEBUG("this relation's part key is not used to generate column");
      } else if (OB_UNLIKELY(target_idx >= part_key_info.key_num_)
                 || OB_UNLIKELY(!part_key_info.part_keys_[target_idx].is_generated_)
                 || OB_UNLIKELY(part_key_info.part_keys_[target_idx].level_ != relation->level_)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WDIAG("fail to get generated key value, source key is not offered",
                 K(col_idx), K(part_key_info.key_num_), K(target_idx), K(ret));
      } else if (OB_FAIL(calc_generated_key_value(*target_obj, part_key_info.part_keys_[col_idx],
                         part_info->is_oracle_mode()))) {
        generated_func = part_key_info.part_keys_[col_idx].func_type_;
        LOG_WDIAG("fail to get generated key value", "target_obj", *target_obj, K(ret));
      } else {
        generated_func = part_key_info.part_keys_[col_idx].func_type_;
        LOG_DEBUG("succ to calculate generated key value", "target_obj", *target_obj, K(ret));
      }
    }

    // set target_obj collation
    if (OB_SUCC(ret)) {
      if (ObHexStringType == target_obj->get_type()) {
        LOG_DEBUG("succ to set hex string obj to binary collation", K(*target_obj));
      } else if (ObStringTC == target_obj->get_type_class()) {
        // in oracle mode, nchar/nvarchar2 obj use ncharacter_set_connection
        if (lib::is_oracle_mode() && target_obj->get_meta().is_nstring() && CHARSET_INVALID != client_info->get_ncharacter_set_connection()) {
          target_obj->set_collation_type(ObCharset::get_default_collation_oracle(static_cast<ObCharsetType>(client_info->get_ncharacter_set_connection())));
          LOG_DEBUG("succ to set nchar/nvarchar2 obj collation to ncharacter_set_connection", K(*target_obj));
        // use collation_connection
        } else if (target_obj->get_collation_type() == ObCharset::get_default_collation(ObCharset::get_default_charset()) ||
                   target_obj->get_collation_type() == CS_TYPE_INVALID) {
          target_obj->set_collation_type(static_cast<common::ObCollationType>(client_info->get_collation_connection()));
          LOG_DEBUG("succ to set string obj collation to connection collation", K(*target_obj));
        } else {
          LOG_DEBUG("succ to set string obj collation to specified collation", K(*target_obj));
        }
      } else {
        LOG_DEBUG("skip setting non string obj collation", K(*target_obj));
      }
    }
  } // end of else
  if (OB_ISNULL(target_obj)) {
    ObObj null_obj;
    null_obj.set_null();
    ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_TOKEN, resolve_token, ret, token_type, token_str, expr_type, generated_func, null_obj);
  } else {
    LOG_DEBUG("succ to route diagnosis resolve token", "target_obj", *target_obj);
    ROUTE_DIAGNOSIS(route_diagnosis_, RESOLVE_TOKEN, resolve_token, ret, token_type, token_str, expr_type, generated_func, *target_obj);
  }
  return ret;
}

void ObExprResolver::set_route_diagnosis(ObRouteDiagnosis *route_diagnosis)
{
  if (OB_NOT_NULL(route_diagnosis_)) {
    route_diagnosis_->dec_ref();
    route_diagnosis_ = NULL;
  }
  if (OB_NOT_NULL(route_diagnosis)) {
    route_diagnosis_ = route_diagnosis;
    route_diagnosis_->inc_ref();
  }
}
/*
 * calculate func token, convert token node to param node to reuse func resolver
 */
int ObExprResolver::calc_token_func_obj(ObProxyTokenNode *token,
                                        ObClientSessionInfo *client_session_info,
                                        ObObj &target_obj,
                                        SqlFieldResult *sql_field_result,
                                        const bool is_oracle_mode,
                                        ObProxyExprType &expr_type)
{
  int ret = OB_SUCCESS;
  ObProxyParamNode *param_node = NULL;
  ObProxyExprFactory factory(allocator_);
  ObFuncExprResolverContext ctx(&allocator_, &factory);
  ObFuncExprResolver resolver(ctx);
  ObProxyExpr *expr;

  if (OB_FAIL(convert_token_node_to_param_node(token, param_node))) {
    LOG_WDIAG("fail to convert func token to param node", K(ret));
  } else if (OB_ISNULL(param_node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to convert func token to param node", K(ret));
  } else if (OB_FAIL(resolver.resolve(param_node, expr))) {
    LOG_WDIAG("proxy expr resolve failed", K(ret));
  } else {
    ObSEArray<ObObj, 4> result_array;
    ObProxyExprCalcItem calc_item(const_cast<SqlFieldResult *>(sql_field_result));
    ObProxyExprCtx expr_ctx(0, TESTLOAD_NON, false, &allocator_, client_session_info);
    expr_ctx.is_oracle_mode = is_oracle_mode;
    if (OB_FAIL(expr->calc(expr_ctx, calc_item, result_array))) {
      LOG_WDIAG("calc expr result failed", K(ret));
    } else if (OB_FAIL(result_array.at(0, target_obj))) {
      LOG_WDIAG("get expr calc result fail", K(ret));
    }
    expr_type = expr->get_expr_type();
  }

  return ret;
}

int ObExprResolver::handle_default_value(ObProxyPartKeyInfo &part_key_info,
                                         proxy::ObClientSessionInfo *client_info,
                                         common::ObNewRange ranges[],
                                         obutils::SqlFieldResult *sql_field_result,
                                         ObIArray<ObBorderFlag> &part_border_flags,
                                         ObIArray<ObBorderFlag> &sub_part_border_flags,
                                         bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObObj *target_obj = NULL;
  void *tmp_buf = NULL;
  for (int i = 0;  OB_SUCC(ret) && i < part_key_info.key_num_; i++) {
    ObProxyPartKeyLevel level = part_key_info.part_keys_[i].level_;
    int64_t column_idx = part_key_info.part_keys_[i].idx_in_part_columns_;
    bool is_need_default_val = false;

    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObObj)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc new obj", K(ret));
    } else if (OB_ISNULL(target_obj = new (tmp_buf) ObObj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to do placement new ObObj", K(ret));
    }

    if(OB_SUCC(ret)){
      if (part_key_info.part_keys_[i].is_generated_ && !part_key_info.part_keys_[i].is_exist_in_sql_) {
        int source_idx = -1;
        ObProxyParseString *var_str = NULL;
        for (int j = 0; j < part_key_info.key_num_; j++) {
          if (part_key_info.part_keys_[j].generated_col_idx_ == i
              && !part_key_info.part_keys_[j].is_exist_in_sql_) {
            source_idx = j;
            break;
          }
        }
        if (source_idx >= 0) {
          int64_t real_source_idx = part_key_info.part_keys_[source_idx].real_source_idx_;
          if (real_source_idx < 0 || real_source_idx > OBPROXY_MAX_PART_KEY_NUM){
            // invalid real source idx
          } else if (FALSE_IT(var_str = &part_key_info.part_keys_[real_source_idx].default_value_) || var_str->str_len_ < 0 ){
          } else if (OB_FAIL(parse_and_resolve_default_value(*var_str, client_info, sql_field_result, target_obj, is_oracle_mode))){
            LOG_WDIAG("parse and resolve default value of partition key failed", K(ret));
          } else if (OB_FAIL(calc_generated_key_value(*target_obj, part_key_info.part_keys_[source_idx], is_oracle_mode))) {
            LOG_WDIAG("fail to get generated key value", K(target_obj), K(ret));
          } else {
            is_need_default_val = true;
          }
        }
      } else if (!part_key_info.part_keys_[i].is_exist_in_sql_) {
        ObProxyParseString &var_str = part_key_info.part_keys_[i].default_value_;
        if (var_str.str_len_ > 0) {
          if (OB_FAIL(parse_and_resolve_default_value(var_str, client_info, sql_field_result, target_obj, is_oracle_mode))) {
            LOG_WDIAG("parse and resolve default value of partition key failed", K(ret));
          } else {
            is_need_default_val = true;
          }
        }
      }
    }

    if (OB_SUCC(ret) && is_need_default_val) {
      if ((PART_KEY_LEVEL_ONE == level || PART_KEY_LEVEL_BOTH == level) &&
           OB_FAIL(place_obj_to_range(F_COMP_EQ, column_idx, target_obj, &ranges[0], part_border_flags))) {
        LOG_WDIAG("fail to place obj to range of part level one", K(ret));
      } else if ((PART_KEY_LEVEL_TWO == level || PART_KEY_LEVEL_BOTH == level) &&
                  OB_FAIL(place_obj_to_range(F_COMP_EQ, column_idx, target_obj, &ranges[1], sub_part_border_flags))) {
        LOG_WDIAG("fail to place obj to range of part level two", K(ret));
      } else {}
    }
    if (OB_NOT_NULL(target_obj)) {
      allocator_.free(target_obj);
    }
  }
  return ret;
}

int ObExprResolver::parse_and_resolve_default_value(ObProxyParseString &default_value,
                                                    ObClientSessionInfo *client_session_info,
                                                    SqlFieldResult *sql_field_result,
                                                    ObObj *target_obj,
                                                    bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  number::ObNumber nb;
  if (default_value.str_len_ > 0 ){
    int64_t tmp_pos = 0;
    if (OB_FAIL(target_obj->deserialize(default_value.str_ , default_value.str_len_, tmp_pos))) {
      LOG_WDIAG("fail to deserialize default value of part key");
    } else if (FALSE_IT(LOG_DEBUG("default value deserialize succ" , K(*target_obj)))) {
    } else if (target_obj->is_varchar() && is_oracle_mode) {
      // oracle mode return the default as a unresolved varchar type obj
      ObString default_value_expr = target_obj->get_varchar();
      if (default_value_expr.empty()) {
        target_obj->set_varchar(ObString());
      } else if ('\'' == default_value_expr[0]) {
        // match string type
        ObString dst;
        if (2 >= default_value_expr.length()) {
          dst = ObString();
          // remove single quotes
        } else if (OB_FAIL(ob_sub_str(allocator_, default_value_expr, 1, default_value_expr.length() - 2, dst))) {
          LOG_WDIAG("get sub stirng of default value failed", K(ret));
        }
        if (OB_SUCC(ret)) {
          target_obj->set_varchar(dst);
        }
      } else if ( OB_SUCCESS == nb.from(default_value_expr.ptr(), default_value_expr.length(), allocator_)) {
        // match positive number
      } else {
        /*
          original resolver can't resolve this case : -(1+5) -> -((1+5)),
          so treat negative number as expr
          server return negative with brackets, -1 -> -(1)
        */
        // match expr
        ObFuncExprParser parser(allocator_, SHARDING_EXPR_FUNC_PARSE_MODE);
        ObFuncExprParseResult result;

        ObProxyExprFactory factory(allocator_);
        ObFuncExprResolverContext ctx(&allocator_, &factory);
        ObFuncExprResolver resolver(ctx);
        ObProxyExpr *expr;

        if (OB_FAIL(parser.parse(default_value_expr, result))) {
          LOG_INFO("parse default value expr failed", K(ret));
        } else if (OB_FAIL(resolver.resolve(result.param_node_, expr))) {
          LOG_INFO("proxy expr resolve failed", K(ret));
        } else {
          ObSEArray<ObObj, 4> result_array;
          ObProxyExprCalcItem calc_item(const_cast<SqlFieldResult *>(sql_field_result));
          ObProxyExprCtx expr_ctx(0, TESTLOAD_NON, false, &allocator_, client_session_info);
          expr_ctx.is_oracle_mode = is_oracle_mode;
          if (OB_FAIL(expr->calc(expr_ctx, calc_item, result_array))) {
            LOG_WDIAG("calc expr result failed", K(ret));
          } else if (OB_FAIL(result_array.at(0, *target_obj))) {
            LOG_WDIAG("get expr calc result fail", K(ret));
          }
        }
      }
    } else {
      // mysql mode return the default value with resolved obj in column's type
      // do nothing
    }
  }
  if (OB_SUCC(ret) && ObStringTC == target_obj->get_type_class()) {
    LOG_DEBUG("parse and resolve default value succ", K(*target_obj), K(ret));
    target_obj->set_collation_type(static_cast<common::ObCollationType>(client_session_info->get_collation_connection()));
  }
  return ret;
}

int ObExprResolver::convert_token_node_to_param_node(ObProxyTokenNode *token,
                                                     ObProxyParamNode *&param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else {
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyParamNode)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc new param node", K(ret));
    } else {
      param = new(tmp_buf) ObProxyParamNode();
      param->next_ = NULL;
      if (TOKEN_INT_VAL == token->type_) {
        param->int_value_ = token->int_value_;
        param->type_ = PARAM_INT_VAL;
      } else if (TOKEN_STR_VAL == token->type_) {
        param->str_value_ = token->str_value_;
        param->type_ = PARAM_STR_VAL;
      } else if (TOKEN_NULL == token->type_) {
        param->type_ = PARAM_NULL;
      } else if (TOKEN_FUNC == token->type_) {
        if (OB_FAIL(recursive_convert_func_token(token, param))) {
          LOG_WDIAG("convert func token node to param node failed", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("unexpected token node type, please check", K(ret), K(param->type_));
      }
    }
  }
  return ret;
}

int ObExprResolver::recursive_convert_func_token(ObProxyTokenNode *token,
                                                 ObProxyParamNode *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(token) || OB_ISNULL(param) || token->type_ != TOKEN_FUNC) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(ret));
  } else {
    if (OB_ISNULL(token->str_value_.str_) || token->str_value_.str_len_ <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid func name", K(ret));
    } else {
      ObString func_name(token->str_value_.str_len_, token->str_value_.str_);
      void *tmp_buf = NULL;
      param->type_ = PARAM_FUNC;
      if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObFuncExprNode)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc new func expr node", K(ret));
      } else {
        param->func_expr_node_ = new(tmp_buf) ObFuncExprNode();
        param->func_expr_node_->func_name_ = token->str_value_;
        param->func_expr_node_->child_ = NULL;

        if (OB_ISNULL(token->child_)) {
          // do nothing
        } else if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObProxyParamNodeList)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc new token list", K(ret));
        } else {
          param->func_expr_node_->child_ = new(tmp_buf) ObProxyParamNodeList();
          ObProxyParamNode head;
          ObProxyParamNode *param_cur = &head;
          ObProxyTokenNode *token_child = token->child_->head_;
          int64_t child_num = 0;
          for (; OB_SUCC(ret) && token_child != NULL; token_child = token_child->next_, param_cur = param_cur->next_) {
            if (OB_FAIL(convert_token_node_to_param_node(token_child, param_cur->next_))) {
              LOG_WDIAG("recursive convert func token failed", K(ret));
            } else {
              child_num++;
            }
          }
          if (OB_SUCC(ret)) {
            param->func_expr_node_->child_->tail_ = param_cur;
            param->func_expr_node_->child_->head_ = head.next_;
            param->func_expr_node_->child_->child_num_ = child_num;
          }
        }
      }
    }
  }
  return ret;
}

int ObExprResolver::calc_token_hex_obj(ObProxyTokenNode *token, ObObj &target_obj)
{
  int ret = OB_SUCCESS;
  ObString hex_str_format_val;
  // may temp used
  char* full_hex_str_format_val = NULL;
  if (OB_ISNULL(token)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("hex token or str val is null pointer", K(ret));
  } else if (TOKEN_HEX_VAL != token->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("hex token type dismatch", K(ret));
  } else if (3 > token->str_value_.str_len_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("hex str val is too short", K(ret));
  } else if (OB_ISNULL(token->str_value_.str_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("hex str val is null pointer", K(ret));
  // like x'86adf554' which represents a sequence of memory '86 ad f5 54'
  } else if (token->str_value_.str_[0] == 'x' || token->str_value_.str_[0] == 'X') {
    hex_str_format_val.assign(token->str_value_.str_ + 2, token->str_value_.str_len_ - 3);
  } else if (token->str_value_.str_[0] == '0' && (token->str_value_.str_[1] == 'x' || token->str_value_.str_[1] == 'X')) {
    hex_str_format_val.assign(token->str_value_.str_ + 2, token->str_value_.str_len_ - 2);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail calc token hex obj", K(hex_str_format_val));
  }
  LOG_DEBUG("to calc hex val token str", K(hex_str_format_val));
  if (OB_SUCC(ret)) {
    char* hex_byte_format_buf = NULL;
    if (hex_str_format_val.empty()) {
      target_obj.set_hex_string(hex_str_format_val);
      LOG_WDIAG("hex str val invalid", K(hex_str_format_val));
    } else {
      // 0x12345 => 0x012345
      if (hex_str_format_val.length() % 2 != 0) {
        if (OB_ISNULL(full_hex_str_format_val = static_cast<char*>(allocator_.alloc(hex_str_format_val.length() + 1)))) {
          LOG_WDIAG("fail to alloc buf", K(ret));
        } else {
          full_hex_str_format_val[0] = '0';
          MEMCPY(full_hex_str_format_val + 1, hex_str_format_val.ptr(), hex_str_format_val.length());
          hex_str_format_val.assign_ptr(full_hex_str_format_val, static_cast<ObString::obstr_size_t>(hex_str_format_val.length() + 1));
          LOG_DEBUG("succ to calc hex val full format", K(hex_str_format_val));
        }
      }
      int64_t byte_len = hex_str_format_val.length() / 2;
      if (OB_SUCC(ret) && OB_ISNULL(hex_byte_format_buf = static_cast<char*>(allocator_.alloc(byte_len)))) {
        LOG_WDIAG("fail to alloc buf", K(ret));
      } else if (hex_str_format_val.length() != static_cast<int64_t>(str_to_hex(hex_str_format_val.ptr(),
                                                             hex_str_format_val.length(),
                                                             hex_byte_format_buf, static_cast<int32_t>(byte_len)))){
        LOG_WDIAG("fail to str to hex byte", K(hex_str_format_val));
      } else {
        ObString hex_byte_val(byte_len, hex_byte_format_buf);
        target_obj.set_hex_string(hex_byte_val);
        LOG_DEBUG("calc hex val byte format", K(hex_byte_val));
      }
    }
    if (OB_NOT_NULL(full_hex_str_format_val)) {
      allocator_.free(full_hex_str_format_val);
    }
  }
  return ret;
}

int ObExprResolver::calc_generated_key_value(ObObj &obj, const ObProxyPartKey &part_key, const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(OB_PROXY_EXPR_TYPE_FUNC_SUBSTR == part_key.func_type_)) {
    //  we only support substr now
    int64_t start_pos = INT64_MAX;
    int64_t sub_len = INT64_MAX;
    if (NULL != part_key.params_[1] && PARAM_INT_VAL == part_key.params_[1]->type_) {
      start_pos = part_key.params_[1]->int_value_;
    }
    if (NULL != part_key.params_[2] && PARAM_INT_VAL == part_key.params_[2]->type_) {
      sub_len = part_key.params_[2]->int_value_;
    }
    ObString src_val;
    if (obj.is_varchar()) {
      if (OB_FAIL(obj.get_varchar(src_val))) {
        LOG_WDIAG("fail to get varchar value", K(obj), K(ret));
      } else {
        if (start_pos < 0) {
          start_pos = src_val.length() + start_pos + 1;
        }
        if (0 == start_pos && is_oracle_mode) {
          start_pos = 1;
        }
        if (INT64_MAX == sub_len) {
          sub_len = src_val.length() - start_pos + 1;
        }
        if (start_pos > 0 && start_pos <= src_val.length()
            && sub_len > 0 && sub_len <= src_val.length()) {
            obj.set_varchar(src_val.ptr() + start_pos - 1, static_cast<int32_t>(sub_len));
        }
      }
    }
  } else {
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WDIAG("unknown generate function type", K(part_key.func_type_), K(ret));
  }
  return ret;
}

int ObExprResolver::get_obj_with_param(ObObj &target_obj,
                                       ObProxyMysqlRequest *client_request,
                                       ObClientSessionInfo *client_info,
                                       ObProxyPartInfo *part_info,
                                       ObPsIdEntry *ps_id_entry,
                                       const int64_t param_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_request) || OB_ISNULL(client_info) || OB_UNLIKELY(param_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(client_request), K(param_index), K(ret));
  } else {
    int64_t execute_param_index = param_index;
    bool need_use_execute_param = false;
    // here parse result means the original parse result for this ps sql or call sql
    ObSqlParseResult &parse_result = client_request->get_parse_result();
    ObProxyCallInfo &call_info = parse_result.call_info_;
    if (parse_result.is_call_stmt() || parse_result.is_text_ps_call_stmt()) {
      if (OB_UNLIKELY(!call_info.is_valid()) || OB_UNLIKELY(param_index >= call_info.params_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid placeholder idx", K(param_index), K(call_info), K(ret));
      } else {
        const ObProxyCallParam* call_param = call_info.params_.at(param_index);
        if (CALL_TOKEN_INT_VAL == call_param->type_) {
          int64_t int_val = 0;
          if (OB_FAIL(get_int_value(call_param->str_value_.config_string_, int_val))) {
            LOG_WDIAG("fail to get int value", K(call_param->str_value_.config_string_), K(ret));
          } else {
            target_obj.set_int(int_val);
          }
        } else if (CALL_TOKEN_STR_VAL == call_param->type_) {
          target_obj.set_varchar(call_param->str_value_.config_string_);
          target_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else if (CALL_TOKEN_PLACE_HOLDER == call_param->type_) {
          need_use_execute_param = true;
          if (OB_FAIL(get_int_value(call_param->str_value_.config_string_, execute_param_index))) {
            LOG_WDIAG("fail to get int value", K(call_param->str_value_.config_string_), K(ret));
          }
        }
      }
    } else {
      need_use_execute_param = true;
    }
    if (OB_SUCC(ret)
        && need_use_execute_param
        && OB_MYSQL_COM_STMT_EXECUTE == client_request->get_packet_meta().cmd_) {
      // for com_stmt_prepare, we have no execute_params, so no need continue, just return
      LOG_DEBUG("will cal obj with value from execute param", K(execute_param_index));
      if (OB_ISNULL(ps_id_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("client ps id entry is null", K(ret), KPC(ps_id_entry));
      } else if (OB_UNLIKELY(execute_param_index >= ps_id_entry->get_param_count())
                 || execute_param_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid placeholder idx", K(execute_param_index), KPC(ps_id_entry), K(ret));
      } else if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_execute_param(ps_id_entry->get_param_count(),
                         ps_id_entry->get_ps_sql_meta().get_param_types(), *client_request, execute_param_index, target_obj))) {
        LOG_WDIAG("fail to analyze execute param", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_use_execute_param && OB_MYSQL_COM_STMT_PREPARE == client_request->get_packet_meta().cmd_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_DEBUG("prepare sql with only placeholder, will return fail", K(ret));
    }

    if (OB_SUCC(ret)
        && need_use_execute_param
        && client_request->get_parse_result().is_text_ps_execute_stmt()) {
      LOG_DEBUG("will cal obj with value from ps execute param", K(execute_param_index));
      ObSqlParseResult &parse_result = client_request->get_parse_result();
      ObProxyTextPsInfo execute_info = parse_result.text_ps_info_;
      if (execute_param_index >= execute_info.params_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("param index is large than param count", K(execute_param_index),
                  K(execute_info.params_.count()), K(ret));
      } else {
        ObProxyTextPsParam* param = execute_info.params_.at(execute_param_index);
        ObString user_variable_name = param->str_value_.config_string_;
        if (client_info->need_use_lower_case_names()) {
          string_to_lower_case(user_variable_name.ptr(), user_variable_name.length());
        }
        if (OB_FAIL(static_cast<const ObClientSessionInfo&>(*client_info).get_user_variable_value(user_variable_name, target_obj))) {
          LOG_WDIAG("get user variable failed", K(ret), K(user_variable_name));
        } else {
          ObString user_var;
          int tmp_ret = OB_SUCCESS;
          if (target_obj.is_varchar()) {
            if (OB_SUCCESS != (tmp_ret = target_obj.get_varchar(user_var))) {
              LOG_WDIAG("get varchar failed", K(tmp_ret));
            } else {
              char* ptr = user_var.ptr();
              int32_t len = user_var.length();
              // user var has store ' into value
              if ((user_var[0] == 0x27 && user_var[len-1] == 0x27) ||
                (user_var[0] == 0x22 && user_var[len-1] == 0x22)) {
                target_obj.set_varchar(ptr + 1, len - 2);
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)
        && need_use_execute_param
        && OB_MYSQL_COM_STMT_PREPARE_EXECUTE == client_request->get_packet_meta().cmd_) {
      LOG_DEBUG("will cal obj with value from execute param", K(execute_param_index));
      if (OB_UNLIKELY(execute_param_index < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid placeholder idx", K(execute_param_index), K(ret));
      } else if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_prepare_execute_param(*client_request, execute_param_index, target_obj))) {
        LOG_WDIAG("fail to analyze execute param", K(ret));
      }
    }

    if (OB_SUCC(ret)
        && need_use_execute_param
        && OB_MYSQL_COM_STMT_SEND_LONG_DATA == client_request->get_packet_meta().cmd_) {
      LOG_DEBUG("will calc obj with execute param for send long data");
      if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_send_long_data_param(*client_request, execute_param_index,
                                                                       part_info, ps_id_entry, target_obj))) {
        LOG_DEBUG("fail to analyze send long data param", K(ret));
      }
    }
  }
  return ret;
}


} // end of opsql
} // end of obproxy
} // end of oceanbase
