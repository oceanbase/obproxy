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
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "proxy/route/obproxy_part_info.h"
#include "proxy/mysqllib/ob_proxy_mysql_request.h"
#include "proxy/mysqllib/ob_mysql_request_analyzer.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obmysql;

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

int ObExprResolver::resolve(ObExprResolverContext &ctx, ObExprResolverResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.relation_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ctx", K(ctx.relation_info_), K(ret));
  } else {
    // ignore ret in for loop
    for (int64_t i = 0; i < ctx.relation_info_->relation_num_; ++i) {
      if (OB_ISNULL(ctx.relation_info_->relations_[i])) {
        LOG_INFO("relations is not valid here, ignore it",
                 K(ctx.relation_info_->relations_[i]), K(i));
      } else if (OB_UNLIKELY(ctx.relation_info_->relations_[i]->level_ == PART_KEY_LEVEL_ZERO)) {
        LOG_INFO("level is zero, ignore it");
      } else {
        int64_t part_idx = (ctx.relation_info_->relations_[i]->level_ >= PART_KEY_LEVEL_BOTH) ? 0
                           : static_cast<int64_t>(ctx.relation_info_->relations_[i]->level_ - 1);
        if (OB_FAIL(resolve_token_list(ctx.relation_info_->relations_[i],
                                       ctx.part_info_,
                                       ctx.client_request_,
                                       ctx.client_info_,
                                       ctx.ps_entry_,
                                       ctx.text_ps_entry_,
                                       result.ranges_[part_idx]))) {
          LOG_INFO("fail to resolve token list, ignore it", K(ret));
        }
      }
    }
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
                                       ObPsEntry *ps_entry,
                                       ObTextPsEntry *text_ps_entry,
                                       ObNewRange &range)
{
  int ret = OB_SUCCESS;
  UNUSED(text_ps_entry);
  if (OB_ISNULL(relation) || OB_ISNULL(part_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("relation or part info is null", K(relation), K(part_info), K(ret));
  } else if (OB_ISNULL(relation->right_value_) || OB_ISNULL(relation->right_value_->head_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_INFO("token list or head is null", K(relation->right_value_), K(ret));
  } else {
    ObObj *target_obj = NULL;
    void *tmp_buf = NULL;
    if (OB_ISNULL(tmp_buf = allocator_.alloc(sizeof(ObObj)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else {
      target_obj = new (tmp_buf) ObObj();

      ObProxyFunctionType &func_type = relation->type_;
      ObProxyTokenNode *token = relation->right_value_->head_;
      int64_t col_idx = relation->column_idx_;
      if (TOKEN_STR_VAL == token->type_) {
        target_obj->set_varchar(token->str_value_.str_, token->str_value_.str_len_);
        target_obj->set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } else if (TOKEN_INT_VAL == token->type_) {
        target_obj->set_int(token->int_value_);
      } else if (TOKEN_PLACE_HOLDER == token->type_) {
        int64_t param_index = token->placeholder_idx_;
        if (OB_FAIL(get_obj_with_param(*target_obj, client_request,
                       client_info, ps_entry, param_index))) {
          LOG_WARN("fail to get target obj with param", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }

      if (part_info->has_generated_key()) {
        int64_t target_idx = -1;
        ObProxyPartKeyInfo &part_key_info = part_info->get_part_key_info();
        if (col_idx >= part_key_info.key_num_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("relation column index is invalid", K(col_idx), K(part_key_info.key_num_), K(ret));
        } else if (part_key_info.part_keys_[col_idx].is_generated_) {
          // do nothing, user sql explicitly contains value for generated key, no need to calculate
        } else if (FALSE_IT(target_idx = part_key_info.part_keys_[col_idx].generated_col_idx_)) {
          // will not come here
        } else if (OB_UNLIKELY(target_idx >= part_key_info.key_num_)
                   || OB_UNLIKELY(target_idx < 0)
                   || OB_UNLIKELY(!part_key_info.part_keys_[target_idx].is_generated_)
                   || OB_UNLIKELY(part_key_info.part_keys_[target_idx].level_ != relation->level_)) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("fail to get generated key value, source key is not offered",
                   K(col_idx), K(part_key_info.key_num_), K(target_idx), K(ret));
        } else if (OB_FAIL(calc_generated_key_value(*target_obj, part_key_info.part_keys_[col_idx],
                           part_info->is_oracle_mode()))) {
          LOG_WARN("fail to get generated key value", K(target_obj), K(ret));
        } else {
          LOG_DEBUG("succ to calculate generated key value", K(target_obj), K(ret));
        }
      }

      if (OB_SUCC(ret) && ObStringTC == target_obj->get_type_class()) {
        // The character set of the string parsed from the parser uses the value of the variable collation_connection
        target_obj->set_collation_type(static_cast<common::ObCollationType>(client_info->get_collation_connection()));
      }

      if (OB_SUCC(ret)) {
        switch (func_type) {
          case F_COMP_EQ:
            range.start_key_.assign(target_obj, 1);
            range.end_key_.assign(target_obj, 1);
            range.border_flag_.set_inclusive_start();
            range.border_flag_.set_inclusive_end();
            break;
          case F_COMP_GE:
            range.start_key_.assign(target_obj, 1);
            range.border_flag_.set_inclusive_start();
            break;
          case F_COMP_GT:
            range.start_key_.assign(target_obj, 1);
            break;
          case F_COMP_LE:
            range.end_key_.assign(target_obj, 1);
            range.border_flag_.set_inclusive_end();
            break;
          case F_COMP_LT:
            range.end_key_.assign(target_obj, 1);
            break;
          default:
            LOG_INFO("this func is not useful for range", "func_type",
                      get_obproxy_function_type(func_type));
            break;
        } // end of switch
      } // end of if
    } // end of else
  } // end of else
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
        LOG_WARN("fail to get varchar value", K(obj), K(ret));
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
    LOG_WARN("unknown generate function type", K(part_key.func_type_), K(ret));
  }
  return ret;
}

int ObExprResolver::get_obj_with_param(ObObj &target_obj, ObProxyMysqlRequest *client_request,
                                       ObClientSessionInfo *client_info,
                                       ObPsEntry *ps_entry, const int64_t param_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(client_request) || OB_ISNULL(client_info) || OB_UNLIKELY(param_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(client_request), K(param_index), K(ret));
  } else {
    int64_t execute_param_index = param_index;
    bool need_use_execute_param = false;
    // here parse result means the original parse result for this ps sql or call sql
    ObSqlParseResult &parse_result = client_request->get_parse_result();
    ObProxyCallInfo &call_info = parse_result.call_info_;
    if (parse_result.is_call_stmt() || parse_result.is_text_ps_call_stmt()) {
      if (OB_UNLIKELY(!call_info.is_valid()) || OB_UNLIKELY(param_index >= call_info.param_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid placeholder idx", K(param_index), K(call_info), K(ret));
      } else {
        const ObProxyCallParam &call_param = call_info.params_.at(param_index);
        if (CALL_TOKEN_INT_VAL == call_param.type_) {
          int64_t int_val = 0;
          if (OB_FAIL(get_int_value(call_param.str_value_.string_, int_val))) {
            LOG_WARN("fail to get int value", K(call_param.str_value_.string_), K(ret));
          } else {
            target_obj.set_int(int_val);
          }
        } else if (CALL_TOKEN_STR_VAL == call_param.type_) {
          target_obj.set_varchar(call_param.str_value_.string_);
          target_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else if (CALL_TOKEN_PLACE_HOLDER == call_param.type_) {
          need_use_execute_param = true;
          if (OB_FAIL(get_int_value(call_param.str_value_.string_, execute_param_index))) {
            LOG_WARN("fail to get int value", K(call_param.str_value_.string_), K(ret));
          }
        }
      }
    } else {
      need_use_execute_param = true;
    }
    if (OB_SUCC(ret) && need_use_execute_param
        && OB_MYSQL_COM_STMT_EXECUTE == client_request->get_packet_meta().cmd_) {
      // for com_stmt_prepare, we have no execute_params, so no need continue, just return
      LOG_DEBUG("will cal obj with value from execute param", K(execute_param_index));
      if (OB_ISNULL(ps_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ps entry is null", K(ret));
      } else if (OB_UNLIKELY(execute_param_index >= ps_entry->get_param_count())
                 || execute_param_index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid placeholder idx", K(execute_param_index), KPC(ps_entry), K(ret));
      } else if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_execute_param(ps_entry->get_param_count(),
                         ps_entry->get_ps_sql_meta().get_param_types(), *client_request, execute_param_index, target_obj))) {
        LOG_WARN("fail to analyze execute param", K(ret));
      }
    }
    if (OB_SUCC(ret) && need_use_execute_param && OB_MYSQL_COM_STMT_PREPARE == client_request->get_packet_meta().cmd_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_DEBUG("prepare sql with only placeholder, will return fail", K(ret));
    }

    if (OB_SUCC(ret) && need_use_execute_param
        && client_request->get_parse_result().is_text_ps_execute_stmt()) {
      ObSqlParseResult &parse_result = client_request->get_parse_result();
      ObProxyTextPsExecuteInfo execute_info = parse_result.text_ps_execute_info_;
      if (param_index >= execute_info.param_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param index is large than param count", K(param_index),
            K(execute_info.param_count_), K(ret));
      } else {
        ObProxyTextPsExecuteParam &param = execute_info.params_.at(param_index);
        ObString user_variable_name = param.str_value_.string_;
        if (OB_FAIL(static_cast<const ObClientSessionInfo&>(*client_info).get_user_variable_value(user_variable_name, target_obj))) {
          LOG_WARN("get user variable failed", K(ret), K(user_variable_name));
        }
      }
    }

    if (OB_SUCC(ret) && need_use_execute_param
        && OB_MYSQL_COM_STMT_PREPARE_EXECUTE == client_request->get_packet_meta().cmd_) {
      LOG_DEBUG("will cal obj with value from execute param", K(execute_param_index));
      if (OB_UNLIKELY(execute_param_index < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid placeholder idx", K(execute_param_index), K(ret));
      } else if (OB_FAIL(ObMysqlRequestAnalyzer::analyze_prepare_execute_param(*client_request, execute_param_index, target_obj))) {
        LOG_WARN("fail to analyze execute param", K(ret));
      }
    }
  }
  return ret;
}

} // end of opsql
} // end of obproxy
} // end of oceanbase
