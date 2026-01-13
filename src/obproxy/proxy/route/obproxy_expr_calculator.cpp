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
#include "obproxy_expr_calculator.h"
#include "opsql/expr_parser/ob_expr_parser.h"
#include "opsql/expr_resolver/ob_expr_resolver.h"
#include "opsql/expr_parser/ob_expr_parser_utils.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "proxy/mysqllib/ob_proxy_session_info.h"
#include "proxy/route/obproxy_part_info.h"
#include "proxy/mysql/ob_prepare_statement_struct.h"
#include "obproxy/utils/ob_proxy_utils.h"
#include "share/part/ob_part_desc.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
#include "proxy/route/ob_server_route.h"
#include "lib/hash/ob_hashset.h"
#include "obkv/table/ob_table_rpc_request.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "proxy/rpc/rpclib/ob_table_query_async_entry.h"
#include "proxy/route/ob_route_diagnosis.h"


using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share::schema;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::obkv;


int ObProxyExprCalculator::handle_hint_route_info(const ObSqlParseResult& parse_result,
                                                  const ObProxyPartKeyInfo& part_key_info,
                                                  ObIArray<ObObj>& equal_obj_arr)
{
  int ret = OB_SUCCESS;
  const ObProxySimpleRouteInfo& info = parse_result.hint_route_info_;
  const ObIArray<PartVarNode>& part_key_values = info.part_key_values_;

  if (part_key_info.key_num_ != equal_obj_arr.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected array length", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_key_values.count(); ++i) {
      const PartVarNode& part_val = part_key_values.at(i);
      const ObString& key_name = part_val.var_name_;

      for (int64_t j = 0; OB_SUCC(ret) && j < part_key_info.key_num_; ++j) {
        const ObProxyParseString& part_key_name_parse = part_key_info.part_keys_[j].name_;
        const ObString part_key_name_string(part_key_name_parse.str_len_, part_key_name_parse.str_);
        if (0 == key_name.case_compare(part_key_name_string)) {
          ObObj& target_obj = equal_obj_arr.at(j);
          if (OB_UNLIKELY(!target_obj.is_unknown())) {
            // has been set, use first value, ignore later
            LOG_DEBUG("multi part key value in hint, ignore others values except the first");
          } else if (SET_VALUE_TYPE_STR == part_val.value_type_) {
            target_obj.set_varchar(part_val.str_value_);
            target_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else if (SET_VALUE_TYPE_INT == part_val.value_type_) {
            target_obj.set_int(part_val.int_value_);
          } else if (SET_VALUE_TYPE_NUMBER == part_val.value_type_) {
            target_obj.set_varchar(part_val.str_value_);
            target_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            // impossible
            target_obj.set_type(ObUnknownType);
          }
          LOG_DEBUG("succ to get part key-value from hint", K(key_name), K(target_obj));
        }
      }
    }
  }

  return ret;
}

int ObProxyExprCalculator::do_expr_parse(const common::ObString &req_sql,
                                         const ObSqlParseResult &parse_result,
                                         ObIAllocator &allocator,
                                         ObExprParseResult &expr_result,
                                         ObCollationType connection_collation)
{
  int ret = OB_SUCCESS;

  // do parse
  ObExprParseMode parse_mode = INVALID_PARSE_MODE;
  if (parse_result.is_select_stmt() || parse_result.is_delete_stmt()
      || parse_result.is_text_ps_select_stmt()
      || parse_result.is_text_ps_delete_stmt()) {
    // we treat delete as select
    parse_mode = SELECT_STMT_PARSE_MODE;
  } else if (parse_result.is_insert_stmt() || parse_result.is_replace_stmt()
             || parse_result.is_update_stmt()
             || parse_result.is_merge_stmt()
             || parse_result.is_text_ps_insert_stmt()
             || parse_result.is_text_ps_replace_stmt()
             || parse_result.is_text_ps_update_stmt()
             || parse_result.is_text_ps_merge_stmt()) {
    parse_mode = INSERT_STMT_PARSE_MODE;
  }
  ObExprParser expr_parser(allocator, parse_mode);

  // init expr result
  expr_result.table_info_.table_name_.str_ = const_cast<char *>(parse_result.get_table_name().ptr());
  expr_result.table_info_.table_name_.str_len_ = parse_result.get_table_name().length();
  expr_result.table_info_.database_name_.str_ = const_cast<char *>(parse_result.get_database_name().ptr());
  expr_result.table_info_.database_name_.str_len_ = parse_result.get_database_name().length();
  expr_result.table_info_.alias_name_.str_ = const_cast<char *>(parse_result.get_alias_name().ptr());
  expr_result.table_info_.alias_name_.str_len_ = parse_result.get_alias_name().length();
  expr_result.has_rowid_ = false;
  expr_result.is_empty_column_insert_stmt_ = false;

  if (OB_FAIL(expr_parser.parse_reqsql(req_sql,  parse_result.get_parsed_length(), expr_result,
                                       parse_result.get_stmt_type(), connection_collation))) {
    LOG_DEBUG("fail to do expr parse_reqsql", K(req_sql), K(ret));
  } else if (OB_FAIL(do_expr_parse_diagnosis(expr_result))) {
    LOG_DEBUG("fail to expr parse diagnosis", K(ret));
  }
  return ret;
}

int ObProxyExprCalculator::do_expr_parse_diagnosis(ObExprParseResult &expr_result)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(route_diagnosis_) && route_diagnosis_->is_diagnostic(EXPR_PARSE)) {
    ObProxyRelationExpr **relations = expr_result.all_relation_info_.relations_;
    char col_val_buf[EXPR_PARSE_MAX_LEN];
    char *store_col_val = NULL;
    int64_t pos = 0;
    for (int i = 0; i < expr_result.all_relation_info_.relation_num_; i++) {
      ObString col, val;
      const char* func_str = NULL;
      char buf[20] { 0 };
      ObProxyRelationExpr *relation = NULL;

      if (OB_NOT_NULL(relations + i)) {
        relation = *(relations + i);
      }
      if (OB_NOT_NULL(relation) &&
          OB_NOT_NULL(relation->left_value_) &&
          OB_NOT_NULL(relation->left_value_->column_node_) &&
          relation->left_value_->column_node_->type_ == TOKEN_COLUMN) {
        col.assign_ptr(relation->left_value_->column_node_->column_name_.str_,
                       relation->left_value_->column_node_->column_name_.str_len_);
      }
      if (OB_NOT_NULL(relation) &&
          OB_NOT_NULL(relation->right_value_) &&
          OB_NOT_NULL(relation->right_value_->head_)) {
        if (relation->right_value_->head_->type_ == TOKEN_STR_VAL ||
            relation->right_value_->head_->type_ == TOKEN_FUNC ||
            relation->right_value_->head_->type_ == TOKEN_HEX_VAL) {
          val.assign_ptr(relation->right_value_->head_->str_value_.str_,
                         relation->right_value_->head_->str_value_.str_len_);
        } else if (relation->right_value_->head_->type_ == TOKEN_INT_VAL) {
          sprintf(buf, "%ld", relation->right_value_->head_->int_value_);
          val.assign_ptr(buf, (ObString::obstr_size_t) strlen(buf));
        } else if (relation->right_value_->head_->type_ == TOKEN_PLACE_HOLDER) {
          buf[0] = '?';
          val.assign_ptr(buf, (ObString::obstr_size_t) strlen(buf));
        } else if (relation->right_value_->head_->type_ == TOKEN_NULL) {
          val = "NULL";
        } 
      }
      if (pos + col.length() + val.length() + 2 > EXPR_PARSE_MAX_LEN) {
        LOG_DEBUG("reach max len of expr parse", K(pos), K(col), K(val), K(i));
      } else {
        if (pos != 0) {
          col_val_buf[pos++] = ',';
        }

        func_str = get_obproxy_function_string(relation->type_);
        sprintf(col_val_buf + pos, "%.*s%s%.*s", col.length(), col.ptr(), func_str, val.length(), val.ptr());
        pos = pos + col.length() + val.length() + 1;
      }
    }
    if (pos != 0) {
      if (OB_NOT_NULL(store_col_val = (char*) route_diagnosis_->get_alloc()->alloc(pos))) {
        MEMCPY(store_col_val, col_val_buf, (size_t) pos);
      } else {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to allocate memory", "size", pos, K(ret));
      }
    }
    ROUTE_DIAGNOSIS(route_diagnosis_, EXPR_PARSE, expr_parse, ret, ObString((ObString::obstr_size_t) pos, store_col_val));
  }
  return ret;
}


int ObProxyExprCalculator::do_partition_id_calc(ObExprResolverV2 &expr_resolver,
                                                ObClientSessionInfo &session_info,
                                                ObProxyPartInfo &part_info,
                                                const ObSqlParseResult &parse_result,
                                                ObIAllocator &allocator,
                                                int64_t &first_part_id,
                                                int64_t &sub_part_id,
                                                int64_t &partition_id,
                                                int64_t &first_part_index,
                                                int64_t &sub_part_index)
{
  int ret = OB_SUCCESS;
  ObProxyPartMgr &part_mgr = part_info.get_part_mgr();
  ObNewRange& first_part_range = expr_resolver.get_first_part_range();
  ObNewRange& sub_part_range = expr_resolver.get_sub_part_range();
  int64_t tablet_id = -1;

  if (OB_UNLIKELY(!part_info.has_first_part())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("not a valid partition table", K(part_info.get_part_level()), K(ret));
  } else if (OB_INVALID_INDEX != partition_id) {
    // get partition_id without calc. nothing
  } else {
    ObPartDescCtx ctx(&session_info, parse_result.is_insert_stmt(), part_info.get_cluster_version());
    ObSEArray<int64_t, 16> part_ids;
    ObSEArray<int64_t, 16> tablet_ids;
    if (OB_INVALID_INDEX == first_part_id) {
      if (OB_FAIL(part_mgr.get_first_part(first_part_range, allocator, part_ids,
                                          ctx, tablet_ids, first_part_index))) {
        LOG_DEBUG("fail to get first part", K(ret));
      } else if (part_ids.count() >= 1) {
        first_part_id = part_ids[0];
      }
      if (OB_SUCC(ret)
          && tablet_ids.count() >= 1
          && !part_info.has_sub_part()) {
        tablet_id = tablet_ids.at(0);
      }
    }

    LOG_DEBUG("do partition id calc", K(first_part_id), K(tablet_id),
              "has sub_part", part_info.has_sub_part());

    if (OB_INVALID_INDEX != first_part_id && part_info.has_sub_part()) {
      ObPartDesc *sub_part_desc_ptr = NULL;
      ObSEArray<int64_t, 1> sub_part_ids;
      ObSEArray<int64_t, 1> tablet_ids;
      if (OB_FAIL(part_mgr.get_sub_part_desc_by_first_part_id(part_info.is_template_table(),
                                                              first_part_id,
                                                              sub_part_desc_ptr,
                                                              part_info.get_cluster_version()))) {
        LOG_DEBUG("fail to get sub part desc by first", K(ret));
      } else if (OB_FAIL(part_mgr.get_sub_part(sub_part_range, allocator, sub_part_desc_ptr,
                                                sub_part_ids, ctx, tablet_ids, sub_part_index))) {
        LOG_DEBUG("fail to get sub part", K(ret));
      } else if (sub_part_ids.count() >= 1) {
        sub_part_id = sub_part_ids[0];
      }

      if (OB_SUCC(ret) && tablet_ids.count() >= 1) {
        tablet_id = tablet_ids[0];
      }
    }


    LOG_DEBUG("do partition id calc", K(sub_part_id), K(tablet_id),
              "has sub_part", part_info.has_sub_part(), K(ret));

    if (OB_DATA_OUT_OF_RANGE == ret) {
      first_part_id = 0;
      sub_part_id = 0;
      LOG_DEBUG("will route to p0sp0 since data out of range", K(ret));
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)
        && (tablet_id != -1
            || (first_part_id != OB_INVALID_INDEX
                && (!part_info.has_sub_part() || sub_part_id != OB_INVALID_INDEX)))) {
      if (tablet_id == -1) {
        partition_id = generate_phy_part_id(first_part_id, sub_part_id, part_info.get_part_level());
      } else {
        partition_id = tablet_id;
      }
      LOG_DEBUG("succ to get part id", K(first_part_id), K(sub_part_id), K(partition_id), K(first_part_index), K(sub_part_index));
    }
  }

  ROUTE_DIAGNOSIS(route_diagnosis_,
                  CALC_PARTITION_ID,
                  calc_partition_id,
                  ret,
                  const_cast<ObPartDesc*>(part_info.get_part_mgr().get_first_part_desc()),
                  const_cast<ObPartDesc*>(part_info.get_part_mgr().get_sub_part_desc()));
  return ret;
}

int ObProxyExprCalculator::calc_part_id_by_random_choose_from_exist(ObProxyPartInfo &part_info,
                                                                    int64_t &first_part_id,
                                                                    int64_t &sub_part_id,
                                                                    int64_t &phy_part_id,
                                                                    int64_t &first_part_index,
                                                                    int64_t &sub_part_index)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = -1;

	ObProxyPartMgr &part_mgr = part_info.get_part_mgr();
  if (part_info.has_first_part() && OB_INVALID_INDEX == first_part_id) {
    int64_t first_part_num = 0;
    if (OB_FAIL(part_info.get_part_mgr().get_first_part_num(first_part_num))) {
      LOG_WDIAG("fail to get first part num", K(ret));
    } else {
      int64_t rand_num = 0;
      if (OB_FAIL(ObRandomNumUtils::get_random_num(0, first_part_num - 1, rand_num))) {
        LOG_WDIAG("fail to get random num in first part", K(first_part_num), K(ret));
      } else {
        if (OB_FAIL(part_mgr.get_first_part_id_by_random(rand_num, first_part_id, tablet_id))) {
          LOG_WDIAG("failed to get first part id by random", K(rand_num), K(ret));
        } else {
          first_part_index = rand_num;
        }
      }
    }
  }
  LOG_DEBUG("choose partition id from exist", K(first_part_id), K(sub_part_id));

  if (OB_SUCC(ret) && part_info.has_sub_part() && OB_INVALID_INDEX == sub_part_id) {
    int64_t sub_part_num = 0;
    if (OB_FAIL(part_mgr.get_sub_part_num_by_first_part_id(part_info, first_part_id, sub_part_num))) {
      LOG_DEBUG("fail to get sub part num in random schedule", K(ret));
    } else {
      int64_t sub_rand_num = 0;
      if (OB_FAIL(ObRandomNumUtils::get_random_num(0, sub_part_num - 1, sub_rand_num))) {
        LOG_WDIAG("fail to get random num in sub part", K(sub_part_num), K(ret));
      } else {
        ObSEArray<int64_t, 1> part_ids;
        ObSEArray<int64_t, 1> tablet_ids;
        ObPartDesc *sub_part_desc_ptr = NULL;
        if (OB_FAIL(part_mgr.get_sub_part_desc_by_first_part_id(part_info.is_template_table(),
                                                                first_part_id,
                                                                sub_part_desc_ptr,
                                                                part_info.get_cluster_version()))) {
          LOG_DEBUG("fail to get sub part desc by first part id", K(first_part_id), K(ret));
        } else if (OB_FAIL(part_mgr.get_sub_part_by_random(sub_rand_num, sub_part_desc_ptr, part_ids, tablet_ids))) {
          LOG_DEBUG("fail to get sub part id by random", K(ret));
        } else {
          sub_part_index = sub_rand_num;
          if (part_ids.count() >= 1) {
            sub_part_id = part_ids[0];
          }
          if (tablet_ids.count() >= 1) {
            tablet_id = tablet_ids[0];
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (tablet_id != -1) {
      phy_part_id = tablet_id;
    } else {
      phy_part_id = generate_phy_part_id(first_part_id, sub_part_id, part_info.get_part_level());
    }
  } else {
    LOG_WDIAG("fail to cal part id by random choose from exist", K(ret));
  }

  return ret;
}

void ObProxyExprCalculator::set_route_diagnosis(ObRouteDiagnosis *route_diagnosis)
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

int ObProxyExprCalculator::calculate_partition_id(common::ObArenaAllocator &allocator,
                                                  const ObString &req_sql,
                                                  const ObSqlParseResult &parse_result,
                                                  ObProxyMysqlRequest &client_request,
                                                  ObClientSessionInfo &client_info,
                                                  ObServerRoute &route,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  ObExprParseResult expr_parse_result;
  // part_info.get_part_key_info() is stored const
  // expr_parse_result.part_key_info_ will be changed when handling differrent SQL
  const ObProxyPartKeyInfo &origin_part_key_info = part_info.get_part_key_info();
  const ObProxyPartKeyInfo &part_key_info = expr_parse_result.part_key_info_;
  const ObProxyRelationInfo& all_relation_info = expr_parse_result.all_relation_info_;
  if (OB_UNLIKELY(part_info.is_oracle_mode() != client_info.is_oracle_mode())) {
    LOG_EDIAG("unexpected is_oracle_mode", K(part_info.is_oracle_mode()), K(client_info.is_oracle_mode()));
  }
  bool old_is_oracle_mode = lib::is_oracle_mode();
  bool is_oracle_mode = part_info.is_oracle_mode();
  expr_parse_result.is_oracle_mode_ = is_oracle_mode;
  lib::set_oracle_mode(is_oracle_mode);

  ObExprResolverV2 expr_resolver(is_oracle_mode, client_request, client_info, part_info, allocator);
  expr_resolver.set_route_diagnosis(route_diagnosis_);

  const common::ObString &print_sql = ObProxyMysqlRequest::get_print_sql(req_sql);
  ObString part_name = parse_result.get_part_name();
  ObMySQLCmd cmd = client_request.get_packet_meta().cmd_;

  int64_t first_part_id = OB_INVALID_INDEX;
  int64_t sub_part_id = OB_INVALID_INDEX;

  // the two values below are used to display part_name in route diagnosis
  int64_t first_part_index = OB_INVALID_INDEX;
  int64_t sub_part_index = OB_INVALID_INDEX;

  int64_t part_key_num = origin_part_key_info.key_num_;
  int64_t relation_num = 0;

  // step 0.preprocess
  if (OB_FAIL(expr_resolver.init(part_key_num))) {
    LOG_WDIAG("fail to do expr_resolver init", K(part_key_num), K(ret));
  } else {
    // deep copy
    expr_parse_result.part_key_info_.key_num_ = origin_part_key_info.key_num_;
    for (int i = 0; i < origin_part_key_info.key_num_; ++i) {
      expr_parse_result.part_key_info_.part_keys_[i] = origin_part_key_info.part_keys_[i];
    }
  }

  // step 1. calc partition id with part name
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_LIKELY(part_name.empty())) {
    // ignore
  } else if (OB_FAIL(part_info.get_part_mgr().get_part_with_part_name(part_name, first_part_id, partition_id))) {
    ret = OB_SUCCESS;
    LOG_DEBUG("fail to get part id with part name, will do calc in normal path", K(part_name), K(ret));
  }


  // step 2. get part key info in SQL hint
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_LIKELY(!parse_result.has_hint_route_info())) {
    // ignore
  } else if (OB_FAIL(handle_hint_route_info(parse_result, part_key_info, expr_resolver.get_equal_array()))) {
    ret = OB_SUCCESS;
    LOG_DEBUG("fail to calc part id with simple part info, will do calc in normal path", K(ret));
  }

  // step 3. parse part key-values in SQL with expr parser
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_FAIL(do_expr_parse(req_sql, parse_result, allocator, expr_parse_result,
                                   static_cast<ObCollationType>(client_info.get_collation_connection())))) {
    LOG_DEBUG("fail to do expr parse", K(print_sql), K(part_info), "expr_parse_result",
              ObExprParseResultPrintWrapper(expr_parse_result));
  }

  // step 4. get relation objs with expr parser result
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else {
    relation_num = all_relation_info.relation_num_;
    if (OB_FAIL(expr_resolver.do_relation_obj_resolve(expr_parse_result))) {
      LOG_WDIAG("fail to expr_resolver do_relation_obj_resolve", K(relation_num), K(ret));
    }
  }

  // step 5. put relation objs into part key value arr
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_FAIL(expr_resolver.do_part_key_obj_prepare(all_relation_info, expr_parse_result.is_empty_column_insert_stmt_))) {
    LOG_WDIAG("fail to expr_resolver do_part_key_obj_prepare", "is_empty_column_insert_stmt",
              expr_parse_result.is_empty_column_insert_stmt_, K(ret));
  }

  /* have got objs of expr parse relation from now on*/
  // step 6. relation objs ratiocination
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (expr_parse_result.is_empty_column_insert_stmt_) {
    // insert stmt without column name can not be ratiocinated
  } else if (OB_FAIL(expr_resolver.do_equal_relation_ratiocination(all_relation_info, part_key_info,
                                      parse_result.get_table_name(), parse_result.get_alias_name()))) {
    LOG_DEBUG("fail to expr_resolver do_equal_relation_ratiocination, but not influence calc totally", K(ret));
    ret = OB_SUCCESS;
  }

  // step 7. relation simplification
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_FAIL(expr_resolver.do_relation_simplication())) {
    LOG_DEBUG("fail to expr_resolver do_relation_simplication, continue", K(ret));
    ret = OB_SUCCESS;
  }

  // step 8. get partition ID or obj from rowID
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (!expr_parse_result.has_rowid_) {
    // no need
  } else if (OB_FAIL(expr_resolver.do_rowid_calc(part_key_info, partition_id))) {
    LOG_WDIAG("fail to expr_resolver do_rowid_calc", K(ret));
  }

  /* have got objs of part key from now on*/
  // step 9. get default obj for insert stmt
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (!parse_result.is_insert_stmt()) {
    // ignore
  } else if (OB_FAIL(expr_resolver.do_default_value_set(part_key_info))) {
    LOG_WDIAG("fail to expr_resolver do_default_value_set", K(ret));
  }

  // step 10. calc obj for generate keys
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_LIKELY(!part_info.has_generated_key())) {
    // ignore
  } else if (OB_FAIL(expr_resolver.do_generated_key_calc(part_key_info))) {
    LOG_WDIAG("fail to expr_resolver do_generated_key_calc", K(ret));
  }

  // step 11. set objs for same part key
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_FAIL(expr_resolver.do_same_part_key_set(part_key_info))) {
    LOG_WDIAG("fail to expr_resolver do_same_part_key_set", K(ret));
  }

  // step 12. calc obj for part key func
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (!part_info.has_part_func_key()) {
    // ignore
  } else if (OB_FAIL(expr_resolver.do_part_key_func_calc(part_key_info, part_info))) {
    LOG_DEBUG("fail to expr_resolver do_part_key_func_calc", K(ret));
    ret = OB_SUCCESS;
  }

  // step 13. generate range with relation objs
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_FAIL(expr_resolver.do_generate_range(part_key_info, part_info))) {
    LOG_WDIAG("fail to expr_resolver do_generate_range", K(ret));
  }

  /* have got ranges of part keys with objs behind from now on*/
  // step 14. calc part id with range
  if (OB_FAIL(ret)) {
    // nothing
  } else if (OB_UNLIKELY(OB_INVALID_INDEX != partition_id)) {
    // no need
  } else if (OB_FAIL(do_partition_id_calc(expr_resolver, client_info, part_info,
                                          parse_result, allocator, first_part_id, sub_part_id, partition_id,
                                          first_part_index, sub_part_index))) {
    if (OB_MYSQL_COM_STMT_PREPARE != cmd) {
      LOG_DEBUG("fail to do expr resolve", K(print_sql), K(expr_resolver), K(part_info));
    }
  } else {
    LOG_DEBUG("succ to calc get partition id", K(cmd), K(first_part_id), K(sub_part_id), K(partition_id));
  }

  // step 15. cannot cacl partition id precisely with no enough information
  //         -just use optimized random choice
  LOG_DEBUG("calc partition info from sql", K(first_part_id), K(sub_part_id), K(partition_id), K(first_part_index), K(sub_part_index));
  if ((OB_FAIL(ret) || partition_id == OB_INVALID_INDEX)) {
    route.is_partition_calc_fail_ = true;
    if (!get_global_proxy_config().enable_primary_zone
        && !get_global_proxy_config().enable_cached_server) {
      // if proxy primary zone route optimization disabled, use random part id optimization
      if (OB_FAIL(calc_part_id_by_random_choose_from_exist(part_info, first_part_id, sub_part_id,
                                                          partition_id, first_part_index, sub_part_index))) {
        LOG_WDIAG("fail to cal part id by random choose", K(first_part_id), K(sub_part_id), K(partition_id), K(ret));
      } else {
        route.no_need_pl_update_ = true;
        LOG_DEBUG("succ to cal part id by random choose", K(first_part_id), K(sub_part_id), K(partition_id));
      }
    } else {
      // // nothing, will use primary zone or cached server
    }
  } else {
    LOG_DEBUG("succ to cal part id with SQL", K(first_part_id), K(sub_part_id), K(partition_id));
  }

  lib::set_oracle_mode(old_is_oracle_mode);
  ROUTE_DIAGNOSIS(route_diagnosis_,
                  PARTITION_ID_CALC_DONE,
                  partition_id_calc,
                  ret,
                  req_sql.length() != client_request.get_sql().length() ? req_sql
                    : (req_sql.case_compare(client_request.get_sql()) == 0 ? ObString() : req_sql),
                  parse_result.get_part_name(),
                  first_part_index,
                  sub_part_index,
                  partition_id,
                  part_info.get_part_level());

  return ret;
}

int ObExprCalcTool::build_dtc_params_with_tz_info(ObClientSessionInfo *session_info,
                                                  ObObjType obj_type,
                                                  ObTimeZoneInfo &tz_info,
                                                  ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info)) {
    if (OB_FAIL(build_tz_info(session_info, obj_type, tz_info))) {
      LOG_WDIAG("fail to build tz info", K(ret));
    } else if (OB_FAIL(build_dtc_params(session_info, obj_type, dtc_params))) {
      LOG_WDIAG("fail to build dtc params", K(ret));
    } else {
      dtc_params.tz_info_ = &tz_info;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fail to build dtc params for null session info", K(ret));
  }

  return ret;
}

/*
 * for ObTimestampLTZType, ObTimestampTZType input timestamp string, and we also need time_zone from session
 * in order to decide the absolutely time
 */
int ObExprCalcTool::build_tz_info(ObClientSessionInfo *session_info,
                                  ObObjType obj_type,
                                  ObTimeZoneInfo &tz_info)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info)) {
    if (ObTimestampLTZType == obj_type || ObTimestampTZType == obj_type
        || ObTimestampType == obj_type) {
      if (OB_FAIL(build_tz_info_for_all_type(session_info, tz_info))) {
        LOG_WDIAG("fail to build time zone info with session", K(ret));
      }
    }
    LOG_DEBUG("try build time zone", K(obj_type));
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fail to build tz info for null session info", K(ret));
  }
  return ret;
}

int ObExprCalcTool::build_tz_info_for_all_type(ObClientSessionInfo *session_info,
                                               ObTimeZoneInfo &tz_info)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  ObString sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_TIME_ZONE);
  if (OB_NOT_NULL(session_info)) {
    if (OB_FAIL(session_info->get_sys_variable_value(sys_key_name, value_obj))) {
      LOG_WDIAG("fail to get sys var from session", K(ret), K(sys_key_name));
    } else {
      ObString value_str = value_obj.get_string();
      if (OB_FAIL(tz_info.set_timezone(value_str))) {
        LOG_WDIAG("fail to set time zone for tz_info", K(ret), K(value_str));
      } else {
        LOG_DEBUG("succ to set time zone for tz_info", K(value_str));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fail to build tz info for null session info", K(ret));
  }
  return ret;
}

int ObExprCalcTool::build_dtc_params(ObClientSessionInfo *session_info,
                                     ObObjType obj_type,
                                     ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(session_info)) {
    ObString sys_key_name;
    switch (obj_type) {
      case ObDateTimeType:
        sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_NLS_DATE_FORMAT);
        break;
      case ObTimestampNanoType:
      case ObTimestampLTZType:
        sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_NLS_TIMESTAMP_FORMAT);
        break;
      case ObTimestampTZType:
        sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_NLS_TIMESTAMP_TZ_FORMAT);
        break;
      default:
        break;
    }

    if (!sys_key_name.empty()) {
      ObObj value_obj;
      int sub_ret = OB_SUCCESS;
      if (OB_SUCCESS != (sub_ret = session_info->get_sys_variable_value(sys_key_name, value_obj))) {
        LOG_WDIAG("fail to get sys var from session, use standard nls format", K(sub_ret), K(sys_key_name));
      } else {
        ObString value_str = value_obj.get_string();
        if (OB_FAIL(dtc_params.set_nls_format_by_type(obj_type, value_str))) {
          LOG_WDIAG("fail to set nls format by type", K(ret), K(obj_type), K(value_str));
        } else {
          LOG_DEBUG("succ to set nls format by type", K(obj_type), K(value_str));
        }
      }
    } else {
      /* other types do not need nls format from session, do nothing here */
      LOG_DEBUG("no need to set nls format", K(obj_type));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fail to build dtc params due to null session", K(ret));
  }

  return ret;
}
                                                         
int ObProxyExprCalculator::calculate_partition_id_for_rpc(common::ObArenaAllocator &allocator,
                                                  ObRpcReq &ob_rpc_req,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  switch (ob_rpc_req.get_rpc_type()) {
  case OBPROXY_RPC_OBRPC:
    ret = calculate_partition_id_for_obkv(allocator, ob_rpc_req, part_info, partition_id);
    break;
  case OBPROXY_RPC_REDIS:
    ret = calculate_partition_id_for_redis(allocator, ob_rpc_req, part_info, partition_id);
    break;
  case OBPROXY_RPC_HBASE:
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unknown rpc type for request", K(ret), "rpc type", ob_rpc_req.get_rpc_type());
    break;
  }

  return ret;
}

int ObProxyExprCalculator::calculate_partition_id_for_redis(common::ObArenaAllocator &allocator,
                                                            ObRpcReq &ob_rpc_req,
                                                            ObProxyPartInfo &part_info,
                                                            int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRedisRequest *redis_request = NULL;
  ObRpcRedisInfo *redis_info = NULL;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  int pcode = obkv_info.pcode_;
  if (OB_ISNULL(redis_info = ob_rpc_req.get_redis_info()) || OB_ISNULL(redis_request = redis_info->get_redis_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("calculate_partition_id_for_redis get a invalid rpc_req", K(ret), K(ob_rpc_req), KP(redis_request), KP(redis_info), K(rpc_trace_id));
  } else if (obkv_info.partition_id_ != common::OB_INVALID_INDEX) {
    partition_id = obkv_info.partition_id_; // not need calc it again
    LOG_DEBUG("calculate_partition_id_for_redis use partition id user set", K(partition_id), K(rpc_trace_id));
  } else { // calc partition id by redis request
    if (OB_FAIL(redis_request->calc_partition_id(allocator, ob_rpc_req, part_info, partition_id))) {
      LOG_WDIAG("fail to calc_partition_id for redis_request", K(ret), K(rpc_trace_id), K(pcode), KP(redis_request));
    } else {
      LOG_DEBUG("redis partition id has done", K(ret), K(pcode), K(partition_id), "shard request", obkv_info.is_shard(),
                K(rpc_trace_id));
    }
    #ifdef ERRSIM
    if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_RPC_SET_SHARD) OB_SUCCESS)) {
      ret = OB_SUCCESS;
      obkv_info.set_definitely_single(false);
      obkv_info.set_partition_id(common::OB_INVALID_INDEX);
      obkv_info.set_ls_id(ObLSID::INVALID_LS_ID);
      obkv_info.set_shard(true);
      partition_id = common::OB_INVALID_INDEX;
    }
    #endif
  }
  return ret;
}

int ObProxyExprCalculator::calculate_partition_id_for_obkv(common::ObArenaAllocator &allocator,
                                                           ObRpcReq &ob_rpc_req,
                                                           ObProxyPartInfo &part_info,
                                                           int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  ObRpcOBKVInfo &obkv_info = ob_rpc_req.get_obkv_info();
  ObRpcRequest *rpc_request = NULL;
  const ObRpcReqTraceId &rpc_trace_id = ob_rpc_req.get_trace_id();
  int pcode = obkv_info.pcode_;
  if (OB_ISNULL(rpc_request = ob_rpc_req.get_rpc_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("calculate_partition_id_for_obkv get a invalid rpc_req", K(ret), K(ob_rpc_req), K(rpc_trace_id));
  } else if (obkv_info.partition_id_ != common::OB_INVALID_INDEX) {
    partition_id = obkv_info.partition_id_; // not need calc it again
    LOG_DEBUG("calculate_partition_id_for_obkv use partition id user set", K(partition_id), K(rpc_trace_id));
  } else { // calc partition id by rpc request
    if (OB_FAIL(rpc_request->calc_partition_id(allocator, ob_rpc_req, part_info, partition_id))) {
      LOG_WDIAG("fail to calc_partition_id for rpc_request", K(ret), K(rpc_trace_id), K(pcode), KPC(rpc_request));
    } else {
      LOG_DEBUG("rpc partition id has done", K(ret), K(pcode), K(partition_id), "shard request", obkv_info.is_shard(),
                K(rpc_trace_id));
    }
    #ifdef ERRSIM
    if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_RPC_SET_SHARD) OB_SUCCESS)) {
      ret = OB_SUCCESS;
      obkv_info.set_definitely_single(false);
      obkv_info.set_partition_id(common::OB_INVALID_INDEX);
      obkv_info.set_ls_id(ObLSID::INVALID_LS_ID);
      obkv_info.set_shard(true);
      partition_id = common::OB_INVALID_INDEX;
    }
    #endif
  }
  return ret;
}

int ObRpcExprCalcTool::do_eval_rowkey_index(ObProxyPartInfo &proxy_part_info,
                                            ObProxyPartKeyLevel level,
                                            const ObString &src_name,
                                            int &src_key_idx,
                                            const common::ObIArray<common::ObString> &rowkey_columns_name,
                                            common::ObIArray<int64_t> &rowkey_index)
{
  int ret = OB_SUCCESS;
  ObProxyPartKeyInfo &part_info = proxy_part_info.get_part_key_info();
  ObString part_key_name;
  int compare_ret = 0;
  // 客户端传rowkey列信息
  if (0 != rowkey_columns_name.count()) {
    for (int j = 0; j < rowkey_columns_name.count(); ++j) {
      compare_ret = rowkey_columns_name.at(j).case_compare(src_name);
      if (0 == compare_ret) {
        rowkey_index.push_back(j);
        break;
      }
    }
  } else if (src_key_idx < 0){
    int64_t idx_in_rowid = 0;
    // 依赖observer返回的idx_in_rowid
    for (int j = 0; OB_SUCC(ret) && j < part_info.key_num_; ++j) {
      if (part_info.part_keys_[j].level_ == level) {
        part_key_name.assign(part_info.part_keys_[j].name_.str_, part_info.part_keys_[j].name_.str_len_);
        compare_ret = part_key_name.case_compare(src_name);
        if (0 == compare_ret) {
          // 1. generated key, will not come here
          // 2. virtual table, will not come here
          // 3. single part table or heap table
          if (OB_UNLIKELY(part_info.part_keys_[j].idx_in_rowid_ < 0)) {
            // TODO: need to check if the table is single part table
            rowkey_index.push_back(idx_in_rowid);
            idx_in_rowid++;
          } else {
            rowkey_index.push_back(part_info.part_keys_[j].idx_in_rowid_);
          }
          break;
        }
      }
    }
  } else {
    // calc generate key, will not come here
    // TODO need get idx_in_rowid
    if (0 == src_name.case_compare("K")) {
      rowkey_index.push_back(0);
    }
  }
  return ret;
}

int ObRpcExprCalcTool::eval_rowkey_index(ObProxyPartInfo &proxy_part_info,
                                         ObProxyPartKeyLevel level,
                                         const common::ObIArray<common::ObString> &rowkey_columns_name,
                                         common::ObIArray<int64_t> &rowkey_index,
                                         common::ObIArray<int64_t> &part_info_index)
{
  int ret = OB_SUCCESS;
  // The table client sends rowkey columns in the Table Query request
  rowkey_index.reset();
  part_info_index.reset();
  ObProxyPartKeyInfo &part_info = proxy_part_info.get_part_key_info();
  bool has_generated_key = proxy_part_info.has_generated_key();
  common::ObIArray<common::ObString> *part_columns_name = NULL;
  if (level == ObProxyPartKeyLevel::PART_KEY_LEVEL_ONE) {
    part_columns_name = &proxy_part_info.get_first_part_columns();
  } else if (level == ObProxyPartKeyLevel::PART_KEY_LEVEL_TWO) {
    part_columns_name = &proxy_part_info.get_sub_part_columns();
  }

  if (0 == part_columns_name->count()) {
    LOG_DEBUG("eval_rowkey_index invalid part_colunms_name", KPC(part_columns_name), K(ret));
  } else {

    for (int i = 0; i < part_columns_name->count(); ++i) {
      const ObString &part_col = part_columns_name->at(i); // part key name from `part_expr`
      ObString part_col_replace;
      LOG_DEBUG("get part columns", K(part_col)); //TODO will be delete in future
      // remove character '`'
      ObRpcExprCalcTool::trim_part_key_name(part_col, part_col_replace);

      // ObString part_key_name;  // part key name from `part_key_name`
      int src_key_idx = -1;
      if (has_generated_key
          && part_info.key_num_ >= 2
          && part_info.key_num_ <= 4
          && part_columns_name->count() == 1) {
        // ET_HKV support generated key calculation
        // 1. support secondary partition
        // 2. only support substring/substr/substring_index now, and column `K` must be the first param of these functions
        // 3. only support abs(T) and column 'T' must be the param of these functions
        // int src_key_idx = -1;
        for (int j = 0; OB_SUCC(ret) && j < part_info.key_num_; ++j) {
          // src key of generated key
          if (part_info.part_keys_[j].generated_col_idx_ >= 0 && part_info.part_keys_[j].level_ == level) {
            src_key_idx = j;
            part_info_index.push_back(j);
            break;
          }
        }
        if (OB_UNLIKELY(src_key_idx < 0)) {
          LOG_DEBUG("maybe do not have generated key int this part_level", K(ret), K(level));
        } else {
          // client transfer column name info, try to match src key of generated column
          ObProxyParseString &src_key_parse_name = part_info.part_keys_[src_key_idx].name_;
          part_col_replace = ObString(src_key_parse_name.str_len_, src_key_parse_name.str_);
        }
        LOG_DEBUG("calc generated key rowkey index", K(rowkey_index), K(part_info_index));
      }

      if (OB_FAIL(do_eval_rowkey_index(proxy_part_info, level, part_col_replace, src_key_idx, rowkey_columns_name, rowkey_index))) {
        LOG_WDIAG("can not find rowkey_index", K(part_col_replace), K(rowkey_index));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (rowkey_index.count() != part_columns_name->count()) {
      ret = OB_ERR_KV_ROWKEY_MISMATCH;
      LOG_WDIAG("eval_rowkey_index get err rowkey_index", K(rowkey_index), K(part_columns_name), K(rowkey_columns_name), K(ret));
    } else {
      LOG_DEBUG("get rowkey_index", K(rowkey_index), K(part_columns_name), K(rowkey_columns_name));
    }
  }

  return ret;
}

int ObRpcExprCalcTool::calculate_partition_id_with_rowkey(common::ObArenaAllocator &allocator,
                                                          ROWKEY_VALUE_PARAM &rowkey_value,
                                                          ROWKEY_COLUMN_PARAM &column_names,
                                                          ObProxyPartInfo &part_info,
                                                          int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  opsql::ObExprResolverResult resolve_result;
  ObRowkey rowkey;
  if (rowkey_value.count() > 0) {
    rowkey.assign(&rowkey_value.at(0), rowkey_value.count());
  }
  ObSEArray<int64_t, 1> partition_ids;
  ObSEArray<int64_t, 1> rowkey_index; // empty array
  ObSEArray<int64_t, 1> part_info_index; // empty array
  obkv::ObTableEntityType entity_type = obkv::ObTableEntityType::ET_DYNAMIC;
  LOG_DEBUG("redis to calculate_partition_id_with_rowkey ", K(rowkey), K(rowkey_value));
  if (part_info.has_first_part()) {
    ObRowkey &eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].start_key_;
    if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_index(part_info, PART_KEY_LEVEL_ONE, column_names,
                                                      rowkey_index, part_info_index))) {
      LOG_WDIAG("fail to call eval rowkey index for first part", K(part_info), K(ret));
    } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_values(part_info, rowkey, allocator, rowkey_index, part_info_index, eval_rowkey, entity_type))) {
      LOG_WDIAG("fail to call eval rowkey for first part", K(rowkey), K(ret));
    } else {
      // for range part, end key must to be set
      resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].end_key_ = eval_rowkey;
      resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].border_flag_.set_inclusive_start();
      resolve_result.ranges_[PARTITION_LEVEL_ONE - 1].border_flag_.set_inclusive_end();
    }
  }
  if (OB_SUCC(ret) && part_info.has_sub_part()) {
    ObRowkey &eval_rowkey = resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].start_key_;
    if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_index(part_info, PART_KEY_LEVEL_TWO, column_names,
                                                      rowkey_index, part_info_index))) {
      LOG_WDIAG("fail to call eval rowkey index for first part", K(part_info), K(ret));
    } else if (OB_FAIL(ObRpcExprCalcTool::eval_rowkey_values(part_info, rowkey, allocator, rowkey_index, part_info_index, eval_rowkey, entity_type))) {
      LOG_WDIAG("fail to call eval rowkey for first part", K(rowkey), K(ret));
    } else {
      // for range part, end key must to be set
      resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].end_key_ = eval_rowkey;
      resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].border_flag_.set_inclusive_start();
      resolve_result.ranges_[PARTITION_LEVEL_TWO - 1].border_flag_.set_inclusive_end();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRpcExprCalcTool::do_partition_id_calc_for_obkv(resolve_result, part_info, allocator, partition_ids))) {
      LOG_WDIAG("fail to calc partition id for table", K(ret));
    } else if (partition_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("obkv single rowkey get part ids/ log stream ids is not one", K(partition_ids), K(ret));
    } else {
      partition_id = partition_ids.at(0);
    }
  } else {
    LOG_WDIAG("fail to calc partition id for table", K(ret));
  }

  return ret;
}

// eval part key from rowkey, stored in eval_rowkey
int ObRpcExprCalcTool::eval_rowkey_values(ObProxyPartInfo &proxy_part_info,
                                          const ObRowkey &rowkey,
                                          common::ObArenaAllocator &allocator,
                                          common::ObIArray<int64_t> &rowkey_index,
                                          common::ObIArray<int64_t> &part_info_index,
                                          ObRowkey &eval_part_rowkey,
                                          const ObTableEntityType entity_type)
{
  int ret = OB_SUCCESS;
  ObProxyPartKeyInfo &part_key_info = proxy_part_info.get_part_key_info();
  ObObj *eval_obj = NULL;
  const ObObj *src_obj = NULL;
  void  *obj_buf = NULL;
  int64_t index = 0;
  int64_t part_info_idx = -1;

  if (0 == rowkey_index.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("empty rowkey_index", K(rowkey_index), K(ret));
  } else if (OB_ISNULL(obj_buf = (void *)allocator.alloc(rowkey_index.count() * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc new obj", K(ret));
  } else {
    eval_obj = new (obj_buf) ObObj[rowkey_index.count()]();
    if (proxy_part_info.has_generated_key()) {
      ObExprResolver resolver(allocator);
      for (int i = 0; OB_SUCC(ret) && i < rowkey_index.count(); ++i) {
        index = rowkey_index.at(i);
        // part_info_idx = part_info_index.at(i);

        if (OB_UNLIKELY(index >= rowkey.get_obj_cnt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid rowkey index and part_key_info index", K(ret), K(index), K(part_info_idx));
        } else {
          src_obj = rowkey.get_obj_ptr();
          eval_obj[i] = src_obj[index];
          // index of generated key
          if (i < part_info_index.count()) {
            part_info_idx = part_info_index.at(i);
            int64_t generated_col_idx = part_key_info.part_keys_[part_info_idx].generated_col_idx_;
            if (src_obj[index].is_max_value() || src_obj[index].is_min_value()) {
              src_obj = rowkey.get_obj_ptr();
              eval_obj[i] = src_obj[index];
            } else if (generated_col_idx >= 0) {
              if (generated_col_idx >= part_key_info.key_num_) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WDIAG("unexpected generated col idx", K(ret), K(generated_col_idx));
              } else if (OB_FAIL(resolver.calc_generated_key_value_for_obkv(
                            eval_obj[i], part_key_info.part_keys_[part_info_idx], entity_type, allocator))) {
                LOG_WDIAG("fail to calculate generated key for obkv", K(ret));
              }
            } else {
              src_obj = rowkey.get_obj_ptr();
              eval_obj[i] = src_obj[index];
            }
          }
        }
      }
    } else {
      for (int i = 0; OB_SUCC(ret) && i < rowkey_index.count(); ++i) {
        index = rowkey_index.at(i);
        if (index >= rowkey.get_obj_cnt()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WDIAG("part key idx in rowid greater than input rowkey obj cnt",
                    K(index), "cnt", rowkey.get_obj_cnt(), K(ret));
        } else {
          src_obj = rowkey.get_obj_ptr();
          eval_obj[i] = src_obj[index];
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    eval_part_rowkey.reset();
    eval_part_rowkey.assign(eval_obj, rowkey_index.count());    // 设置新obj值

    LOG_DEBUG("obkv eval part key from rowkey", K(rowkey), K(eval_part_rowkey), K(rowkey_index));
  }

  return ret;
}

int ObRpcExprCalcTool::do_partition_id_calc_for_obkv(opsql::ObExprResolverResult &resolve_result,
                                                     ObProxyPartInfo &part_info,
                                                     common::ObIAllocator &allocator,
                                                     common::ObIArray<int64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  ObProxyPartMgr &part_mgr = part_info.get_part_mgr();
  int64_t first_part_id = OB_INVALID_INDEX;
  int64_t sub_part_id = OB_INVALID_INDEX;
  int64_t partition_id;

  if (part_info.has_first_part()) {
    // Currently obkv does not handle timestamp variables and accurate check
    ObPartDescCtx ctx(NULL, false, part_info.get_cluster_version());
    ObSEArray<int64_t, 1> part_ids;
    ObSEArray<int64_t, 1> tablet_ids;

    if (OB_FAIL(part_mgr.get_first_part_for_obkv(resolve_result.ranges_[PARTITION_LEVEL_ONE - 1], allocator, part_ids,
                                                 ctx, tablet_ids))) {
      LOG_DEBUG("fail to get first part", K(ret));
    } else if (tablet_ids.count() >= 1 && tablet_ids.count() != part_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("part_ids count is not equal to tablet_ids count",
               "part_ids count", part_ids.count(),
               "tablet_ids count", tablet_ids.count());
    }

    LOG_DEBUG("do partition id calc for rpc", K(part_ids), K(tablet_ids), K(part_info.has_sub_part()));

    for (int i = 0; OB_SUCC(ret) && i < part_ids.count(); ++i) {
      first_part_id = part_ids.at(i);
      if (OB_INVALID_INDEX == first_part_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("do partition id calc for rpc get OB_INVALID_INDEX", K(partition_id));
      } else if (part_info.has_sub_part()) {
        ObPartDesc *sub_part_desc_ptr = NULL;
        ObSEArray<int64_t, 1> sub_part_ids;
        ObSEArray<int64_t, 1> tablet_ids;

        /**
         * @brief
         *  For obkv secondary partition range calculation
         *    1. If there is only one first-level partition
         *      1.1. Calculate according to the previous logic
         *    2. If there are multiple first-level partitions
         *      2.1. For the first partition, use the start key in the passed range to calculate the [left, max] sub partition id
         *      2.2. For the last partition, use the end key in the passed range to calculate the [min, right] sub partition id
         *      2.3. Else, calc the whole sub partition id
         */
        if (1 == part_ids.count()) {
          // do nothing
        } else {
          if (0 == i) {
            ctx.set_calc_first_partition(true);
            ctx.set_calc_last_partition(false);
            ctx.set_need_get_whole_range(false);
          } else if (part_ids.count() - 1 == i) {
            ctx.set_calc_first_partition(false);
            ctx.set_calc_last_partition(true);
            ctx.set_need_get_whole_range(false);
          } else {
            ctx.set_calc_first_partition(false);
            ctx.set_calc_last_partition(false);
            ctx.set_need_get_whole_range(true);
          }
        }

        if (OB_FAIL(part_mgr.get_sub_part_desc_by_first_part_id(part_info.is_template_table(),
                                                                first_part_id,
                                                                sub_part_desc_ptr,
                                                                part_info.get_cluster_version()))) {
          LOG_WDIAG("fail to get sub part desc by first", K(ret));
        } else if (OB_FAIL(part_mgr.get_sub_part_for_obkv(resolve_result.ranges_[PARTITION_LEVEL_TWO - 1],
                                                          allocator,
                                                          sub_part_desc_ptr,
                                                          sub_part_ids,
                                                          ctx,
                                                          tablet_ids))) {
          LOG_WDIAG("fail to get sub part", K(ret));
        } else {
          if (tablet_ids.count() > 0) {
            for (int i = 0; i < tablet_ids.count(); ++i) {
              partition_ids.push_back(tablet_ids.at(i));
            }
          } else {
            for (int i = 0; i < sub_part_ids.count(); ++i) {
              sub_part_id = sub_part_ids.at(i);
              partition_id = generate_phy_part_id(first_part_id, sub_part_id, PARTITION_LEVEL_TWO);
              partition_ids.push_back(partition_id);
            }
          }
        }
      } else {
        if (tablet_ids.count() > 0) {
          partition_ids.push_back(tablet_ids.at(i));
        } else {
          partition_ids.push_back(first_part_id);
        }
      }
    }

    LOG_DEBUG("do partition id calc done", K(partition_ids));
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("not a valid partition table", K(part_info.get_part_level()), K(ret));
  }

  return ret;
}

void ObRpcExprCalcTool::trim_part_key_name(const ObString &part_key_name, ObString &trim_name)
{
  trim_name = ObString();
  int32_t len = part_key_name.length();
  const char *ptr = part_key_name.ptr();
  if (3 <= len && '`' == ptr[0] && '`' == ptr[len - 1]) {
    len -= 2;
    ptr += 1;
    trim_name.assign_ptr(ptr, len);
  } else {
    trim_name = part_key_name;
  }
}

bool ObExprCalcTool::is_contains_null_params(ObSEArray<ObObj, 4> &param_result)
{
  int64_t len = param_result.count();
  bool contains_null = false;
  for (int i = 0; i < len; i++) {
    if (param_result.at(i).is_null()) {
      contains_null = true;
      break;
    }
  }
  return contains_null;
}
