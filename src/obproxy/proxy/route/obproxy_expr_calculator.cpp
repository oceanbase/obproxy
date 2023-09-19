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
#include "lib/rowid/ob_urowid.h"
#include "obproxy/utils/ob_proxy_utils.h"
#include "share/part/ob_part_desc.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/timezone/ob_timezone_info.h"
#include "proxy/route/ob_server_route.h"
#include "proxy/route/ob_route_diagnosis.h"


using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy;

int ObProxyExprCalculator::calculate_partition_id(common::ObArenaAllocator &allocator,
                                                  const ObString &req_sql,
                                                  const ObSqlParseResult &parse_result,
                                                  ObProxyMysqlRequest &client_request,
                                                  ObClientSessionInfo &client_info,
                                                  ObServerRoute &route,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id,
                                                  ObRouteDiagnosis *rd)
{
  int ret = OB_SUCCESS;
  ObString part_name = parse_result.get_part_name();
  bool old_is_oracle_mode = lib::is_oracle_mode();
  lib::set_oracle_mode(client_info.is_oracle_mode());
  int64_t part_idx = OB_INVALID_INDEX;
  int64_t sub_part_idx = OB_INVALID_INDEX;
  if (!part_name.empty()) {
    if (OB_FAIL(part_info.get_part_mgr().get_part_with_part_name(part_name, partition_id, part_info, route, *this))) {
      LOG_WARN("fail to get part id with part name", K(part_name), K(ret));
    }
  }
  if (OB_INVALID_INDEX == partition_id && parse_result.has_simple_route_info()) {
    if (OB_FAIL(calc_part_id_with_simple_route_info(allocator, parse_result, client_info,
                                                    route, part_info, partition_id,
                                                    part_idx, sub_part_idx))) {
      LOG_WARN("fail to calc part id with simple part info, will do calc in normal path", K(ret));
    }
  }
  if (OB_INVALID_INDEX == partition_id) {
    ObExprParseResult expr_parse_result;
    expr_parse_result.is_oracle_mode_ = client_info.is_oracle_mode();
    ObExprResolverResult resolve_result;
    const common::ObString &print_sql = ObProxyMysqlRequest::get_print_sql(req_sql);
    ObPsIdEntry *ps_id_entry = NULL;
    ObTextPsEntry *text_ps_entry = NULL;
    ObTextPsNameEntry* text_ps_name_entry = NULL;
    ObMySQLCmd cmd = client_request.get_packet_meta().cmd_;

    if (OB_MYSQL_COM_STMT_EXECUTE == cmd || OB_MYSQL_COM_STMT_SEND_LONG_DATA == cmd) {
      // parse execute param value for OB_MYSQL_COM_STMT_EXECUTE
      // try to get param types from OB_MYSQL_COM_STMT_EXECUTE while handling OB_MYSQL_COM_STMT_SEND_LONG_DATA
      ps_id_entry = client_info.get_ps_id_entry();
      if (OB_ISNULL(ps_id_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client ps id entry is null", K(ret));
      }
    } else if (parse_result.is_text_ps_execute_stmt()) {
      if (OB_ISNULL(text_ps_name_entry = client_info.get_text_ps_name_entry())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client text ps name entry is null", K(ret));
      } else if (OB_ISNULL(text_ps_entry = text_ps_name_entry->text_ps_entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client text ps entry is null", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(do_expr_parse(req_sql, parse_result, part_info, allocator, expr_parse_result,
                                static_cast<ObCollationType>(client_info.get_collation_connection())))) {
        LOG_DEBUG("fail to do expr parse", K(print_sql), K(part_info), "expr_parse_result",
                 ObExprParseResultPrintWrapper(expr_parse_result));
      } else if (OB_FAIL(do_expr_resolve(expr_parse_result, client_request, &client_info, ps_id_entry,
                                         text_ps_entry, part_info, allocator, resolve_result,
                                         parse_result, partition_id, rd))) {
        LOG_DEBUG("fail to do expr resolve", K(print_sql), "expr_parse_result",
                 ObExprParseResultPrintWrapper(expr_parse_result),
                 K(part_info), KPC(ps_id_entry), KPC(text_ps_entry), K(resolve_result));
      } else if (partition_id == OB_INVALID_INDEX) {
        if (OB_FAIL(do_partition_id_calc(resolve_result, client_info, route, part_info,
                                         parse_result, allocator, partition_id,
                                         part_idx, sub_part_idx))) {
          if (COM_STMT_PREPARE != cmd) {
            LOG_DEBUG("fail to do expr resolve", K(print_sql), K(resolve_result), K(part_info));
          }
        }
      } else {
        LOG_DEBUG("succ to get partition id(tabletid) from rowid", K(partition_id));
      }
    }

    if ((OB_FAIL(ret) || partition_id == OB_INVALID_INDEX)
        && !get_global_proxy_config().enable_primary_zone
        && !get_global_proxy_config().enable_cached_server) {
      // if proxy primary zone route optimization disabled, use random part id optimization
      int64_t tmp_first_part_id = OB_INVALID_INDEX;
      int64_t tmp_sub_part_id = OB_INVALID_INDEX;
      if (OB_FAIL(calc_part_id_by_random_choose_from_exist(part_info,
                                                           tmp_first_part_id,
                                                           tmp_sub_part_id,
                                                           partition_id))) {
        LOG_WARN("fail to cal part id by random choose", K(tmp_first_part_id), K(tmp_sub_part_id), K(ret));
      } else {
        route.no_need_pl_update_ = true;
        LOG_DEBUG("succ to cal part id by random choose", K(tmp_first_part_id), K(tmp_sub_part_id), K(partition_id));
      }
    }
  }

  lib::set_oracle_mode(old_is_oracle_mode);
  ROUTE_DIAGNOSIS(rd,
                  PARTITION_ID_CALC_DONE,
                  partition_id_calc,
                  ret,
                  req_sql,
                  parse_result.get_part_name(),
                  part_idx,
                  sub_part_idx,
                  part_info.get_part_level());
  return ret;
}

int ObProxyExprCalculator::calc_part_id_with_simple_route_info(ObArenaAllocator &allocator,
                                                               const ObSqlParseResult &parse_result,
                                                               ObClientSessionInfo &client_info,
                                                               ObServerRoute &route,
                                                               ObProxyPartInfo &part_info,
                                                               int64_t &part_id,  
                                                               int64_t &part_idx,
                                                               int64_t &sub_part_idx)
{
  int ret = OB_SUCCESS;
  const ObProxySimpleRouteInfo &info = parse_result.route_info_;
  // we only calulate id with simple part info for first part table
  // and table name must be equal with parse result
  if (info.is_valid()
      && parse_result.get_table_name().case_compare(info.table_name_buf_) == 0
      && part_info.has_first_part()
      && !part_info.has_sub_part()) {
    ObExprResolverResult resolve_result;
    if (OB_FAIL(do_resolve_with_part_key(parse_result, allocator, resolve_result))) {
      LOG_WARN("fail to do_resolve_with_part_key", K(ret));
    } else if (OB_FAIL(do_partition_id_calc(resolve_result, client_info, route, part_info,
                                            parse_result, allocator, part_id,
                                            part_idx, sub_part_idx))) {
      LOG_INFO("fail to do_partition_id_calc", K(resolve_result), K(part_info));
    }
  }
  return ret;
}

int ObProxyExprCalculator::do_expr_parse(const common::ObString &req_sql,
                                         const ObSqlParseResult &parse_result,
                                         ObProxyPartInfo &part_info,
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
  // deep copy
  ObProxyPartKeyInfo &key_info = part_info.get_part_key_info();
  expr_result.part_key_info_.key_num_ = key_info.key_num_;
  for (int i = 0; i < key_info.key_num_; ++i) {
    expr_result.part_key_info_.part_keys_[i] = key_info.part_keys_[i];
  }

  if (OB_FAIL(expr_parser.parse_reqsql(req_sql,  parse_result.get_parsed_length(), expr_result,
                                       parse_result.get_stmt_type(), connection_collation))) {
    LOG_DEBUG("fail to do expr parse_reqsql", K(req_sql), K(ret));
  }
  return ret;
}

int ObProxyExprCalculator::do_resolve_with_part_key(const ObSqlParseResult &parse_result,
                                                    ObIAllocator &allocator,
                                                    ObExprResolverResult &resolve_result)
{
  int ret = OB_SUCCESS;
  const ObProxySimpleRouteInfo &info = parse_result.route_info_;
  ObExprResolver expr_resolver(allocator);
  ObObj *target_obj = NULL;
  void *tmp_buf = NULL;
  if (OB_ISNULL(tmp_buf = allocator.alloc(sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc new obj", K(ret));
  } else {
    target_obj = new (tmp_buf) ObObj();
    target_obj->set_varchar(info.part_key_buf_);
    target_obj->set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  }
  if (OB_SUCC(ret)) {
    resolve_result.ranges_[0].start_key_.assign(target_obj, 1);
    resolve_result.ranges_[0].end_key_.assign(target_obj, 1);
    resolve_result.ranges_[0].border_flag_.set_inclusive_start();
    resolve_result.ranges_[0].border_flag_.set_inclusive_end();
    LOG_DEBUG("succ to do resolve with part key", K(resolve_result.ranges_[0]));
  }
  return ret;
}

int ObProxyExprCalculator::do_expr_resolve(ObExprParseResult &parse_result,
                                           const ObProxyMysqlRequest &client_request,
                                           ObClientSessionInfo *client_info,
                                           ObPsIdEntry *ps_id_entry,
                                           ObTextPsEntry *text_ps_entry,
                                           ObProxyPartInfo &part_info,
                                           ObIAllocator &allocator,
                                           ObExprResolverResult &resolve_result,
                                           const ObSqlParseResult &sql_parse_result,
                                           int64_t &partition_id,
                                           ObRouteDiagnosis *rd)
{
  int ret = OB_SUCCESS;
  ObExprResolverContext ctx;
  ctx.relation_info_ = &parse_result.relation_info_;
  ctx.part_info_ = &part_info;
  ctx.client_request_ = const_cast<ObProxyMysqlRequest *>(&client_request);
  ctx.ps_id_entry_ = ps_id_entry;
  ctx.text_ps_entry_ = text_ps_entry;
  ctx.client_info_ = client_info;
  ctx.parse_result_ = &parse_result;
  ctx.is_insert_stm_ = sql_parse_result.is_insert_stmt();
  ctx.route_diagnosis_ = rd;
  ObSqlParseResult &result = const_cast<ObSqlParseResult &>(sql_parse_result);
  ctx.sql_field_result_ = &result.get_sql_filed_result();
  ObExprResolver expr_resolver(allocator);

  if (parse_result.has_rowid_) {
    if (OB_FAIL(calc_partition_id_using_rowid(ctx, resolve_result, allocator, partition_id))) {
      LOG_DEBUG("calc partition id using rowid failed", K(ret));
    }
  } else if (OB_FAIL(expr_resolver.resolve(ctx, resolve_result))) {
    LOG_DEBUG("fail to do expr resolve", K(ret));
  } else {
    LOG_DEBUG("succ to do expr resolve", K(resolve_result));
  }

  return ret;
}

int ObProxyExprCalculator::do_partition_id_calc(ObExprResolverResult &resolve_result,
                                                ObClientSessionInfo &session_info,
                                                ObServerRoute &route,
                                                ObProxyPartInfo &part_info,
                                                const ObSqlParseResult &parse_result,
                                                ObIAllocator &allocator,
                                                int64_t &partition_id,
                                                int64_t &part_idx,
                                                int64_t &sub_part_idx)
{
  int ret = OB_SUCCESS;
  ObProxyPartMgr &part_mgr = part_info.get_part_mgr();
  int64_t first_part_id = OB_INVALID_INDEX;
  int64_t sub_part_id = OB_INVALID_INDEX;
  int64_t tablet_id = -1;
  if (part_info.has_first_part()) {
    ObPartDescCtx ctx(&session_info, parse_result.is_insert_stmt(), part_info.get_cluster_version());
    ObSEArray<int64_t, 1> part_ids;
    ObSEArray<int64_t, 1> tablet_ids;
    if (OB_FAIL(part_mgr.get_first_part(resolve_result.ranges_[PARTITION_LEVEL_ONE - 1],
                                        allocator,
                                        part_ids,
                                        ctx,
                                        tablet_ids,
                                        part_idx))) {
      LOG_WARN("fail to get first part", K(ret));
    } else if (part_ids.count() >= 1) {
      first_part_id = part_ids[0];
    }
    if (OB_SUCC(ret) && tablet_ids.count() >= 1) {
      tablet_id = tablet_ids.at(0);
    }

    LOG_DEBUG("do partition id calc", K(first_part_id), K(tablet_id), K(part_info.has_sub_part()));

    if (OB_INVALID_INDEX != first_part_id && part_info.has_sub_part()) {
      ObPartDesc *sub_part_desc_ptr = NULL;
      ObSEArray<int64_t, 1> sub_part_ids;
      ObSEArray<int64_t, 1> tablet_ids;
      if (OB_FAIL(part_mgr.get_sub_part_desc_by_first_part_id(part_info.is_template_table(),
                                                              first_part_id,
                                                              sub_part_desc_ptr,
                                                              part_info.get_cluster_version()))) {
        LOG_WARN("fail to get sub part desc by first", K(ret));
      } else if (OB_FAIL(part_mgr.get_sub_part(resolve_result.ranges_[PARTITION_LEVEL_TWO - 1],
                                               allocator,
                                               sub_part_desc_ptr,
                                               sub_part_ids,
                                               ctx,
                                               tablet_ids,
                                               sub_part_idx))) {
        LOG_WARN("fail to get sub part", K(ret));
      } else if (sub_part_ids.count() >= 1) {
        sub_part_id = sub_part_ids[0];
      }

      if (OB_SUCC(ret) && tablet_ids.count() >= 1) {
        tablet_id = tablet_ids[0];
      }
    }

    LOG_DEBUG("do partition id calc", K(sub_part_id), K(tablet_id), K(part_info.has_sub_part()));

    if (OB_DATA_OUT_OF_RANGE == ret) {
      first_part_id = 0;
      sub_part_id = 0;
      LOG_WARN("will route to p0sp0 since data out of range", K(ret));
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)
        && (tablet_id != -1 || (first_part_id != OB_INVALID_INDEX && (!part_info.has_sub_part() || sub_part_id != OB_INVALID_INDEX)))) {
      if (tablet_id == -1) {
        partition_id = generate_phy_part_id(first_part_id, sub_part_id, part_info.get_part_level());
      } else {
        partition_id = tablet_id;
      }
      LOG_DEBUG("succ to get part id", K(first_part_id), K(sub_part_id), K(partition_id));
    } else if (!get_global_proxy_config().enable_primary_zone
               && !get_global_proxy_config().enable_cached_server) {
      // if proxy primary zone route optimization disabled, use random part id optimization
      if (OB_FAIL(calc_part_id_by_random_choose_from_exist(part_info, first_part_id, sub_part_id, partition_id))) {
        LOG_WARN("fail to get part id at last", K(first_part_id), K(sub_part_id), K(ret));
      } else {
        // get part id by random, no need update pl
        route.no_need_pl_update_ = true;
        LOG_DEBUG("succ to get part id by random", K(first_part_id), K(sub_part_id), K(partition_id));
      }
    } else {
      // nothing
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not a valid partition table", K(part_info.get_part_level()), K(ret));
  }

  return ret;
}

int ObProxyExprCalculator::calc_part_id_by_random_choose_from_exist(ObProxyPartInfo &part_info,
                                                                    int64_t &first_part_id,
                                                                    int64_t &sub_part_id,
                                                                    int64_t &phy_part_id)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = -1;

	ObProxyPartMgr &part_mgr = part_info.get_part_mgr();
  if (part_info.has_first_part() && OB_INVALID_INDEX == first_part_id) {
    int64_t first_part_num = 0;
    if (OB_FAIL(part_info.get_part_mgr().get_first_part_num(first_part_num))) {
      LOG_WARN("fail to get first part num", K(ret));
    } else {
      int64_t rand_num = 0;
      if (OB_FAIL(ObRandomNumUtils::get_random_num(0, first_part_num - 1, rand_num))) {
        LOG_WARN("fail to get random num in first part", K(first_part_num), K(ret));
      } else {
        if (OB_FAIL(part_mgr.get_first_part_id_by_random(rand_num, first_part_id, tablet_id))) {
          LOG_WARN("failed to get first part id by random", K(rand_num), K(ret));
        } else {
          //nothing;
        }
      }
    }
  }
  LOG_DEBUG("choose partition id from exist", K(first_part_id), K(sub_part_id));

  if (OB_SUCC(ret) && part_info.has_sub_part() && OB_INVALID_INDEX == sub_part_id) {
    int64_t sub_part_num = 0;
    if (OB_FAIL(part_mgr.get_sub_part_num_by_first_part_id(part_info, first_part_id, sub_part_num))) {
      LOG_WARN("fail to get sub part num in random schedule", K(ret));
    } else {
      int64_t sub_rand_num = 0;
      if (OB_FAIL(ObRandomNumUtils::get_random_num(0, sub_part_num - 1, sub_rand_num))) {
        LOG_WARN("fail to get random num in sub part", K(sub_part_num), K(ret));
      } else {
        ObSEArray<int64_t, 1> part_ids;
        ObSEArray<int64_t, 1> tablet_ids;
        ObPartDesc *sub_part_desc_ptr = NULL;
        if (OB_FAIL(part_mgr.get_sub_part_desc_by_first_part_id(part_info.is_template_table(),
                                                                first_part_id,
                                                                sub_part_desc_ptr,
                                                                part_info.get_cluster_version()))) {
          LOG_WARN("fail to get sub part desc by first part id", K(first_part_id), K(ret));
        } else if (OB_FAIL(part_mgr.get_sub_part_by_random(sub_rand_num, sub_part_desc_ptr, part_ids, tablet_ids))) {
          LOG_WARN("fail to get sub part id by random", K(ret));
        } else {
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
    LOG_WARN("fail to cal part id by random choose from exist", K(ret));
  }

  return ret;
}

int ObProxyExprCalculator::calc_partition_id_using_rowid(ObExprResolverContext &ctx,
                                                         ObExprResolverResult &resolve_result,
                                                         common::ObIAllocator &allocator,
                                                         int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  const ObProxyRelationInfo *relation_info = ctx.relation_info_;

  if (OB_ISNULL(relation_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ctx relation info", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_info->relation_num_; ++i) {
      if (OB_ISNULL(relation_info->relations_[i])) {
        LOG_INFO("invalid relation, continue.", K(i));
      } else {
        ObProxyRelationExpr *relation = relation_info->relations_[i];
        if (relation->type_ == F_COMP_EQ
            && relation->left_value_ != NULL
            && relation->left_value_->column_node_ != NULL
            && relation->right_value_ != NULL
            && relation->right_value_->head_ != NULL
            && is_equal_to_rowid(&relation->left_value_->column_node_->column_name_)) {
          if (OB_FAIL(calc_partition_id_with_rowid(relation, ctx, allocator, resolve_result, partition_id) )) {
            LOG_INFO("fail to calc partition id with rowid", K(ret));
          } else {
            LOG_DEBUG("succ to calc partition id with rowid", K(partition_id));
          }
        }
      }
    } // for
  }

  return ret;
}

int ObProxyExprCalculator::calc_partition_id_with_rowid(ObProxyRelationExpr *relation,
                                                        ObExprResolverContext &ctx,
                                                        common::ObIAllocator &allocator,
                                                        ObExprResolverResult &resolve_result,
                                                        int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  ObObj *target_obj = NULL;
  void *buf = NULL;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else if (OB_ISNULL(target_obj = new (buf) ObObj())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new mem", K(ret));
  } else {
    ObExprResolver expr_resolver(allocator);
    if (OB_FAIL(expr_resolver.resolve_token_list(relation, ctx.part_info_, ctx.client_request_, ctx.client_info_,
                                                 ctx.ps_id_entry_, ctx.text_ps_entry_,
                                                 target_obj, ctx.sql_field_result_, true))) {
      LOG_INFO("fail to resolve token list with rowid", K(ret));
    } else {
      if (!target_obj->is_varchar()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_INFO("expected obj type after resolved from execute", K(ret), K(target_obj->get_type()));
      } else {
        ObString obj_str = target_obj->get_varchar();
        if (OB_FAIL(calc_partition_id_with_rowid_str(obj_str.ptr(), obj_str.length(), allocator,
                                                     resolve_result, *ctx.part_info_, partition_id))) {
          LOG_INFO("fail to calc partition id with rowid str within execute", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObProxyExprCalculator::calc_partition_id_with_rowid_str(const char *str,
                                                            const int64_t str_len,
                                                            common::ObIAllocator &allocator,
                                                            ObExprResolverResult &resolve_result,
                                                            ObProxyPartInfo &part_info,
                                                            int64_t &partition_id)
{
  int ret = OB_SUCCESS;

  ObURowIDData rowid_data;
  if (OB_FAIL(ObURowIDData::decode2urowid(str, str_len, allocator, rowid_data))) {
    LOG_WARN("decode2urowid failed", K(ret));
  } else if (OB_FAIL(rowid_data.get_obobj_or_partition_id_from_decoded(part_info, resolve_result, partition_id, allocator))) {
    LOG_WARN("fail to get obobj or partition id by rowid data", K(ret));
  } else {
    // nothing
  }

  return ret;
}

int ObExprCalcTool::build_dtc_params_with_tz_info(ObClientSessionInfo *session_info,
                                                  ObObjType obj_type,
                                                  ObTimeZoneInfo &tz_info,
                                                  ObDataTypeCastParams &dtc_params)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_tz_info(session_info, obj_type, tz_info))) {
    LOG_WARN("fail to build tz info", K(ret));
  } else if (OB_FAIL(build_dtc_params(session_info, obj_type, dtc_params))) {
    LOG_WARN("fail to build dtc params", K(ret));
  } else {
    dtc_params.tz_info_ = &tz_info;
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
  if (ObTimestampLTZType == obj_type || ObTimestampTZType == obj_type) {
    if (OB_FAIL(build_tz_info_for_all_type(session_info, tz_info))) {
      LOG_WARN("fail to build time zone info with session", K(ret));
    }
  }
  return ret;
}

int ObExprCalcTool::build_tz_info_for_all_type(ObClientSessionInfo *session_info,
                                               ObTimeZoneInfo &tz_info)
{
  int ret = OB_SUCCESS;
  ObObj value_obj;
  ObString sys_key_name = ObString::make_string(oceanbase::sql::OB_SV_TIME_ZONE);

  if (OB_FAIL(session_info->get_sys_variable_value(sys_key_name, value_obj))) {
    LOG_WARN("fail to get sys var from session", K(ret), K(sys_key_name));
  } else {
    ObString value_str = value_obj.get_string();
    if (OB_FAIL(tz_info.set_timezone(value_str))) {
      LOG_WARN("fail to set time zone for tz_info", K(ret), K(value_str));
    } else {
      LOG_DEBUG("succ to set time zone for tz_info", K(value_str));
    }
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
        LOG_WARN("fail to get sys var from session, use standard nls format", K(sub_ret), K(sys_key_name));
      } else {
        ObString value_str = value_obj.get_string();
        if (OB_FAIL(dtc_params.set_nls_format_by_type(obj_type, value_str))) {
          LOG_WARN("fail to set nls format by type", K(ret), K(obj_type), K(value_str));
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
    LOG_WARN("fail to build dtc params due to null session", K(ret));
  }

  return ret;
}

