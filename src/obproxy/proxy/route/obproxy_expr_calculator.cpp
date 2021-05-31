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

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::obmysql;
using namespace oceanbase::obproxy::opsql;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::proxy;

int ObProxyExprCalculator::calculate_partition_id(common::ObArenaAllocator &allocator,
                                                  const ObString &req_sql,
                                                  const ObSqlParseResult &parse_result,
                                                  ObProxyMysqlRequest &client_request,
                                                  ObClientSessionInfo &client_info,
                                                  ObProxyPartInfo &part_info,
                                                  int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  ObString part_name = parse_result.get_part_name();
  if (!part_name.empty()) {
    if (OB_FAIL(part_info.get_part_mgr().get_part_with_part_name(part_name, partition_id))) {
      LOG_WARN("fail to get part id with part name", K(part_name), K(ret));
    }
  }
  if (OB_INVALID_INDEX == partition_id && parse_result.has_simple_route_info()) {
    if (OB_FAIL(calc_part_id_with_simple_route_info(allocator, parse_result, part_info, partition_id))) {
      LOG_WARN("fail to calc part id with simple part info, will do calc in normal path", K(ret));
    }
  }
  if (OB_INVALID_INDEX == partition_id) {
    ObExprParseResult expr_parse_result;
    expr_parse_result.is_oracle_mode_ = client_info.is_oracle_mode();
    ObExprResolverResult resolve_result;
    const common::ObString &print_sql = ObProxyMysqlRequest::get_print_sql(req_sql);
    ObPsEntry *ps_entry = NULL;
    ObTextPsEntry *text_ps_entry = NULL;
    if (OB_MYSQL_COM_STMT_EXECUTE == client_request.get_packet_meta().cmd_) {
      // parse execute param value
      if (OB_ISNULL(ps_entry = client_info.get_ps_entry())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client ps entry is null", K(ret));
      }
    } else if (parse_result.is_text_ps_execute_stmt()) {
      if (OB_ISNULL(text_ps_entry = client_info.get_text_ps_entry())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("client text ps entry is null", K(ret));
      }
    }

    if (FAILEDx(do_expr_parse(req_sql, parse_result, part_info, allocator, expr_parse_result))) {
      LOG_INFO("fail to do expr parse", K(print_sql),
               K(part_info), "expr_parse_result", ObExprParseResultPrintWrapper(expr_parse_result));
    } else if (OB_FAIL(do_expr_resolve(expr_parse_result, client_request, &client_info, ps_entry,
                                       text_ps_entry, part_info, allocator, resolve_result))) {
      LOG_INFO("fail to do expr resolve", K(print_sql),
               "expr_parse_result", ObExprParseResultPrintWrapper(expr_parse_result),
               K(part_info), KPC(ps_entry), KPC(text_ps_entry), K(resolve_result));
    } else if (OB_FAIL(do_partition_id_calc(resolve_result, part_info, allocator, partition_id))) {
      if (OB_MYSQL_COM_STMT_PREPARE != client_request.get_packet_meta().cmd_) {
        LOG_INFO("fail to do expr resolve", K(print_sql), K(resolve_result), K(part_info));
      }
    } else {
    // do nothing here
    }
  }

  return ret;
}

int ObProxyExprCalculator::calc_part_id_with_simple_route_info(ObArenaAllocator &allocator,
                                                               const ObSqlParseResult &parse_result,
                                                               ObProxyPartInfo &part_info,
                                                               int64_t &part_id)
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
    } else if (OB_FAIL(do_partition_id_calc(resolve_result, part_info, allocator, part_id))) {
      LOG_INFO("fail to do_partition_id_calc", K(resolve_result), K(part_info));
    }
  }
  return ret;
}

int ObProxyExprCalculator::do_expr_parse(const common::ObString &req_sql,
                                         const ObSqlParseResult &parse_result,
                                         ObProxyPartInfo &part_info,
                                         ObIAllocator &allocator,
                                         ObExprParseResult &expr_result)
{
  int ret = OB_SUCCESS;

  // do parse
  ObExprParseMode parse_mode = INVLIAD_PARSE_MODE;
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

  expr_result.target_mask_ = 0;
  if (PARTITION_LEVEL_ONE == part_info.get_part_level()) {
    expr_result.target_mask_ = FIRST_PART_MASK;
  } else if (PARTITION_LEVEL_TWO == part_info.get_part_level()) {
    expr_result.target_mask_ = BOTH_PART_MASK;
  } else {
    // do nothing
  }
  if (OB_FAIL(expr_parser.parse_reqsql(req_sql,  parse_result.get_parsed_length(), expr_result, parse_result.get_stmt_type()))) {
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
                                           ObPsEntry *ps_entry,
                                           ObTextPsEntry *text_ps_entry,
                                           ObProxyPartInfo &part_info,
                                           ObIAllocator &allocator,
                                           ObExprResolverResult &resolve_result)
{
  int ret = OB_SUCCESS;
  ObExprResolverContext ctx;
  ctx.relation_info_ = &parse_result.relation_info_;
  ctx.part_info_ = &part_info;
  ctx.client_request_ = const_cast<ObProxyMysqlRequest *>(&client_request);
  ctx.ps_entry_ = ps_entry;
  ctx.text_ps_entry_ = text_ps_entry;
  ctx.client_info_ = client_info;

  ObExprResolver expr_resolver(allocator);

  if (parse_result.has_rowid_) {
    if (OB_FAIL(calc_partition_id_using_rowid(parse_result, part_info, resolve_result, allocator))) {
      LOG_WARN("calc partition id using rowid failed", K(ret));
    }
  } else if (OB_FAIL(expr_resolver.resolve(ctx, resolve_result))) {
    LOG_DEBUG("fail to do expr resolve", K(ret));
  } else {
    LOG_DEBUG("succ to do expr resolve", K(resolve_result));
  }

  return ret;
}

int ObProxyExprCalculator::do_partition_id_calc(ObExprResolverResult &resolve_result,
                                                ObProxyPartInfo &part_info,
                                                ObIAllocator &allocator,
                                                int64_t &partition_id)
{
  int ret = OB_SUCCESS;
  ObProxyPartMgr part_mgr = part_info.get_part_mgr();
  int64_t first_part_id = OB_INVALID_INDEX;
  int64_t sub_part_id = OB_INVALID_INDEX;
  if (part_info.has_first_part()) {
    ObSEArray<int64_t, 1> part_ids;
    if (OB_FAIL(part_mgr.get_first_part(resolve_result.ranges_[PARTITION_LEVEL_ONE - 1],
                                        allocator,
                                        part_ids))) {
      LOG_DEBUG("fail to get part", K(ret));
    } else if (part_ids.count() >= 1) {
      first_part_id = part_ids[0];
    } else {
      // do nothing
    }

    ObSEArray<int64_t, 1> sub_part_ids;
    if (OB_INVALID_INDEX != first_part_id && part_info.has_sub_part()) {
      if (OB_FAIL(part_mgr.get_sub_part(part_info.is_template_table(),
                                        first_part_id,
                                        resolve_result.ranges_[PARTITION_LEVEL_TWO - 1],
                                        allocator,
                                        sub_part_ids))) {
        LOG_DEBUG("fail to get sub part", K(ret));
      } else if (sub_part_ids.count() >= 1) {
        sub_part_id = sub_part_ids[0];
      }
    }

    if (OB_SUCC(ret)) {
      partition_id = generate_phy_part_id(first_part_id, sub_part_id, part_info.get_part_level());
      LOG_DEBUG("succ to get partition_id", K(first_part_id), K(sub_part_id), K(partition_id));
    } else {
      LOG_DEBUG("fail to get partition_id", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not a valid partition table", K(part_info.get_part_level()), K(ret));
  }

  return ret;
}

int ObProxyExprCalculator::calc_partition_id_using_rowid(const ObExprParseResult &parse_result,
                                                         ObProxyPartInfo &part_info,
                                                         ObExprResolverResult &resolve_result,
                                                         common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObProxyParseString &rowid_str = parse_result.rowid_str_;
  ObURowIDData rowid_data;
  ObArray<ObObj> pk_vals;
  if (OB_FAIL(ObURowIDData::decode2urowid(rowid_str.str_, rowid_str.str_len_, allocator, rowid_data))) {
    LOG_WARN("decode2urowid failed", K(ret));
  } else if (OB_FAIL(rowid_data.get_pk_vals(pk_vals))) {
    LOG_WARN("get pk vals failed", K(ret));
  } else {
    ObProxyPartKeyInfo &key_info = part_info.get_part_key_info();
    bool set_level_one_obj = false;
    bool set_level_two_obj = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < key_info.key_num_; i++) {
      ObProxyPartKey &part_key = key_info.part_keys_[i];
      if ((PART_KEY_LEVEL_ONE == part_key.level_
          || PART_KEY_LEVEL_BOTH == part_key.level_)
          && !set_level_one_obj) {
        if (part_key.idx_in_rowid_ < pk_vals.count()) {
          ObObj &obj = pk_vals.at(part_key.idx_in_rowid_);
          // handle rowid = xxx
          resolve_result.ranges_[0].start_key_.assign(&obj, 1);
          resolve_result.ranges_[0].end_key_.assign(&obj, 1);
          resolve_result.ranges_[0].border_flag_.set_inclusive_start();
          resolve_result.ranges_[0].border_flag_.set_inclusive_end();
          set_level_one_obj = true;
          LOG_DEBUG("get level one partition val from rowid", K(obj));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("calc partition id using rowid failed", K(part_key.idx_in_rowid_), K(pk_vals.count()));
        }
      }
      if (PART_KEY_LEVEL_TWO == part_key.level_
          && !set_level_two_obj) {
        if (part_key.idx_in_rowid_ < pk_vals.count()) {
          ObObj &obj = pk_vals.at(part_key.idx_in_rowid_);
          // handle rowid = xxx
          resolve_result.ranges_[1].start_key_.assign(&obj, 1);
          resolve_result.ranges_[1].end_key_.assign(&obj, 1);
          resolve_result.ranges_[1].border_flag_.set_inclusive_start();
          resolve_result.ranges_[1].border_flag_.set_inclusive_end();
          set_level_two_obj = true;
          LOG_DEBUG("get level two partition val from rowid", K(obj));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("calc partition id using rowid failed", K(part_key.idx_in_rowid_), K(pk_vals.count()));
        }
      }
    }
  }

  return ret;
}
