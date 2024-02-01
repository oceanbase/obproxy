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
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_sql_parser.h"
#include "obutils/ob_proxy_stmt.h"
#include "opsql/parser/ob_proxy_parser.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "proxy/shard/obproxy_shard_utils.h"
#include "obproxy/utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

void ObSqlParseResult::clear_proxy_stmt()
{
  if (NULL != proxy_stmt_) {
    //op_free(proxy_stmt_);
    proxy_stmt_->~ObProxyStmt();
    proxy_stmt_ = NULL;
  }
}

int ObSqlParseResult::set_real_table_name(const char *table_name, int64_t len)
{
  int ret = OB_SUCCESS;
  ObString tmp_str(len, table_name);
  if (OB_UNLIKELY(tmp_str.empty()) || OB_UNLIKELY(len > OB_MAX_TABLE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(table_name), K(len));
  } else if (table_name_ != tmp_str) {
    // if the table name is getted from parser result or real physic table name returned by observer
    //   1. if mysql mode, case-insensitive
    //   2. if oracle mode, cmp directly
    MEMCPY(dml_buf_.table_name_buf_, table_name, len);
    table_name_.assign_ptr(dml_buf_.table_name_buf_, static_cast<int32_t>(len));
  }
  return ret;
}

int ObSqlParseResult::set_db_name(const ObProxyParseString &database_name,
                                         const bool use_lower_case_name/*false*/,
                                         const bool drop_origin_db_table_name /*false*/)
{
  int ret = OB_SUCCESS;

  if (NULL != database_name.str_ && 0 != database_name.str_len_) {
    if (OB_UNLIKELY(database_name.str_len_ > OB_MAX_DATABASE_NAME_LENGTH)
        || OB_UNLIKELY(database_name.str_len_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid argument", K(ret));
    } else {
      MEMCPY(dml_buf_.database_name_buf_, database_name.str_, database_name.str_len_);

      if (use_lower_case_name) {
        string_to_lower_case(dml_buf_.database_name_buf_, database_name.str_len_);
      }
      database_name_.assign_ptr(dml_buf_.database_name_buf_, database_name.str_len_);
      database_name_quote_ = database_name.quote_type_;

      if (!drop_origin_db_table_name) {
        MEMCPY(origin_dml_buf_.database_name_buf_, database_name.str_, database_name.str_len_);
        origin_database_name_.assign_ptr(origin_dml_buf_.database_name_buf_, database_name.str_len_);
      }
    }
  }

  return ret;
}

inline int ObSqlParseResult::set_db_table_name(const ObProxyParseString &database_name,
                                               const ObProxyParseString &package_name,
                                               const ObProxyParseString &table_name,
                                               const ObProxyParseString &alias_name,
                                               const bool use_lower_case_name/*false*/,
                                               const bool drop_origin_db_table_name /*false*/)
{
  int ret = OB_SUCCESS;
  if (NULL != table_name.str_ && 0 != table_name.str_len_) {
    if (OB_UNLIKELY(table_name.str_len_ > OB_MAX_TABLE_NAME_LENGTH)
        || OB_UNLIKELY(table_name.str_len_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid argument", K(table_name.str_len_), K(ret));
    } else {
      MEMCPY(dml_buf_.table_name_buf_, table_name.str_, table_name.str_len_);
      if (use_lower_case_name) {
        string_to_lower_case(dml_buf_.table_name_buf_, table_name.str_len_);
      }
      table_name_.assign_ptr(dml_buf_.table_name_buf_, table_name.str_len_);
      table_name_quote_ = table_name.quote_type_;
      if (OB_LIKELY(!drop_origin_db_table_name)) {
        MEMCPY(origin_dml_buf_.table_name_buf_, table_name.str_, table_name.str_len_);
        origin_table_name_.assign_ptr(origin_dml_buf_.table_name_buf_, table_name.str_len_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    // assign package name when table name is valid
    if (OB_UNLIKELY(NULL != package_name.str_ && 0 != package_name.str_len_)) {
      if (OB_UNLIKELY(package_name.str_len_ > OB_MAX_TABLE_NAME_LENGTH)
          || OB_UNLIKELY(package_name.str_len_ < 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid argument", K(ret));
      } else {
        MEMCPY(dml_buf_.package_name_buf_, package_name.str_, package_name.str_len_);
        if (use_lower_case_name) {
          string_to_lower_case(dml_buf_.package_name_buf_, package_name.str_len_);
        }
        package_name_.assign_ptr(dml_buf_.package_name_buf_, package_name.str_len_);
        package_name_quote_ = package_name.quote_type_;
      }
    }
  }

  // assign database name when table name is valid
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_db_name(database_name, use_lower_case_name, drop_origin_db_table_name))) {
      LOG_WDIAG("fail to set db name", K(database_name.str_len_), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL != alias_name.str_ && 0 != alias_name.str_len_)) {
      if (OB_UNLIKELY(alias_name.str_len_ > OB_MAX_TABLE_NAME_LENGTH)
          || OB_UNLIKELY(alias_name.str_len_ < 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid argument", K(ret));
      } else {
        MEMCPY(dml_buf_.alias_name_buf_, alias_name.str_, alias_name.str_len_);
        if (use_lower_case_name) {
          string_to_lower_case(dml_buf_.alias_name_buf_, alias_name.str_len_);
        }
        alias_name_.assign_ptr(dml_buf_.alias_name_buf_, alias_name.str_len_);
        alias_name_quote_ = alias_name.quote_type_;
      }
    }
  }
  return ret;
}

inline int ObSqlParseResult::set_part_name(const ObProxyParseString &part_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_name.str_len_ > OB_MAX_PARTITION_NAME_LENGTH)
             || OB_UNLIKELY(part_name.str_len_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(part_name.str_len_), K(ret));
  } else if (NULL != part_name.str_ && 0 != part_name.str_len_) {
    MEMCPY(part_name_buf_.part_name_buf_, part_name.str_, part_name.str_len_);
    part_name_.assign_ptr(part_name_buf_.part_name_buf_, part_name.str_len_);
  }
  return ret;
}

inline int ObSqlParseResult::set_col_name(const ObProxyParseString &col_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_internal_select())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("not a internal select request should not set col name", K(ret));
  } else if (OB_UNLIKELY(col_name.str_len_ > OB_MAX_COLUMN_NAME_LENGTH)
             || OB_UNLIKELY(col_name.str_len_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(col_name.str_len_), K(ret));
  } else if (NULL != col_name.str_ && 0 != col_name.str_len_) {
    MEMCPY(internal_select_buf_.col_name_buf_, col_name.str_, col_name.str_len_);
    col_name_.assign_ptr(internal_select_buf_.col_name_buf_, col_name.str_len_);
    col_name_quote_ = col_name.quote_type_;
  }
  return ret;
}

inline int ObSqlParseResult::set_call_prarms(const ObProxyCallParseInfo &call_parse_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_call_stmt() && !is_text_ps_call_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("not a call stmt should not set col name", K(ret));
  } else if (OB_UNLIKELY(call_parse_info.node_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(call_parse_info.node_count_), K(ret));
  } else if (call_parse_info.node_count_ >= 0) {
    call_info_.is_param_valid_ = true;
    call_info_.param_count_ = call_parse_info.node_count_;
    ObProxyCallParam* tmp_param = NULL;
    ObProxyCallParseNode *tmp_node = call_parse_info.head_;
    while(OB_SUCC(ret) && tmp_node) {
      tmp_param = NULL;
      if (OB_FAIL(ObProxyCallParam::alloc_call_param(tmp_param))) {
        LOG_WDIAG("fail to alloc call param", K(ret));
      } else {
        tmp_param->type_ = tmp_node->type_;
        if (CALL_TOKEN_INT_VAL == tmp_node->type_) {
          tmp_param->str_value_.set_integer(tmp_node->int_value_);
        } else if (CALL_TOKEN_PLACE_HOLDER == tmp_node->type_) {
          // for place holder, store its pos
          tmp_param->str_value_.set_integer(tmp_node->placeholder_idx_);
        } else {
          tmp_param->str_value_.set_value(tmp_node->str_value_.str_len_, tmp_node->str_value_.str_);
        } // end node_type
        if (OB_FAIL(call_info_.params_.push_back(tmp_param))) {
          LOG_WDIAG("fail to push back call param", K(tmp_param), K(ret));
        }

        if (OB_FAIL(ret) && NULL != tmp_param) {
          tmp_param->reset();
          tmp_param = NULL;
        }

        tmp_node = tmp_node->next_;
      }
    }
  }
  return ret;
}

inline int ObSqlParseResult::set_simple_route_info(const ObProxyParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  const ObProxySimpleRouteParseInfo &info = parse_result.simple_route_info_;
  if (NULL != info.table_name_.str_
      && info.table_name_.str_len_ > 0
      && info.table_name_.str_len_ <= OB_MAX_TABLE_NAME_LENGTH) {
    if (info.table_start_ptr_ > parse_result.start_pos_
        && info.table_start_ptr_ < info.table_name_.end_ptr_
        && info.table_name_.end_ptr_ < parse_result.end_pos_) {
      route_info_.table_offset_ = info.table_start_ptr_ - parse_result.start_pos_;
      route_info_.table_len_ = info.table_name_.end_ptr_ - info.table_start_ptr_;
      if (OBPROXY_QUOTE_T_INVALID != info.table_name_.quote_type_) {
        ++route_info_.table_len_;
      }
      MEMCPY(route_info_.table_name_buf_, info.table_name_.str_, info.table_name_.str_len_);
      route_info_.table_name_buf_[info.table_name_.str_len_] = '\0';
      if (NULL != info.part_key_.str_
          && info.part_key_.str_len_ > 0
          && info.part_key_.str_len_ <= OBPROXY_MAX_STRING_VALUE_LENGTH) {
        if (info.part_key_start_ptr_ > parse_result.start_pos_
            && info.part_key_start_ptr_ < info.part_key_.end_ptr_
            && info.part_key_.end_ptr_ < parse_result.end_pos_) {
          route_info_.part_key_offset_ = info.part_key_start_ptr_ - parse_result.start_pos_;
          route_info_.part_key_len_ = info.part_key_.end_ptr_ - info.part_key_start_ptr_;
          if (OBPROXY_QUOTE_T_INVALID != info.part_key_.quote_type_) {
            ++route_info_.part_key_len_;
          }
          MEMCPY(route_info_.part_key_buf_, info.part_key_.str_, info.part_key_.str_len_);
          route_info_.part_key_buf_[info.part_key_.str_len_] = '\0';
        }
      }
    }
  }
  if (OB_SUCC(ret) && route_info_.is_valid()) {
    has_simple_route_info_ = true;
  }
  return ret;
}

int ObSqlParseResult::set_dbmesh_route_info(const ObProxyParseResult &parse_result)
{
  int ret = OB_SUCCESS;
  const ObDbMeshRouteInfo &route_info = parse_result.dbmesh_route_info_;
  const ObDbpRouteInfo &dbp_route_info = parse_result.dbp_route_info_;

  ObString tmp_str;
  int64_t tmp_int = OBPROXY_MAX_DBMESH_ID;

  has_dbmesh_hint_ = false;
  // set dbmesh_route_info
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.table_name_str_.str_, route_info.table_name_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_UNLIKELY(tmp_str.length() > OB_MAX_TABLE_NAME_LENGTH)) {
      // do nothing, invalid table name, no need ret error
      LOG_WDIAG("invalid table name, string length is overflow", K(tmp_str));
    } else {
      has_dbmesh_hint_ = true;
      MEMCPY(dbmesh_route_info_.table_name_str_, tmp_str.ptr(), static_cast<int32_t>(tmp_str.length()));
      dbmesh_route_info_.table_name_.assign_ptr(dbmesh_route_info_.table_name_str_, static_cast<int32_t>(tmp_str.length()));
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.tb_idx_str_.str_, route_info.tb_idx_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (!dbmesh_route_info_.table_name_.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("table name and table id should not together at the same time", K(ret));
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WDIAG("fail to get int value for tb index", K(ret));
    } else {
      has_dbmesh_hint_ = true;
      dbmesh_route_info_.tb_idx_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.group_idx_str_.str_, route_info.group_idx_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WDIAG("fail to get int value for group index", K(ret));
    } else {
      has_dbmesh_hint_ = true;
      dbmesh_route_info_.group_idx_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.tnt_id_str_.str_, route_info.tnt_id_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_UNLIKELY(tmp_str.length() > OBPROXY_MAX_TNT_ID_LENGTH)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WDIAG("invalid tnt id, string length is overflow", K(tmp_str));
    } else {
      MEMCPY(dbmesh_route_info_.tnt_id_str_, tmp_str.ptr(), static_cast<int32_t>(tmp_str.length()));
      dbmesh_route_info_.tnt_id_.assign_ptr(dbmesh_route_info_.tnt_id_str_, static_cast<int32_t>(tmp_str.length()));
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.es_idx_str_.str_, route_info.es_idx_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WDIAG("fail to get int value for es index", K(ret));
    } else {
      has_dbmesh_hint_ = true;
      dbmesh_route_info_.es_idx_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.testload_str_.str_, route_info.testload_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WDIAG("fail to get int value for testload", K(ret));
    } else {
      has_dbmesh_hint_ = true;
      dbmesh_route_info_.testload_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.disaster_status_str_.str_, route_info.disaster_status_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_UNLIKELY(tmp_str.length() > OBPROXY_MAX_DISASTER_STATUS_LENGTH)) {
      // do nothing, invalid disaster status, no need ret error
      LOG_WDIAG("invalid disaster status, string length is overflow", K(tmp_str));
    } else {
      has_dbmesh_hint_ = true;
      MEMCPY(dbmesh_route_info_.disaster_status_str_, tmp_str.ptr(), static_cast<int32_t>(tmp_str.length()));
      dbmesh_route_info_.disaster_status_.assign_ptr(dbmesh_route_info_.disaster_status_str_, static_cast<int32_t>(tmp_str.length()));
    }
  }
  if (OB_SUCC(ret) && route_info.index_count_ > 0
      && route_info.index_count_ <= OBPROXY_MAX_HINT_INDEX_COUNT) {
    bool find_index_tb = false;
    for (int64_t i = 0; !find_index_tb && i < route_info.index_count_; ++i) {
      tmp_str.assign_ptr(route_info.index_tb_name_[i].str_, route_info.index_tb_name_[i].str_len_);
      if (tmp_str.case_compare(table_name_) == 0) {
        find_index_tb = true;
        dbmesh_route_info_.index_tb_pos_ = route_info.index_tb_name_[i].str_ - parse_result.start_pos_;
      }
    }
  }
  if (OB_SUCC(ret) && route_info.node_count_ > 0) {
    has_dbmesh_hint_ = true;
    ObShardColumnNode *tmp_node = route_info.head_;
    fileds_result_.field_num_ = 0;
    fileds_result_.fields_.reuse();
    SqlField* field = NULL;
    while(tmp_node) {
      field = NULL;
      tmp_str.assign_ptr(tmp_node->tb_name_.str_, tmp_node->tb_name_.str_len_);
      const ObString table_name = tmp_str.trim();
      if (is_dual_request_ || table_name.case_compare(table_name_) == 0) {
        tmp_str.assign_ptr(tmp_node->col_name_.str_, tmp_node->col_name_.str_len_);
        if (OB_FAIL(SqlField::alloc_sql_field(field))) {
          LOG_WDIAG("fail to alloc sql field", K(ret));
        } else {
          field->column_name_.set_value(tmp_str);
          SqlColumnValue column_value;
          if (DBMESH_TOKEN_STR_VAL == tmp_node->type_) {
            column_value.value_type_ = TOKEN_STR_VAL;
            tmp_str.assign_ptr(tmp_node->col_str_value_.str_, tmp_node->col_str_value_.str_len_);
            column_value.column_value_.set_value(tmp_str);
            if (OB_FAIL(field->column_values_.push_back(column_value))) {
              LOG_WDIAG("field push back column values failed", K(column_value), K(ret));
            }
          } else {
            LOG_INFO("unknown column value type", "token type",
                     get_obproxy_dbmesh_token_type(tmp_node->type_));
          }
          if (OB_SUCC(ret) && field->is_valid() && OB_SUCCESS == fileds_result_.fields_.push_back(field)) {
            ++fileds_result_.field_num_;
          } else if (NULL != field) {
            field->reset();
            field = NULL;
          }
        }
      }
      tmp_node = tmp_node->next_;
    }
  }

  if (OB_SUCC(ret) && !has_dbmesh_hint_) {
    //set dbp_route_info
    if (dbp_route_info.has_group_info_) {
      use_dbp_hint_ = true;
      dbp_route_info_.has_group_info_ = dbp_route_info.has_group_info_;
      tmp_str.assign_ptr(dbp_route_info.group_idx_str_.str_, dbp_route_info.group_idx_str_.str_len_);
      if (tmp_str.empty()) {
        //do nothing
      } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
        LOG_WDIAG("fail to get int value for group index", K(ret));
      } else {
        dbp_route_info_.group_idx_ = tmp_int;
      }
      if (OB_SUCC(ret)) {
        tmp_str.assign_ptr(dbp_route_info.table_name_.str_, dbp_route_info.table_name_.str_len_);
        if (tmp_str.empty()) {
          //do nothing
        } else if (OB_UNLIKELY(tmp_str.length() > OB_MAX_TABLE_NAME_LENGTH)) {
          // do nothing, invalid table name, no need ret error
          LOG_WDIAG("invalid table name, string length is overflow", K(tmp_str));
        } else {
          MEMCPY(dbp_route_info_.table_name_str_, tmp_str.ptr(), static_cast<int32_t>(tmp_str.length()));
          dbp_route_info_.table_name_.assign_ptr(dbp_route_info_.table_name_str_, static_cast<int32_t>(tmp_str.length()));
        }
      }
    }
    if (OB_SUCC(ret) && dbp_route_info.scan_all_) {
      use_dbp_hint_ = true;
      dbp_route_info_.scan_all_ = dbp_route_info.scan_all_;
    }

    if (OB_SUCC(ret) && dbp_route_info.sticky_session_) {
      use_dbp_hint_ = true;
      dbp_route_info_.sticky_session_ = dbp_route_info.sticky_session_;
    }

    if (OB_SUCC(ret) && dbp_route_info.has_shard_key_) {
      use_dbp_hint_ = true;
      fileds_result_.field_num_ = 0;
      fileds_result_.fields_.reuse();
      dbp_route_info_.has_shard_key_ = dbp_route_info.has_shard_key_;
      for (int i = 0; OB_SUCC(ret) && i < dbp_route_info.shard_key_count_;i++) {
        SqlField* field = NULL;
        SqlColumnValue column_value;
        if (OB_FAIL(SqlField::alloc_sql_field(field))) {
          LOG_WDIAG("fail to alloc sql field", K(ret));
        } else {
          field->column_name_.set_value(dbp_route_info.shard_key_infos_[i].left_str_.str_len_,
                  dbp_route_info.shard_key_infos_[i].left_str_.str_);
          column_value.column_value_.set_value(dbp_route_info.shard_key_infos_[i].right_str_.str_len_,
                                               dbp_route_info.shard_key_infos_[i].right_str_.str_);
          column_value.value_type_ = TOKEN_STR_VAL;
          if (OB_FAIL(field->column_values_.push_back(column_value))) {
            LOG_WDIAG("fail to push back to column_values", K(ret));
          } else if (field->is_valid() && OB_SUCC(fileds_result_.fields_.push_back(field))) {
            ++fileds_result_.field_num_;
          }

          if (OB_FAIL(ret) || !field->is_valid()) {
            field->reset();
            field = NULL;
          }
        }
      }
    }
    if (OB_SUCC(ret) && use_dbp_hint_) {
      if (dbp_route_info_.is_error_info()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("dbp_route_info is invalid", K(dbp_route_info_), K(ret));
      } else {
        LOG_DEBUG("dbp_route_info is valid", K(dbp_route_info_));
      }
    }
  }
  return ret;
}

int ObSqlParseResult::set_var_info(const ObProxyParseResult &parse_result)
{
  int ret = OB_SUCCESS;

  const ObProxySetParseInfo &set_parse_info = parse_result.set_parse_info_;
  if (set_parse_info.node_count_ > 0) {
    set_info_.node_count_ = 0;
    set_info_.var_nodes_.reuse();

    ObString tmp_str;
    ObProxySetVarNode *tmp_node = set_parse_info.head_;
    SetVarNode* var_node = NULL;
    while(OB_SUCC(ret) && tmp_node) {
      var_node = NULL;
      if (OB_FAIL(SetVarNode::alloc_var_node(var_node))) {
        LOG_WDIAG("fail to alloc var node", K(ret));
      } else {
        var_node->var_type_ = tmp_node->type_;
        tmp_str.assign_ptr(tmp_node->name_.str_, tmp_node->name_.str_len_);
        var_node->var_name_.set_value(tmp_str);

        var_node->value_type_ = tmp_node->value_type_;
        if (SET_VALUE_TYPE_INT == tmp_node->value_type_) {
          var_node->int_value_ = tmp_node->int_value_;
          //Floating point numbers will be converted to double type when saving
        } else if (SET_VALUE_TYPE_NUMBER == tmp_node->value_type_) {
          tmp_str.assign_ptr(tmp_node->str_value_.str_, tmp_node->str_value_.str_len_);
          var_node->str_value_.set_value(tmp_str);
        } else {
          // compatible observer:
          //  for user var: the var will be returned from observer by OK packet:
          //    1. if varchar is digital, observer return value not with '
          //    2. if varchar is string, observer return vaue with '
          //   so, varchar type, do not add ' when format SQL
          //  for sys var: varchar type will add ' on format SQL
          if (SET_VAR_USER == tmp_node->type_) {
            tmp_str.assign_ptr(tmp_node->str_value_.str_, tmp_node->str_value_.str_len_);
            if (OBPROXY_QUOTE_T_SINGLE == tmp_node->str_value_.quote_type_) {
              var_node->str_value_.set_value_with_quote(tmp_str, '\'');
            } else if (OBPROXY_QUOTE_T_DOUBLE == tmp_node->str_value_.quote_type_) {
              var_node->str_value_.set_value_with_quote(tmp_str, '"');
            }
          } else {
            tmp_str.assign_ptr(tmp_node->str_value_.str_, tmp_node->str_value_.str_len_);
            var_node->str_value_.set_value(tmp_str);
          }
        }

        if (OB_SUCCESS == set_info_.var_nodes_.push_back(var_node)) {
          ++set_info_.node_count_;
        } else {
          var_node->reset();
          var_node = NULL;
        }

        tmp_node = tmp_node->next_;
      }
    }
  }

  return ret;
}

int ObSqlParseResult::set_text_ps_info(ObProxyTextPsInfo& text_ps_info,
    const ObProxyTextPsParseInfo& parse_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_text_ps_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("not text ps stmt", K(ret));
  } else if (OB_UNLIKELY(parse_info.node_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(parse_info.node_count_), K(ret));
  } else if (parse_info.node_count_ > 0) {
    text_ps_info.is_param_valid_ = true;
    text_ps_info.param_count_ = parse_info.node_count_;
    ObProxyTextPsParam* tmp_param = NULL;
    ObProxyTextPsParseNode *tmp_node = parse_info.head_;
    while (OB_SUCC(ret) && OB_NOT_NULL(tmp_node)) {
      tmp_param = NULL;
      if (OB_FAIL(ObProxyTextPsParam::alloc_text_ps_param(tmp_node->str_value_.str_, tmp_node->str_value_.str_len_, tmp_param))) {
        LOG_WDIAG("fail to alloc text ps param", K(ret));
      } else {
        const ObString tmp_string(tmp_node->str_value_.str_len_, tmp_node->str_value_.str_);
        tmp_param->str_value_.set_value(tmp_string);
        if (OB_FAIL(text_ps_info.params_.push_back(tmp_param))) {
          tmp_param->reset();
          tmp_param = NULL;
          LOG_WDIAG("fail to push back text ps execute info", KPC(tmp_param), K(ret));
        }
      }
      tmp_node = tmp_node->next_;
    }
  }

  return ret;
}

int ObSqlParseResult::load_result(const ObProxyParseResult &parse_result,
                                  const bool use_lower_case_name/*false*/,
                                  const bool drop_origin_db_table_name /*false*/,
                                  const bool is_sharding_request /*false*/)
{
  int ret = OB_SUCCESS;

  // set basic variables
  is_dual_request_ = parse_result.is_dual_request_;
  has_found_rows_ = parse_result.has_found_rows_;
  has_row_count_  = parse_result.has_row_count_;
  has_explain_ = parse_result.has_explain_;
  has_explain_route_ = parse_result.has_explain_route_;
  has_shard_comment_ = parse_result.has_shard_comment_;
  has_last_insert_id_ = parse_result.has_last_insert_id_;
  hint_query_timeout_ = parse_result.query_timeout_;
  has_anonymous_block_ = parse_result.has_anonymous_block_;
  has_trace_log_hint_ = parse_result.has_trace_log_hint_;
  stmt_type_ = parse_result.stmt_type_;
  cmd_sub_type_ = parse_result.sub_stmt_type_;
  has_connection_id_ = parse_result.has_connection_id_;
  has_sys_context_ = parse_result.has_sys_context_;
  hint_consistency_level_ = static_cast<ObConsistencyLevel>(parse_result.read_consistency_type_);
  parsed_length_ = static_cast<int64_t>(parse_result.end_pos_ - parse_result.start_pos_);
  text_ps_inner_stmt_type_ = parse_result.text_ps_inner_stmt_type_;
  is_binlog_related_ = parse_result.is_binlog_related_;

  if (OB_UNLIKELY(is_sharding_request && NULL != parse_result.table_info_.table_name_.str_
                  && parse_result.table_info_.table_name_.str_len_ > 0)) {
    dbmesh_route_info_.tb_pos_ = parse_result.table_info_.table_name_.str_ - parse_result.start_pos_;
  }

  if (OB_UNLIKELY(is_sharding_request && NULL != parse_result.table_info_.database_name_.str_
                  && parse_result.table_info_.database_name_.str_len_ > 0)) {
    dbmesh_route_info_.db_pos_ = parse_result.table_info_.database_name_.str_ - parse_result.start_pos_;
  }

  if (OB_UNLIKELY(NULL != parse_result.trace_id_.str_
      && 0 < parse_result.trace_id_.str_len_
      && OB_MAX_OBPROXY_TRACE_ID_LENGTH > parse_result.trace_id_.str_len_)) {
    MEMCPY(trace_id_buf_, parse_result.trace_id_.str_, parse_result.trace_id_.str_len_);
    trace_id_.assign_ptr(trace_id_buf_, parse_result.trace_id_.str_len_);
  }

  if (OB_UNLIKELY(NULL != parse_result.rpc_id_.str_
      && 0 < parse_result.rpc_id_.str_len_
      && OB_MAX_OBPROXY_TRACE_ID_LENGTH > parse_result.rpc_id_.str_len_)) {
    MEMCPY(rpc_id_buf_, parse_result.rpc_id_.str_, parse_result.rpc_id_.str_len_);
    rpc_id_.assign_ptr(rpc_id_buf_, parse_result.rpc_id_.str_len_);
  }

  if (OB_UNLIKELY(NULL != parse_result.target_db_server_.str_
      && 0 < parse_result.target_db_server_.str_len_)) {
    if (OB_ISNULL(target_db_server_) && OB_ISNULL(target_db_server_ = op_alloc(ObTargetDbServer))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory for target db server", K(ret));
    } else if (OB_FAIL(target_db_server_->init(parse_result.target_db_server_.str_, parse_result.target_db_server_.str_len_))) {
      LOG_WDIAG("fail to init target db server from sql comment", K(ret));
    } else {
      LOG_DEBUG("succ to init target db server from sql comment", K(ret));
    }
  }

  // if is dml stmt, then set db/table name
  if (OB_LIKELY(is_dml_stmt() || is_call_stmt() || (is_text_ps_stmt() && is_text_ps_inner_dml_stmt())
      || is_show_stmt() || is_desc_table_stmt())) {
    if (OB_FAIL(set_db_table_name(parse_result.table_info_.database_name_,
                                  parse_result.table_info_.package_name_,
                                  parse_result.table_info_.table_name_,
                                  parse_result.table_info_.alias_name_,
                                  use_lower_case_name,
                                  drop_origin_db_table_name))) {
      LOG_WDIAG("failed to set db table name", K(use_lower_case_name), K(ret));
    } else if (OB_UNLIKELY(is_sharding_request && OB_FAIL(set_dbmesh_route_info(parse_result)))) {
      LOG_WDIAG("fail to set dbmesh route info", K(ret));
      if (is_sharding_request) {
        stmt_type_ = OBPROXY_T_INVALID;
      }
    } else if (OB_UNLIKELY(is_call_stmt() || is_text_ps_call_stmt())) {
      if (OB_FAIL(set_call_prarms(parse_result.call_parse_info_))) {
        LOG_WDIAG("failed to set_call_prarms", K(ret));
      }
    }
    if (OB_SUCC(ret) && parse_result.has_simple_route_info_) {
      if (OB_FAIL(set_simple_route_info(parse_result))) {
        LOG_WDIAG("failed to set_simple_route_info", K(ret));
      }
    }
    if (OB_SUCC(ret) && parse_result.part_name_.str_len_ > 0) {
      if (OB_FAIL(set_part_name(parse_result.part_name_))) {
        LOG_WDIAG("fail to set part name", K(ret));
      }
    }
  } else if (is_use_db_stmt()) {
    if (OB_FAIL(set_db_name(parse_result.table_info_.database_name_,
                            use_lower_case_name,
                            drop_origin_db_table_name))) {
      LOG_WDIAG("fail to set db name", K(ret));
    }
  } else if (is_set_stmt()) {
    if (OB_FAIL(set_var_info(parse_result))) {
      LOG_WDIAG("fail to set var info", K(ret));
    }
  } else if (is_show_elastic_id_stmt()) {
    ObString tmp_string(parse_result.cmd_info_.string_[0].str_len_, parse_result.cmd_info_.string_[0].str_);
    if (!tmp_string.empty()) {
      cmd_info_.string_[0].set(tmp_string);
    }
    tmp_string.assign_ptr(parse_result.cmd_info_.string_[1].str_, parse_result.cmd_info_.string_[1].str_len_);
    if (!tmp_string.empty()) {
      cmd_info_.string_[1].set(tmp_string);
    }
  } else if (is_stop_ddl_task_stmt() || is_retry_ddl_task_stmt()) {
    // cmd_integer_[0] stores the task id for sharding ddl
    cmd_info_.integer_[0] = parse_result.cmd_info_.integer_[0];
  } else if (is_set_route_addr()) {
    // cmd_integer_[0] stores the obproxy_route_addr
    cmd_info_.integer_[0] = parse_result.cmd_info_.integer_[0];
  } else if (is_internal_cmd() || is_ping_proxy_cmd()) {
    cmd_sub_type_ = parse_result.cmd_info_.sub_type_;
    cmd_err_type_ = parse_result.cmd_info_.err_type_;
    for (int64_t i = 0; OB_SUCC(ret) && i < OBPROXY_ICMD_MAX_VALUE_COUNT; ++i) {
      cmd_info_.integer_[i] = parse_result.cmd_info_.integer_[i];
      const ObString tmp_string(parse_result.cmd_info_.string_[i].str_len_, parse_result.cmd_info_.string_[i].str_);

      if (tmp_string.length() >= OBPROXY_MAX_STRING_VALUE_LENGTH) {
        ret = OB_INTERNAL_CMD_VALUE_TOO_LONG;
        LOG_WDIAG("value is too long", K(tmp_string), K(ret));
      } else {
        cmd_info_.string_[i].set(tmp_string);
      }
    }
  } else if (is_internal_select()) {
    if (OB_FAIL(set_col_name(parse_result.col_name_))) {
      LOG_WDIAG("failed to set col name", K(ret));
    }
  } else if (is_sharding_request && is_ddl_stmt()) {
    if (OB_FAIL(set_dbmesh_route_info(parse_result))) {
      LOG_WDIAG("fail to set dbmesh route info", K(ret));
      stmt_type_ = OBPROXY_T_INVALID;
    }
  } else {
    LOG_DEBUG("not a dml sql", K(ret));
  }

  if (OB_SUCC(ret) && is_text_ps_stmt()) {
    if (OB_ISNULL(parse_result.text_ps_name_.str_) || 0 == parse_result.text_ps_name_.str_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("text ps name is null", K(ret));
    } else {
      if (NULL != text_ps_buf_) {
        allocator_.free(text_ps_buf_);
        text_ps_buf_ = NULL;
        text_ps_buf_len_ = 0;
      }
      if (OB_ISNULL(text_ps_buf_ = static_cast<char *>(allocator_.alloc(parse_result.text_ps_name_.str_len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem", K(parse_result.text_ps_name_.str_len_), K(ret));
      } else {
        text_ps_buf_len_ = parse_result.text_ps_name_.str_len_;
        MEMCPY(text_ps_buf_, parse_result.text_ps_name_.str_, parse_result.text_ps_name_.str_len_);
        text_ps_name_.assign_ptr(text_ps_buf_, text_ps_buf_len_);
        string_to_upper_case(text_ps_name_.ptr(), text_ps_name_.length());
        if (OBPROXY_T_TEXT_PS_EXECUTE == parse_result.stmt_type_ || OBPROXY_T_TEXT_PS_PREPARE == parse_result.stmt_type_) {
          if (OB_FAIL(set_text_ps_info(text_ps_info_, parse_result.text_ps_parse_info_))) {
            LOG_WDIAG("fail to set text ps execute info", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

DEF_TO_STRING(ObProxyCmdInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = 0; i < OBPROXY_ICMD_MAX_VALUE_COUNT; ++i) {
    J_KV("integer", integer_[i], "string", string_[i]);
    J_COMMA();
  }
  J_OBJ_END();
  return pos;
}

int64_t ObSqlParseResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("stmt_type", get_obproxy_stmt_name(stmt_type_),
       K_(hint_query_timeout),
       "hint_read_consistency", get_consistency_level_str(hint_consistency_level_),
       K_(has_found_rows),
       K_(has_row_count),
       K_(has_last_insert_id),
       K_(has_explain),
       K_(has_explain_route),
       K_(has_shard_comment),
       K_(has_simple_route_info),
       K_(parsed_length),
       K_(is_binlog_related),
       "database_name", get_database_name(),
       "database_quote_type", get_obproxy_quote_name(get_database_name_quote()),
       "package_name", get_package_name(),
       "package_quote_type", get_obproxy_quote_name(get_package_name_quote()),
       "table_name", get_table_name(),
       "table_quote_type", get_obproxy_quote_name(get_table_name_quote()),
       "alias_name", get_alias_name(),
       "alias_quote_type", get_obproxy_quote_name(get_alias_name_quote()),
       "internal select col_name", get_col_name(),
       "sub_type", get_obproxy_sub_stmt_name(cmd_sub_type_),
       "err_type", get_obproxy_err_stmt_name(cmd_err_type_),
       "part_name", get_part_name());
  J_KV(K_(cmd_info),
       K_(call_info),
       K_(route_info),
       K_(dbmesh_route_info),
       K_(set_info),
       K_(dbp_route_info),
       K_(trace_id),
       K_(rpc_id),
       K_(fileds_result));
  J_OBJ_END();
  return pos;
}

int64_t ObProxyCallParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("type", get_obproxy_call_token_type(type_), K_(str_value), K_(is_alloc));
  J_OBJ_END();
  return pos;
}

int64_t ObProxyCallInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_param_valid), K_(param_count), "params", params_);
  J_OBJ_END();
  return pos;
}

ObProxyCallInfo::ObProxyCallInfo(const ObProxyCallInfo &other)
{
  int ret = OB_SUCCESS;
  is_param_valid_ = other.is_param_valid_;
  param_count_ = other.param_count_;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count_; i++) {
    ObProxyCallParam *param = other.params_.at(i);
    ObProxyCallParam *tmp_param = NULL;
    if (OB_FAIL(ObProxyCallParam::alloc_call_param(tmp_param))) {
      LOG_WDIAG("fail to alloc call param", K(ret));
    } else {
      *tmp_param = *param;
      if (OB_FAIL(params_.push_back(tmp_param))) {
        tmp_param->reset();
        tmp_param = NULL;
        LOG_EDIAG("fail to assign param", K(ret));
      }
    }
  }
}

ObProxyCallInfo& ObProxyCallInfo::operator=(const ObProxyCallInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    is_param_valid_ = other.is_param_valid_;
    param_count_ = other.param_count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count_; i++) {
      ObProxyCallParam *param = other.params_.at(i);
      ObProxyCallParam *tmp_param = NULL;
      if (OB_FAIL(ObProxyCallParam::alloc_call_param(tmp_param))) {
        LOG_WDIAG("fail to alloc call param", K(ret));
      } else {
        *tmp_param = *param;
        if (OB_FAIL(params_.push_back(tmp_param))) {
          tmp_param->reset();
          tmp_param = NULL;
          LOG_EDIAG("fail to assign param", K(ret));
        }
      }
    }
  }
  return *this;
}

int64_t ObProxyTextPsParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(str_value), K_(is_alloc));
  J_OBJ_END();
  return pos;
}

int64_t ObProxyTextPsInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_param_valid), K_(param_count), "params", params_);
  J_OBJ_END();
  return pos;
}

ObProxyTextPsInfo::ObProxyTextPsInfo(const ObProxyTextPsInfo &other)
{

  int ret = OB_SUCCESS;
  is_param_valid_ = other.is_param_valid_;
  param_count_ = other.param_count_;
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count_; i++) {
    ObProxyTextPsParam *param = other.params_.at(i);
    ObProxyTextPsParam *tmp_param = NULL;
    if (OB_FAIL(ObProxyTextPsParam::alloc_text_ps_param(param->str_value_.ptr(), param->str_value_.length(), tmp_param))) {
      LOG_WDIAG("fai lto alloc text ps param", K(ret));
    } else {
      *tmp_param = *param;
      if (OB_FAIL(params_.push_back(tmp_param))) {
        tmp_param->reset();
        tmp_param = NULL;
        LOG_WDIAG("fail to push back text ps param", K(ret));
      }
    }
  }
}

ObProxyTextPsInfo& ObProxyTextPsInfo::operator=(const ObProxyTextPsInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    is_param_valid_ = other.is_param_valid_;
    param_count_ = other.param_count_;
    for (int64_t i = 0; OB_SUCC(ret) && i < param_count_; i++) {
      ObProxyTextPsParam *param = other.params_.at(i);
      ObProxyTextPsParam *tmp_param = NULL;
      if (OB_FAIL(ObProxyTextPsParam::alloc_text_ps_param(param->str_value_.ptr(), param->str_value_.length(), tmp_param))) {
        LOG_WDIAG("fai lto alloc text ps param", K(ret));
      } else {
        *tmp_param = *param;
        if (OB_FAIL(params_.push_back(tmp_param))) {
          tmp_param->reset();
          tmp_param = NULL;
          LOG_WDIAG("fail to push back text ps param", K(ret));
        }
      }
    }
  }
  return *this;
}

int64_t ObProxySimpleRouteInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("table_name", table_name_buf_,
       "part_key", part_key_buf_,
       K_(table_offset), K_(table_len),
       K_(part_key_offset), K_(part_key_len));
  J_OBJ_END();
  return pos;
}

int64_t SqlField::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(column_name));
  J_ARRAY_START();
  for (int i = 0; i < column_values_.count(); i++) {
    J_COMMA();
    J_KV("i", i, "column_value", column_values_.at(i));
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

int64_t SqlFieldResult::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(fields_.count()));
  J_ARRAY_START();
  for (int i = 0; i < fields_.count();i++) {
    J_COMMA();
    J_KV("i", i,  "filed", fields_.at(i));
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

int64_t DbMeshRouteInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(testload), K_(group_idx), K_(tb_idx), K_(es_idx), K_(db_pos),
       K_(tb_pos), K_(index_tb_pos), K_(table_name), K_(disaster_status), K_(tnt_id));
  J_OBJ_END();
  return pos;
}

int64_t SetVarNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(var_type),
       K_(var_name),
       K_(is_alloc));
  if (SET_VALUE_TYPE_INT == value_type_) {
    J_COMMA();
    J_KV(K_(int_value));
  } else {
    J_COMMA();
    J_KV(K_(str_value));
  }
  J_OBJ_END();
  return pos;
}

int64_t ObProxySetInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(node_count), K_(var_nodes));
  J_OBJ_END();
  return pos;
}

int64_t DbpRouteInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(has_group_info), K_(group_idx), K_(scan_all), K_(table_name), K_(has_shard_key));
  J_OBJ_END();
  return pos;
}

// ====ObProxySqlParser====
int ObProxySqlParser::get_parse_allocator(ObArenaAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  static __thread ObArenaAllocator *arena_allocator = NULL;
  if (OB_UNLIKELY(NULL == arena_allocator)) {
    if (NULL == (arena_allocator = new (std::nothrow) ObArenaAllocator(common::ObModIds::OB_PROXY_SQL_PARSE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc arena allocator", K(ret));
    } else {
      allocator = arena_allocator;
    }
  } else {
    allocator = arena_allocator;
  }

  return ret;
}

int ObProxySqlParser::parse_sql(const ObString &sql,
                                const ObProxyParseMode parse_mode,
                                ObSqlParseResult &sql_parse_result,
                                const bool use_lower_case_name,
                                ObCollationType connection_collation,
                                const bool drop_origin_db_table_name /*false*/,
                                const bool is_sharding_request /*false*/)
{
  int ret = OB_SUCCESS;
  //int64_t start_time = ObTimeUtility::current_time();
  ObArenaAllocator *allocator = NULL;
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid sql", K(sql), K(ret));
  } else if (OB_FAIL(get_parse_allocator(allocator))) {
    LOG_WDIAG("fail to get parse allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("allocator is null", K(ret));
  } else {
    ObProxyParser obproxy_parser(*allocator, parse_mode);
    ObProxyParseResult obproxy_parse_result;

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = obproxy_parser.parse(sql, obproxy_parse_result, connection_collation))) {
      LOG_INFO("fail to parse sql, will go on anyway", K(sql), K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = sql_parse_result.load_result(obproxy_parse_result,
                                                                     use_lower_case_name,
                                                                     drop_origin_db_table_name, is_sharding_request))) {
      if (sql_parse_result.is_internal_cmd()  && OB_INTERNAL_CMD_VALUE_TOO_LONG == tmp_ret) {
        ret = OB_ERR_PARSE_SQL;
      } else {
        LOG_INFO("fail to load result, will go on anyway", K(sql), K(use_lower_case_name), K(tmp_ret));
      }
    } else {
      sql_parse_result.set_multi_semicolon_in_stmt(ObProxySqlParser::is_multi_semicolon_in_stmt(sql));
      // if a start trans sql contains multi semicolon, do not hold it to avoid wrong sql syntax
      if (sql_parse_result.is_start_trans_stmt() && sql_parse_result.is_multi_semicolon_in_stmt()) {
        sql_parse_result.set_stmt_type(OBPROXY_T_INVALID);
      }
      LOG_DEBUG("success to do proxy parse", K(sql_parse_result));
    }

    allocator->reuse();
  }
  return ret;
}

int ObProxySqlParser::init_ob_parser_node(ObArenaAllocator &allocator, ObParseNode *&ob_node)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = (char*) allocator.alloc(sizeof(ObParseNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("allocate ObParseNode node failed!", "ret", ret, "size", sizeof(ObParseNode));
  } else {
    ob_node = new (buf) ObParseNode();
  }
  return ret;
}

int ObProxySqlParser::ob_load_testload_parse_node(ParseNode *node, const int level,
                                                  ObSEArray<ObParseNode*, 1> &relation_table_node,
                                                  ObSEArray<ObParseNode*, 1> &hint_option_list,
                                                  //ObSEArray<ObParseNode*, 1> &ref_column_node, ObSEArray<ObParseNode*, 1> &hint_option_list,
                                                  AliasTableMap &all_table_map,
                                                  AliasTableMap &alias_table_map,
                                                  ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(node)) {
    return ret;
  }

  bool need_skip_children = false;
  char *buf = NULL;
  ObParseNode *pn = NULL;

  switch (node->type_) {
     case T_RELATION_FACTOR: { //table entry, two children node
       if (OB_T_RELATION_FACTOR_NUM_CHILD /* 2 */ != node->num_child_) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WDIAG("T_RELATION_FACTOR children node is not 2", K(ret), K(level), K(node->num_child_));
       }  else if (OB_ISNULL(node->children_[1]) || T_IDENT != node->children_[1]->type_
                      || OB_ISNULL(node->children_[1]->str_value_)
                      || 0 >= node->children_[1]->token_len_) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WDIAG("T_RELATION_FACTOR unexpected table entry", K(ret), K(level), K(node->children_[1]));
       } else if (FALSE_IT(need_skip_children = true)) {
         // not to be here
       } else if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
           LOG_WDIAG("init ObParseNode failed", K(ret), K(level));
       } else if (FALSE_IT(pn->set_node(node))) {
         // not to be here
       } else if (OB_FAIL(relation_table_node.push_back(pn))) {
         LOG_WDIAG("T_RELATION_FACTOR fail add table entry", "ret", ret, "level", level, "node", pn);
       } else if (OB_ISNULL(buf = (char*) allocator.alloc(node->children_[1]->token_len_))) {
           ret = OB_ALLOCATE_MEMORY_FAILED;
           LOG_WDIAG("allocate ObParseNode node failed!", "ret", ret, "size", sizeof(ObParseNode));
       } else {
         MEMCPY(buf, node->children_[1]->str_value_, node->children_[1]->token_len_);
         buf[node->children_[1]->token_len_] = '\0';
         ObString origin_str = ObString::make_string(buf);
         string_to_upper_case(origin_str.ptr(), origin_str.length());
         if (OB_FAIL(all_table_map.set_refactored(origin_str, pn))) { /* same table keep last one. */
           LOG_WDIAG("T_RELATION_FACTOR fail add node", K(ret), K(level), K(origin_str), K(pn), K(all_table_map.size()));
         } else {
           LOG_DEBUG("T_RELATION_FACTOR add node", K(origin_str), K(pn), K(all_table_map.size()), K(ret));
         }
         LOG_DEBUG("T_RELATION_FACTOR add table entry", "ret", ret, "node", pn); //TODO need to check
       }
       break;
     }
     case T_COLUMN_REF: {
       if (OB_T_COLUMN_REF_NUM_CHILD /* 3 */ != node->num_child_) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WDIAG("T_COLUMN_REF children node is not 3", K(ret), K(node->num_child_));
       } else if (OB_ISNULL(node->children_[2])
                      || (T_IDENT != node->children_[2]->type_
                      && T_STAR != node->children_[2]->type_)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WDIAG("T_COLUMN_REF unexpected column entry", K(ret), K(node->children_[2]->type_), K(node->children_[1]));
       } else if (FALSE_IT(need_skip_children = true)) {
         // not to be here
       } else if (OB_ISNULL(node->children_[1])
                      || OB_ISNULL(node->children_[1]->str_value_)
                      || 0 >= node->children_[1]->token_len_) { // table entry is NULL, skip directly
         LOG_DEBUG("skip column without table info", K(ret));
       } else if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
         LOG_WDIAG("init ObParseNode failed", K(ret));
       } else if (FALSE_IT(pn->set_node(node))) {
         // not to be here
       } else if (OB_FAIL(relation_table_node.push_back(pn))) {
         LOG_WDIAG("T_COLUMN_REF fail add column entry", "ret", ret, "node", pn);
       } else {
         LOG_DEBUG("T_COLUMN_REF  add column entry", "ret", ret, "node", pn);
       }
       break;
     }
     case T_ALIAS: {
       if (OB_T_ALIAS_CLUMN_NAME_NUM_CHILD /* 2 */ != node->num_child_
                  && OB_T_ALIAS_TABLE_NAME_NUM_CHILD /* 5 */ != node->num_child_) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WDIAG("T_ALIAS children node is not 2 or 5", K(ret), K(node->num_child_));
       } else if (OB_T_ALIAS_CLUMN_NAME_NUM_CHILD /* 2 */ == node->num_child_) {
         need_skip_children = false; // alias column, check db and table info next
       } else if (OB_T_ALIAS_TABLE_NAME_NUM_CHILD /* 5 */ == node->num_child_) {
         if (OB_ISNULL(node->children_[0])
                   || OB_ISNULL(node->children_[1])
                   || OB_ISNULL(node->children_[1]->str_value_)
                   || 0 >= node->children_[1]->token_len_
                   || T_RELATION_FACTOR != node->children_[0]->type_
                   || T_IDENT != node->children_[1]->type_) {
           ret = OB_ERR_UNEXPECTED;
           LOG_WDIAG("T_ALIAS children node is not 2", K(ret), K(node->num_child_));
         } else {
           // alias not need to change it to upper style
           // string_to_upper_case(node->children_[1]->str_value_, node->children_[1]->token_len_);
           // set_refactored / get_refactored
           ObString str = ObString::make_string(node->children_[1]->str_value_);
           if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
             LOG_WDIAG("init ObParseNode failed", K(ret));
           } else if (FALSE_IT(pn->set_node(node))) {
             // not to be here
           } else if (OB_FAIL(alias_table_map.set_refactored(str, pn))) {
               LOG_WDIAG("T_ALIAS fail add node", K(str), K(pn), K(alias_table_map.size()), K(ret));
           } else {
             LOG_DEBUG("T_ALIAS add node", K(str), K(pn), K(alias_table_map.size()), K(ret));
           }
           if (OB_SUCC(ret) && OB_FAIL(init_ob_parser_node(allocator, pn))) {
             LOG_WDIAG("init ObParseNode failed", K(ret));
           } else if (FALSE_IT(pn->set_node(node->children_[0]))) {
             // not to be here
           } else if (OB_FAIL(relation_table_node.push_back(pn))) {
             LOG_WDIAG("T_RELATION_FACTOR fail add node", K(str), K(pn), K(relation_table_node.count()),
                        K(ret));
           } else if (OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[0]->children_[1])
                        || 0 >= node->children_[0]->children_[1]->token_len_
                        || OB_ISNULL(node->children_[0]->children_[1]->str_value_)) {
             ret = OB_ERR_UNEXPECTED;
             LOG_WDIAG("T_RELATION_FACTOR is invalid", K(ret));
           } else if (OB_ISNULL(buf = (char*) allocator.alloc(node->children_[0]->children_[1]->token_len_))) {
               ret = OB_ALLOCATE_MEMORY_FAILED;
               LOG_WDIAG("allocate ObParseNode node failed!", "ret", ret, "size", sizeof(ObParseNode));
           } else {

             MEMCPY(buf, node->children_[0]->children_[1]->str_value_, node->children_[0]->children_[1]->token_len_);
             buf[node->children_[0]->children_[1]->token_len_] = '\0';
             ObString origin_str = ObString::make_string(buf);
             string_to_upper_case(origin_str.ptr(), origin_str.length()); // change all to upper to store

             if (OB_FAIL(all_table_map.set_refactored(origin_str, pn))) { /* same table keep last one. */
               LOG_WDIAG("T_RELATION_FACTOR fail add node", K(ret), K(origin_str));
             } else {
             LOG_DEBUG("T_RELATION_FACTOR add node", K(str), K(pn), K(relation_table_node.count()),
                       K(all_table_map.size()), K(ret));
             }
           }
         }
         need_skip_children = true;
       }
       break;
     }
     case T_HINT_OPTION_LIST: {
       if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
         LOG_WDIAG("init ObParseNode failed", K(ret));
       } else if (FALSE_IT(pn->set_node(node))) {
         // not to be here
       } else if (OB_FAIL(hint_option_list.push_back(pn))) {
           LOG_WDIAG("T_HINT_OPTION_LIST fail add node", K(ret), K(pn));
       } else {
         LOG_DEBUG("T_HINT_OPTION_LIST add node", K(ret), K(pn));
       }
       need_skip_children = true;
       break;
     }
     default: {
       LOG_DEBUG("other type not need add", "node_type", get_type_name(node->type_), K(ret));
       need_skip_children = false;
       break;
     }
   }

   if (OB_SUCC(ret) && !need_skip_children) {
     for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
       if (OB_NOT_NULL(node->children_[i])) {
         int l = level + 1;
         if(OB_FAIL(ob_load_testload_parse_node(node->children_[i], l, relation_table_node,
                            hint_option_list, all_table_map, alias_table_map, allocator))) {
           LOG_WDIAG("ob_load_testload_parse_node fail", K(ret), K(node->children_[i]));
         }
       }
     }
   }

  return ret;
}

int ObSqlParseResult::get_result_tree_str(ParseNode *root, const int level, char* buf, int64_t& pos, int64_t length)
{
  int ret = OB_SUCCESS;
  if (NULL == root || NULL == buf || length < 0) {
    return ret;
  } else {
    for (int i = 0 ; i < level *2; i++) {
      pos += snprintf(buf + pos, length - pos, "-");
      if (OB_UNLIKELY(pos >= length)) {
        pos = length - 1;
        break;
      }
    }
    pos += snprintf (buf + pos, length - pos, " %s %s:pos:%ld, text_len:%ld, num_child_:%d, token_off:%d, token_len:%d\n",
                     get_type_name(root->type_),
                     root->str_value_, root->pos_,
                     root->text_len_, root->num_child_,
                     root->token_off_, root->token_len_);
    if (OB_UNLIKELY(pos >= length)) {
      pos = length - 1;
    }

    for (int i = 0; OB_SUCC(ret) && i < root->num_child_; i++) {
      if (NULL == root->children_[i]) {
        for (int i = 0 ; i < (level + 1) *2; i++) {
          pos += snprintf(buf + pos, length - pos, "-");
          if (OB_UNLIKELY(pos >= length)) {
            pos = length - 1;
            break;
          }
        }
        pos += snprintf(buf + pos, length - pos, " There is one NULL child node\n");
        if (OB_UNLIKELY(pos >= length)) {
          pos = length - 1;
          break;
        }
      } else if (OB_FAIL(get_result_tree_str(root->children_[i], level + 1, buf, pos, length))) {
        LOG_WDIAG("get_result_tree_str failed", K(ret), K(i), K(pos), K(length));
      }
    }
  }
  return ret;
}

int ObSqlParseResult::ob_parse_resul_to_string(const ParseResult &parse_result, const common::ObString& sql,
                                               char* buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_result_tree_str(parse_result.result_tree_, 0, buf, pos, buf_len))) {
    LOG_WDIAG("get_result_tree_str failed", K(ret), K(sql));
  } else if (parse_result.comment_list_ != NULL) {
    for (int j = 0; j < parse_result.comment_cnt_; j++) {
      TokenPosInfo& token_info = parse_result.comment_list_[j];
      pos += snprintf(buf + pos, buf_len - pos, "comment[%d] is %.*s\n", j,
          token_info.token_len_, sql.ptr()+ token_info.token_off_);
      if (OB_UNLIKELY(pos >= buf_len)) {
        pos = buf_len - 1;
        break;
      }
    }
  }
  return ret;
}

int ObSqlParseResult::load_ob_parse_result(const ParseResult &parse_result,
                                           const common::ObString& sql,
                                           const bool need_handle_result)
{
  int ret = OB_SUCCESS;
  //just for debug , ignore ret
  if (OB_UNLIKELY(IS_DEBUG_ENABLED())) {
    const int64_t buf_len = 64 * 1024;
    int64_t pos = 0;
    char tree_str_buf[buf_len];
    ob_parse_resul_to_string(parse_result, sql, tree_str_buf, buf_len, pos);
    ObString tree_str(pos, tree_str_buf);
    LOG_DEBUG("result_tree_ is \n", K(tree_str));
  }
  ParseNode* node = parse_result.result_tree_;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("node is unexpected null", K(ret));
  } else if (T_STMT_LIST != node->type_ || node->num_child_ < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", "node_type", get_type_name(node->type_), K(node->num_child_), K(ret));
  } else if (OB_ISNULL(node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_DEBUG("unexpected null child", K(ret));
  } else if (OB_UNLIKELY(node->type_ == T_EXPLAIN)) {
    if ((node->num_child_ >= 2) && OB_NOT_NULL(node->children_[1])) {
      node = node->children_[1];
      LOG_DEBUG("succ to parse explain stmt", K(sql), K(ret));
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WDIAG("fail to parse explain stmt", K(sql), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    switch(node->type_) {
      case T_SELECT:
        if (need_handle_result) {
          char* buf = NULL;
          ObProxySelectStmt* select_stmt= NULL;
          if (OB_ISNULL(buf = (char*)allocator_.alloc(sizeof(ObProxySelectStmt)))) {
            LOG_WDIAG("failed to alloc buf");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_ISNULL(select_stmt = new (buf) ObProxySelectStmt(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("failed to new ObProxySelectStmt", K(ret));
          } else if (OB_FAIL(select_stmt->init())) {
            LOG_WDIAG("init failed", K(ret));
          } else {
            select_stmt->set_sql_string(sql);
            select_stmt->set_stmt_type(OBPROXY_T_SELECT);
            select_stmt->set_field_results(&fileds_result_);
            select_stmt->set_table_name(origin_table_name_);
            proxy_stmt_ = select_stmt;
            if (OB_FAIL(proxy_stmt_->handle_parse_result(parse_result))) {
              LOG_WDIAG("handle select parse result failed", K(ret));
            } else {
              has_for_update_ = select_stmt->has_for_update();
            }
          }
        }
        break;
      case T_INSERT:
        if (need_handle_result) {
          char* buf = NULL;
          ObProxyInsertStmt* insert_stmt= NULL;
          if (OB_ISNULL(buf = (char*)allocator_.alloc(sizeof(ObProxyInsertStmt)))) {
            LOG_WDIAG("failed to alloc buf");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_ISNULL(insert_stmt = new (buf) ObProxyInsertStmt(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("failed to new ObProxyInsertStmt", K(ret));
          } else if (OB_FAIL(insert_stmt->init())) {
            LOG_WDIAG("init failed", K(ret));
          } else {
            insert_stmt->set_sql_string(sql);
            insert_stmt->set_stmt_type(OBPROXY_T_INSERT);
            insert_stmt->set_field_results(&fileds_result_);
            insert_stmt->set_table_name(origin_table_name_);
            proxy_stmt_ = insert_stmt;
            if (OB_FAIL(proxy_stmt_->handle_parse_result(parse_result))) {
              LOG_WDIAG("handle Insert parse result failed", K(ret));
            }
          }
        }
        break;
      case T_DELETE:
        if (need_handle_result) {
          char* buf = NULL;
          ObProxyDeleteStmt* delete_stmt= NULL;
          if (OB_ISNULL(buf = (char*)allocator_.alloc(sizeof(ObProxyDeleteStmt)))) {
            LOG_WDIAG("failed to alloc buf");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_ISNULL(delete_stmt = new (buf) ObProxyDeleteStmt(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("failed to new ObProxyDeleteStmt", K(ret));
          } else if (OB_FAIL(delete_stmt->init())) {
            LOG_WDIAG("init failed", K(ret));
          } else {
            delete_stmt->set_sql_string(sql);
            delete_stmt->set_stmt_type(OBPROXY_T_DELETE);
            delete_stmt->set_field_results(&fileds_result_);
            delete_stmt->set_table_name(origin_table_name_);
            proxy_stmt_ = delete_stmt;
            if (OB_FAIL(proxy_stmt_->handle_parse_result(parse_result))) {
              LOG_WDIAG("handle delete parse result failed", K(ret));
            }
          }
        }
        break;
      //TODO
      case T_UPDATE:
        if (need_handle_result) {
          char* buf = NULL;
          ObProxyUpdateStmt* update_stmt= NULL;
          if (OB_ISNULL(buf = (char*)allocator_.alloc(sizeof(ObProxyUpdateStmt)))) {
            LOG_WDIAG("failed to alloc buf");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (OB_ISNULL(update_stmt = new (buf) ObProxyUpdateStmt(allocator_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("failed to new ObProxyUpdateStmt", K(ret));
          } else if (OB_FAIL(update_stmt->init())) {
            LOG_WDIAG("init failed", K(ret));
          } else {
            update_stmt->set_sql_string(sql);
            update_stmt->set_stmt_type(OBPROXY_T_UPDATE);
            update_stmt->set_field_results(&fileds_result_);
            update_stmt->set_table_name(origin_table_name_);
            proxy_stmt_ = update_stmt;
            if (OB_FAIL(proxy_stmt_->handle_parse_result(parse_result))) {
              LOG_WDIAG("handle update parse result failed", K(ret));
            }
          }
        }
        break;
      default:
        LOG_DEBUG("node type", "node_type", get_type_name(node->type_));
    }
  }

  if (OB_SUCC(ret)) {
    ob_parser_result_ = const_cast<ParseResult*>(&parse_result);
  }
  return ret;
}

int ObProxySqlParser::parse_sql_by_obparser(const ObString &sql,
                                            const ObProxyParseMode parse_mode,
                                            ObSqlParseResult &sql_parse_result,
                                            const bool need_handle_result)
{
  int ret = OB_SUCCESS;
  //int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid sql", K(sql), K(ret));
  } else {
    ObProxyParser obproxy_parser(sql_parse_result.allocator_, parse_mode);
    char *buf = NULL;
    ParseResult *parser_result = NULL;
    if (OB_ISNULL(buf = (char*)sql_parse_result.allocator_.alloc(sizeof(ParseResult)))) {
      LOG_WDIAG("failed to alloc buf");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(parser_result = new (buf) ParseResult())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("failed to new ParseResult", K(ret));
    } else {
      ParseResult &ob_parse_result = *parser_result;

      if (OB_FAIL(obproxy_parser.obparse(sql, ob_parse_result))) {
        if (OB_ERR_PARSE_SQL == ret) {
          ret = OB_ERR_PARSER_SYNTAX;
        }
        LOG_INFO("fail to parse sql", K(sql), K(ret));
      } else if (OB_FAIL(sql_parse_result.load_ob_parse_result(ob_parse_result, sql, need_handle_result))) {
        LOG_WDIAG("fail to load_ob_parse_result", K(sql), K(ret));
      } else {
        LOG_DEBUG("success to do parse_sql_by_obparser", K(sql));
      }
    }
  }
  //int64_t end_time = ObTimeUtility::current_time();
  //LOG_TRACE("finish parse sql", "total cost time(us)", end_time - start_time,
  //          K(sql), K(sql_parse_result), K(ret));
  return ret;
}

// A simplified version from observer
int ObProxySqlParser::split_multiple_stmt(const ObString &stmt,
                                  ObIArray<ObString> &queries)
{
  int ret = OB_SUCCESS;

  int64_t offset = 0;
  int64_t remain = stmt.length();

  trim_multi_stmt(stmt, remain);

  // Special handling for empty statements
  if (OB_UNLIKELY(0 >= remain)) {
    ObString part;
    ret = queries.push_back(part);
  }

  while (remain > 0 && OB_SUCC(ret)) {
    int64_t str_len = 0;

    //calc the end position of a single sql.
    get_single_sql(stmt, offset, remain, str_len);
    str_len = str_len == remain ? str_len : str_len + 1;
    ObString query(static_cast<int32_t>(str_len), stmt.ptr() + offset);

    remain -= str_len;
    offset += str_len;

    if (remain < 0 || offset > stmt.length()) {
      LOG_EDIAG("split_multiple_stmt data error",
                K(remain), K(offset), K(stmt.length()), K(ret));
    } else if(OB_FAIL(queries.push_back(query))){
      LOG_WDIAG("fail to push back part of multi stmt", K(stmt), K(ret));
    }
  }

  return ret;
}

// avoid separeting sql by semicolons in quotes or comment.
void ObProxySqlParser::get_single_sql(const common::ObString &stmt, int64_t offset, int64_t remain, int64_t &str_len) {
  /* following two flags are used to mark wether we are in comment, if in comment, ';' can't be used to split sql*/
  // in -- comment
  bool comment_flag = false;
  // in /*! comment */ or /* comment */
  bool c_comment_flag = false;
  /* follwing three flags are used to mark wether we are in quotes.*/
  // in '', single quotes
  bool sq_flag = false;
  // in "", double quotes
  bool dq_flag = false;
  // in ``, backticks.
  bool bt_flag = false;
  //bool is_escape = false;

  bool in_comment = false;
  bool in_string  = false;
  while (str_len < remain && (in_comment || in_string || (stmt[str_len + offset] != ';'))) {
    if (!in_comment && !in_string) {
      if (str_len + 1 >= remain) {
      } else if ((stmt[str_len + offset] == '-' && stmt[str_len + offset + 1] == '-') || stmt[str_len + offset + 1] == '#') {
        comment_flag = true;
      } else if (stmt[str_len + offset] == '/' && stmt[str_len + offset + 1] == '*') {
        c_comment_flag = true;
      } else if (stmt[str_len + offset] == '\'') {
        sq_flag = true;
      } else if (stmt[str_len + offset] == '"') {
        dq_flag = true;
      } else if (stmt[str_len + offset] == '`') {
        bt_flag = true;
      }
    } else if (in_comment) {
      if (comment_flag) {
        if (stmt[str_len + offset] == '\r' || stmt[str_len + offset] == '\n') {
          comment_flag = false;
        }
      } else if (c_comment_flag) {
        if (str_len + 1 >= remain) {

        } else if (stmt[str_len + offset] == '*' && (str_len + 1 < remain) && stmt[str_len + offset + 1] == '/') {
          c_comment_flag = false;
        }
      }
    } else if (in_string) {
      if (str_len + 1 >= remain) {
      } else if (!bt_flag && stmt[str_len + offset] == '\\') {
        // in mysql mode, handle the escape char in '' and ""
        ++ str_len;
      } else if (sq_flag) {
        if (stmt[str_len + offset] == '\'') {
          sq_flag = false;
        }
      } else if (dq_flag) {
        if (stmt[str_len + offset] == '"') {
          dq_flag = false;
        }
      } else if (bt_flag) {
        if (stmt[str_len + offset] == '`') {
          bt_flag = false;
        }
      }
    }
    ++ str_len;

    // update states.
    in_comment = comment_flag || c_comment_flag;
    in_string = sq_flag || bt_flag || dq_flag;
  }
}

int ObProxySqlParser::preprocess_multi_stmt(ObArenaAllocator &allocator,
                                            char* &multi_sql_buf,
                                            const int64_t origin_sql_length,
                                            ObSEArray<ObString, 4> &sql_array)
{
  int ret = OB_SUCCESS;

  const int64_t PARSE_EXTRA_CHAR_NUM = 2;
  const int64_t total_sql_length = origin_sql_length
                                   + (sql_array.count() * PARSE_EXTRA_CHAR_NUM);

  if (OB_ISNULL(multi_sql_buf = static_cast<char*>(allocator.alloc(total_sql_length)))) {
    ret = OB_REACH_MEMORY_LIMIT;
    LOG_WDIAG("fail to alloc memory for multi_sql_buf", K(ret), K(total_sql_length));
  } else {
    MEMSET(multi_sql_buf, '\0', total_sql_length);
    int64_t pos = 0;
    for (int64_t i = 0; i < sql_array.count(); ++i) {
      ObString& sql = sql_array.at(i);
      MEMCPY(multi_sql_buf + pos ,sql.ptr(), sql.length());
      sql.assign_ptr(multi_sql_buf + pos, sql.length());
      pos += sql.length() + PARSE_EXTRA_CHAR_NUM;
    }
  }

  return ret;
}

void ObProxySqlParser::trim_multi_stmt(const common::ObString &stmt, int64_t &remain)
{
  // Bypass parser's unfriendly approach to empty query processing: remove the trailing spaces by yourself
  while (remain > 0 && ISSPACE(stmt[remain - 1])) {
    --remain;
  }
  //Remove the last '\0' to be compatible with mysql
  if (remain > 0 && '\0' == stmt[remain - 1]) {
    --remain;
  }
  //remove trailing spaces
  while (remain > 0 && ISSPACE(stmt[remain - 1])) {
    --remain;
  }
}

bool ObProxySqlParser::is_multi_semicolon_in_stmt(const common::ObString &stmt)
{
  bool is_multi = false;
  int64_t offset = 0;
  int64_t remain = stmt.length();
  int64_t str_len = 0;
  trim_multi_stmt(stmt, remain);

  // Special handling for empty statements
  if (OB_UNLIKELY(0 >= remain)) {
  } else {
    get_single_sql(stmt, offset, remain, str_len);
    remain -= str_len;
    // 2: remain space for `;` and `\0`
    if (remain > 2) {
      is_multi = true;
    }
  }
  return is_multi;
}

int64_t ObParseNode::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(node_)) {
    databuff_printf(buf, buf_len, pos, "{NULL}");
  } else {
    databuff_printf(buf, buf_len, pos,
                    "{%s %s:pos:%ld, text_len:%ld,num_child_:%d, token_off:%d, token_len:%d}",
                    get_type_name(node_->type_),
                    node_->str_value_,
                    node_->pos_,
                    node_->text_len_,
                    node_->num_child_,
                    node_->token_off_,
                    node_->token_len_
        );
  }
  return pos;
}

void ObProxyCallParam::reset()
{
  type_ = CALL_TOKEN_NONE;
  str_value_.reset();
  if (is_alloc_) {
    is_alloc_ = false;
    ob_free(this);
  }
}

int ObProxyCallParam::alloc_call_param(ObProxyCallParam*& param)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t alloc_size = sizeof(ObProxyCallParam);
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = common::ObModIds::OB_PROXY_SQL_PARSE;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(alloc_size, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for call param", K(alloc_size), K(ret));
  } else {
    param = new (buf) ObProxyCallParam();
    param->is_alloc_ = true;
  }
  return ret;
}

void ObProxyTextPsParam::reset()
{
  str_value_.reset();
  if (is_alloc_) {
    is_alloc_ = false;
    ob_free(this);
  }
}

int ObProxyTextPsParam::alloc_text_ps_param(const char* str, const int str_len, ObProxyTextPsParam*& param)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t alloc_size = sizeof(ObProxyTextPsParam);
  param = NULL;
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = ObModIds::OB_PROXY_PS_RELATED;
  if (OB_UNLIKELY(NULL == str || str_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(str), K(str_len), K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(alloc_size, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for param", K(alloc_size), K(ret));
  } else {
    param = new (buf) ObProxyTextPsParam();
    param->is_alloc_ = true;
  }

  if (OB_SUCC(ret)) {
    param->str_value_.set_value(str_len, str);
  }

  if (OB_FAIL(ret) && NULL != param) {
    param->reset();
  }
  return ret;
}

void SetVarNode::reset()
{
  value_type_ = SET_VALUE_TYPE_NONE;
  var_type_ = SET_VAR_TYPE_NONE;
  var_name_.reset();
  int_value_ = 0;
  str_value_.reset();
  if (is_alloc_) {
    is_alloc_ = false;
    ob_free(this);
  }
}

int SetVarNode::alloc_var_node(SetVarNode*& node)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t alloc_size = sizeof(SetVarNode);
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = ObModIds::OB_PROXY_SQL_PARSE;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(alloc_size, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for var node", K(alloc_size), K(ret));
  } else {
    node = new (buf) SetVarNode();
    node->is_alloc_ = true;
  }
  return ret;
}

int SqlField::alloc_sql_field(SqlField*& field)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  int64_t alloc_size = sizeof(SqlField);
  ObMemAttr mem_attr;
  mem_attr.mod_id_ = ObModIds::OB_PROXY_SQL_PARSE;
  if (OB_ISNULL(buf = static_cast<char *>(ob_malloc(alloc_size, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for call param", K(alloc_size), K(ret));
  } else {
    field = new (buf) SqlField();
    field->is_alloc_ = true;
  }
  return ret;
}

SqlField& SqlField::operator=(const SqlField& other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    column_name_ = other.column_name_;
    value_type_ = other.value_type_;
    column_int_value_ = other.column_int_value_;
    column_value_ = other.column_value_;
    for (int i = 0; OB_SUCC(ret) && i < other.column_values_.count(); i++) {
      if (OB_FAIL(column_values_.push_back(other.column_values_.at(i)))) {
        LOG_WDIAG("fail to push back column values", K(ret));
      }
    }
  }
  return *this;
}

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
