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

using namespace oceanbase::common;
using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{

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
    LOG_WARN("invalid argument", K(table_name), K(len));
  } else if (table_name_ != tmp_str) {
    // if the table name is getted from parser result or real physic table name returned by observer
    //   1. if mysql mode, case-insensitive
    //   2. if oracle mode, cmp directly
    MEMCPY(dml_buf_.table_name_buf_, table_name, len);
    table_name_.assign_ptr(dml_buf_.table_name_buf_, static_cast<int32_t>(len));
  }
  return ret;
}

inline int ObSqlParseResult::set_db_name(const ObProxyParseString &database_name,
                                         const bool use_lower_case_name/*false*/,
                                         const bool drop_origin_db_table_name /*false*/)
{
  int ret = OB_SUCCESS;

  if (NULL != database_name.str_ && 0 != database_name.str_len_) {
    if (OB_UNLIKELY(database_name.str_len_ > OB_MAX_DATABASE_NAME_LENGTH)
        || OB_UNLIKELY(database_name.str_len_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
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
      LOG_WARN("invalid argument", K(table_name.str_len_), K(ret));
    } else {
      MEMCPY(dml_buf_.table_name_buf_, table_name.str_, table_name.str_len_);
      if (use_lower_case_name) {
        string_to_lower_case(dml_buf_.table_name_buf_, table_name.str_len_);
      }
      table_name_.assign_ptr(dml_buf_.table_name_buf_, table_name.str_len_);
      table_name_quote_ = table_name.quote_type_;
      if (!drop_origin_db_table_name) {
        MEMCPY(origin_dml_buf_.table_name_buf_, table_name.str_, table_name.str_len_);
        origin_table_name_.assign_ptr(origin_dml_buf_.table_name_buf_, table_name.str_len_);
      }

      // assign package name when table name is valid
      if (NULL != package_name.str_ && 0 != package_name.str_len_) {
        if (OB_UNLIKELY(package_name.str_len_ > OB_MAX_TABLE_NAME_LENGTH)
            || OB_UNLIKELY(package_name.str_len_ < 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
        } else {
          MEMCPY(dml_buf_.package_name_buf_, package_name.str_, package_name.str_len_);
          if (use_lower_case_name) {
            string_to_lower_case(dml_buf_.package_name_buf_, package_name.str_len_);
          }
          package_name_.assign_ptr(dml_buf_.package_name_buf_, package_name.str_len_);
          package_name_quote_ = package_name.quote_type_;
        }
      }

      // assign database name when table name is valid
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_db_name(database_name, use_lower_case_name, drop_origin_db_table_name))) {
          LOG_WARN("fail to set db name", K(database_name.str_len_), K(ret));
        }
      }

      if (NULL != alias_name.str_ && 0 != alias_name.str_len_) {
        if (OB_UNLIKELY(alias_name.str_len_ > OB_MAX_TABLE_NAME_LENGTH)
            || OB_UNLIKELY(alias_name.str_len_ < 0)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret));
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
  }
  return ret;
}

inline int ObSqlParseResult::set_part_name(const ObProxyParseString &part_name)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_name.str_len_ > OB_MAX_PARTITION_NAME_LENGTH)
             || OB_UNLIKELY(part_name.str_len_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_name.str_len_), K(ret));
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
    LOG_WARN("not a internal select request should not set col name", K(ret));
  } else if (OB_UNLIKELY(col_name.str_len_ > OB_MAX_COLUMN_NAME_LENGTH)
             || OB_UNLIKELY(col_name.str_len_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(col_name.str_len_), K(ret));
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
  if (OB_UNLIKELY(!is_call_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not a call stmt should not set col name", K(ret));
  } else if (OB_UNLIKELY(call_parse_info.node_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(call_parse_info.node_count_), K(ret));
  } else if (call_parse_info.node_count_ > 0) {
    call_info_.is_param_valid_ = true;
    call_info_.param_count_ = call_parse_info.node_count_;
    ObProxyCallParam tmp_param;
    ObProxyCallParseNode *tmp_node = call_parse_info.head_;
    while(tmp_node && OB_SUCC(ret)) {
      tmp_param.reset();
      tmp_param.type_ = tmp_node->type_;
      if (CALL_TOKEN_INT_VAL == tmp_node->type_) {
        tmp_param.str_value_.set_integer(tmp_node->int_value_);
      } else if (CALL_TOKEN_PLACE_HOLDER == tmp_node->type_) {
        // for place holder, store its pos
        tmp_param.str_value_.set_integer(tmp_node->placeholder_idx_);
      } else {
        const ObString tmp_string(tmp_node->str_value_.str_len_, tmp_node->str_value_.str_);
        tmp_param.str_value_.set(tmp_string);
      } // end node_type
      if (OB_FAIL(call_info_.params_.push_back(tmp_param))) {
        LOG_WARN("fail to push back call param", K(tmp_param), K(ret));
      }
      tmp_node = tmp_node->next_;
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
  bool has_dbmesh_hint = false;
  int64_t tmp_int = OBPROXY_MAX_DBMESH_ID;

  // set dbmesh_route_info
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.table_name_str_.str_, route_info.table_name_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_UNLIKELY(tmp_str.length() > OB_MAX_TABLE_NAME_LENGTH)) {
      // do nothing, invalid table name, no need ret error
      LOG_WARN("invalid table name, string length is overflow", K(tmp_str));
    } else {
      has_dbmesh_hint = true;
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
      LOG_WARN("table name and table id should not together at the same time", K(ret));
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WARN("fail to get int value for tb index", K(ret));
    } else {
      has_dbmesh_hint = true;
      dbmesh_route_info_.tb_idx_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.group_idx_str_.str_, route_info.group_idx_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WARN("fail to get int value for group index", K(ret));
    } else {
      has_dbmesh_hint = true;
      dbmesh_route_info_.group_idx_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.tnt_id_str_.str_, route_info.tnt_id_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_UNLIKELY(tmp_str.length() > OBPROXY_MAX_TNT_ID_LENGTH)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tnt id, string length is overflow", K(tmp_str));
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
      LOG_WARN("fail to get int value for es index", K(ret));
    } else {
      has_dbmesh_hint = true;
      dbmesh_route_info_.es_idx_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.testload_str_.str_, route_info.testload_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
      LOG_WARN("fail to get int value for testload", K(ret));
    } else {
      has_dbmesh_hint = true;
      dbmesh_route_info_.testload_ = tmp_int;
    }
  }
  if (OB_SUCC(ret)) {
    tmp_str.assign_ptr(route_info.disaster_status_str_.str_, route_info.disaster_status_str_.str_len_);
    if (tmp_str.empty()) {
      // do nothing
    } else if (OB_UNLIKELY(tmp_str.length() > OBPROXY_MAX_DISASTER_STATUS_LENGTH)) {
      // do nothing, invalid disaster status, no need ret error
      LOG_WARN("invalid disaster status, string length is overflow", K(tmp_str));
    } else {
      has_dbmesh_hint = true;
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
    has_dbmesh_hint = true;
    ObShardColumnNode *tmp_node = route_info.head_;
    fileds_result_.field_num_ = 0;
    fileds_result_.fields_.reuse();
    SqlField field;
    while(tmp_node) {
      field.reset();
      tmp_str.assign_ptr(tmp_node->tb_name_.str_, tmp_node->tb_name_.str_len_);
      const ObString table_name = tmp_str.trim();
      if (is_dual_request_ || table_name.case_compare(table_name_) == 0) {
        tmp_str.assign_ptr(tmp_node->col_name_.str_, tmp_node->col_name_.str_len_);
        field.column_name_.set(tmp_str);
        SqlColumnValue column_value;
        if (DBMESH_TOKEN_STR_VAL == tmp_node->type_) {
          column_value.value_type_ = TOKEN_STR_VAL;
          tmp_str.assign_ptr(tmp_node->col_str_value_.str_, tmp_node->col_str_value_.str_len_);
          column_value.column_value_.set(tmp_str);
          field.column_values_.push_back(column_value);
        } else {
          LOG_INFO("unknown column value type", "token type",
                   get_obproxy_dbmesh_token_type(tmp_node->type_));
        }
        if (field.is_valid() && OB_SUCCESS == fileds_result_.fields_.push_back(field)) {
          ++fileds_result_.field_num_;
        }
      }
      tmp_node = tmp_node->next_;
    }
  }

  if (OB_SUCC(ret) && !has_dbmesh_hint) {
    //set dbp_route_info
    if (dbp_route_info.has_group_info_) {
      use_dbp_hint_ = true;
      dbp_route_info_.has_group_info_ = dbp_route_info.has_group_info_;
      tmp_str.assign_ptr(dbp_route_info.group_idx_str_.str_, dbp_route_info.group_idx_str_.str_len_);
      if (tmp_str.empty()) {
        //do nothing
      } else if (OB_FAIL(get_int_value(tmp_str, tmp_int))) {
        LOG_WARN("fail to get int value for group index", K(ret));
      } else {
        dbp_route_info_.group_idx_ = tmp_int;
      }
      if (OB_SUCC(ret)) {
        tmp_str.assign_ptr(dbp_route_info.table_name_.str_, dbp_route_info.table_name_.str_len_);
        if (tmp_str.empty()) {
          //do nothing
        } else if (OB_UNLIKELY(tmp_str.length() > OB_MAX_TABLE_NAME_LENGTH)) {
          // do nothing, invalid table name, no need ret error
          LOG_WARN("invalid table name, string length is overflow", K(tmp_str));
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
    if (OB_SUCC(ret) && dbp_route_info.has_shard_key_) {
      use_dbp_hint_ = true;
      fileds_result_.field_num_ = 0;
      fileds_result_.fields_.reuse();
      dbp_route_info_.has_shard_key_ = dbp_route_info.has_shard_key_;
      SqlField field;
      for (int i = 0; OB_SUCC(ret) && i < dbp_route_info.shard_key_count_;i++) {
        SqlColumnValue column_value;
        field.reset();
        field.column_name_.set_string(dbp_route_info.shard_key_infos_[i].left_str_.str_,
            dbp_route_info.shard_key_infos_[i].left_str_.str_len_);
        column_value.column_value_.set_string(dbp_route_info.shard_key_infos_[i].right_str_.str_,
            dbp_route_info.shard_key_infos_[i].right_str_.str_len_);
        column_value.value_type_ = TOKEN_STR_VAL;
        field.column_values_.push_back(column_value);
        if (field.is_valid() && OB_SUCC(fileds_result_.fields_.push_back(field))) {
          ++fileds_result_.field_num_;
        }
      }
    }
    if (OB_SUCC(ret) && use_dbp_hint_) {
      if (dbp_route_info_.is_error_info()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dbp_route_info is invalid", K(dbp_route_info_), K(ret));
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
    SetVarNode var_node;
    while(tmp_node) {
      var_node.reset();

      var_node.var_type_ = tmp_node->type_;

      tmp_str.assign_ptr(tmp_node->name_.str_, tmp_node->name_.str_len_);
      var_node.var_name_.set(tmp_str);

      var_node.value_type_ = tmp_node->value_type_;
      if (SET_VALUE_TYPE_INT == tmp_node->value_type_) {
        var_node.int_value_ = tmp_node->int_value_;
        // float will be coverted to double
      } else if (SET_VALUE_TYPE_NUMBER == tmp_node->value_type_) {
        tmp_str.assign_ptr(tmp_node->str_value_.str_, tmp_node->str_value_.str_len_);
        var_node.str_value_.set(tmp_str);
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
            var_node.str_value_.set_with_squote(tmp_str);
          } else if (OBPROXY_QUOTE_T_DOUBLE == tmp_node->str_value_.quote_type_) {
            var_node.str_value_.set_with_dquote(tmp_str);
          }
        } else {
          tmp_str.assign_ptr(tmp_node->str_value_.str_, tmp_node->str_value_.str_len_);
          var_node.str_value_.set(tmp_str);
        }
      }

      if (OB_SUCCESS == set_info_.var_nodes_.push_back(var_node)) {
        ++set_info_.node_count_;
      }

      tmp_node = tmp_node->next_;
    }
  }

  return ret;
}

int ObSqlParseResult::set_text_ps_execute_info(const ObProxyTextPsExecuteParseInfo &execute_parse_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_text_ps_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not text ps stmt", K(ret));
  } else if (OB_UNLIKELY(execute_parse_info.node_count_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(execute_parse_info.node_count_), K(ret));
  } else if (execute_parse_info.node_count_ > 0) {
    text_ps_execute_info_.is_param_valid_ = true;
    text_ps_execute_info_.param_count_ = execute_parse_info.node_count_;
    ObProxyTextPsExecuteParam tmp_param;
    ObProxyTextPsExecuteParseNode *tmp_node = execute_parse_info.head_;
    while (OB_SUCC(ret) && OB_NOT_NULL(tmp_node)) {
      tmp_param.reset();
      const ObString tmp_string(tmp_node->str_value_.str_len_, tmp_node->str_value_.str_);
      tmp_param.str_value_.set(tmp_string);

      if (OB_FAIL(text_ps_execute_info_.params_.push_back(tmp_param))) {
        LOG_WARN("fail to push back text ps execute info", K(tmp_param), K(ret));
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
  has_shard_comment_ = parse_result.has_shard_comment_;
  has_last_insert_id_ = parse_result.has_last_insert_id_;
  hint_query_timeout_ = parse_result.query_timeout_;
  has_anonymous_block_ = parse_result.has_anonymous_block_;
  stmt_type_ = parse_result.stmt_type_;
  cmd_sub_type_ = parse_result.sub_stmt_type_;
  hint_consistency_level_ = static_cast<ObConsistencyLevel>(parse_result.read_consistency_type_);
  parsed_length_ = static_cast<int64_t>(parse_result.end_pos_ - parse_result.start_pos_);
  text_ps_inner_stmt_type_ = parse_result.text_ps_inner_stmt_type_;

  if (NULL != parse_result.table_info_.table_name_.str_ && parse_result.table_info_.table_name_.str_len_ > 0) {
    dbmesh_route_info_.tb_pos_ = parse_result.table_info_.table_name_.str_ - parse_result.start_pos_;
  }

  if (NULL != parse_result.trace_id_.str_
      && 0 < parse_result.trace_id_.str_len_
      && OB_MAX_OBPROXY_TRACE_ID_LENGTH > parse_result.trace_id_.str_len_) {
    MEMCPY(trace_id_buf_, parse_result.trace_id_.str_, parse_result.trace_id_.str_len_);
    trace_id_.assign_ptr(trace_id_buf_, parse_result.trace_id_.str_len_);
  }

  if (NULL != parse_result.rpc_id_.str_
      && 0 < parse_result.rpc_id_.str_len_
      && OB_MAX_OBPROXY_TRACE_ID_LENGTH > parse_result.rpc_id_.str_len_) {
    MEMCPY(rpc_id_buf_, parse_result.rpc_id_.str_, parse_result.rpc_id_.str_len_);
    rpc_id_.assign_ptr(rpc_id_buf_, parse_result.rpc_id_.str_len_);
  }

  // if is dml stmt, then set db/table name
  if (is_dml_stmt() || is_call_stmt() || (is_text_ps_stmt() && is_text_ps_inner_dml_stmt())
      || is_show_create_table_stmt() || is_desc_table_stmt()) {
    if (OB_FAIL(set_db_table_name(parse_result.table_info_.database_name_,
                                  parse_result.table_info_.package_name_,
                                  parse_result.table_info_.table_name_,
                                  parse_result.table_info_.alias_name_,
                                  use_lower_case_name,
                                  drop_origin_db_table_name))) {
      LOG_WARN("failed to set db table name", K(use_lower_case_name), K(ret));
    } else if (OB_FAIL(set_dbmesh_route_info(parse_result))) {
      LOG_WARN("fail to set dbmesh route info", K(ret));
      if (is_sharding_request) {
        stmt_type_ = OBPROXY_T_INVALID;
      }
    } else if (is_call_stmt()) {
      if (OB_FAIL(set_call_prarms(parse_result.call_parse_info_))) {
        LOG_WARN("failed to set_call_prarms", K(ret));
      }
    }
    if (OB_SUCC(ret) && parse_result.has_simple_route_info_) {
      if (OB_FAIL(set_simple_route_info(parse_result))) {
        LOG_WARN("failed to set_simple_route_info", K(ret));
      }
    }
    if (OB_SUCC(ret) && parse_result.part_name_.str_len_ > 0) {
      if (OB_FAIL(set_part_name(parse_result.part_name_))) {
        LOG_WARN("fail to set part name", K(ret));
      }
    }
  } else if (is_use_db_stmt()) {
    if (OB_FAIL(set_db_name(parse_result.table_info_.database_name_,
                            use_lower_case_name,
                            drop_origin_db_table_name))) {
      LOG_WARN("fail to set db name", K(ret));
    }
  } else if (is_set_stmt()) {
    if (OB_FAIL(set_var_info(parse_result))) {
      LOG_WARN("fail to set var info", K(ret));
    }
  } else if (is_show_topology_stmt()) {
    ObString tmp_string(parse_result.cmd_info_.string_[0].str_len_, parse_result.cmd_info_.string_[0].str_);
    if (!tmp_string.empty()) {
      cmd_info_.string_[0].set(tmp_string);
    }
    tmp_string.assign_ptr(parse_result.cmd_info_.string_[1].str_, parse_result.cmd_info_.string_[1].str_len_);
    if (!tmp_string.empty()) {
      cmd_info_.string_[1].set(tmp_string);
    }
  } else if (is_set_route_addr()) {
    // cmd_integer_[0] stores the obproxy_route_addr
    cmd_info_.integer_[0] = parse_result.cmd_info_.integer_[0];
  } else if (is_internal_cmd() || is_ping_proxy_cmd()) {
    cmd_sub_type_ = parse_result.cmd_info_.sub_type_;
    cmd_err_type_ = parse_result.cmd_info_.err_type_;
    for (int64_t i = 0; OB_SUCC(ret) && i < OBPROXY_ICMD_MAX_VALUE_COUNT; ++i) {
      cmd_info_.integer_[i] = parse_result.cmd_info_.integer_[i];
      const ObString tmp_string(parse_result.cmd_info_.string_[i].str_len_, parse_result.cmd_info_.string_[i].str_);
      cmd_info_.string_[i].set(tmp_string);
    }
  } else if (is_internal_select()) {
    if (OB_FAIL(set_col_name(parse_result.col_name_))) {
      LOG_WARN("failed to set col name", K(ret));
    }
  } else {
    LOG_DEBUG("not a dml sql", K(ret));
  }

  if (OB_SUCC(ret) && is_text_ps_stmt()) {
    if (OB_ISNULL(parse_result.text_ps_name_.str_) || 0 == parse_result.text_ps_name_.str_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("text ps name is null", K(ret));
    } else {
      MEMCPY(text_ps_buf_.text_ps_name_buf_, parse_result.text_ps_name_.str_, parse_result.text_ps_name_.str_len_);
      text_ps_name_.assign_ptr(text_ps_buf_.text_ps_name_buf_, parse_result.text_ps_name_.str_len_);
      if (OBPROXY_T_TEXT_PS_EXECUTE == parse_result.stmt_type_) {
        if (OB_FAIL(set_text_ps_execute_info(parse_result.text_ps_execute_parse_info_))) {
          LOG_WARN("fail to set text ps execute info", K(ret));
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
       K_(has_shard_comment),
       K_(has_simple_route_info),
       K_(parsed_length),
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
  J_KV("type", get_obproxy_call_token_type(type_),
       K_(str_value));
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

int64_t ObProxyTextPsExecuteParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(str_value));
  J_OBJ_END();
  return pos;
}

int64_t ObProxyTextPsExecuteInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_param_valid), K_(param_count), "params", params_);
  J_OBJ_END();
  return pos;
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
  J_KV(K_(testload), K_(group_idx), K_(tb_idx), K_(es_idx),
       K_(tb_pos), K_(index_tb_pos), K_(table_name), K_(disaster_status), K_(tnt_id));
  J_OBJ_END();
  return pos;
}

int64_t SetVarNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(var_type),
       K_(var_name));
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
  if (NULL == arena_allocator) {
    if (NULL == (arena_allocator = new (std::nothrow) ObArenaAllocator(common::ObModIds::OB_PROXY_SQL_PARSE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc arena allocator", K(ret));
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
                                const bool drop_origin_db_table_name /*false*/,
                                const bool is_sharding_request /*false*/)
{
  int ret = OB_SUCCESS;
  //int64_t start_time = ObTimeUtility::current_time();
  ObArenaAllocator *allocator = NULL;
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else if (OB_FAIL(get_parse_allocator(allocator))) {
    LOG_WARN("fail to get parse allocator", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    ObProxyParser obproxy_parser(*allocator, parse_mode);
    ObProxyParseResult obproxy_parse_result;

    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = obproxy_parser.parse(sql, obproxy_parse_result))) {
      LOG_INFO("fail to parse sql, will go on anyway", K(sql), K(tmp_ret));
    } else if (OB_SUCCESS != (tmp_ret = sql_parse_result.load_result(obproxy_parse_result, use_lower_case_name,
                                                                     drop_origin_db_table_name, is_sharding_request))) {
      LOG_INFO("fail to load result, will go on anyway", K(sql), K(use_lower_case_name), K(tmp_ret));
    } else {
      LOG_DEBUG("success to do proxy parse", K(sql_parse_result));
    }

    allocator->reuse();
  }
  //int64_t end_time = ObTimeUtility::current_time();
  //LOG_TRACE("finish parse sql", "total cost time(us)", end_time - start_time,
  //          K(sql), K(sql_parse_result), K(ret));
  if (OB_SUCC(ret) && need_parser_by_obparser(sql_parse_result)) {
    if (OB_FAIL(parse_sql_by_obparser(sql, parse_mode, sql_parse_result))) {
      LOG_WARN("parse_sql_by_obparser failed", K(ret), K(sql));
    }
  }
  return ret;
}

bool ObProxySqlParser::need_parser_by_obparser(ObSqlParseResult &sql_parse_result)
{
  return (sql_parse_result.is_select_stmt() && sql_parse_result.get_dbp_route_info().scan_all_)
         || (sql_parse_result.get_dbmesh_route_info().testload_ != oceanbase::obproxy::dbconfig::TESTLOAD_NON
             && sql_parse_result.get_dbmesh_route_info().testload_ != OBPROXY_MAX_DBMESH_ID)
         ; //ADDED BY FUTURE
}

int ObProxySqlParser::init_ob_parser_node(ObArenaAllocator &allocator, ObParseNode *&ob_node)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = (char*) allocator.alloc(sizeof(ObParseNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate ObParseNode node failed!", "ret", ret, "size", sizeof(ObParseNode));
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
         LOG_WARN("T_RELATION_FACTOR children node is not 2", K(ret), K(level), K(node->num_child_));
       }  else if (OB_ISNULL(node->children_[1]) || T_IDENT != node->children_[1]->type_
                      || OB_ISNULL(node->children_[1]->str_value_)
                      || 0 >= node->children_[1]->token_len_) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("T_RELATION_FACTOR unexpected table entry", K(ret), K(level), K(node->children_[1]));
       } else if (FALSE_IT(need_skip_children = true)) {
         // not to be here
       } else if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
           LOG_WARN("init ObParseNode failed", K(ret), K(level));
       } else if (FALSE_IT(pn->set_node(node))) {
         // not to be here
       } else if (OB_FAIL(relation_table_node.push_back(pn))) {
         LOG_WARN("T_RELATION_FACTOR fail add table entry", "ret", ret, "level", level, "node", pn);
       } else if (OB_ISNULL(buf = (char*) allocator.alloc(node->children_[1]->token_len_))) {
           ret = OB_ALLOCATE_MEMORY_FAILED;
           LOG_WARN("allocate ObParseNode node failed!", "ret", ret, "size", sizeof(ObParseNode));
       } else {
         MEMCPY(buf, node->children_[1]->str_value_, node->children_[1]->token_len_);
         buf[node->children_[1]->token_len_] = '\0';
         ObString origin_str = ObString::make_string(buf);
         string_to_upper_case(origin_str.ptr(), origin_str.length());
         if (OB_FAIL(all_table_map.set_refactored(origin_str, pn))) { /* same table keep last one. */
           LOG_WARN("T_RELATION_FACTOR fail add node", K(ret), K(level), K(origin_str), K(pn), K(all_table_map.size()));
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
         LOG_WARN("T_COLUMN_REF children node is not 3", K(ret), K(node->num_child_));
       } else if (OB_ISNULL(node->children_[2])
                      || (T_IDENT != node->children_[2]->type_
                      && T_STAR != node->children_[2]->type_)) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("T_COLUMN_REF unexpected column entry", K(ret), K(node->children_[2]->type_), K(node->children_[1]));
       } else if (FALSE_IT(need_skip_children = true)) {
         // not to be here
       } else if (OB_ISNULL(node->children_[1])
                      || OB_ISNULL(node->children_[1]->str_value_)
                      || 0 >= node->children_[1]->token_len_) { // table entry is NULL, skip directly
         LOG_DEBUG("skip column without table info", K(ret));
       } else if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
         LOG_WARN("init ObParseNode failed", K(ret));
       } else if (FALSE_IT(pn->set_node(node))) {
         // not to be here
       } else if (OB_FAIL(relation_table_node.push_back(pn))) {
         LOG_WARN("T_COLUMN_REF fail add column entry", "ret", ret, "node", pn);
       } else {
         LOG_DEBUG("T_COLUMN_REF  add column entry", "ret", ret, "node", pn);
       }
       break;
     }
     case T_ALIAS: {
       if (OB_T_ALIAS_CLUMN_NAME_NUM_CHILD /* 2 */ != node->num_child_
                  && OB_T_ALIAS_TABLE_NAME_NUM_CHILD /* 5 */ != node->num_child_) {
         ret = OB_ERR_UNEXPECTED;
         LOG_WARN("T_ALIAS children node is not 2 or 5", K(ret), K(node->num_child_));
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
           LOG_WARN("T_ALIAS children node is not 2", K(ret), K(node->num_child_));
         } else {
           // alias not need to change it to upper style
           // string_to_upper_case(node->children_[1]->str_value_, node->children_[1]->token_len_);
           // set_refactored / get_refactored
           ObString str = ObString::make_string(node->children_[1]->str_value_);
           if (OB_FAIL(init_ob_parser_node(allocator, pn))) {
             LOG_WARN("init ObParseNode failed", K(ret));
           } else if (FALSE_IT(pn->set_node(node))) {
             // not to be here
           } else if (OB_FAIL(alias_table_map.set_refactored(str, pn))) {
               LOG_WARN("T_ALIAS fail add node", K(str), K(pn), K(alias_table_map.size()), K(ret));
           } else {
             LOG_DEBUG("T_ALIAS add node", K(str), K(pn), K(alias_table_map.size()), K(ret));
           }
           if (OB_SUCC(ret) && OB_FAIL(init_ob_parser_node(allocator, pn))) {
             LOG_WARN("init ObParseNode failed", K(ret));
           } else if (FALSE_IT(pn->set_node(node->children_[0]))) {
             // not to be here
           } else if (OB_FAIL(relation_table_node.push_back(pn))) {
             LOG_WARN("T_RELATION_FACTOR fail add node", K(str), K(pn), K(relation_table_node.count()),
                        K(ret));
           } else if (OB_ISNULL(node->children_[0]) || OB_ISNULL(node->children_[0]->children_[1])
                        || 0 >= node->children_[0]->children_[1]->token_len_
                        || OB_ISNULL(node->children_[0]->children_[1]->str_value_)) {
             ret = OB_ERR_UNEXPECTED;
             LOG_WARN("T_RELATION_FACTOR is invalid", K(ret));
           } else if (OB_ISNULL(buf = (char*) allocator.alloc(node->children_[0]->children_[1]->token_len_))) {
               ret = OB_ALLOCATE_MEMORY_FAILED;
               LOG_WARN("allocate ObParseNode node failed!", "ret", ret, "size", sizeof(ObParseNode));
           } else {

             MEMCPY(buf, node->children_[0]->children_[1]->str_value_, node->children_[0]->children_[1]->token_len_);
             buf[node->children_[0]->children_[1]->token_len_] = '\0';
             ObString origin_str = ObString::make_string(buf);
             string_to_upper_case(origin_str.ptr(), origin_str.length()); // change all to upper to store

             if (OB_FAIL(all_table_map.set_refactored(origin_str, pn))) { /* same table keep last one. */
               LOG_WARN("T_RELATION_FACTOR fail add node", K(ret), K(origin_str));
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
         LOG_WARN("init ObParseNode failed", K(ret));
       } else if (FALSE_IT(pn->set_node(node))) {
         // not to be here
       } else if (OB_FAIL(hint_option_list.push_back(pn))) {
           LOG_WARN("T_HINT_OPTION_LIST fail add node", K(ret), K(pn));
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
           LOG_WARN("ob_load_testload_parse_node fail", K(ret), K(node->children_[i]));
         }
       }
     }
   }

  return ret;
}

int ObSqlParseResult::get_result_tree_str(ParseNode *root, const int level, char* buf, int& pos, int64_t length)
{
  int ret = OB_SUCCESS;
  if (NULL == root || NULL == buf || length < 0) {
    return ret;
  } else {
    for (int i = 0 ; i < level *2; i++) {
      pos += snprintf(buf + pos, length - pos, "-");
    }
    pos += snprintf (buf + pos, length - pos, " %s %s:pos:%ld, text_len:%ld, num_child_:%d, token_off:%d, token_len:%d\n",
                     get_type_name(root->type_),
                     root->str_value_, root->pos_,
                     root->text_len_, root->num_child_,
                     root->token_off_, root->token_len_);
    for (int i = 0; OB_SUCC(ret) && i < root->num_child_; i++) {
      if (NULL == root->children_[i]) {
        for (int i = 0 ; i < (level + 1) *2; i++) {
          pos += snprintf(buf + pos, length - pos, "-");
        }
        pos += snprintf(buf + pos, length - pos, " There is one NULL child node\n");
      } else if (OB_FAIL(get_result_tree_str(root->children_[i], level + 1, buf, pos, length - pos))) {
        LOG_WARN("get_result_tree_str failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSqlParseResult::ob_parse_resul_to_string(const ParseResult &parse_result, const common::ObString& sql,
                                               char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int pos = 0;
  if (OB_FAIL(get_result_tree_str(parse_result.result_tree_, 0, buf, pos, buf_len))) {
    LOG_WARN("get_result_tree_str failed", K(ret), K(sql));
  } else if (parse_result.comment_list_ != NULL) {
    for (int j = 0; j < parse_result.comment_cnt_; j++) {
      TokenPosInfo& token_info = parse_result.comment_list_[j];
      pos += snprintf(buf + pos, buf_len - pos, "comment[%d] is %.*s\n", j,
          token_info.token_len_, sql.ptr()+ token_info.token_off_);
    }
  }
  return ret;
}

int ObSqlParseResult::load_ob_parse_result(const ParseResult &parse_result,
                                           const common::ObString& sql)
{
  int ret = OB_SUCCESS;
  const int64_t buf_len = 64 * 1024;
  char tree_str_buf[buf_len];
  //just for debug , ignore ret
  ob_parse_resul_to_string(parse_result, sql, tree_str_buf, buf_len);
  ObString tree_str(buf_len, tree_str_buf);
  LOG_DEBUG("result_tree_ is \n", K(tree_str));
  ParseNode* node = parse_result.result_tree_;
  ObProxySelectStmt* select_stmt= NULL;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is unexpected null", K(ret));
  } else if (T_STMT_LIST != node->type_ || node->num_child_ < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", "node_type", get_type_name(node->type_), K(node->num_child_), K(ret));
  } else if (OB_ISNULL(node = node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_DEBUG("unexpected null child", K(ret));
  } else {
    char* buf = NULL;
    switch(node->type_) {
      case T_SELECT:
        if (OB_ISNULL(buf = (char*)allocator_.alloc(sizeof(ObProxySelectStmt)))) {
          LOG_WARN("failed to alloc buf");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_ISNULL(select_stmt = new (buf) ObProxySelectStmt())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to new ObProxySelectStmt", K(ret));
        } else if (OB_FAIL(select_stmt->init())) {
          LOG_WARN("init failed", K(ret));
        } else {
          select_stmt->set_sql_string(sql);
          select_stmt->set_stmt_type(OBPROXY_T_SELECT);
          select_stmt->set_allocator(&allocator_);
          select_stmt->field_results_ = &fileds_result_;
          proxy_stmt_ = select_stmt;
          if (OB_FAIL(proxy_stmt_->handle_parse_result(parse_result))) {
            LOG_WARN("handle_parse_result failed", K(ret));
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
                                            ObSqlParseResult &sql_parse_result)
{
  int ret = OB_SUCCESS;
  //int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(sql.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sql", K(sql), K(ret));
  } else {
    ObProxyParser obproxy_parser(sql_parse_result.allocator_, parse_mode);
    char *buf = NULL;
    ParseResult *parser_result = NULL;
    if (OB_ISNULL(buf = (char*)sql_parse_result.allocator_.alloc(sizeof(ParseResult)))) {
      LOG_WARN("failed to alloc buf");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(parser_result = new (buf) ParseResult())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ParseResult", K(ret));
    } else {
      ParseResult &ob_parse_result = *parser_result;

      if (OB_FAIL(obproxy_parser.obparse(sql, ob_parse_result))) {
        LOG_INFO("fail to parse sql", K(sql), K(ret));
      } else if (OB_FAIL(sql_parse_result.load_ob_parse_result(ob_parse_result, sql))) {
        LOG_WARN("fail to load_ob_parse_result", K(sql), K(ret));
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

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
