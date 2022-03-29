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

#ifndef OBPROXY_SQL_PARSER_H
#define OBPROXY_SQL_PARSER_H
#include "lib/ob_define.h"
#include "opsql/parser/ob_proxy_parse_result.h"
#include "opsql/expr_parser/ob_expr_parse_result.h"
#include "opsql/expr_parser/ob_expr_parser_utils.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_sql_string.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_hint.h"
#include "lib/utility/ob_print_utils.h"
#include "utils/ob_proxy_lib.h"
#include "obutils/ob_proxy_string_utils.h"
#include <ob_sql_parser.h>
#include <parse_malloc.h>
#include <parse_node.h>

#define OBPROXY_MAX_STRING_VALUE_LENGTH 512

#define OBPROXY_CALL_NODE_COUNT 20
#define OBPROXY_TEXT_PS_EXECUTE_NODE_COUNT 20

using namespace oceanbase::obproxy::opsql;

namespace oceanbase
{
namespace common
{
class ObString;
class ObArenaAllocator;
}
namespace obproxy
{
namespace obutils
{
class ObCachedVariables;
class ObParseNode;
class ObProxyStmt;

struct ObDmlBuf {
  char table_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH];
  char package_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH];
  char alias_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH];
  char database_name_buf_[common::OB_MAX_DATABASE_NAME_LENGTH];
};

struct ObPartNameBuf {
  char part_name_buf_[common::OB_MAX_PARTITION_NAME_LENGTH];
};

struct ObInternalSelectBuf {
  char col_name_buf_[common::OB_MAX_COLUMN_NAME_LENGTH];
};

struct ObTextPsBuf {
  char text_ps_name_buf_[common::OB_MAX_TEXT_PS_NAME_LENGTH];
};

template<int size>
class ObFixSizeString {
public:
  ObFixSizeString() : string_() { memset(buf_, 0, size); }
  ~ObFixSizeString() { string_.reset(); }
  int64_t get_max_size() const { return size - 2; }

  ObFixSizeString(const ObFixSizeString &other)
  {
    *this = other;
  }

  ObFixSizeString& operator=(const ObFixSizeString &other)
  {
    if (this != &other) {
      const int32_t min_len = std::min(other.string_.length(), size);
      MEMCPY(buf_, other.buf_, min_len);
      string_.assign_ptr(buf_, min_len);
    }
    return *this;
  }

  void reset()
  {
    string_.reset();
  }

  void set(const common::ObString &other)
  {
    if (other.empty()) {
      string_.reset();
    } else {
      const int32_t min_len = std::min(other.length(), size);
      MEMCPY(buf_, other.ptr(), min_len);
      string_.assign_ptr(buf_, min_len);
    }
  }

  void set_string(const char *str, int32_t str_len)
  {
    if (NULL != str && str_len > 0) {
      const int32_t min_len = std::min(str_len, size);
      MEMCPY(buf_, str, min_len);
      string_.assign_ptr(buf_, min_len);
    } else {
      string_.reset();
    }
  }

  //add " befor and after string
  void set_with_dquote(const common::ObString &other)
  {
    if (other.empty()) {
      string_.reset();
    } else {
      const int32_t min_len = std::min(other.length(), size - 2);
      buf_[0] = '"';
      MEMCPY(buf_ + 1, other.ptr(), min_len);
      buf_[min_len + 1] = '"';
      string_.assign_ptr(buf_, min_len + 2);
    }
  }

  //add ' befor and after string
  void set_with_squote(const common::ObString &other)
  {
    if (other.empty()) {
      string_.reset();
    } else {
      const int32_t min_len = std::min(other.length(), size - 2);
      buf_[0] = '\'';
      MEMCPY(buf_ + 1, other.ptr(), min_len);
      buf_[min_len + 1] = '\'';
      string_.assign_ptr(buf_, min_len + 2);
    }
  }

  void set_integer(const int64_t other)
  {
    const int32_t pos = snprintf(buf_, size, "%ld", other);
    string_.assign_ptr(buf_, pos);
  }

  TO_STRING_KV(K_(string), "len", string_.length());

public:
  common::ObString string_;
  char buf_[size];
};

struct ObProxyCmdInfo
{
  ObProxyCmdInfo() { reset(); }
  DECLARE_TO_STRING;
  void reset()
  {
    int i = 0;
    for (; i < OBPROXY_ICMD_MAX_VALUE_COUNT; i++) {
      integer_[i] = 0;
      string_[0].reset();
    }
  }

  int64_t integer_[OBPROXY_ICMD_MAX_VALUE_COUNT];
  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> string_[OBPROXY_ICMD_MAX_VALUE_COUNT];
};

struct ObProxyCallParam
{
  ObProxyCallParam() { reset(); }
  ~ObProxyCallParam() { reset(); }
  DECLARE_TO_STRING;
  void reset()
  {
    type_ = CALL_TOKEN_NONE;
    str_value_.reset();
  }
  static int32_t get_fix_buf_size() { return OBPROXY_MAX_STRING_VALUE_LENGTH; }

  ObProxyCallTokenType type_;
  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> str_value_;
};

struct ObProxyCallInfo
{
  ObProxyCallInfo() { reset(); }
  ~ObProxyCallInfo() { reset(); }
  DECLARE_TO_STRING;
  ObProxyCallInfo& operator=(const ObProxyCallInfo &other)
  {
    if (this != &other) {
      is_param_valid_ = other.is_param_valid_;
      param_count_ = other.param_count_;
      params_.assign(other.params_);
    }
    return *this;
  }

  void reset()
  {
    param_count_ = 0;
    is_param_valid_ = false;
    params_.reset();
  }
  bool is_valid() const { return is_param_valid_ && 0 <= param_count_; }
  common::ObSEArray<ObProxyCallParam, OBPROXY_CALL_NODE_COUNT> params_;
  int32_t param_count_;
  bool is_param_valid_;//whether size is valid
};

struct ObProxyTextPsExecuteParam
{
  ObProxyTextPsExecuteParam() { reset(); }
  ~ObProxyTextPsExecuteParam() { reset(); }
  DECLARE_TO_STRING;
  void reset()
  {
    str_value_.reset();
  }
  static int32_t get_fix_buf_size() { return OBPROXY_MAX_STRING_VALUE_LENGTH; }

  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> str_value_;
};

struct ObProxyTextPsExecuteInfo
{
  ObProxyTextPsExecuteInfo() { reset(); }
  ~ObProxyTextPsExecuteInfo() { reset(); }
  DECLARE_TO_STRING;
  ObProxyTextPsExecuteInfo& operator=(const ObProxyTextPsExecuteInfo &other)
  {
    if (this != &other) {
      is_param_valid_ = other.is_param_valid_;
      param_count_ = other.param_count_;
      params_.assign(other.params_);
    }
    return *this;
  }

  void reset()
  {
    param_count_ = 0;
    is_param_valid_ = false;
    params_.reset();
  }
  bool is_valid() const { return is_param_valid_ && 0 <= param_count_; }
  common::ObSEArray<ObProxyTextPsExecuteParam, OBPROXY_TEXT_PS_EXECUTE_NODE_COUNT> params_;
  int32_t param_count_;
  bool is_param_valid_;//whether size is valid
};

struct ObProxySimpleRouteInfo
{
  ObProxySimpleRouteInfo() { reset(); }
  ~ObProxySimpleRouteInfo() { reset(); }
  DECLARE_TO_STRING;

  int64_t table_offset_;
  int64_t table_len_;
  int64_t part_key_offset_;
  int64_t part_key_len_;
  char table_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH + 1];
  char part_key_buf_[OBPROXY_MAX_STRING_VALUE_LENGTH + 1];

  bool is_valid() const
  {
    return strlen(table_name_buf_) > 0 && strlen(part_key_buf_) > 0
           && table_offset_ > 0 && table_len_ > 0
           && part_key_offset_ > 0 && part_key_len_ > 0;
  }

  inline void reset()
  {
    table_offset_ = 0;
    table_len_ = 0;
    part_key_offset_ = 0;
    part_key_len_ = 0;
    table_name_buf_[0] = '\0';
    part_key_buf_[0] = '\0';
  }
};

struct SqlColumnValue
{
  SqlColumnValue() { reset(); }
  ~SqlColumnValue() { reset(); }
  void reset() {
    value_type_ = TOKEN_NONE;
    column_int_value_ = 0;
    column_value_.reset();
  }
  TO_STRING_KV(K_(value_type), K(column_int_value_), K(column_value_));
  ObProxyTokenType value_type_;
  int64_t  column_int_value_;
  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> column_value_;
};

struct SqlField {
  SqlField() { reset(); }
  ~SqlField() { reset(); }

  void reset() {
    column_name_.reset();
    value_type_ = TOKEN_NONE;
    column_int_value_ = 0;
    column_value_.reset();
    column_values_.reset();
  }
  DECLARE_TO_STRING;

  bool is_valid() const { return column_values_.count() > 0; }

  SqlField& operator=(const SqlField& other)
  {
    if (this != &other) {
      column_name_ = other.column_name_;
      value_type_ = other.value_type_;
      column_int_value_ = other.column_int_value_;
      column_value_ = other.column_value_;
      for (int i = 0; i < other.column_values_.count(); i++) {
        column_values_.push_back(other.column_values_.at(i));
      }
    }
    return *this;
  }

  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> column_name_;
  common::ObSEArray<SqlColumnValue, 3> column_values_;
  // TODO: erase deprecated
  ObProxyTokenType value_type_;
  int64_t  column_int_value_;
  obutils::ObProxyVariantString column_value_;
};

struct SqlFieldResult {
  SqlFieldResult() : field_num_(0), fields_() {}
  ~SqlFieldResult() { reset(); }

  void reset()
  {
    field_num_ = 0;
    fields_.reset();
  }
  DECLARE_TO_STRING;

  int field_num_;
  common::ObSEArray<SqlField, 5> fields_;
};

struct DbMeshRouteInfo {
  DbMeshRouteInfo() { reset(); }
  ~DbMeshRouteInfo() { reset(); }

  void reset()
  {
    group_idx_ = OBPROXY_MAX_DBMESH_ID;
    tb_idx_ = OBPROXY_MAX_DBMESH_ID;
    es_idx_ = OBPROXY_MAX_DBMESH_ID;
    testload_ = OBPROXY_MAX_DBMESH_ID;
    tb_pos_ = common::OB_INVALID_INDEX;
    index_tb_pos_ = common::OB_INVALID_INDEX;
    table_name_.reset();
    disaster_status_.reset();
    tnt_id_.reset();
  }
  DECLARE_TO_STRING;

  bool is_valid() const
  {
    return OBPROXY_MAX_DBMESH_ID != group_idx_
           || OBPROXY_MAX_DBMESH_ID != tb_idx_
           || OBPROXY_MAX_DBMESH_ID != es_idx_
           || OBPROXY_MAX_DBMESH_ID != testload_
           || !table_name_.empty()
           || !disaster_status_.empty();
  }

  bool use_random_eid() const { return -1 == es_idx_; }

  int64_t group_idx_;
  int64_t tb_idx_;
  int64_t es_idx_;
  int64_t testload_;
  int64_t tb_pos_; // dml table name pos, not hint table pos
  int64_t index_tb_pos_; // index table pos from hint
  common::ObString table_name_;
  char table_name_str_[common::OB_MAX_TABLE_NAME_LENGTH];
  common::ObString disaster_status_;
  char disaster_status_str_[OBPROXY_MAX_DISASTER_STATUS_LENGTH];
  common::ObString tnt_id_;
  char tnt_id_str_[OBPROXY_MAX_TNT_ID_LENGTH];
};

struct SetVarNode {
  SetVarNode() { reset(); }
  ~SetVarNode() { reset(); }

  void reset() {
    var_name_.reset();
    str_value_.reset();
    int_value_ = 0;
    value_type_ = SET_VALUE_TYPE_ONE;
    var_type_ = SET_VAR_TYPE_NONE;
  }
  DECLARE_TO_STRING;

  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> var_name_;

  int64_t  int_value_;
  ObFixSizeString<OBPROXY_MAX_STRING_VALUE_LENGTH> str_value_;

  ObProxySetValueType value_type_;
  ObProxySetVarType var_type_;
};

struct ObProxySetInfo {
  ObProxySetInfo() { reset(); }
  ~ObProxySetInfo() { reset(); }

  void reset()
  {
    node_count_ = 0;
    var_nodes_.reset();
  }
  DECLARE_TO_STRING;

  int node_count_;
  common::ObSEArray<SetVarNode, 5> var_nodes_;
};

struct DbpRouteInfo {
  DbpRouteInfo() { reset(); }
  ~DbpRouteInfo() { reset(); }

  void reset()
  {
    has_group_info_ = false;
    group_idx_ = OBPROXY_MAX_DBMESH_ID;
    scan_all_ = false;
    table_name_.reset();
    has_shard_key_ = false;
  }
  DECLARE_TO_STRING;

  bool is_valid() const
  {
    return !is_error_info();
  }

  bool is_group_info_valid() const {
    // table_name can be empty
    return OBPROXY_MAX_DBMESH_ID != group_idx_ && group_idx_ >= 0 && !scan_all_;
  }

  bool is_shard_key_valid() const {
    return !scan_all_;
  }

  bool is_error_info() const
  {
    bool ret = false;
    if (has_group_info_ && has_shard_key_) {
      ret = true;
    } else if (has_group_info_ && !is_group_info_valid()) {
      ret = true;
    } else if (has_shard_key_ && !is_shard_key_valid()) {
      ret = true;
    }
    return ret;
  }

  bool has_group_info_;
  int64_t group_idx_;
  bool scan_all_;
  common::ObString table_name_;
  char table_name_str_[common::OB_MAX_TABLE_NAME_LENGTH];
  bool has_shard_key_;
};
#define OBPROXY_DUAL_MAX_FIELDS_NUM 10

typedef enum ObProxyDualRelationType
{
  F_DUAL_NONE = 0,
  F_EQUAL,
  F_MAX
} ObProxyDualRelationType;

typedef struct _ObProxyDualSelectFieldInfo
{
  common::ObString seq_name_;
  common::ObString seq_field_;
  common::ObString alias_name_;
} ObProxyDualSelectFieldInfo;

typedef struct  _ObProxyDualParseResult
{
  ObProxyDualSelectFieldInfo select_fields_[OBPROXY_DUAL_MAX_FIELDS_NUM];
  int select_fields_size_;
  int where_fields_size_;
  bool need_db_timestamp_;
  bool need_value_;
  ObExprParseResult expr_result_;
} ObProxyDualParseResult;

struct ObSqlParseResult
{
  ObSqlParseResult() : allocator_(common::ObModIds::OB_PROXY_SHARDING_PARSE),
    ob_parser_result_(NULL), proxy_stmt_(NULL) { reset(); }
  ~ObSqlParseResult() { reset(); }
  void clear_proxy_stmt();
  void reset(bool is_reset_origin_db_table = true);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObProxyBasicStmtType get_stmt_type() const { return stmt_type_; }
  ObProxyBasicStmtSubType get_cmd_sub_type() const { return cmd_sub_type_; }
  ObProxyErrorStmtType get_cmd_err_type() const { return cmd_err_type_; }
  bool is_invalid_stmt() const { return OBPROXY_T_INVALID == stmt_type_; }
  bool is_start_trans_stmt() const { return OBPROXY_T_BEGIN == stmt_type_; }
  bool is_select_stmt() const { return OBPROXY_T_SELECT == stmt_type_; }
  bool is_select_database_stmt() const { return OBPROXY_T_SELECT == stmt_type_ && OBPROXY_T_SUB_SELECT_DATABASE == cmd_sub_type_; }
  bool is_update_stmt() const { return OBPROXY_T_UPDATE == stmt_type_; }
  bool is_replace_stmt() const { return OBPROXY_T_REPLACE == stmt_type_; }
  bool is_insert_stmt() const { return OBPROXY_T_INSERT == stmt_type_; }
  bool is_delete_stmt() const { return OBPROXY_T_DELETE == stmt_type_; }
  bool is_merge_stmt() const { return OBPROXY_T_MERGE == stmt_type_; }
  bool is_set_stmt() const { return OBPROXY_T_SET == stmt_type_; }
  bool is_set_names_stmt() const { return OBPROXY_T_SET_NAMES == stmt_type_; }
  bool is_use_db_stmt() const { return OBPROXY_T_USE_DB == stmt_type_; }
  bool is_call_stmt() const { return OBPROXY_T_CALL == stmt_type_; }
  bool is_help_stmt() const { return OBPROXY_T_HELP == stmt_type_; }
  bool is_multi_stmt() const { return OBPROXY_T_MULTI_STMT == stmt_type_; }
  bool is_show_warnings_stmt() const { return OBPROXY_T_SHOW_WARNINGS == stmt_type_; }
  bool is_show_errors_stmt() const { return OBPROXY_T_SHOW_ERRORS == stmt_type_; }
  bool is_show_trace_stmt() const { return OBPROXY_T_SHOW_TRACE == stmt_type_; }
  bool is_show_session_stmt() const { return OBPROXY_T_ICMD_SHOW_SESSION == stmt_type_; }
  bool is_select_tx_ro() const { return OBPROXY_T_SELECT_TX_RO == stmt_type_; }
  bool is_select_proxy_version() const { return OBPROXY_T_SELECT_PROXY_VERSION == stmt_type_; }
  bool is_set_autocommit_0() const { return OBPROXY_T_SET_AC_0 == stmt_type_; }
  bool is_select_route_addr() const { return OBPROXY_T_SELECT_ROUTE_ADDR == stmt_type_; }
  bool is_set_route_addr() const { return OBPROXY_T_SET_ROUTE_ADDR == stmt_type_; }
  bool is_set_ob_read_consistency() const { return OBPROXY_T_SET_OB_READ_CONSISTENCY == stmt_type_; }
  bool is_set_tx_read_only() const { return OBPROXY_T_SET_TX_READ_ONLY == stmt_type_; }
  bool is_commit_stmt() const { return OBPROXY_T_COMMIT == stmt_type_; }
  bool is_rollback_stmt() const { return OBPROXY_T_ROLLBACK == stmt_type_; }
  bool is_show_stmt() const
  {
    return (OBPROXY_T_SHOW == stmt_type_
            || is_show_warnings_stmt()
            || is_show_errors_stmt()
            || is_show_trace_stmt());
  }
  bool is_show_databases_stmt() const { return OBPROXY_T_SHOW == stmt_type_ && OBPROXY_T_SUB_SHOW_DATABASES == cmd_sub_type_; }
  bool is_show_tables_stmt() const { return OBPROXY_T_SHOW == stmt_type_ && OBPROXY_T_SUB_SHOW_TABLES == cmd_sub_type_; }
  bool is_show_create_table_stmt() const { return OBPROXY_T_SHOW == stmt_type_ && OBPROXY_T_SUB_SHOW_CREATE_TABLE == cmd_sub_type_; }
  bool is_show_topology_stmt() const { return OBPROXY_T_SHOW == stmt_type_ && OBPROXY_T_SUB_SHOW_TOPOLOGY == cmd_sub_type_; }
  bool is_show_db_version_stmt() const { return OBPROXY_T_SHOW == stmt_type_ && OBPROXY_T_SUB_SHOW_DB_VERSION == cmd_sub_type_; }
  bool is_desc_table_stmt() const { return OBPROXY_T_DESC == stmt_type_ && OBPROXY_T_SUB_DESC_TABLE == cmd_sub_type_; }
  bool is_desc_stmt() const { return OBPROXY_T_DESC == stmt_type_; }
  bool is_create_stmt() const { return OBPROXY_T_CREATE == stmt_type_; }
  bool is_drop_stmt() const { return OBPROXY_T_DROP == stmt_type_; }
  bool is_alter_stmt() const { return OBPROXY_T_ALTER == stmt_type_; }
  bool is_truncate_stmt() const { return OBPROXY_T_TRUNCATE == stmt_type_; }
  bool is_rename_stmt() const { return OBPROXY_T_RENAME == stmt_type_; }
  bool is_grant_stmt() const { return OBPROXY_T_GRANT == stmt_type_; }
  bool is_revoke_stmt() const { return OBPROXY_T_REVOKE == stmt_type_; }
  bool is_purge_stmt() const { return OBPROXY_T_PURGE == stmt_type_; }
  bool is_analyze_stmt() const { return OBPROXY_T_ANALYZE == stmt_type_; }
  bool is_comment_stmt() const { return OBPROXY_T_COMMENT == stmt_type_; }
  bool is_flashback_stmt() const { return OBPROXY_T_FLASHBACK == stmt_type_; }
  bool is_audit_stmt() const { return OBPROXY_T_AUDIT == stmt_type_; }
  bool is_noaudit_stmt() const { return OBPROXY_T_NOAUDIT == stmt_type_; }
  bool is_ddl_stmt() const
  {
    return is_create_stmt()
           || is_drop_stmt()
           || is_alter_stmt()
           || is_truncate_stmt()
           || is_rename_stmt()
           || is_grant_stmt()
           || is_revoke_stmt()
           || is_purge_stmt()
           || is_analyze_stmt()
           || is_comment_stmt()
           || is_flashback_stmt()
           || is_audit_stmt()
           || is_noaudit_stmt();
  }

  bool is_text_ps_stmt() const { return is_text_ps_prepare_stmt() || is_text_ps_execute_stmt(); }
  bool is_text_ps_prepare_stmt() const { return OBPROXY_T_TEXT_PS_PREPARE == stmt_type_; }
  bool is_text_ps_execute_stmt() const { return OBPROXY_T_TEXT_PS_EXECUTE == stmt_type_; }

  bool is_internal_select() const { return is_select_tx_ro() || is_select_proxy_version(); }
  bool is_dual_request() const {return is_dual_request_;}
  bool is_internal_request() const;
  bool is_dml_stmt() const;
  bool is_text_ps_inner_dml_stmt() const;
  bool is_text_ps_select_stmt() const;
  bool is_text_ps_delete_stmt() const;
  bool is_text_ps_insert_stmt() const;
  bool is_text_ps_replace_stmt() const;
  bool is_text_ps_update_stmt() const;
  bool is_text_ps_merge_stmt() const;
  bool is_text_ps_call_stmt() const;
  bool is_write_stmt() const;
  bool is_internal_cmd() const;
  bool is_kill_query_cmd() const;
  bool is_kill_session_cmd() const;
  bool is_ping_proxy_cmd() const { return OBPROXY_T_PING_PROXY == stmt_type_; }
  bool is_internal_error_cmd() const { return OBPROXY_T_ERR_INVALID != cmd_err_type_; }
  bool is_mysql_compatible_cmd() const;
  bool is_need_resp_ok_cmd() const;

  bool is_shard_special_cmd() const;

  bool has_last_insert_id() const { return has_last_insert_id_; }
  bool has_found_rows() const { return has_found_rows_; }
  bool has_row_count() const { return has_row_count_; }
  bool has_explain() const { return has_explain_; }
  bool has_simple_route_info() const { return has_simple_route_info_; }
  bool has_shard_comment() const { return has_shard_comment_; }
  bool has_anonymous_block() const { return has_anonymous_block_; }
  bool has_for_update() const { return has_for_update_; }

  bool is_simple_route_info_valid() const { return route_info_.is_valid(); }

  bool has_show_errors() const { return is_show_errors_stmt(); }
  bool has_show_warnings() const { return is_show_warnings_stmt(); }

  bool need_hold_start_trans() const { return is_start_trans_stmt(); }
  // has a function depend on the sql last executed, such as found_rows , row_count, etc.
  bool has_dependent_func() const;
  // whether a sql is not supported by PROXY (BUT it is supported by observer)
  bool is_not_supported() const;

  int64_t get_hint_query_timeout() const { return hint_query_timeout_; }
  common::ObConsistencyLevel get_hint_consistency_level() const { return hint_consistency_level_; }
  int64_t get_parsed_length() const { return parsed_length_; }
  const common::ObString get_table_name() const { return table_name_; }
  const common::ObString get_package_name() const { return package_name_; }
  const common::ObString get_database_name() const { return database_name_; }
  const common::ObString get_origin_table_name() const { return origin_table_name_; }
  const common::ObString get_origin_database_name() const { return origin_database_name_;}
  const common::ObString get_col_name() const { return col_name_; }
  const common::ObString get_alias_name() const { return alias_name_; }
  const common::ObString get_part_name() const { return part_name_; }
  const common::ObString get_text_ps_name() const { return text_ps_name_; }
  ObProxyParseQuoteType get_table_name_quote() const { return table_name_quote_; }
  ObProxyParseQuoteType get_package_name_quote() const { return package_name_quote_; }
  ObProxyParseQuoteType get_database_name_quote() const { return database_name_quote_; }
  ObProxyParseQuoteType get_alias_name_quote() const { return alias_name_quote_; }
  ObProxyParseQuoteType get_col_name_quote() const { return col_name_quote_; }
  const common::ObString get_trace_id() const { return trace_id_; }
  const common::ObString get_rpc_id() const { return rpc_id_; }

  char *get_table_name_str() { return table_name_.ptr(); }
  int64_t get_table_name_length() const { return table_name_.length(); }
  char *get_package_name_str() { return package_name_.ptr(); }
  int64_t get_package_name_length() const { return package_name_.length(); }
  char *get_database_name_str() { return database_name_.ptr(); }
  int64_t get_database_name_length() const { return database_name_.length(); }
  char *get_col_name_str() { return col_name_.ptr(); }
  int64_t get_col_name_length() const { return col_name_.length(); }
  char *get_alias_name_str() { return alias_name_.ptr(); }
  int64_t get_alias_name_length() const { return alias_name_.length(); }
  char *get_part_name_str() { return part_name_.ptr(); }
  int64_t get_part_name_length() const { return part_name_.length(); }
  char *get_text_ps_name_str() { return text_ps_name_.ptr(); }
  int64_t get_text_ps_name_length() const { return text_ps_name_.length(); }

  void set_stmt_type(const ObProxyBasicStmtType type) { stmt_type_ = type; }
  void set_err_stmt_type(const ObProxyErrorStmtType type) { cmd_err_type_ = type; }
  int set_db_name(const ObProxyParseString &database_name,
                  const bool use_lower_case_name = false,
                  const bool drop_origin_db_table_name = false);
  int set_db_table_name(const ObProxyParseString &database_name, const ObProxyParseString &package_name,
                        const ObProxyParseString &table_name, const ObProxyParseString &alias_name,
                        const bool use_lower_case_name = false, const bool save_origin_db_table_name = false);
  int set_real_table_name(const char *table_name, int64_t len);
  int set_col_name(const ObProxyParseString &col_name);
  int set_call_prarms(const ObProxyCallParseInfo &call_parse_info);
  int set_part_name(const ObProxyParseString &part_name);
  int set_simple_route_info(const ObProxyParseResult &parse_result);
  int set_dbmesh_route_info(const ObProxyParseResult &obproxy_parse_result);
  int set_var_info(const ObProxyParseResult &parse_result);
  int set_text_ps_execute_info(const ObProxyTextPsExecuteParseInfo &execute_parse_info);
  int load_result(const ObProxyParseResult &obproxy_parse_result,
                  const bool use_lower_case_name = false,
                  const bool save_origin_db_table_name = false,
                  const bool is_sharding_request = false);
  ObProxyBasicStmtType get_text_ps_inner_stmt_type() const { return text_ps_inner_stmt_type_; }

  int load_ob_parse_result(const ParseResult &parse_result,
                           const common::ObString& sql,
                           const bool need_handle_result);
  static int ob_parse_resul_to_string(const ParseResult &parse_result, const common::ObString& sql,
                                      char* buf, int64_t buf_len, int64_t &pos);
  static int get_result_tree_str(ParseNode *root, const int level, char* buf, int64_t &pos, int64_t length);
  SqlFieldResult& get_sql_filed_result() { return fileds_result_; }
  int64_t get_batch_insert_values_count() { return batch_insert_values_count_; }
  void set_batch_insert_values_count(int64_t count) { batch_insert_values_count_ = count; }
  DbMeshRouteInfo& get_dbmesh_route_info() { return dbmesh_route_info_; }
  DbpRouteInfo& get_dbp_route_info() { return dbp_route_info_; }
  bool is_use_dbp_hint() {return use_dbp_hint_;}
  ObProxySetInfo& get_set_info() { return set_info_; }
  ObProxyDualParseResult& get_dual_result() {return dual_result_;}
  ParseResult* get_ob_parser_result() { return ob_parser_result_; }
  ObProxyStmt* get_proxy_stmt() { return proxy_stmt_; };

  ObSqlParseResult& operator=(const ObSqlParseResult &other)
  {
    if (this != &other) {
      cmd_info_ = other.cmd_info_;
      call_info_ = other.call_info_;
      route_info_ = other.route_info_;
      text_ps_execute_info_ = other.text_ps_execute_info_;
      has_last_insert_id_ = other.has_last_insert_id_;
      has_found_rows_ = other.has_found_rows_;
      has_row_count_ = other.has_row_count_;
      has_explain_ = other.has_explain_;
      has_simple_route_info_ = other.has_simple_route_info_;
      has_anonymous_block_ = other.has_anonymous_block_;
      has_for_update_ = other.has_for_update_;
      stmt_type_ = other.stmt_type_;
      hint_query_timeout_ = other.hint_query_timeout_;
      parsed_length_ = other.parsed_length_;
      cmd_sub_type_ = other.cmd_sub_type_;
      cmd_err_type_ = other.cmd_err_type_;
      hint_consistency_level_ = other.hint_consistency_level_;
      dml_buf_ = other.dml_buf_;
      internal_select_buf_ = other.internal_select_buf_;
      text_ps_buf_ = other.text_ps_buf_;
      part_name_buf_ = other.part_name_buf_;
      table_name_quote_ = other.table_name_quote_;
      package_name_quote_ = other.package_name_quote_;
      database_name_quote_ = other.database_name_quote_;
      alias_name_quote_ = other.alias_name_quote_;
      col_name_quote_ = other.col_name_quote_;
      text_ps_inner_stmt_type_ = other.text_ps_inner_stmt_type_;
      table_name_.assign_ptr(dml_buf_.table_name_buf_, other.table_name_.length());
      package_name_.assign_ptr(dml_buf_.package_name_buf_, other.package_name_.length());
      database_name_.assign_ptr(dml_buf_.database_name_buf_, other.database_name_.length());
      alias_name_.assign_ptr(dml_buf_.alias_name_buf_, other.alias_name_.length());
      col_name_.assign_ptr(internal_select_buf_.col_name_buf_, other.col_name_.length());
      part_name_.assign_ptr(part_name_buf_.part_name_buf_, other.part_name_.length());
      text_ps_name_.assign_ptr(text_ps_buf_.text_ps_name_buf_, other.text_ps_name_.length());

      /* some sharding variablies do not copy */
      has_shard_comment_ = other.has_shard_comment_;
      is_dual_request_ = other.is_dual_request_;
      origin_dml_buf_ = other.origin_dml_buf_;
      origin_table_name_.assign_ptr(origin_dml_buf_.table_name_buf_, other.origin_table_name_.length());
      origin_database_name_.assign_ptr(origin_dml_buf_.database_name_buf_, other.origin_database_name_.length());
      MEMCPY(trace_id_buf_, other.trace_id_buf_, other.trace_id_.length());
      trace_id_.assign_ptr(trace_id_buf_, other.trace_id_.length());
      MEMCPY(rpc_id_buf_, other.rpc_id_buf_, other.rpc_id_.length());
      rpc_id_.assign_ptr(rpc_id_buf_, other.rpc_id_.length());
    }
    return *this;
  }

  void set_text_ps_info(const ObSqlParseResult &other)
  {
    cmd_info_ = other.cmd_info_;
    call_info_ = other.call_info_;
    route_info_ = other.route_info_;
    has_simple_route_info_ = other.has_simple_route_info_;
    hint_query_timeout_ = other.hint_query_timeout_;
    parsed_length_ = other.parsed_length_;
    cmd_sub_type_ = other.cmd_sub_type_;
    cmd_err_type_ = other.cmd_err_type_;
    hint_consistency_level_ = other.hint_consistency_level_;
    text_ps_inner_stmt_type_ = other.text_ps_inner_stmt_type_;
    dml_buf_ = other.dml_buf_;
    internal_select_buf_ = other.internal_select_buf_;
    part_name_buf_ = other.part_name_buf_;
    table_name_quote_ = other.table_name_quote_;
    package_name_quote_ = other.package_name_quote_;
    database_name_quote_ = other.database_name_quote_;
    alias_name_quote_ = other.alias_name_quote_;
    col_name_quote_ = other.col_name_quote_;
    table_name_.assign_ptr(dml_buf_.table_name_buf_, other.table_name_.length());
    package_name_.assign_ptr(dml_buf_.package_name_buf_, other.package_name_.length());
    database_name_.assign_ptr(dml_buf_.database_name_buf_, other.database_name_.length());
    alias_name_.assign_ptr(dml_buf_.alias_name_buf_, other.alias_name_.length());
    col_name_.assign_ptr(internal_select_buf_.col_name_buf_, other.col_name_.length());
    part_name_.assign_ptr(part_name_buf_.part_name_buf_, other.part_name_.length());

    /* some sharding variablies do not copy */
    has_shard_comment_ = other.has_shard_comment_;
    is_dual_request_ = other.is_dual_request_;
    origin_dml_buf_ = other.origin_dml_buf_;
    origin_table_name_.assign_ptr(origin_dml_buf_.table_name_buf_, other.origin_table_name_.length());
    origin_database_name_.assign_ptr(origin_dml_buf_.database_name_buf_, other.origin_database_name_.length());
    MEMCPY(trace_id_buf_, other.trace_id_buf_, other.trace_id_.length());
    trace_id_.assign_ptr(trace_id_buf_, other.trace_id_.length());
    MEMCPY(rpc_id_buf_, other.rpc_id_buf_, other.rpc_id_.length());
    rpc_id_.assign_ptr(rpc_id_buf_, other.rpc_id_.length());
  }

  ObProxyCmdInfo cmd_info_;
  ObProxyCallInfo call_info_;
  ObProxySimpleRouteInfo route_info_;
  ObProxyTextPsExecuteInfo text_ps_execute_info_;
  oceanbase::common::ObArenaAllocator allocator_;
private:
  bool has_last_insert_id_;
  bool has_found_rows_;
  bool has_row_count_;
  bool has_explain_;
  bool has_simple_route_info_;
  bool has_shard_comment_;
  bool is_dual_request_;
  bool has_anonymous_block_;
  bool has_for_update_;
  ObProxyBasicStmtType stmt_type_;
  int64_t hint_query_timeout_;
  int64_t parsed_length_; // next parser can starts with (orig_sql + parsed_length_)

  ObProxyBasicStmtSubType cmd_sub_type_;
  ObProxyErrorStmtType cmd_err_type_;

  // dml info
  common::ObString table_name_;
  common::ObString package_name_;
  common::ObString alias_name_;
  common::ObString database_name_;
  common::ObString origin_table_name_;
  common::ObString origin_database_name_;
  ObProxyParseQuoteType table_name_quote_;
  ObProxyParseQuoteType package_name_quote_;
  ObProxyParseQuoteType database_name_quote_;
  ObProxyParseQuoteType alias_name_quote_;

  // internal select info
  common::ObString col_name_;
  common::ObConsistencyLevel hint_consistency_level_;
  ObProxyParseQuoteType col_name_quote_;

  common::ObString part_name_;
  ObPartNameBuf part_name_buf_;
  // text ps
  common::ObString text_ps_name_;
  ObTextPsBuf text_ps_buf_;
  ObProxyBasicStmtType text_ps_inner_stmt_type_;

  common::ObString trace_id_;
  char trace_id_buf_[common::OB_MAX_OBPROXY_TRACE_ID_LENGTH];
  common::ObString rpc_id_;
  char rpc_id_buf_[common::OB_MAX_OBPROXY_TRACE_ID_LENGTH];

  SqlFieldResult fileds_result_;
  int64_t batch_insert_values_count_;
  DbMeshRouteInfo dbmesh_route_info_;
  ObProxySetInfo set_info_;
  DbpRouteInfo dbp_route_info_;
  bool use_dbp_hint_;
  ObProxyDualParseResult dual_result_;
  ObDmlBuf origin_dml_buf_;

  // buffer holder
  union {
    ObDmlBuf dml_buf_;
    ObInternalSelectBuf internal_select_buf_;
  };
  ParseResult *ob_parser_result_;
  ObProxyStmt* proxy_stmt_;
};

const int OB_T_IDENT_NUM_CHILD                   = 0;
const int OB_T_RELATION_FACTOR_NUM_CHILD         = 2;
const int OB_T_COLUMN_REF_NUM_CHILD              = 3;
const int OB_T_ALIAS_TABLE_NAME_NUM_CHILD        = 5;
const int OB_T_ALIAS_CLUMN_NAME_NUM_CHILD        = 2;
const int OB_T_INDEX_NUM_CHILD                   = 3;
const int OB_T_RELATION_FACTOR_IN_HINT_NUM_CHILD = 2;

class ObProxySqlParser
{
public:
  ObProxySqlParser() {}
  ~ObProxySqlParser() {}
  int parse_sql(const common::ObString &sql,
                const ObProxyParseMode parse_mode,
                ObSqlParseResult &sql_parse_result,
                const bool use_lower_case_name,
                common::ObCollationType connection_collation,
                const bool drop_origin_db_table_name = false,
                const bool is_sharding_request = false);

  int parse_sql_by_obparser(const common::ObString &sql,
                            const ObProxyParseMode parse_mode,
                            ObSqlParseResult &sql_parse_result,
                            const bool need_handle_result);
  typedef common::hash::ObHashMap<common::ObString, ObParseNode*> AliasTableMap;
  static int ob_load_testload_parse_node(ParseNode *root, const int level,
                              common::ObSEArray<ObParseNode*, 1> &relation_table_node,
                              //common::ObSEArray<ObParseNode*, 1> &ref_column_node,
                              common::ObSEArray<ObParseNode*, 1> &hint_option_list,
                              AliasTableMap &all_table_map,
                              AliasTableMap &alias_table_map,
                              oceanbase::common::ObArenaAllocator &allocator);
  static int init_ob_parser_node(oceanbase::common::ObArenaAllocator &allocator, ObParseNode *&ob_node);
  static int get_parse_allocator(common::ObArenaAllocator *&allocator);
};

inline void ObSqlParseResult::reset(bool is_reset_origin_db_table /* true */)
{
  has_last_insert_id_ = false;
  has_found_rows_ = false;
  has_row_count_ = false;
  has_explain_ = false;
  has_simple_route_info_ = false;
  has_shard_comment_ = false;
  is_dual_request_ = false;
  has_anonymous_block_ = false;
  has_for_update_ = false;
  stmt_type_ = OBPROXY_T_INVALID;
  cmd_sub_type_ = OBPROXY_T_SUB_INVALID;
  cmd_err_type_ = OBPROXY_T_ERR_INVALID;
  hint_consistency_level_ = common::INVALID_CONSISTENCY;
  hint_query_timeout_ = 0;
  parsed_length_ = 0;
  table_name_.reset();
  package_name_.reset();
  database_name_.reset();
  col_name_.reset();
  alias_name_.reset();
  part_name_.reset();
  trace_id_.reset();
  rpc_id_.reset();

  if (is_reset_origin_db_table) {
    origin_table_name_.reset();
    origin_database_name_.reset();
  }
  clear_proxy_stmt();

  // do not memset buf for efficient as the string has been reset
  cmd_info_.reset();
  call_info_.reset();
  route_info_.reset();
  text_ps_execute_info_.reset();
  table_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  package_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  database_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  alias_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  col_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  text_ps_name_.reset();
  text_ps_inner_stmt_type_ = OBPROXY_T_INVALID;

  fileds_result_.reset();
  batch_insert_values_count_ = 0; // numbers of values like insert into xx(x1,x2) values(..), (..), (..);
  dbmesh_route_info_.reset();
  set_info_.reset();
  allocator_.reset();
  table_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  database_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  alias_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  col_name_quote_ = OBPROXY_QUOTE_T_INVALID;
  dbp_route_info_.reset();
  use_dbp_hint_ = false;
  ob_parser_result_ = NULL; //TODO check delete it
}

inline bool ObSqlParseResult::has_dependent_func() const
{
  // these function are depend on last executed sql:
  // 1. found_rows()
  // 2. row_count()
  // 3. show errors/warnings
  return (has_row_count()
          || has_found_rows()
          || has_show_errors()
          || has_show_warnings()
          || is_show_trace_stmt());
}

inline bool ObSqlParseResult::is_not_supported() const
{
  // we do NOT support last_insert_id and dependent_func in the same sql
  return (has_last_insert_id() && has_dependent_func());
}

inline bool ObSqlParseResult::is_dml_stmt() const
{
  // explain stmt can contains db/table name
  return (is_select_stmt()
          || is_insert_stmt()
          || is_update_stmt()
          || is_replace_stmt()
          || is_delete_stmt()
          || is_merge_stmt()
          || is_multi_stmt());
}

inline bool ObSqlParseResult::is_text_ps_inner_dml_stmt() const
{
  return OBPROXY_T_SELECT == text_ps_inner_stmt_type_
         || OBPROXY_T_INSERT == text_ps_inner_stmt_type_
         || OBPROXY_T_UPDATE == text_ps_inner_stmt_type_
         || OBPROXY_T_REPLACE == text_ps_inner_stmt_type_
         || OBPROXY_T_DELETE == text_ps_inner_stmt_type_
         || OBPROXY_T_CALL == text_ps_inner_stmt_type_
         || OBPROXY_T_MERGE == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_select_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_SELECT == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_delete_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_DELETE == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_insert_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_INSERT == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_replace_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_REPLACE == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_update_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_UPDATE == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_merge_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_MERGE == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_text_ps_call_stmt() const
{
  return is_text_ps_stmt() && OBPROXY_T_CALL == text_ps_inner_stmt_type_;
}

inline bool ObSqlParseResult::is_write_stmt() const
{
  return (is_insert_stmt()
          || is_update_stmt()
          || is_replace_stmt()
          || is_delete_stmt()
          || is_merge_stmt());
}

inline bool ObSqlParseResult::is_internal_request() const
{
  return (is_not_supported()
          || is_internal_select()
          || is_set_autocommit_0()
          || is_set_route_addr()
          || is_select_route_addr()
          || is_ping_proxy_cmd());
}

inline bool ObSqlParseResult::is_internal_cmd() const
{
  return (OBPROXY_T_INVALID < stmt_type_ && stmt_type_ < OBPROXY_T_ICMD_MAX);
}

inline bool ObSqlParseResult::is_shard_special_cmd() const
{
  return (is_select_database_stmt()
          || is_use_db_stmt()
          || is_show_databases_stmt()
          || is_show_db_version_stmt()
          || is_show_topology_stmt());
}

inline bool ObSqlParseResult::is_kill_query_cmd() const
{
  return (OBPROXY_T_ICMD_KILL_MYSQL == stmt_type_ && OBPROXY_T_SUB_KILL_QUERY == cmd_sub_type_);
}

inline bool ObSqlParseResult::is_kill_session_cmd() const
{
  return OBPROXY_T_ICMD_KILL_SESSION == stmt_type_;
}

inline bool ObSqlParseResult::is_mysql_compatible_cmd() const
{
  return (OBPROXY_T_ICMD_SHOW_PROCESSLIST == stmt_type_ || OBPROXY_T_ICMD_KILL_MYSQL == stmt_type_);
}

inline bool ObSqlParseResult::is_need_resp_ok_cmd() const
{
  return (is_set_stmt()
          || is_start_trans_stmt()
          || is_set_autocommit_0()
          || is_commit_stmt()
          || is_rollback_stmt());
}

class ObParseNode {
public:
  ObParseNode():node_(NULL) {}
  ParseNode* get_node() { return node_; }
  void set_node(ParseNode* node) { node_ = node; }
  //void to_string();
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  ParseNode* node_;
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_SQL_PARSER_H
