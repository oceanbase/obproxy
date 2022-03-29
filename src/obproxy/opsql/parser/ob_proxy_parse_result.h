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

#ifndef OBPROXY_PARSE_RESULT_H
#define OBPROXY_PARSE_RESULT_H

#include <stdint.h>
#include <stdbool.h>
#include <setjmp.h>
#include "opsql/ob_proxy_parse_type.h"

#define OBPROXY_ERR_MSG_LENGTH 1024
// max of db/table name length
#define OBPROXY_MAX_NAME_LENGTH 128

#define OBPROXY_ICMD_MAX_VALUE_COUNT 3

#define OBPROXY_MAX_HINT_INDEX_COUNT 5

#define OBPROXY_MAX_DBP_SHARD_KEY_NUM 64

typedef enum ObProxyBasicStmtType
{
  OBPROXY_T_INVALID = 0,

  // internal cmd
  OBPROXY_T_ICMD_SHOW_NET,
  OBPROXY_T_ICMD_SHOW_PROCESSLIST,
  OBPROXY_T_ICMD_SHOW_SESSION,
  OBPROXY_T_ICMD_SHOW_SM,
  OBPROXY_T_ICMD_SHOW_CONFIG,
  OBPROXY_T_ICMD_SHOW_CLUSTER,
  OBPROXY_T_ICMD_SHOW_RESOURCE,
  OBPROXY_T_ICMD_SHOW_CONGESTION,
  OBPROXY_T_ICMD_SHOW_ROUTE,
  OBPROXY_T_ICMD_SHOW_VIP,
  OBPROXY_T_ICMD_SHOW_MEMORY,
  OBPROXY_T_ICMD_SHOW_SQLAUDIT,
  OBPROXY_T_ICMD_SHOW_WARNLOG,
  OBPROXY_T_ICMD_SHOW_STAT,
  OBPROXY_T_ICMD_SHOW_TRACE,
  OBPROXY_T_ICMD_SHOW_INFO,
  OBPROXY_T_ICMD_ALTER_CONFIG,
  OBPROXY_T_ICMD_ALTER_RESOURCE,
  OBPROXY_T_ICMD_KILL_SESSION,
  OBPROXY_T_ICMD_KILL_MYSQL,
  OBPROXY_T_ICMD_PING,
  OBPROXY_T_ICMD_DUAL,
  OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION,
  OBPROXY_T_ICMD_KILL_GLOBAL_SESSION,
  OBPROXY_T_ICMD_MAX,

  // dml
  OBPROXY_T_SELECT,
  OBPROXY_T_UPDATE,
  OBPROXY_T_INSERT,
  OBPROXY_T_REPLACE,
  OBPROXY_T_DELETE,
  OBPROXY_T_MERGE,

  // ddl
  OBPROXY_T_CREATE,
  OBPROXY_T_DROP,
  OBPROXY_T_ALTER,
  OBPROXY_T_TRUNCATE,
  OBPROXY_T_RENAME,

  // oracle ddl
  OBPROXY_T_GRANT,
  OBPROXY_T_REVOKE,
  OBPROXY_T_ANALYZE,
  OBPROXY_T_PURGE,
  OBPROXY_T_FLASHBACK,
  OBPROXY_T_COMMENT,
  OBPROXY_T_AUDIT,
  OBPROXY_T_NOAUDIT,

  // internal request
  OBPROXY_T_BEGIN,
  OBPROXY_T_SELECT_TX_RO,
  OBPROXY_T_SET_AC_0,
  OBPROXY_T_PING_PROXY,
  OBPROXY_T_SELECT_ROUTE_ADDR,
  OBPROXY_T_SET_ROUTE_ADDR,
  OBPROXY_T_SELECT_PROXY_VERSION,

  // use last session
  OBPROXY_T_SHOW_WARNINGS,
  OBPROXY_T_SHOW_ERRORS,
  OBPROXY_T_SHOW_TRACE,

  // others
  OBPROXY_T_SET,
  OBPROXY_T_SET_GLOBAL,
  OBPROXY_T_SET_NAMES,
  OBPROXY_T_SET_CHARSET,
  OBPROXY_T_SET_PASSWORD,
  OBPROXY_T_SET_DEFAULT,
  OBPROXY_T_USE_DB,
  OBPROXY_T_CALL,
  OBPROXY_T_HELP,
  OBPROXY_T_MULTI_STMT,
  OBPROXY_T_COMMIT,
  OBPROXY_T_ROLLBACK,
  OBPROXY_T_SHOW,
  OBPROXY_T_DESC,
  OBPROXY_T_OTHERS,

  OBPROXY_T_SET_OB_READ_CONSISTENCY,
  OBPROXY_T_SET_TX_READ_ONLY,

  // text ps
  OBPROXY_T_TEXT_PS_PREPARE,
  OBPROXY_T_TEXT_PS_EXECUTE,

  // only for print obproxy_stat log
  OBPROXY_T_LOGIN,

  OBPROXY_T_MAX
} ObProxyBasicStmtType;

typedef enum ObProxyBasicStmtSubType
{
  OBPROXY_T_SUB_INVALID = 0,

  //net
  OBPROXY_T_SUB_NET_THREAD,
  OBPROXY_T_SUB_NET_CONNECTION,

  //processlist
  OBPROXY_T_SUB_SESSION_LIST,
  //session
  OBPROXY_T_SUB_SESSION_LIST_INTERNAL,
  OBPROXY_T_SUB_SESSION_ATTRIBUTE,
  OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL,
  OBPROXY_T_SUB_SESSION_VARIABLES_ALL,
  OBPROXY_T_SUB_SESSION_STAT,

  // global session
  OBPROXY_T_SUB_GLOBAL_SESSION_LIST,
  OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO,
  OBPROXY_T_SUB_GLOBAL_SESSION_LIST_LIKE,
  OBPROXY_T_SUB_GLOBAL_SESSION_LIST_INFO_ALL,

  //congestion
  OBPROXY_T_SUB_CONGEST_ALL,

  //info
  OBPROXY_T_SUB_INFO_BINARY,
  OBPROXY_T_SUB_INFO_UPGRADE,
  OBPROXY_T_SUB_INFO_IDC,

  //stat
  OBPROXY_T_SUB_STAT_REFRESH,

  //trace
  OBPROXY_T_SUB_TRACE_LIMIT,

  //sqlaudit
  OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID,
  OBPROXY_T_SUB_SQLAUDIT_SM_ID,

  //memory
  OBPROXY_T_SUB_MEMORY_OBJPOOL,

  //config
  OBPROXY_T_SUB_CONFIG_INT_VAULE,
  OBPROXY_T_SUB_CONFIG_DIFF,
  OBPROXY_T_SUB_CONFIG_DIFF_USER,

  //kill
  OBPROXY_T_SUB_KILL_CS,
  OBPROXY_T_SUB_KILL_SS,
  OBPROXY_T_SUB_KILL_CONNECTION,
  OBPROXY_T_SUB_KILL_QUERY,
  OBPROXY_T_SUB_KILL_GLOBAL_SS_ID,
  OBPROXY_T_SUB_KILL_GLOBAL_SS_DBKEY,

  //route
  OBPROXY_T_SUB_ROUTE_PARTITION,
  OBPROXY_T_SUB_ROUTE_ROUTINE,

  //show
  OBPROXY_T_SUB_SHOW_DATABASES,
  OBPROXY_T_SUB_SHOW_TABLES,
  OBPROXY_T_SUB_SHOW_CREATE_TABLE,
  OBPROXY_T_SUB_SHOW_TOPOLOGY,
  OBPROXY_T_SUB_SHOW_DB_VERSION,

  //select
  OBPROXY_T_SUB_SELECT_DATABASE,

  //desc
  OBPROXY_T_SUB_DESC_TABLE,

  //ddl
  OBPROXY_T_SUB_CREATE_TABLE,
  OBPROXY_T_SUB_CREATE_INDEX,

  OBPROXY_T_SUB_MAX
} ObProxyBasicStmtSubType;

typedef enum ObProxyErrorStmtType
{
  OBPROXY_T_ERR_INVALID = 0,
  OBPROXY_T_ERR_PARSE,
  OBPROXY_T_ERR_INVALID_NUM,
  OBPROXY_T_ERR_NOT_SUPPORTED,
  OBPROXY_T_ERR_NEED_RESP_OK,
} ObProxyErrorStmtType;

typedef enum ObProxyParseMode
{
  NORMAL_PARSE_MODE = 0,
  IN_TRANS_PARSE_MODE,
} ObProxyParseMode;

//read consistency, we only support weak and strong now
typedef enum ObProxyReadConsistencyType
{
  OBPROXY_READ_CONSISTENCY_INVALID = -1,
  OBPROXY_READ_CONSISTENCY_FROZEN = 1,
  OBPROXY_READ_CONSISTENCY_WEAK,
  OBPROXY_READ_CONSISTENCY_STRONG,
} ObProxyReadConsistencyType;

typedef struct _ObProxyInternalCmdInfo
{
  ObProxyBasicStmtSubType sub_type_;
  ObProxyErrorStmtType err_type_;
  int64_t integer_[OBPROXY_ICMD_MAX_VALUE_COUNT];
  ObProxyParseString string_[OBPROXY_ICMD_MAX_VALUE_COUNT];
} ObProxyInternalCmdInfo;

typedef enum ObProxyCallTokenType
{
  CALL_TOKEN_NONE = 0,
  CALL_TOKEN_STR_VAL,
  CALL_TOKEN_INT_VAL,
  CALL_TOKEN_NUMBER_VAL,
  CALL_TOKEN_SYS_VAR,
  CALL_TOKEN_USER_VAR,
  CALL_TOKEN_PLACE_HOLDER
} ObProxyCallTokenType;

typedef struct _ObProxyCallParseNode
{
  ObProxyCallTokenType type_;
  union
  {
    int64_t             int_value_;
    int64_t             placeholder_idx_;
    ObProxyParseString  str_value_;
  };
  struct _ObProxyCallParseNode *next_;
} ObProxyCallParseNode;

typedef struct _ObProxyCallParseInfo
{
  ObProxyCallParseNode *head_;
  ObProxyCallParseNode *tail_;
  int32_t node_count_;
} ObProxyCallParseInfo;

typedef struct _ObProxyTextPsExecuteParseNode
{
  ObProxyParseString str_value_;
  struct _ObProxyTextPsExecuteParseNode *next_;
} ObProxyTextPsExecuteParseNode;

typedef struct _ObProxyTextPsExecuteParseInfo
{
  ObProxyTextPsExecuteParseNode *head_;
  ObProxyTextPsExecuteParseNode *tail_;
  int32_t node_count_;
} ObProxyTextPsExecuteParseInfo;

typedef struct _ObProxySimpleRouteParseInfo
{
  const char *table_start_ptr_;
  const char *part_key_start_ptr_;
  ObProxyParseString table_name_;
  ObProxyParseString part_key_;
} ObProxySimpleRouteParseInfo;

typedef enum ObDbMeshTokenType
{
  DBMESH_TOKEN_NONE = 0,
  DBMESH_TOKEN_STR_VAL,
  DBMESH_TOKEN_INT_VAL,
  DBMESH_TOKEN_COLUMN,
} ObDbMeshTokenType;

typedef struct _ObShardColumnNode
{
  ObProxyParseString  tb_name_;
  ObProxyParseString  col_name_;
  ObDbMeshTokenType type_;
  ObProxyParseString  col_str_value_;
  struct _ObShardColumnNode *next_;
} ObShardColumnNode;

typedef struct _ObDbMeshRouteInfo
{
  ObProxyParseString  tb_idx_str_;
  ObProxyParseString  table_name_str_;
  ObProxyParseString  group_idx_str_;
  ObProxyParseString  es_idx_str_;
  ObProxyParseString  testload_str_;
  ObProxyParseString  disaster_status_str_;
  ObProxyParseString  tnt_id_str_;
  ObShardColumnNode *head_;
  ObShardColumnNode *tail_;
  int64_t node_count_;
  int64_t index_count_;
  ObProxyParseString  index_tb_name_[OBPROXY_MAX_HINT_INDEX_COUNT];
} ObDbMeshRouteInfo;

typedef enum _ObProxySetValueType
{
  SET_VALUE_TYPE_ONE = 0,
  SET_VALUE_TYPE_STR,
  SET_VALUE_TYPE_INT,
  SET_VALUE_TYPE_NUMBER,
} ObProxySetValueType;

typedef enum _ObProxySetVarType
{
  SET_VAR_TYPE_NONE = 0,
  SET_VAR_USER,
  SET_VAR_SYS,
} ObProxySetVarType;

typedef struct _ObProxySetVarNode
{
  ObProxyParseString  name_;

  ObProxySetValueType value_type_;
  union
  {
    int64_t             int_value_;
    ObProxyParseString  str_value_;
  };

  ObProxySetVarType type_;
  struct _ObProxySetVarNode *next_;
} ObProxySetVarNode;

typedef struct _ObProxySetParseInfo
{
  ObProxySetVarNode *head_;
  ObProxySetVarNode *tail_;
  int64_t node_count_;
} ObProxySetParseInfo;

typedef struct _ObDbpShardKeyInfo
{
  ObProxyParseString left_str_;
  ObProxyParseString right_str_;
} ObDbpShardKeyInfo;

typedef struct _ObDbpRouteInfo
{
  bool has_group_info_;
  ObProxyParseString  table_name_;
  ObProxyParseString  group_idx_str_;
  bool scan_all_;
  bool has_shard_key_;
  ObDbpShardKeyInfo shard_key_infos_[OBPROXY_MAX_DBP_SHARD_KEY_NUM];
  int shard_key_count_;
} ObDbpRouteInfo;

typedef struct _ObProxyParseResult
{
  // input argument
  void *malloc_pool_; // ObIAllocator
  ObProxyParseMode parse_mode_;

  // scanner buffer
  void *yyscan_info_; // yy_scan_t
  char *tmp_buf_;
  char *tmp_start_ptr_;
  int32_t tmp_len_;
  jmp_buf jmp_buf_; // handle fatal error
  ObProxyBasicStmtType cur_stmt_type_;
  bool has_ignored_word_;
  bool is_dual_request_;
  const char *start_pos_;
  const char *end_pos_;

  const char *comment_begin_;
  const char *comment_end_;

  // call function placeholder index
  int64_t placeholder_list_idx_;

  // result argument
  bool has_last_insert_id_;
  bool has_found_rows_;
  bool has_row_count_;
  bool has_explain_;
  bool has_simple_route_info_;
  bool has_anonymous_block_;
  ObProxyBasicStmtType stmt_type_;
  ObProxyBasicStmtSubType sub_stmt_type_; //for sharding
  int64_t stmt_count_;
  int64_t query_timeout_;
  char *accept_pos_; // parse accpet pos, use for the next parse

  // use to store internal select
  // example: select @@tx_read_only we store @@tx_read_only here
  ObProxyParseString col_name_;
  ObProxyParseString trace_id_;
  ObProxyParseString rpc_id_;
  // read_consistency
  ObProxyReadConsistencyType read_consistency_type_;
  // db/table name
  ObProxyTableInfo table_info_;
  // internal cmd
  ObProxyInternalCmdInfo cmd_info_;
  //call cmd
  ObProxyCallParseInfo call_parse_info_;
  //simple route info
  ObProxySimpleRouteParseInfo simple_route_info_;
  // partiton(p1)
  ObProxyParseString part_name_;
  // text ps execute user variables
  ObProxyTextPsExecuteParseInfo text_ps_execute_parse_info_;
  // text ps name
  ObProxyParseString text_ps_name_;
  // text ps inner sql stmt type
  ObProxyBasicStmtType text_ps_inner_stmt_type_;
  // set cmd
  ObProxySetParseInfo set_parse_info_;
  int64_t session_var_count_;
  // dbmesh route info
  bool has_shard_comment_;
  ObDbMeshRouteInfo dbmesh_route_info_;
  ObDbpRouteInfo dbp_route_info_;
} ObProxyParseResult;

#ifdef __cplusplus
extern "C" const char* get_print_stmt_name(const ObProxyBasicStmtType type);
extern "C" const char* get_obproxy_stmt_name(const ObProxyBasicStmtType type);
extern "C" const char* get_obproxy_sub_stmt_name(const ObProxyBasicStmtSubType type);
extern "C" const char* get_obproxy_err_stmt_name(const ObProxyErrorStmtType type);
extern "C" const char* get_obproxy_call_token_type(const ObProxyCallTokenType type);
extern "C" const char* get_obproxy_dbmesh_token_type(const ObDbMeshTokenType type);
extern "C" const char* get_obproxy_read_consistency_string(const ObProxyReadConsistencyType type);
extern "C" const char* get_obproxy_quote_name(const ObProxyParseQuoteType type);
#else
const char* get_obproxy_stmt_name(const ObProxyBasicStmtType type);
const char* get_obproxy_sub_stmt_name(const ObProxyBasicStmtSubType type);
const char* get_obproxy_err_stmt_name(const ObProxyErrorStmtType type);
const char* get_obproxy_call_token_type(const ObProxyCallTokenType type);
const char* get_obproxy_dbmesh_token_type(const ObDbMeshTokenType type);
const char* get_obproxy_read_consistency_string(const ObProxyReadConsistencyType type);
const char* get_obproxy_quote_name(const ObProxyParseQuoteType type);
#endif

#endif // end of OBPROXY_PARSE_RESULT_H
