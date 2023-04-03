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

#include "opsql/parser/ob_proxy_parse_result.h"
const char* get_print_stmt_name(const ObProxyBasicStmtType type)
{
  const char* str_ret = "";
  switch (type) {
    // internal cmd
    case OBPROXY_T_ICMD_SHOW_NET:
    case OBPROXY_T_ICMD_SHOW_PROCESSLIST:
    case OBPROXY_T_ICMD_SHOW_SESSION:
    case OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION:
    case OBPROXY_T_ICMD_SHOW_SM:
    case OBPROXY_T_ICMD_SHOW_CONFIG:
    case OBPROXY_T_ICMD_SHOW_CLUSTER:
    case OBPROXY_T_ICMD_SHOW_RESOURCE:
    case OBPROXY_T_ICMD_SHOW_CONGESTION:
    case OBPROXY_T_ICMD_SHOW_ROUTE:
    case OBPROXY_T_ICMD_SHOW_VIP:
    case OBPROXY_T_ICMD_SHOW_MEMORY:
    case OBPROXY_T_ICMD_SHOW_SQLAUDIT:
    case OBPROXY_T_ICMD_SHOW_WARNLOG:
    case OBPROXY_T_ICMD_SHOW_STAT:
    case OBPROXY_T_ICMD_SHOW_TRACE:
    case OBPROXY_T_ICMD_SHOW_INFO:
    case OBPROXY_T_ICMD_ALTER_CONFIG:
    case OBPROXY_T_ICMD_ALTER_RESOURCE:
    case OBPROXY_T_ICMD_KILL_SESSION:
    case OBPROXY_T_ICMD_KILL_GLOBAL_SESSION:
    case OBPROXY_T_ICMD_KILL_MYSQL:
    case OBPROXY_T_ICMD_PING:
    case OBPROXY_T_ICMD_MAX:
      str_ret = "ICMD";
      break;
    case OBPROXY_T_ICMD_DUAL:
      str_ret = "SEQUENCE";
      break;

    // dml
    case OBPROXY_T_SELECT:
      str_ret = "SELECT";
      break;
    case OBPROXY_T_UPDATE:
      str_ret = "UPDATE";
      break;
    case OBPROXY_T_INSERT:
      str_ret = "INSERT";
      break;
    case OBPROXY_T_REPLACE:
      str_ret = "REPLACE";
      break;
    case OBPROXY_T_DELETE:
      str_ret = "DELETE";
      break;
    case OBPROXY_T_MERGE:
      str_ret = "MERGE";
      break;

    // ddl
    case OBPROXY_T_CREATE:
      str_ret = "CREATE";
      break;
    case OBPROXY_T_DROP:
      str_ret = "DROP";
      break;
    case OBPROXY_T_ALTER:
      str_ret = "ALTER";
      break;
    case OBPROXY_T_TRUNCATE:
      str_ret = "TRUNCATE";
      break;
    case OBPROXY_T_RENAME:
      str_ret = "RENAME";
      break;

    // oracle ddl
    case OBPROXY_T_GRANT:
      str_ret = "GRANT";
      break;
    case OBPROXY_T_REVOKE:
      str_ret = "REVOKE";
      break;
    case OBPROXY_T_ANALYZE:
      str_ret = "ANALYZE";
      break;
    case OBPROXY_T_PURGE:
      str_ret = "PURGE";
      break;
    case OBPROXY_T_FLASHBACK:
      str_ret = "FLASHBACK";
      break;
    case OBPROXY_T_COMMENT:
      str_ret = "COMMENT";
      break;
    case OBPROXY_T_AUDIT:
      str_ret = "AUDIT";
      break;
    case OBPROXY_T_NOAUDIT:
      str_ret = "NOAUDIT";
      break;

    // internal request
    case OBPROXY_T_BEGIN:
      str_ret = "BEGIN";
      break;
    case OBPROXY_T_SELECT_TX_RO:
      str_ret = "SELECT_TX_RO";
      break;
    case OBPROXY_T_PING_PROXY:
      str_ret = "PING";
      break;
    case OBPROXY_T_SELECT_ROUTE_ADDR:
      str_ret = "SELECT_ROUTE_ADDR";
      break;
    case OBPROXY_T_SET_ROUTE_ADDR:
      str_ret = "SET_ROUTE_ADDR";
      break;
    case OBPROXY_T_SELECT_PROXY_VERSION:
      str_ret = "SELECT_PROXY_VERSION";
      break;

    // use last session
    case OBPROXY_T_SHOW_WARNINGS:
      str_ret = "SHOW";
      break;
    case OBPROXY_T_SHOW_ERRORS:
      str_ret = "SHOW";
      break;
    case OBPROXY_T_SHOW_TRACE:
      str_ret = "SHOW";
      break;

    // others
    case OBPROXY_T_SET:
    case OBPROXY_T_SET_GLOBAL:
    case OBPROXY_T_SET_NAMES:
    case OBPROXY_T_SET_CHARSET:
    case OBPROXY_T_SET_PASSWORD:
    case OBPROXY_T_SET_DEFAULT:
    case OBPROXY_T_SET_OB_READ_CONSISTENCY:
    case OBPROXY_T_SET_TX_READ_ONLY:
      str_ret = "SET";
      break;
    case OBPROXY_T_USE_DB:
      str_ret = "USE_DB";
      break;
    case OBPROXY_T_CALL:
      str_ret = "CALL";
      break;
    case OBPROXY_T_HELP:
      str_ret = "HELP";
      break;
    case OBPROXY_T_MULTI_STMT:
      str_ret = "MULTI_STMT";
      break;
    case OBPROXY_T_COMMIT:
      str_ret = "COMMIT";
      break;
    case OBPROXY_T_ROLLBACK:
      str_ret = "ROLLBACK";
      break;
    case OBPROXY_T_SHOW:
      str_ret = "SHOW";
      break;
    case OBPROXY_T_TEXT_PS_PREPARE:
      str_ret = "TEXT_PS_PREPARE";
      break;
    case OBPROXY_T_TEXT_PS_EXECUTE:
      str_ret = "TEXT_PS_EXECUTE";
      break;
    case OBPROXY_T_TEXT_PS_DROP:
      str_ret = "TEXT_PS_DROP";
      break;
    case OBPROXY_T_LOGIN:
      str_ret = "LOGIN";
      break;
    case OBPROXY_T_DESC:
      str_ret = "DESC";
      break;

    case OBPROXY_T_SHOW_MASTER_STATUS:
      str_ret = "SHOW_MASTER_STATUS";
      break;
    case OBPROXY_T_SHOW_BINLOG_EVENTS:
      str_ret = "SHOW_BINLOG_EVENTS";
      break;
    case OBPROXY_T_PURGE_BINARY_LOGS:
      str_ret = "PURGE_BINARY_LOGS";
      break;
    case OBPROXY_T_RESET_MASTER:
      str_ret = "RESET_MASTER";
      break;
    case OBPROXY_T_SHOW_BINLOG_SERVER_FOR_TENANT:
      str_ret = "SHOW_BINLOG_SERVER_FOR_TENANT";
      break;
    case OBPROXY_T_SHOW_BINARY_LOGS:
      str_ret = "SHOW_BINARY_LOGS";
      break;

    case OBPROXY_T_INVALID:
    case OBPROXY_T_MAX:
    case OBPROXY_T_OTHERS:
      str_ret = "OTHERS";
      break;
  }
  return str_ret;
}

const char* get_obproxy_stmt_name(const ObProxyBasicStmtType type)
{
  const char* str_ret = "";
  switch (type) {
    case OBPROXY_T_INVALID:
      str_ret = "OBPROXY_T_INVALID";
      break;
    case OBPROXY_T_ICMD_SHOW_NET:
      str_ret = "OBPROXY_T_ICMD_SHOW_NET";
      break;
    case OBPROXY_T_ICMD_SHOW_SESSION:
      str_ret = "OBPROXY_T_ICMD_SHOW_SESSION";
      break;
    case OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION:
      str_ret = "OBPROXY_T_ICMD_SHOW_GLOBAL_SESSION";
      break;
    case OBPROXY_T_ICMD_KILL_GLOBAL_SESSION:
      str_ret = "OBPROXY_T_ICMD_KILL_GLOBAL_SESSION";
      break;
    case OBPROXY_T_ICMD_SHOW_PROCESSLIST:
      str_ret = "OBPROXY_T_ICMD_SHOW_PROCESSLIST";
      break;
    case OBPROXY_T_ICMD_SHOW_SM:
      str_ret = "OBPROXY_T_ICMD_SHOW_SM";
      break;
    case OBPROXY_T_ICMD_SHOW_CONFIG:
      str_ret = "OBPROXY_T_ICMD_SHOW_CONFIG";
      break;
    case OBPROXY_T_ICMD_SHOW_CLUSTER:
      str_ret = "OBPROXY_T_ICMD_SHOW_CLUSTER";
      break;
    case OBPROXY_T_ICMD_SHOW_RESOURCE:
      str_ret = "OBPROXY_T_ICMD_SHOW_RESOURCE";
      break;
    case OBPROXY_T_ICMD_SHOW_CONGESTION:
      str_ret = "OBPROXY_T_ICMD_SHOW_CONGESTION";
      break;
    case OBPROXY_T_ICMD_SHOW_ROUTE:
      str_ret = "OBPROXY_T_ICMD_SHOW_ROUTE";
      break;
    case OBPROXY_T_ICMD_SHOW_VIP:
      str_ret = "OBPROXY_T_ICMD_SHOW_VIP";
      break;
    case OBPROXY_T_ICMD_SHOW_MEMORY:
      str_ret = "OBPROXY_T_ICMD_SHOW_MEMORY";
      break;
    case OBPROXY_T_ICMD_SHOW_SQLAUDIT:
      str_ret = "OBPROXY_T_ICMD_SHOW_SQLAUDIT";
      break;
    case OBPROXY_T_ICMD_SHOW_WARNLOG:
      str_ret = "OBPROXY_T_ICMD_SHOW_WARNLOG";
      break;
    case OBPROXY_T_ICMD_SHOW_STAT:
      str_ret = "OBPROXY_T_ICMD_SHOW_STAT";
      break;
    case OBPROXY_T_ICMD_SHOW_TRACE:
      str_ret = "OBPROXY_T_ICMD_SHOW_TRACE";
      break;
    case OBPROXY_T_ICMD_SHOW_INFO:
      str_ret = "OBPROXY_T_ICMD_SHOW_INFO";
      break;
    case OBPROXY_T_ICMD_ALTER_CONFIG:
      str_ret = "OBPROXY_T_ICMD_ALTER_CONFIG";
      break;
    case OBPROXY_T_ICMD_ALTER_RESOURCE:
      str_ret = "OBPROXY_T_ICMD_ALTER_RESOURCE";
      break;
    case OBPROXY_T_ICMD_KILL_SESSION:
      str_ret = "OBPROXY_T_ICMD_KILL_SESSION";
      break;
    case OBPROXY_T_ICMD_KILL_MYSQL:
      str_ret = "OBPROXY_T_ICMD_KILL_MYSQL";
      break;
    case OBPROXY_T_ICMD_PING:
      str_ret = "OBPROXY_T_ICMD_PING";
      break;
    case OBPROXY_T_ICMD_DUAL:
      str_ret = "OBPROXY_T_ICMD_DUAL";
      break;
    case OBPROXY_T_ICMD_MAX:
      str_ret = "OBPROXY_T_ICMD_MAX";
      break;
    case OBPROXY_T_SELECT:
      str_ret = "OBPROXY_T_SELECT";
      break;
    case OBPROXY_T_UPDATE:
      str_ret = "OBPROXY_T_UPDATE";
      break;
    case OBPROXY_T_INSERT:
      str_ret = "OBPROXY_T_INSERT";
      break;
    case OBPROXY_T_REPLACE:
      str_ret = "OBPROXY_T_REPLACE";
      break;
    case OBPROXY_T_DELETE:
      str_ret = "OBPROXY_T_DELETE";
      break;
    case OBPROXY_T_MERGE:
      str_ret = "OBPROXY_T_MERGE";
      break;
    case OBPROXY_T_CREATE:
      str_ret = "OBPROXY_T_CREATE";
      break;
    case OBPROXY_T_DROP:
      str_ret = "OBPROXY_T_DROP";
      break;
    case OBPROXY_T_ALTER:
      str_ret = "OBPROXY_T_ALTER";
      break;
    case OBPROXY_T_TRUNCATE:
      str_ret = "OBPROXY_T_TRUNCATE";
      break;
    case OBPROXY_T_RENAME:
      str_ret = "OBPROXY_T_RENAME";
      break;
    case OBPROXY_T_REVOKE:
      str_ret = "OBPROXY_T_REVOKE";
      break;
    case OBPROXY_T_GRANT:
      str_ret = "OBPROXY_T_GRANT";
      break;
    case OBPROXY_T_ANALYZE:
      str_ret = "OBPROXY_T_ANALYZE";
      break;
    case OBPROXY_T_PURGE:
      str_ret = "OBPROXY_T_PURGE";
      break;
    case OBPROXY_T_FLASHBACK:
      str_ret = "OBPROXY_T_FLASHBACK";
      break;
    case OBPROXY_T_COMMENT:
      str_ret = "OBPROXY_T_COMMENT";
      break;
    case OBPROXY_T_AUDIT:
      str_ret = "OBPROXY_T_AUDIT";
      break;
    case OBPROXY_T_NOAUDIT:
      str_ret = "OBPROXY_T_NOAUDIT";
      break;
    case OBPROXY_T_BEGIN:
      str_ret = "OBPROXY_T_BEGIN";
      break;
    case OBPROXY_T_SELECT_TX_RO:
      str_ret = "OBPROXY_T_SELECT_TX_RO";
      break;
    case OBPROXY_T_SELECT_PROXY_VERSION:
      str_ret = "OBPROXY_T_SELECT_PROXY_VERSION";
      break;
    case OBPROXY_T_SHOW_WARNINGS:
      str_ret = "OBPROXY_T_SHOW_WARNINGS";
      break;
    case OBPROXY_T_SHOW_ERRORS:
      str_ret = "OBPROXY_T_SHOW_ERRORS";
      break;
    case OBPROXY_T_SHOW_TRACE:
      str_ret = "OBPROXY_T_SHOW_TRACE";
      break;
    case OBPROXY_T_MULTI_STMT:
      str_ret = "OBPROXY_T_MULTI_STMT";
      break;
    case OBPROXY_T_USE_DB:
      str_ret = "OBPROXY_T_USE_DB";
      break;
    case OBPROXY_T_CALL:
      str_ret = "OBPROXY_T_CALL";
      break;
    case OBPROXY_T_HELP:
      str_ret = "OBPROXY_T_HELP";
      break;
    case OBPROXY_T_SET:
      str_ret = "OBPROXY_T_SET";
      break;
    case OBPROXY_T_SET_GLOBAL:
      str_ret = "OBPROXY_T_SET_GLOBAL";
      break;
    case OBPROXY_T_SET_NAMES:
      str_ret = "OBPROXY_T_SET_NAMES";
      break;
    case OBPROXY_T_SET_CHARSET:
      str_ret = "OBPROXY_T_SET_CHARSET";
      break;
    case OBPROXY_T_SET_PASSWORD:
      str_ret = "OBPROXY_T_SET_PASSWORD";
      break;
    case OBPROXY_T_SET_DEFAULT:
      str_ret = "OBPROXY_T_SET_DEFAULT";
      break;
    case OBPROXY_T_OTHERS:
      str_ret = "OBPROXY_T_OTHERS";
      break;
    case OBPROXY_T_COMMIT:
      str_ret = "OBPROXY_T_COMMIT";
      break;
    case OBPROXY_T_ROLLBACK:
      str_ret = "OBPROXY_T_ROLLBACK";
      break;
    case OBPROXY_T_SHOW:
      str_ret = "OBPROXY_T_SHOW";
      break;
    case OBPROXY_T_TEXT_PS_PREPARE:
      str_ret = "OBPROXY_T_TEXT_PS_PREPARE";
      break;
    case OBPROXY_T_TEXT_PS_EXECUTE:
      str_ret = "OBPROXY_T_TEXT_PS_EXECUTE";
      break;
    case OBPROXY_T_TEXT_PS_DROP:
      str_ret = "OBPROXY_T_TEXT_PS_DROP";
      break;
    case OBPROXY_T_LOGIN:
      str_ret = "OBPROXY_T_LOGIN";
      break;
    case OBPROXY_T_DESC:
      str_ret = "OBPROXY_T_DESC";
      break;
    case OBPROXY_T_MAX:
      str_ret = "OBPROXY_T_MAX";
      break;
    default:
      str_ret = "ERROR";
      break;
  }
  return str_ret;
}

const char* get_obproxy_sub_stmt_name(const ObProxyBasicStmtSubType type)
{
  const char* str_ret = "";
  switch (type) {
    case OBPROXY_T_SUB_INVALID:
      str_ret = "OBPROXY_T_SUB_INVALID";
      break;
    case OBPROXY_T_SUB_NET_THREAD:
      str_ret = "OBPROXY_T_SUB_NET_THREAD";
      break;
    case OBPROXY_T_SUB_NET_CONNECTION:
      str_ret = "OBPROXY_T_SUB_NET_CONNECTION";
      break;
    case OBPROXY_T_SUB_SESSION_LIST:
      str_ret = "OBPROXY_T_SUB_SESSION_LIST";
      break;
    case OBPROXY_T_SUB_SESSION_LIST_INTERNAL:
      str_ret = "OBPROXY_T_SUB_SESSION_LIST_INTERNAL";
      break;
    case OBPROXY_T_SUB_SESSION_ATTRIBUTE:
      str_ret = "OBPROXY_T_SUB_SESSION_ATTRIBUTE";
      break;
    case OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL:
      str_ret = "OBPROXY_T_SUB_SESSION_VARIABLES_LOCAL";
      break;
    case OBPROXY_T_SUB_SESSION_VARIABLES_ALL:
      str_ret = "OBPROXY_T_SUB_SESSION_VARIABLES_ALL";
      break;
    case OBPROXY_T_SUB_SESSION_STAT:
      str_ret = "OBPROXY_T_SUB_SESSION_STAT";
      break;
    case OBPROXY_T_SUB_TRACE_LIMIT:
      str_ret = "OBPROXY_T_SUB_TRACE_LIMIT";
      break;
    case OBPROXY_T_SUB_CONGEST_ALL:
      str_ret = "OBPROXY_T_SUB_CONGEST_ALL";
      break;
    case OBPROXY_T_SUB_INFO_BINARY:
      str_ret = "OBPROXY_T_SUB_INFO_BINARY";
      break;
    case OBPROXY_T_SUB_INFO_UPGRADE:
      str_ret = "OBPROXY_T_SUB_INFO_UPGRADE";
      break;
    case OBPROXY_T_SUB_INFO_IDC:
      str_ret = "OBPROXY_T_SUB_INFO_IDC";
      break;
    case OBPROXY_T_SUB_STAT_REFRESH:
      str_ret = "OBPROXY_T_SUB_STAT_REFRESH";
      break;
    case OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID:
      str_ret = "OBPROXY_T_SUB_SQLAUDIT_AUDIT_ID";
      break;
    case OBPROXY_T_SUB_SQLAUDIT_SM_ID:
      str_ret = "OBPROXY_T_SUB_SQLAUDIT_SM_ID";
      break;
    case OBPROXY_T_SUB_MEMORY_OBJPOOL:
      str_ret = "OBPROXY_T_SUB_MEMORY_OBJPOOL";
      break;
    case OBPROXY_T_SUB_CONFIG_INT_VAULE:
      str_ret = "OBPROXY_T_SUB_CONFIG_INT_VAULE";
      break;
    case OBPROXY_T_SUB_CONFIG_DIFF:
      str_ret = "OBPROXY_T_SUB_CONFIG_DIFF";
      break;
    case OBPROXY_T_SUB_CONFIG_DIFF_USER:
      str_ret = "OBPROXY_T_SUB_CONFIG_DIFF_USER";
      break;
    case OBPROXY_T_SUB_KILL_CS:
      str_ret = "OBPROXY_T_SUB_KILL_CS";
      break;
    case OBPROXY_T_SUB_KILL_SS:
      str_ret = "OBPROXY_T_SUB_KILL_SS";
      break;
    case OBPROXY_T_SUB_KILL_CONNECTION:
      str_ret = "OBPROXY_T_SUB_KILL_CONNECTION";
      break;
    case OBPROXY_T_SUB_KILL_QUERY:
      str_ret = "OBPROXY_T_SUB_KILL_QUERY";
      break;
    case OBPROXY_T_SUB_ROUTE_PARTITION:
      str_ret = "OBPROXY_T_SUB_ROUTE_PARTITION";
      break;
    case OBPROXY_T_SUB_ROUTE_ROUTINE:
      str_ret = "OBPROXY_T_SUB_ROUTE_ROUTINE";
      break;
    case OBPROXY_T_SUB_SHOW_ELASTIC_ID:
      str_ret = "OBPROXY_T_SUB_SHOW_ELASTIC_ID";
      break;
    case OBPROXY_T_SUB_SHOW_TOPOLOGY:
      str_ret = "OBPROXY_T_SUB_SHOW_TOPOLOGY";
      break;
    case OBPROXY_T_SUB_SHOW_DB_VERSION:
      str_ret = "OBPROXY_T_SUB_SHOW_DB_VERSION";
      break;
    case OBPROXY_T_SUB_SELECT_DATABASE:
      str_ret = "OBPROXY_T_SUB_SELECT_DATABASE";
      break;
    case OBPROXY_T_SUB_SHOW_DATABASES:
      str_ret = "OBPROXY_T_SUB_SHOW_DATABASES";
      break;
    case OBPROXY_T_SUB_SHOW_TABLES:
      str_ret = "OBPROXY_T_SUB_SHOW_TABLES";
      break;
    case OBPROXY_T_SUB_SHOW_FULL_TABLES:
      str_ret = "OBPROXY_T_SUB_SHOW_FULL_TABLES";
      break;
    case OBPROXY_T_SUB_SHOW_TABLE_STATUS:
      str_ret = "OBPROXY_T_SUB_SHOW_TABLE_STATUS";
      break;
    case OBPROXY_T_SUB_SHOW_CREATE_TABLE:
      str_ret = "OBPROXY_T_SUB_SHOW_CREATE_TABLE";
      break;
    case OBPROXY_T_SUB_SHOW_COLUMNS:
      str_ret = "OBPROXY_T_SUB_SHOW_COLUMNS";
      break;
    case OBPROXY_T_SUB_SHOW_INDEX:
      str_ret = "OBPROXY_T_SUB_SHOW_INDEX";
      break;
    case OBPROXY_T_SUB_DESC_TABLE:
      str_ret = "OBPROXY_T_SUB_DESC_TABLE";
      break;
    case OBPROXY_T_SUB_CREATE_TABLE:
      str_ret = "OBPROXY_T_SUB_CREATE_TABLE";
      break;
    case OBPROXY_T_SUB_CREATE_INDEX:
      str_ret = "OBPROXY_T_SUB_CREATE_INDEX";
      break;
    case OBPROXY_T_SUB_MAX:
      str_ret = "OBPROXY_T_SUB_MAX";
      break;
    default:
      str_ret = "ERROR";
      break;
  }
  return str_ret;
}

const char* get_obproxy_err_stmt_name(const ObProxyErrorStmtType type)
{
  const char* str_ret = "";
  switch (type) {
    case OBPROXY_T_ERR_INVALID:
      str_ret = "OBPROXY_T_ERR_INVALID";
      break;
    case OBPROXY_T_ERR_PARSE:
      str_ret = "OBPROXY_T_ERR_PARSE";
      break;
    case OBPROXY_T_ERR_INVALID_NUM:
      str_ret = "OBPROXY_T_ERR_INVALID_NUM";
      break;
    case OBPROXY_T_ERR_NOT_SUPPORTED:
      str_ret = "OBPROXY_T_ERR_NOT_SUPPORTED";
      break;
    case OBPROXY_T_ERR_NEED_RESP_OK:
      str_ret = "OBPROXY_T_ERR_NEED_RESP_OK";
      break;
    default:
      str_ret = "ERROR";
      break;
  }
  return str_ret;
}

const char* get_obproxy_read_consistency_string(const ObProxyReadConsistencyType type)
{
  const char* str_ret = "";
  switch (type) {
    case OBPROXY_READ_CONSISTENCY_STRONG:
      str_ret = "STRONG";
      break;
    case OBPROXY_READ_CONSISTENCY_WEAK:
      str_ret = "WEAK";
      break;
    case OBPROXY_READ_CONSISTENCY_FROZEN:
      str_ret = "FROZEN";
      break;
    case OBPROXY_READ_CONSISTENCY_INVALID:
    default:
      str_ret = "INVALID";
      break;
  }
  return str_ret;
}

const char* get_obproxy_call_token_type(const ObProxyCallTokenType type)
{
  const char *str_ret = "invalid token type";
  switch (type) {
    case CALL_TOKEN_NONE:
      str_ret = "CALL_TOKEN_NONE";
      break;
    case CALL_TOKEN_STR_VAL:
      str_ret = "CALL_TOKEN_STR_VAL";
      break;
    case CALL_TOKEN_INT_VAL:
      str_ret = "CALL_TOKEN_INT_VAL";
      break;
    case CALL_TOKEN_NUMBER_VAL:
      str_ret = "CALL_TOKEN_NUMBER_VAL";
      break;
    case CALL_TOKEN_SYS_VAR:
      str_ret = "CALL_TOKEN_SYS_VAR";
      break;
    case CALL_TOKEN_USER_VAR:
      str_ret = "CALL_TOKEN_USER_VAR";
      break;
    case CALL_TOKEN_PLACE_HOLDER:
      str_ret = "CALL_TOKEN_PLACE_HOLDER";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_obproxy_dbmesh_token_type(const ObDbMeshTokenType type)
{
  const char *str_ret = "invalid token type";
  switch (type) {
    case DBMESH_TOKEN_NONE:
      str_ret = "DBMESH_TOKEN_NONE";
      break;
    case DBMESH_TOKEN_STR_VAL:
      str_ret = "DBMESH_TOKEN_STR_VAL";
      break;
    case DBMESH_TOKEN_INT_VAL:
      str_ret = "DBMESH_TOKEN_INT_VAL";
      break;
    case DBMESH_TOKEN_COLUMN:
      str_ret = "DBMESH_TOKEN_COLUMN";
      break;
    default:
      break;
  }
  return str_ret;
}

const char* get_obproxy_quote_name(const ObProxyParseQuoteType type)
{
  const char *str_ret = "";
  switch (type) {
    case OBPROXY_QUOTE_T_INVALID:
      str_ret = "NON QUOTE";
      break;
    case OBPROXY_QUOTE_T_SINGLE:
      str_ret = "SINGLE QUOTE";
      break;
    case OBPROXY_QUOTE_T_DOUBLE:
      str_ret = "DOUBLE QUOTE";
      break;
    case OBPROXY_QUOTE_T_BACK:
     str_ret = "BACKTICK QUOTE";
     break;
    default:
      str_ret = "INVALID";
      break;
  }
  return str_ret;
}
