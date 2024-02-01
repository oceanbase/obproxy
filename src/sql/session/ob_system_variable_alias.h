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

#ifndef OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_ALIAS_
#define OCEANBASE_SQL_SESSION_OB_SYSTEM_VARIABLE_ALIAS_
namespace oceanbase
{
namespace sql
{
  static const char* const OB_SV_AUTO_INCREMENT_INCREMENT = "auto_increment_increment";
  static const char* const OB_SV_AUTO_INCREMENT_OFFSET = "auto_increment_offset";
  static const char* const OB_SV_AUTOCOMMIT = "autocommit";
  static const char* const OB_SV_CHARACTER_SET_CLIENT = "character_set_client";
  static const char* const OB_SV_CHARACTER_SET_CONNECTION = "character_set_connection";
  static const char* const OB_SV_CHARACTER_SET_DATABASE = "character_set_database";
  static const char* const OB_SV_CHARACTER_SET_RESULTS = "character_set_results";
  static const char* const OB_SV_CHARACTER_SET_SERVER = "character_set_server";
  static const char* const OB_SV_CHARACTER_SET_SYSTEM = "character_set_system";
  static const char* const OB_SV_COLLATION_CONNECTION = "collation_connection";
  static const char* const OB_SV_COLLATION_DATABASE = "collation_database";
  static const char* const OB_SV_COLLATION_SERVER = "collation_server";
  static const char* const OB_SV_INTERACTIVE_TIMEOUT = "interactive_timeout";
  static const char* const OB_SV_LAST_INSERT_ID = "last_insert_id";
  static const char* const OB_SV_MAX_ALLOWED_PACKET = "max_allowed_packet";
  static const char* const OB_SV_SQL_MODE = "sql_mode";
  static const char* const OB_SV_TIME_ZONE = "time_zone";
  static const char* const OB_SV_TX_ISOLATION = "tx_isolation";
  static const char* const OB_SV_VERSION_COMMENT = "version_comment";
  static const char* const OB_SV_WAIT_TIMEOUT = "wait_timeout";
  static const char* const OB_SV_BINLOG_ROW_IMAGE = "binlog_row_image";
  static const char* const OB_SV_CHARACTER_SET_FILESYSTEM = "character_set_filesystem";
  static const char* const OB_SV_CONNECT_TIMEOUT = "connect_timeout";
  static const char* const OB_SV_DATADIR = "datadir";
  static const char* const OB_SV_DEBUG_SYNC = "debug_sync";
  static const char* const OB_SV_DIV_PRECISION_INCREMENT = "div_precision_increment";
  static const char* const OB_SV_EXPLICIT_DEFAULTS_FOR_TIMESTAMP = "explicit_defaults_for_timestamp";
  static const char* const OB_SV_GROUP_CONCAT_MAX_LEN = "group_concat_max_len";
  static const char* const OB_SV_IDENTITY = "identity";
  static const char* const OB_SV_LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
  static const char* const OB_SV_NET_READ_TIMEOUT = "net_read_timeout";
  static const char* const OB_SV_NET_WRITE_TIMEOUT = "net_write_timeout";
  static const char* const OB_SV_READ_ONLY = "read_only";
  static const char* const OB_SV_SQL_AUTO_IS_NULL = "sql_auto_is_null";
  static const char* const OB_SV_SQL_SELECT_LIMIT = "sql_select_limit";
  static const char* const OB_SV_TIMESTAMP = "timestamp";
  static const char* const OB_SV_TX_READ_ONLY = "tx_read_only";
  static const char* const OB_SV_VERSION = "version";
  static const char* const OB_SV_SQL_WARNINGS = "sql_warnings";
  static const char* const OB_SV_MAX_USER_CONNECTIONS = "max_user_connections";
  static const char* const OB_SV_INIT_CONNECT = "init_connect";
  static const char* const OB_SV_LICENSE = "license";
  static const char* const OB_SV_NET_BUFFER_LENGTH = "net_buffer_length";
  static const char* const OB_SV_SYSTEM_TIME_ZONE = "system_time_zone";
  static const char* const OB_SV_QUERY_CACHE_SIZE = "query_cache_size";
  static const char* const OB_SV_QUERY_CACHE_TYPE = "query_cache_type";
  static const char* const OB_SV_DEFAULT_REPLICA_NUM = "ob_default_replica_num";
  static const char* const OB_SV_INTERM_RESULT_MEM_LIMIT = "ob_interm_result_mem_limit";
  static const char* const OB_SV_PROXY_PARTITION_HIT = "ob_proxy_partition_hit";
  static const char* const OB_SV_LOG_LEVEL = "ob_log_level";
  static const char* const OB_SV_MAX_PARALLEL_DEGREE = "ob_max_parallel_degree";
  static const char* const OB_SV_QUERY_TIMEOUT = "ob_query_timeout";
  static const char* const OB_SV_READ_CONSISTENCY = "ob_read_consistency";
  static const char* const OB_SV_ENABLE_TRANSFORMATION = "ob_enable_transformation";
  static const char* const OB_SV_TRX_TIMEOUT = "ob_trx_timeout";
  static const char* const OB_SV_ENABLE_PLAN_CACHE = "ob_enable_plan_cache";
  static const char* const OB_SV_ENABLE_INDEX_DIRECT_SELECT = "ob_enable_index_direct_select";
  static const char* const OB_SV_PROXY_SET_TRX_EXECUTED = "ob_proxy_set_trx_executed";
  static const char* const OB_SV_PROXY_SESSION_TEMPORARY_TABLE_USED = "_ob_proxy_session_temporary_table_used";
  static const char* const OB_SV_ENABLE_AGGREGATION_PUSHDOWN = "ob_enable_aggregation_pushdown";
  static const char* const OB_SV_LAST_SCHEMA_VERSION = "ob_last_schema_version";
  static const char* const OB_SV_GLOBAL_DEBUG_SYNC = "ob_global_debug_sync";
  static const char* const OB_SV_PROXY_GLOBAL_VARIABLES_VERSION = "ob_proxy_global_variables_version";
  static const char* const OB_SV_ENABLE_TRACE_LOG = "ob_enable_trace_log";
  static const char* const OB_SV_ENABLE_HASH_GROUP_BY = "ob_enable_hash_group_by";
  static const char* const OB_SV_ENABLE_BLK_NESTEDLOOP_JOIN = "ob_enable_blk_nestedloop_join";
  static const char* const OB_SV_BNL_JOIN_CACHE_SIZE = "ob_bnl_join_cache_size";
  static const char* const OB_SV_PROXY_USER_PRIVILEGE = "ob_proxy_user_privilege";
  static const char* const OB_SV_ORG_CLUSTER_ID = "ob_org_cluster_id";
  static const char* const OB_SV_PLAN_CACHE_PERCENTAGE = "ob_plan_cache_percentage";
  static const char* const OB_SV_PLAN_CACHE_EVICT_HIGH_PERCENTAGE = "ob_plan_cache_evict_high_percentage";
  static const char* const OB_SV_PLAN_CACHE_EVICT_LOW_PERCENTAGE = "ob_plan_cache_evict_low_percentage";
  static const char* const OB_SV_CAPABILITY_FLAG = "ob_capability_flag";
  static const char* const OB_SV_SAFE_WEAK_READ_SNAPSHOT = "ob_safe_weak_read_snapshot";
  static const char* const OB_SV_ROUTE_POLICY = "ob_route_policy";
  static const char* const OB_SV_ENABLE_TRANSMISSION_CHECKSUM = "ob_enable_transmission_checksum";
  static const char* const OB_SV_STATEMENT_TRACE_ID = "ob_statement_trace_id";
  static const char* const OB_SV_CLIENT_REROUTE_INFO = "ob_client_reroute_info";
  static const char* const OB_SV_NLS_DATE_FORMAT = "nls_date_format";
  static const char* const OB_SV_NLS_TIMESTAMP_FORMAT = "nls_timestamp_format";
  static const char* const OB_SV_NLS_TIMESTAMP_TZ_FORMAT = "nls_timestamp_tz_format";

  // Feedback on whether weak read sql routed to all leader replica or all follower replica
  // no _ob_proxy_weakread_feedback  -> all follower
  // _ob_proxy_weakread_feedback = 1 -> invalid replica
  // _ob_proxy_weakread_feedback = 2 -> all leader
  // _ob_proxy_weakread_feedback = 3 -> part leader / part follower
  static const char* const OB_SV_WEAK_READ_REPLICA_HIT = "_ob_proxy_weakread_feedback";
  static const char* const OB_SV_NCHARACTER_SET_CONNECTION = "ncharacter_set_connection";
  static const char* const OB_SV_NLS_CURRENCY = "nls_currency";
  static const char* const OB_SV_NLS_ISO_CURRENCY = "nls_iso_currency";
  static const char* const OB_SV_NLS_DUAL_CURRENCY = "nls_dual_currency";
  static const char* const OB_SV_NLS_NUMERIC_CHARACTERS = "nls_numeric_characters";
}
}
#endif //OCEANBASE_COMMON_OB_SYSTEM_VARIABLE_ALIAS_H_

