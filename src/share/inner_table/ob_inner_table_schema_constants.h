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

#ifndef _OB_INNER_TABLE_SCHEMA_CONSTANTS_H_
#define _OB_INNER_TABLE_SCHEMA_CONSTANTS_H_

#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
}
}

namespace share
{
const uint64_t MAX_CORE_TABLE_ID                               = 100;
const uint64_t MAX_SYS_TABLE_ID                                = 10000;
const uint64_t MAX_VIRTUAL_TABLE_ID                            = 20000;
const uint64_t MAX_SYS_VIEW_TABLE_ID                           = 30000;

const uint64_t OB_ALL_CORE_TABLE_TID                           = 1; // "__all_core_table"
const uint64_t OB_ALL_ROOT_TABLE_TID                           = 2; // "__all_root_table"
const uint64_t OB_ALL_TABLE_TID                                = 3; // "__all_table"
const uint64_t OB_ALL_COLUMN_TID                               = 4; // "__all_column"
const uint64_t OB_ALL_DDL_OPERATION_TID                        = 5; // "__all_ddl_operation"
const uint64_t OB_ALL_META_TABLE_TID                           = 101; // "__all_meta_table"
const uint64_t OB_ALL_USER_TID                                 = 102; // "__all_user"
const uint64_t OB_ALL_USER_HISTORY_TID                         = 103; // "__all_user_history"
const uint64_t OB_ALL_DATABASE_TID                             = 104; // "__all_database"
const uint64_t OB_ALL_DATABASE_HISTORY_TID                     = 105; // "__all_database_history"
const uint64_t OB_ALL_TABLEGROUP_TID                           = 106; // "__all_tablegroup"
const uint64_t OB_ALL_TABLEGROUP_HISTORY_TID                   = 107; // "__all_tablegroup_history"
const uint64_t OB_ALL_TENANT_TID                               = 108; // "__all_tenant"
const uint64_t OB_ALL_TENANT_HISTORY_TID                       = 109; // "__all_tenant_history"
const uint64_t OB_ALL_TABLE_PRIVILEGE_TID                      = 110; // "__all_table_privilege"
const uint64_t OB_ALL_TABLE_PRIVILEGE_HISTORY_TID              = 111; // "__all_table_privilege_history"
const uint64_t OB_ALL_DATABASE_PRIVILEGE_TID                   = 112; // "__all_database_privilege"
const uint64_t OB_ALL_DATABASE_PRIVILEGE_HISTORY_TID           = 113; // "__all_database_privilege_history"
const uint64_t OB_ALL_TABLE_HISTORY_TID                        = 114; // "__all_table_history"
const uint64_t OB_ALL_COLUMN_HISTORY_TID                       = 115; // "__all_column_history"
const uint64_t OB_ALL_ZONE_TID                                 = 116; // "__all_zone"
const uint64_t OB_ALL_SERVER_TID                               = 117; // "__all_server"
const uint64_t OB_ALL_SYS_PARAMETER_TID                        = 118; // "__all_sys_parameter"
const uint64_t OB_ALL_SYS_VARIABLE_TID                         = 120; // "__all_sys_variable"
const uint64_t OB_ALL_SYS_STAT_TID                             = 121; // "__all_sys_stat"
const uint64_t OB_ALL_COLUMN_STATISTIC_TID                     = 122; // "__all_column_statistic"
const uint64_t OB_ALL_UNIT_TID                                 = 123; // "__all_unit"
const uint64_t OB_ALL_UNIT_CONFIG_TID                          = 124; // "__all_unit_config"
const uint64_t OB_ALL_RESOURCE_POOL_TID                        = 125; // "__all_resource_pool"
const uint64_t OB_ALL_TENANT_RESOURCE_USAGE_TID                = 126; // "__all_tenant_resource_usage"
const uint64_t OB_ALL_SEQUENCE_TID                             = 127; // "__all_sequence"
const uint64_t OB_ALL_CHARSET_TID                              = 128; // "__all_charset"
const uint64_t OB_ALL_COLLATION_TID                            = 129; // "__all_collation"
const uint64_t OB_HELP_TOPIC_TID                               = 130; // "help_topic"
const uint64_t OB_HELP_CATEGORY_TID                            = 131; // "help_category"
const uint64_t OB_HELP_KEYWORD_TID                             = 132; // "help_keyword"
const uint64_t OB_HELP_RELATION_TID                            = 133; // "help_relation"
const uint64_t OB_ALL_LOCAL_INDEX_STATUS_TID                   = 134; // "__all_local_index_status"
const uint64_t OB_ALL_DUMMY_TID                                = 135; // "__all_dummy"
const uint64_t OB_ALL_FROZEN_MAP_TID                           = 136; // "__all_frozen_map"
const uint64_t OB_ALL_CLOG_HISTORY_INFO_TID                    = 137; // "__all_clog_history_info"
const uint64_t OB_ALL_CLOG_HISTORY_INFO_V2_TID                 = 139; // "__all_clog_history_info_v2"
const uint64_t OB_ALL_ROOTSERVICE_EVENT_HISTORY_TID            = 140; // "__all_rootservice_event_history"
const uint64_t OB_ALL_PRIVILEGE_TID                            = 141; // "__all_privilege"
const uint64_t OB_ALL_OUTLINE_TID                              = 142; // "__all_outline"
const uint64_t OB_ALL_OUTLINE_HISTORY_TID                      = 143; // "__all_outline_history"
const uint64_t OB_ALL_ELECTION_EVENT_HISTORY_TID               = 144; // "__all_election_event_history"
const uint64_t OB_ALL_RECYCLEBIN_TID                           = 145; // "__all_recyclebin"
const uint64_t OB_ALL_PART_TID                                 = 146; // "__all_part"
const uint64_t OB_ALL_PART_HISTORY_TID                         = 147; // "__all_part_history"
const uint64_t OB_ALL_SUB_PART_TID                             = 148; // "__all_sub_part"
const uint64_t OB_ALL_SUB_PART_HISTORY_TID                     = 149; // "__all_sub_part_history"
const uint64_t OB_ALL_PART_INFO_TID                            = 150; // "__all_part_info"
const uint64_t OB_ALL_PART_INFO_HISTORY_TID                    = 151; // "__all_part_info_history"
const uint64_t OB_ALL_DEF_SUB_PART_TID                         = 152; // "__all_def_sub_part"
const uint64_t OB_ALL_DEF_SUB_PART_HISTORY_TID                 = 153; // "__all_def_sub_part_history"
const uint64_t OB_ALL_SERVER_EVENT_HISTORY_TID                 = 154; // "__all_server_event_history"
const uint64_t OB_ALL_TIME_ZONE_TID                            = 155; // "__all_time_zone"
const uint64_t OB_ALL_TIME_ZONE_NAME_TID                       = 156; // "__all_time_zone_name"
const uint64_t OB_ALL_TIME_ZONE_TRANSITION_TID                 = 157; // "__all_time_zone_transition"
const uint64_t OB_ALL_TIME_ZONE_TRANSITION_TYPE_TID            = 158; // "__all_time_zone_transition_type"
const uint64_t OB_TENANT_VIRTUAL_ALL_TABLE_TID                 = 10001; // "__tenant_virtual_all_table"
const uint64_t OB_TENANT_VIRTUAL_TABLE_COLUMN_TID              = 10002; // "__tenant_virtual_table_column"
const uint64_t OB_TENANT_VIRTUAL_TABLE_INDEX_TID               = 10003; // "__tenant_virtual_table_index"
const uint64_t OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID      = 10004; // "__tenant_virtual_show_create_database"
const uint64_t OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID         = 10005; // "__tenant_virtual_show_create_table"
const uint64_t OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID          = 10006; // "__tenant_virtual_session_variable"
const uint64_t OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID           = 10007; // "__tenant_virtual_privilege_grant"
const uint64_t OB_ALL_VIRTUAL_PROCESSLIST_TID                  = 10008; // "__all_virtual_processlist"
const uint64_t OB_TENANT_VIRTUAL_WARNING_TID                   = 10009; // "__tenant_virtual_warning"
const uint64_t OB_TENANT_VIRTUAL_CURRENT_TENANT_TID            = 10010; // "__tenant_virtual_current_tenant"
const uint64_t OB_TENANT_VIRTUAL_DATABASE_STATUS_TID           = 10011; // "__tenant_virtual_database_status"
const uint64_t OB_TENANT_VIRTUAL_TENANT_STATUS_TID             = 10012; // "__tenant_virtual_tenant_status"
const uint64_t OB_TENANT_VIRTUAL_INTERM_RESULT_TID             = 10013; // "__tenant_virtual_interm_result"
const uint64_t OB_TENANT_VIRTUAL_PARTITION_STAT_TID            = 10014; // "__tenant_virtual_partition_stat"
const uint64_t OB_TENANT_VIRTUAL_STATNAME_TID                  = 10015; // "__tenant_virtual_statname"
const uint64_t OB_TENANT_VIRTUAL_EVENT_NAME_TID                = 10016; // "__tenant_virtual_event_name"
const uint64_t OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID           = 10017; // "__tenant_virtual_global_variable"
const uint64_t OB_ALL_VIRTUAL_CORE_META_TABLE_TID              = 11001; // "__all_virtual_core_meta_table"
const uint64_t OB_ALL_VIRTUAL_ZONE_STAT_TID                    = 11002; // "__all_virtual_zone_stat"
const uint64_t OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TID              = 11003; // "__all_virtual_plan_cache_stat"
const uint64_t OB_ALL_VIRTUAL_PLAN_STAT_TID                    = 11004; // "__all_virtual_plan_stat"
const uint64_t OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TID        = 11006; // "__all_virtual_mem_leak_checker_info"
const uint64_t OB_ALL_VIRTUAL_LATCH_TID                        = 11007; // "__all_virtual_latch"
const uint64_t OB_ALL_VIRTUAL_KVCACHE_INFO_TID                 = 11008; // "__all_virtual_kvcache_info"
const uint64_t OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID              = 11009; // "__all_virtual_data_type_class"
const uint64_t OB_ALL_VIRTUAL_DATA_TYPE_TID                    = 11010; // "__all_virtual_data_type"
const uint64_t OB_ALL_VIRTUAL_SERVER_STAT_TID                  = 11011; // "__all_virtual_server_stat"
const uint64_t OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TID          = 11012; // "__all_virtual_rebalance_task_stat"
const uint64_t OB_ALL_VIRTUAL_SESSION_EVENT_TID                = 11013; // "__all_virtual_session_event"
const uint64_t OB_ALL_VIRTUAL_SESSION_WAIT_TID                 = 11014; // "__all_virtual_session_wait"
const uint64_t OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID         = 11015; // "__all_virtual_session_wait_history"
const uint64_t OB_ALL_VIRTUAL_SYSTEM_EVENT_TID                 = 11017; // "__all_virtual_system_event"
const uint64_t OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TID         = 11018; // "__all_virtual_tenant_memstore_info"
const uint64_t OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TID      = 11019; // "__all_virtual_concurrency_object_pool"
const uint64_t OB_ALL_VIRTUAL_SESSTAT_TID                      = 11020; // "__all_virtual_sesstat"
const uint64_t OB_ALL_VIRTUAL_SYSSTAT_TID                      = 11021; // "__all_virtual_sysstat"
const uint64_t OB_ALL_VIRTUAL_STORAGE_STAT_TID                 = 11022; // "__all_virtual_storage_stat"
const uint64_t OB_ALL_VIRTUAL_DISK_STAT_TID                    = 11023; // "__all_virtual_disk_stat"
const uint64_t OB_ALL_VIRTUAL_MEMSTORE_INFO_TID                = 11024; // "__all_virtual_memstore_info"
const uint64_t OB_ALL_VIRTUAL_PARTITION_INFO_TID               = 11025; // "__all_virtual_partition_info"
const uint64_t OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID           = 11026; // "__all_virtual_upgrade_inspection"
const uint64_t OB_ALL_VIRTUAL_TRANS_STAT_TID                   = 11027; // "__all_virtual_trans_stat"
const uint64_t OB_ALL_VIRTUAL_TRANS_MGR_STAT_TID               = 11028; // "__all_virtual_trans_mgr_stat"
const uint64_t OB_ALL_VIRTUAL_ELECTION_INFO_TID                = 11029; // "__all_virtual_election_info"
const uint64_t OB_ALL_VIRTUAL_ELECTION_MEM_STAT_TID            = 11030; // "__all_virtual_election_mem_stat"
const uint64_t OB_ALL_VIRTUAL_SQL_AUDIT_TID                    = 11031; // "__all_virtual_sql_audit"
const uint64_t OB_ALL_VIRTUAL_TRANS_MEM_STAT_TID               = 11032; // "__all_virtual_trans_mem_stat"
const uint64_t OB_ALL_VIRTUAL_PARTITION_SSTABLE_IMAGE_INFO_TID = 11033; // "__all_virtual_partition_sstable_image_info"
const uint64_t OB_ALL_VIRTUAL_CORE_ROOT_TABLE_TID              = 11034; // "__all_virtual_core_root_table"
const uint64_t OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID               = 11035; // "__all_virtual_core_all_table"
const uint64_t OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID            = 11036; // "__all_virtual_core_column_table"
const uint64_t OB_ALL_VIRTUAL_MEMORY_INFO_TID                  = 11037; // "__all_virtual_memory_info"
const uint64_t OB_ALL_VIRTUAL_TENANT_STAT_TID                  = 11038; // "__all_virtual_tenant_stat"
const uint64_t OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TID           = 11039; // "__all_virtual_sys_parameter_stat"
const uint64_t OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_TID      = 11040; // "__all_virtual_partition_replay_status"
const uint64_t OB_ALL_VIRTUAL_CLOG_STAT_TID                    = 11041; // "__all_virtual_clog_stat"
const uint64_t OB_ALL_VIRTUAL_TRACE_LOG_TID                    = 11042; // "__all_virtual_trace_log"
const uint64_t OB_ALL_VIRTUAL_ENGINE_TID                       = 11043; // "__all_virtual_engine"
const uint64_t OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TID            = 11045; // "__all_virtual_proxy_server_stat"
const uint64_t OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TID           = 11046; // "__all_virtual_proxy_sys_variable"
const uint64_t OB_ALL_VIRTUAL_PROXY_SCHEMA_TID                 = 11047; // "__all_virtual_proxy_schema"
const uint64_t OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TID      = 11048; // "__all_virtual_plan_cache_plan_explain"
const uint64_t OB_ALL_VIRTUAL_OBRPC_STAT_TID                   = 11049; // "__all_virtual_obrpc_stat"
const uint64_t OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TID             = 11050; // "__all_virtual_sql_plan_monitor"
const uint64_t OB_ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO_TID = 11051; // "__all_virtual_partition_sstable_merge_info"
const uint64_t OB_ALL_VIRTUAL_SQL_MONITOR_TID                  = 11052; // "__all_virtual_sql_monitor"
const uint64_t OB_TENANT_VIRTUAL_OUTLINE_TID                   = 11053; // "__tenant_virtual_outline"
const uint64_t OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID      = 11054; // "__tenant_virtual_concurrent_limit_sql"
const uint64_t OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_TID          = 11055; // "__all_virtual_sql_plan_statistics"
const uint64_t OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_TID = 11056; // "__all_virtual_partition_sstable_macro_info"
const uint64_t OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TID         = 11057; // "__all_virtual_proxy_partition_info"
const uint64_t OB_ALL_VIRTUAL_PROXY_PARTITION_TID              = 11058; // "__all_virtual_proxy_partition"
const uint64_t OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TID          = 11059; // "__all_virtual_proxy_sub_partition"
const uint64_t OB_ALL_VIRTUAL_PROXY_ROUTE_TID                  = 11060; // "__all_virtual_proxy_route"
const uint64_t OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TID        = 11061; // "__all_virtual_rebalance_tenant_stat"
const uint64_t OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TID          = 11062; // "__all_virtual_rebalance_unit_stat"
const uint64_t OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TID       = 11063; // "__all_virtual_rebalance_replica_stat"
const uint64_t OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_TID = 11064; // "__all_virtual_partition_amplification_stat"
const uint64_t OB_ALL_VIRTUAL_ELECTION_EVENT_HISTORY_TID       = 11067; // "__all_virtual_election_event_history"
const uint64_t OB_ALL_VIRTUAL_PARTITION_SORTED_STORES_INFO_TID = 11068; // "__all_virtual_partition_sorted_stores_info"
const uint64_t OB_ALL_VIRTUAL_LEADER_STAT_TID                  = 11069; // "__all_virtual_leader_stat"
const uint64_t OB_ALL_VIRTUAL_PARTITION_MIGRATION_STATUS_TID   = 11070; // "__all_virtual_partition_migration_status"
const uint64_t OB_ALL_VIRTUAL_SYS_TASK_STATUS_TID              = 11071; // "__all_virtual_sys_task_status"
const uint64_t OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TID    = 11072; // "__all_virtual_macro_block_marker_status"
const uint64_t OB_COLUMNS_TID                                  = 12000; // "COLUMNS"
const uint64_t OB_SESSION_VARIABLES_TID                        = 12001; // "SESSION_VARIABLES"
const uint64_t OB_TABLE_PRIVILEGES_TID                         = 12002; // "TABLE_PRIVILEGES"
const uint64_t OB_USER_PRIVILEGES_TID                          = 12003; // "USER_PRIVILEGES"
const uint64_t OB_SCHEMA_PRIVILEGES_TID                        = 12004; // "SCHEMA_PRIVILEGES"
const uint64_t OB_TABLE_CONSTRAINTS_TID                        = 12005; // "TABLE_CONSTRAINTS"
const uint64_t OB_GLOBAL_STATUS_TID                            = 12006; // "GLOBAL_STATUS"
const uint64_t OB_PARTITIONS_TID                               = 12007; // "PARTITIONS"
const uint64_t OB_SESSION_STATUS_TID                           = 12008; // "SESSION_STATUS"
const uint64_t OB_USER_TID                                     = 12009; // "user"
const uint64_t OB_DB_TID                                       = 12010; // "db"
const uint64_t OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TID           = 12011; // "__all_virtual_server_memory_info"
const uint64_t OB_GV_PLAN_CACHE_STAT_TID                       = 20001; // "gv$plan_cache_stat"
const uint64_t OB_GV_PLAN_CACHE_PLAN_STAT_TID                  = 20002; // "gv$plan_cache_plan_stat"
const uint64_t OB_SCHEMATA_TID                                 = 20003; // "SCHEMATA"
const uint64_t OB_CHARACTER_SETS_TID                           = 20004; // "CHARACTER_SETS"
const uint64_t OB_GLOBAL_VARIABLES_TID                         = 20005; // "GLOBAL_VARIABLES"
const uint64_t OB_STATISTICS_TID                               = 20006; // "STATISTICS"
const uint64_t OB_VIEWS_TID                                    = 20007; // "VIEWS"
const uint64_t OB_TABLES_TID                                   = 20008; // "TABLES"
const uint64_t OB_COLLATIONS_TID                               = 20009; // "COLLATIONS"
const uint64_t OB_COLLATION_CHARACTER_SET_APPLICABILITY_TID    = 20010; // "COLLATION_CHARACTER_SET_APPLICABILITY"
const uint64_t OB_PROCESSLIST_TID                              = 20011; // "PROCESSLIST"
const uint64_t OB_KEY_COLUMN_USAGE_TID                         = 20012; // "KEY_COLUMN_USAGE"
const uint64_t OB_DBA_OUTLINES_TID                             = 20013; // "DBA_OUTLINES"
const uint64_t OB_GV_SESSION_EVENT_TID                         = 21000; // "gv$session_event"
const uint64_t OB_GV_SESSION_WAIT_TID                          = 21001; // "gv$session_wait"
const uint64_t OB_GV_SESSION_WAIT_HISTORY_TID                  = 21002; // "gv$session_wait_history"
const uint64_t OB_GV_SYSTEM_EVENT_TID                          = 21003; // "gv$system_event"
const uint64_t OB_GV_SESSTAT_TID                               = 21004; // "gv$sesstat"
const uint64_t OB_GV_SYSSTAT_TID                               = 21005; // "gv$sysstat"
const uint64_t OB_V_STATNAME_TID                               = 21006; // "v$statname"
const uint64_t OB_V_EVENT_NAME_TID                             = 21007; // "v$event_name"
const uint64_t OB_V_SESSION_EVENT_TID                          = 21008; // "v$session_event"
const uint64_t OB_V_SESSION_WAIT_TID                           = 21009; // "v$session_wait"
const uint64_t OB_V_SESSION_WAIT_HISTORY_TID                   = 21010; // "v$session_wait_history"
const uint64_t OB_V_SESSTAT_TID                                = 21011; // "v$sesstat"
const uint64_t OB_V_SYSSTAT_TID                                = 21012; // "v$sysstat"
const uint64_t OB_V_SYSTEM_EVENT_TID                           = 21013; // "v$system_event"
const uint64_t OB_GV_SQL_AUDIT_TID                             = 21014; // "gv$sql_audit"
const uint64_t OB_GV_LATCH_TID                                 = 21015; // "gv$latch"
const uint64_t OB_GV_MEMORY_TID                                = 21016; // "gv$memory"
const uint64_t OB_V_MEMORY_TID                                 = 21017; // "v$memory"
const uint64_t OB_GV_MEMSTORE_TID                              = 21018; // "gv$memstore"
const uint64_t OB_V_MEMSTORE_TID                               = 21019; // "v$memstore"
const uint64_t OB_GV_MEMSTORE_INFO_TID                         = 21020; // "gv$memstore_info"
const uint64_t OB_V_MEMSTORE_INFO_TID                          = 21021; // "v$memstore_info"
const uint64_t OB_V_PLAN_CACHE_STAT_TID                        = 21022; // "v$plan_cache_stat"
const uint64_t OB_V_PLAN_CACHE_PLAN_STAT_TID                   = 21023; // "v$plan_cache_plan_stat"
const uint64_t OB_GV_PLAN_CACHE_PLAN_EXPLAIN_TID               = 21024; // "gv$plan_cache_plan_explain"
const uint64_t OB_V_PLAN_CACHE_PLAN_EXPLAIN_TID                = 21025; // "v$plan_cache_plan_explain"
const uint64_t OB_V_SQL_AUDIT_TID                              = 21026; // "v$sql_audit"
const uint64_t OB_V_LATCH_TID                                  = 21027; // "v$latch"
const uint64_t OB_GV_OBRPC_OUTGOING_TID                        = 21028; // "gv$obrpc_outgoing"
const uint64_t OB_V_OBRPC_OUTGOING_TID                         = 21029; // "v$obrpc_outgoing"
const uint64_t OB_GV_OBRPC_INCOMING_TID                        = 21030; // "gv$obrpc_incoming"
const uint64_t OB_V_OBRPC_INCOMING_TID                         = 21031; // "v$obrpc_incoming"
const uint64_t OB_GV_SQL_TID                                   = 21032; // "gv$sql"
const uint64_t OB_V_SQL_TID                                    = 21033; // "v$sql"
const uint64_t OB_GV_SQL_MONITOR_TID                           = 21034; // "gv$sql_monitor"
const uint64_t OB_V_SQL_MONITOR_TID                            = 21035; // "v$sql_monitor"
const uint64_t OB_GV_SQL_PLAN_MONITOR_TID                      = 21036; // "gv$sql_plan_monitor"
const uint64_t OB_V_SQL_PLAN_MONITOR_TID                       = 21037; // "v$sql_plan_monitor"
const uint64_t OB_USER_RECYCLEBIN_TID                          = 21038; // "USER_RECYCLEBIN"
const uint64_t OB_GV_OUTLINE_TID                               = 21039; // "gv$outline"
const uint64_t OB_GV_CONCURRENT_LIMIT_SQL_TID                  = 21040; // "gv$concurrent_limit_sql"
const uint64_t OB_GV_SQL_PLAN_STATISTICS_TID                   = 21041; // "gv$sql_plan_statistics"
const uint64_t OB_V_SQL_PLAN_STATISTICS_TID                    = 21042; // "v$sql_plan_statistics"
const uint64_t OB_GV_SERVER_MEMSTORE_TID                       = 21043; // "gv$server_memstore"
const uint64_t OB_TIME_ZONE_TID                                = 21044; // "time_zone"
const uint64_t OB_TIME_ZONE_NAME_TID                           = 21045; // "time_zone_name"
const uint64_t OB_TIME_ZONE_TRANSITION_TID                     = 21046; // "time_zone_transition"
const uint64_t OB_TIME_ZONE_TRANSITION_TYPE_TID                = 21047; // "time_zone_transition_type"
const uint64_t OB_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TID           = 19999; // "__all_virtual_plan_cache_stat"
const uint64_t OB_ALL_VIRTUAL_SESSION_EVENT_I1_TID             = 19998; // "__all_virtual_session_event"
const uint64_t OB_ALL_VIRTUAL_SESSION_WAIT_I1_TID              = 19997; // "__all_virtual_session_wait"
const uint64_t OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TID      = 19996; // "__all_virtual_session_wait_history"
const uint64_t OB_ALL_VIRTUAL_SYSTEM_EVENT_I1_TID              = 19995; // "__all_virtual_system_event"
const uint64_t OB_ALL_VIRTUAL_SESSTAT_I1_TID                   = 19994; // "__all_virtual_sesstat"
const uint64_t OB_ALL_VIRTUAL_SYSSTAT_I1_TID                   = 19993; // "__all_virtual_sysstat"
const uint64_t OB_ALL_VIRTUAL_SQL_AUDIT_I1_TID                 = 19992; // "__all_virtual_sql_audit"

const char *const OB_ALL_CORE_TABLE_TNAME                           = "__all_core_table";
const char *const OB_ALL_ROOT_TABLE_TNAME                           = "__all_root_table";
const char *const OB_ALL_TABLE_TNAME                                = "__all_table";
const char *const OB_ALL_COLUMN_TNAME                               = "__all_column";
const char *const OB_ALL_DDL_OPERATION_TNAME                        = "__all_ddl_operation";
const char *const OB_ALL_META_TABLE_TNAME                           = "__all_meta_table";
const char *const OB_ALL_USER_TNAME                                 = "__all_user";
const char *const OB_ALL_USER_HISTORY_TNAME                         = "__all_user_history";
const char *const OB_ALL_DATABASE_TNAME                             = "__all_database";
const char *const OB_ALL_DATABASE_HISTORY_TNAME                     = "__all_database_history";
const char *const OB_ALL_TABLEGROUP_TNAME                           = "__all_tablegroup";
const char *const OB_ALL_TABLEGROUP_HISTORY_TNAME                   = "__all_tablegroup_history";
const char *const OB_ALL_TENANT_TNAME                               = "__all_tenant";
const char *const OB_ALL_TENANT_HISTORY_TNAME                       = "__all_tenant_history";
const char *const OB_ALL_TABLE_PRIVILEGE_TNAME                      = "__all_table_privilege";
const char *const OB_ALL_TABLE_PRIVILEGE_HISTORY_TNAME              = "__all_table_privilege_history";
const char *const OB_ALL_DATABASE_PRIVILEGE_TNAME                   = "__all_database_privilege";
const char *const OB_ALL_DATABASE_PRIVILEGE_HISTORY_TNAME           = "__all_database_privilege_history";
const char *const OB_ALL_TABLE_HISTORY_TNAME                        = "__all_table_history";
const char *const OB_ALL_COLUMN_HISTORY_TNAME                       = "__all_column_history";
const char *const OB_ALL_ZONE_TNAME                                 = "__all_zone";
const char *const OB_ALL_SERVER_TNAME                               = "__all_server";
const char *const OB_ALL_SYS_PARAMETER_TNAME                        = "__all_sys_parameter";
const char *const OB_ALL_SYS_VARIABLE_TNAME                         = "__all_sys_variable";
const char *const OB_ALL_SYS_STAT_TNAME                             = "__all_sys_stat";
const char *const OB_ALL_COLUMN_STATISTIC_TNAME                     = "__all_column_statistic";
const char *const OB_ALL_UNIT_TNAME                                 = "__all_unit";
const char *const OB_ALL_UNIT_CONFIG_TNAME                          = "__all_unit_config";
const char *const OB_ALL_RESOURCE_POOL_TNAME                        = "__all_resource_pool";
const char *const OB_ALL_TENANT_RESOURCE_USAGE_TNAME                = "__all_tenant_resource_usage";
const char *const OB_ALL_SEQUENCE_TNAME                             = "__all_sequence";
const char *const OB_ALL_CHARSET_TNAME                              = "__all_charset";
const char *const OB_ALL_COLLATION_TNAME                            = "__all_collation";
const char *const OB_HELP_TOPIC_TNAME                               = "help_topic";
const char *const OB_HELP_CATEGORY_TNAME                            = "help_category";
const char *const OB_HELP_KEYWORD_TNAME                             = "help_keyword";
const char *const OB_HELP_RELATION_TNAME                            = "help_relation";
const char *const OB_ALL_LOCAL_INDEX_STATUS_TNAME                   = "__all_local_index_status";
const char *const OB_ALL_DUMMY_TNAME                                = "__all_dummy";
const char *const OB_ALL_FROZEN_MAP_TNAME                           = "__all_frozen_map";
const char *const OB_ALL_CLOG_HISTORY_INFO_TNAME                    = "__all_clog_history_info";
const char *const OB_ALL_CLOG_HISTORY_INFO_V2_TNAME                 = "__all_clog_history_info_v2";
const char *const OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME            = "__all_rootservice_event_history";
const char *const OB_ALL_PRIVILEGE_TNAME                            = "__all_privilege";
const char *const OB_ALL_OUTLINE_TNAME                              = "__all_outline";
const char *const OB_ALL_OUTLINE_HISTORY_TNAME                      = "__all_outline_history";
const char *const OB_ALL_ELECTION_EVENT_HISTORY_TNAME               = "__all_election_event_history";
const char *const OB_ALL_RECYCLEBIN_TNAME                           = "__all_recyclebin";
const char *const OB_ALL_PART_TNAME                                 = "__all_part";
const char *const OB_ALL_PART_HISTORY_TNAME                         = "__all_part_history";
const char *const OB_ALL_SUB_PART_TNAME                             = "__all_sub_part";
const char *const OB_ALL_SUB_PART_HISTORY_TNAME                     = "__all_sub_part_history";
const char *const OB_ALL_PART_INFO_TNAME                            = "__all_part_info";
const char *const OB_ALL_PART_INFO_HISTORY_TNAME                    = "__all_part_info_history";
const char *const OB_ALL_DEF_SUB_PART_TNAME                         = "__all_def_sub_part";
const char *const OB_ALL_DEF_SUB_PART_HISTORY_TNAME                 = "__all_def_sub_part_history";
const char *const OB_ALL_SERVER_EVENT_HISTORY_TNAME                 = "__all_server_event_history";
const char *const OB_ALL_TIME_ZONE_TNAME                            = "__all_time_zone";
const char *const OB_ALL_TIME_ZONE_NAME_TNAME                       = "__all_time_zone_name";
const char *const OB_ALL_TIME_ZONE_TRANSITION_TNAME                 = "__all_time_zone_transition";
const char *const OB_ALL_TIME_ZONE_TRANSITION_TYPE_TNAME            = "__all_time_zone_transition_type";
const char *const OB_TENANT_VIRTUAL_ALL_TABLE_TNAME                 = "__tenant_virtual_all_table";
const char *const OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME              = "__tenant_virtual_table_column";
const char *const OB_TENANT_VIRTUAL_TABLE_INDEX_TNAME               = "__tenant_virtual_table_index";
const char *const OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TNAME      = "__tenant_virtual_show_create_database";
const char *const OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME         = "__tenant_virtual_show_create_table";
const char *const OB_TENANT_VIRTUAL_SESSION_VARIABLE_TNAME          = "__tenant_virtual_session_variable";
const char *const OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TNAME           = "__tenant_virtual_privilege_grant";
const char *const OB_ALL_VIRTUAL_PROCESSLIST_TNAME                  = "__all_virtual_processlist";
const char *const OB_TENANT_VIRTUAL_WARNING_TNAME                   = "__tenant_virtual_warning";
const char *const OB_TENANT_VIRTUAL_CURRENT_TENANT_TNAME            = "__tenant_virtual_current_tenant";
const char *const OB_TENANT_VIRTUAL_DATABASE_STATUS_TNAME           = "__tenant_virtual_database_status";
const char *const OB_TENANT_VIRTUAL_TENANT_STATUS_TNAME             = "__tenant_virtual_tenant_status";
const char *const OB_TENANT_VIRTUAL_INTERM_RESULT_TNAME             = "__tenant_virtual_interm_result";
const char *const OB_TENANT_VIRTUAL_PARTITION_STAT_TNAME            = "__tenant_virtual_partition_stat";
const char *const OB_TENANT_VIRTUAL_STATNAME_TNAME                  = "__tenant_virtual_statname";
const char *const OB_TENANT_VIRTUAL_EVENT_NAME_TNAME                = "__tenant_virtual_event_name";
const char *const OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TNAME           = "__tenant_virtual_global_variable";
const char *const OB_ALL_VIRTUAL_CORE_META_TABLE_TNAME              = "__all_virtual_core_meta_table";
const char *const OB_ALL_VIRTUAL_ZONE_STAT_TNAME                    = "__all_virtual_zone_stat";
const char *const OB_ALL_VIRTUAL_PLAN_CACHE_STAT_TNAME              = "__all_virtual_plan_cache_stat";
const char *const OB_ALL_VIRTUAL_PLAN_STAT_TNAME                    = "__all_virtual_plan_stat";
const char *const OB_ALL_VIRTUAL_MEM_LEAK_CHECKER_INFO_TNAME        = "__all_virtual_mem_leak_checker_info";
const char *const OB_ALL_VIRTUAL_LATCH_TNAME                        = "__all_virtual_latch";
const char *const OB_ALL_VIRTUAL_KVCACHE_INFO_TNAME                 = "__all_virtual_kvcache_info";
const char *const OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TNAME              = "__all_virtual_data_type_class";
const char *const OB_ALL_VIRTUAL_DATA_TYPE_TNAME                    = "__all_virtual_data_type";
const char *const OB_ALL_VIRTUAL_SERVER_STAT_TNAME                  = "__all_virtual_server_stat";
const char *const OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TNAME          = "__all_virtual_rebalance_task_stat";
const char *const OB_ALL_VIRTUAL_SESSION_EVENT_TNAME                = "__all_virtual_session_event";
const char *const OB_ALL_VIRTUAL_SESSION_WAIT_TNAME                 = "__all_virtual_session_wait";
const char *const OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TNAME         = "__all_virtual_session_wait_history";
const char *const OB_ALL_VIRTUAL_SYSTEM_EVENT_TNAME                 = "__all_virtual_system_event";
const char *const OB_ALL_VIRTUAL_TENANT_MEMSTORE_INFO_TNAME         = "__all_virtual_tenant_memstore_info";
const char *const OB_ALL_VIRTUAL_CONCURRENCY_OBJECT_POOL_TNAME      = "__all_virtual_concurrency_object_pool";
const char *const OB_ALL_VIRTUAL_SESSTAT_TNAME                      = "__all_virtual_sesstat";
const char *const OB_ALL_VIRTUAL_SYSSTAT_TNAME                      = "__all_virtual_sysstat";
const char *const OB_ALL_VIRTUAL_STORAGE_STAT_TNAME                 = "__all_virtual_storage_stat";
const char *const OB_ALL_VIRTUAL_DISK_STAT_TNAME                    = "__all_virtual_disk_stat";
const char *const OB_ALL_VIRTUAL_MEMSTORE_INFO_TNAME                = "__all_virtual_memstore_info";
const char *const OB_ALL_VIRTUAL_PARTITION_INFO_TNAME               = "__all_virtual_partition_info";
const char *const OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TNAME           = "__all_virtual_upgrade_inspection";
const char *const OB_ALL_VIRTUAL_TRANS_STAT_TNAME                   = "__all_virtual_trans_stat";
const char *const OB_ALL_VIRTUAL_TRANS_MGR_STAT_TNAME               = "__all_virtual_trans_mgr_stat";
const char *const OB_ALL_VIRTUAL_ELECTION_INFO_TNAME                = "__all_virtual_election_info";
const char *const OB_ALL_VIRTUAL_ELECTION_MEM_STAT_TNAME            = "__all_virtual_election_mem_stat";
const char *const OB_ALL_VIRTUAL_SQL_AUDIT_TNAME                    = "__all_virtual_sql_audit";
const char *const OB_ALL_VIRTUAL_TRANS_MEM_STAT_TNAME               = "__all_virtual_trans_mem_stat";
const char *const OB_ALL_VIRTUAL_PARTITION_SSTABLE_IMAGE_INFO_TNAME = "__all_virtual_partition_sstable_image_info";
const char *const OB_ALL_VIRTUAL_CORE_ROOT_TABLE_TNAME              = "__all_virtual_core_root_table";
const char *const OB_ALL_VIRTUAL_CORE_ALL_TABLE_TNAME               = "__all_virtual_core_all_table";
const char *const OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TNAME            = "__all_virtual_core_column_table";
const char *const OB_ALL_VIRTUAL_MEMORY_INFO_TNAME                  = "__all_virtual_memory_info";
const char *const OB_ALL_VIRTUAL_TENANT_STAT_TNAME                  = "__all_virtual_tenant_stat";
const char *const OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TNAME           = "__all_virtual_sys_parameter_stat";
const char *const OB_ALL_VIRTUAL_PARTITION_REPLAY_STATUS_TNAME      = "__all_virtual_partition_replay_status";
const char *const OB_ALL_VIRTUAL_CLOG_STAT_TNAME                    = "__all_virtual_clog_stat";
const char *const OB_ALL_VIRTUAL_TRACE_LOG_TNAME                    = "__all_virtual_trace_log";
const char *const OB_ALL_VIRTUAL_ENGINE_TNAME                       = "__all_virtual_engine";
const char *const OB_ALL_VIRTUAL_PROXY_SERVER_STAT_TNAME            = "__all_virtual_proxy_server_stat";
const char *const OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TNAME           = "__all_virtual_proxy_sys_variable";
const char *const OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME                 = "__all_virtual_proxy_schema";
const char *const OB_ALL_VIRTUAL_PLAN_CACHE_PLAN_EXPLAIN_TNAME      = "__all_virtual_plan_cache_plan_explain";
const char *const OB_ALL_VIRTUAL_OBRPC_STAT_TNAME                   = "__all_virtual_obrpc_stat";
const char *const OB_ALL_VIRTUAL_SQL_PLAN_MONITOR_TNAME             = "__all_virtual_sql_plan_monitor";
const char *const OB_ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO_TNAME = "__all_virtual_partition_sstable_merge_info";
const char *const OB_ALL_VIRTUAL_SQL_MONITOR_TNAME                  = "__all_virtual_sql_monitor";
const char *const OB_TENANT_VIRTUAL_OUTLINE_TNAME                   = "__tenant_virtual_outline";
const char *const OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TNAME      = "__tenant_virtual_concurrent_limit_sql";
const char *const OB_ALL_VIRTUAL_SQL_PLAN_STATISTICS_TNAME          = "__all_virtual_sql_plan_statistics";
const char *const OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_TNAME = "__all_virtual_partition_sstable_macro_info";
const char *const OB_ALL_VIRTUAL_PROXY_PARTITION_INFO_TNAME         = "__all_virtual_proxy_partition_info";
const char *const OB_ALL_VIRTUAL_PROXY_PARTITION_TNAME              = "__all_virtual_proxy_partition";
const char *const OB_ALL_VIRTUAL_PROXY_SUB_PARTITION_TNAME          = "__all_virtual_proxy_sub_partition";
const char *const OB_ALL_VIRTUAL_PROXY_ROUTE_TNAME                  = "__all_virtual_proxy_route";
const char *const OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TNAME        = "__all_virtual_rebalance_tenant_stat";
const char *const OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TNAME          = "__all_virtual_rebalance_unit_stat";
const char *const OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TNAME       = "__all_virtual_rebalance_replica_stat";
const char *const OB_ALL_VIRTUAL_PARTITION_AMPLIFICATION_STAT_TNAME = "__all_virtual_partition_amplification_stat";
const char *const OB_ALL_VIRTUAL_ELECTION_EVENT_HISTORY_TNAME       = "__all_virtual_election_event_history";
const char *const OB_ALL_VIRTUAL_PARTITION_SORTED_STORES_INFO_TNAME = "__all_virtual_partition_sorted_stores_info";
const char *const OB_ALL_VIRTUAL_LEADER_STAT_TNAME                  = "__all_virtual_leader_stat";
const char *const OB_ALL_VIRTUAL_PARTITION_MIGRATION_STATUS_TNAME   = "__all_virtual_partition_migration_status";
const char *const OB_ALL_VIRTUAL_SYS_TASK_STATUS_TNAME              = "__all_virtual_sys_task_status";
const char *const OB_ALL_VIRTUAL_MACRO_BLOCK_MARKER_STATUS_TNAME    = "__all_virtual_macro_block_marker_status";
const char *const OB_COLUMNS_TNAME                                  = "COLUMNS";
const char *const OB_SESSION_VARIABLES_TNAME                        = "SESSION_VARIABLES";
const char *const OB_TABLE_PRIVILEGES_TNAME                         = "TABLE_PRIVILEGES";
const char *const OB_USER_PRIVILEGES_TNAME                          = "USER_PRIVILEGES";
const char *const OB_SCHEMA_PRIVILEGES_TNAME                        = "SCHEMA_PRIVILEGES";
const char *const OB_TABLE_CONSTRAINTS_TNAME                        = "TABLE_CONSTRAINTS";
const char *const OB_GLOBAL_STATUS_TNAME                            = "GLOBAL_STATUS";
const char *const OB_PARTITIONS_TNAME                               = "PARTITIONS";
const char *const OB_SESSION_STATUS_TNAME                           = "SESSION_STATUS";
const char *const OB_USER_TNAME                                     = "user";
const char *const OB_DB_TNAME                                       = "db";
const char *const OB_ALL_VIRTUAL_SERVER_MEMORY_INFO_TNAME           = "__all_virtual_server_memory_info";
const char *const OB_GV_PLAN_CACHE_STAT_TNAME                       = "gv$plan_cache_stat";
const char *const OB_GV_PLAN_CACHE_PLAN_STAT_TNAME                  = "gv$plan_cache_plan_stat";
const char *const OB_SCHEMATA_TNAME                                 = "SCHEMATA";
const char *const OB_CHARACTER_SETS_TNAME                           = "CHARACTER_SETS";
const char *const OB_GLOBAL_VARIABLES_TNAME                         = "GLOBAL_VARIABLES";
const char *const OB_STATISTICS_TNAME                               = "STATISTICS";
const char *const OB_VIEWS_TNAME                                    = "VIEWS";
const char *const OB_TABLES_TNAME                                   = "TABLES";
const char *const OB_COLLATIONS_TNAME                               = "COLLATIONS";
const char *const OB_COLLATION_CHARACTER_SET_APPLICABILITY_TNAME    = "COLLATION_CHARACTER_SET_APPLICABILITY";
const char *const OB_PROCESSLIST_TNAME                              = "PROCESSLIST";
const char *const OB_KEY_COLUMN_USAGE_TNAME                         = "KEY_COLUMN_USAGE";
const char *const OB_DBA_OUTLINES_TNAME                             = "DBA_OUTLINES";
const char *const OB_GV_SESSION_EVENT_TNAME                         = "gv$session_event";
const char *const OB_GV_SESSION_WAIT_TNAME                          = "gv$session_wait";
const char *const OB_GV_SESSION_WAIT_HISTORY_TNAME                  = "gv$session_wait_history";
const char *const OB_GV_SYSTEM_EVENT_TNAME                          = "gv$system_event";
const char *const OB_GV_SESSTAT_TNAME                               = "gv$sesstat";
const char *const OB_GV_SYSSTAT_TNAME                               = "gv$sysstat";
const char *const OB_V_STATNAME_TNAME                               = "v$statname";
const char *const OB_V_EVENT_NAME_TNAME                             = "v$event_name";
const char *const OB_V_SESSION_EVENT_TNAME                          = "v$session_event";
const char *const OB_V_SESSION_WAIT_TNAME                           = "v$session_wait";
const char *const OB_V_SESSION_WAIT_HISTORY_TNAME                   = "v$session_wait_history";
const char *const OB_V_SESSTAT_TNAME                                = "v$sesstat";
const char *const OB_V_SYSSTAT_TNAME                                = "v$sysstat";
const char *const OB_V_SYSTEM_EVENT_TNAME                           = "v$system_event";
const char *const OB_GV_SQL_AUDIT_TNAME                             = "gv$sql_audit";
const char *const OB_GV_LATCH_TNAME                                 = "gv$latch";
const char *const OB_GV_MEMORY_TNAME                                = "gv$memory";
const char *const OB_V_MEMORY_TNAME                                 = "v$memory";
const char *const OB_GV_MEMSTORE_TNAME                              = "gv$memstore";
const char *const OB_V_MEMSTORE_TNAME                               = "v$memstore";
const char *const OB_GV_MEMSTORE_INFO_TNAME                         = "gv$memstore_info";
const char *const OB_V_MEMSTORE_INFO_TNAME                          = "v$memstore_info";
const char *const OB_V_PLAN_CACHE_STAT_TNAME                        = "v$plan_cache_stat";
const char *const OB_V_PLAN_CACHE_PLAN_STAT_TNAME                   = "v$plan_cache_plan_stat";
const char *const OB_GV_PLAN_CACHE_PLAN_EXPLAIN_TNAME               = "gv$plan_cache_plan_explain";
const char *const OB_V_PLAN_CACHE_PLAN_EXPLAIN_TNAME                = "v$plan_cache_plan_explain";
const char *const OB_V_SQL_AUDIT_TNAME                              = "v$sql_audit";
const char *const OB_V_LATCH_TNAME                                  = "v$latch";
const char *const OB_GV_OBRPC_OUTGOING_TNAME                        = "gv$obrpc_outgoing";
const char *const OB_V_OBRPC_OUTGOING_TNAME                         = "v$obrpc_outgoing";
const char *const OB_GV_OBRPC_INCOMING_TNAME                        = "gv$obrpc_incoming";
const char *const OB_V_OBRPC_INCOMING_TNAME                         = "v$obrpc_incoming";
const char *const OB_GV_SQL_TNAME                                   = "gv$sql";
const char *const OB_V_SQL_TNAME                                    = "v$sql";
const char *const OB_GV_SQL_MONITOR_TNAME                           = "gv$sql_monitor";
const char *const OB_V_SQL_MONITOR_TNAME                            = "v$sql_monitor";
const char *const OB_GV_SQL_PLAN_MONITOR_TNAME                      = "gv$sql_plan_monitor";
const char *const OB_V_SQL_PLAN_MONITOR_TNAME                       = "v$sql_plan_monitor";
const char *const OB_USER_RECYCLEBIN_TNAME                          = "USER_RECYCLEBIN";
const char *const OB_GV_OUTLINE_TNAME                               = "gv$outline";
const char *const OB_GV_CONCURRENT_LIMIT_SQL_TNAME                  = "gv$concurrent_limit_sql";
const char *const OB_GV_SQL_PLAN_STATISTICS_TNAME                   = "gv$sql_plan_statistics";
const char *const OB_V_SQL_PLAN_STATISTICS_TNAME                    = "v$sql_plan_statistics";
const char *const OB_GV_SERVER_MEMSTORE_TNAME                       = "gv$server_memstore";
const char *const OB_TIME_ZONE_TNAME                                = "time_zone";
const char *const OB_TIME_ZONE_NAME_TNAME                           = "time_zone_name";
const char *const OB_TIME_ZONE_TRANSITION_TNAME                     = "time_zone_transition";
const char *const OB_TIME_ZONE_TRANSITION_TYPE_TNAME                = "time_zone_transition_type";
const char *const OB_ALL_VIRTUAL_PLAN_CACHE_STAT_I1_TNAME           = "__idx_1099511638779_i1";
const char *const OB_ALL_VIRTUAL_SESSION_EVENT_I1_TNAME             = "__idx_1099511638789_i1";
const char *const OB_ALL_VIRTUAL_SESSION_WAIT_I1_TNAME              = "__idx_1099511638790_i1";
const char *const OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_I1_TNAME      = "__idx_1099511638791_i1";
const char *const OB_ALL_VIRTUAL_SYSTEM_EVENT_I1_TNAME              = "__idx_1099511638793_i1";
const char *const OB_ALL_VIRTUAL_SESSTAT_I1_TNAME                   = "__idx_1099511638796_i1";
const char *const OB_ALL_VIRTUAL_SYSSTAT_I1_TNAME                   = "__idx_1099511638797_i1";
const char *const OB_ALL_VIRTUAL_SQL_AUDIT_I1_TNAME                 = "__idx_1099511638807_i1";

// initial data for __all_privilege
struct PrivilegeRow {
  const char *privilege_;
  const char *context_;
  const char *comment_;
};

const char* const ALTER_TB_MSG         = "To alter the table";
const char* const CREATE_DB_TB_MSG     = "To create new databases and tables";
const char* const CREATE_VIEW_MSG      = "To create new views";
const char* const CREATE_USER_MSG      = "To create new users";
const char* const DELETE_ROWS_MSG      = "To delete existing rows";
const char* const DROP_DB_TB_VIEWS_MSG = "To drop databases, tables, and views";
const char* const GRANT_OPTION_MSG     = "To give to other users those privileges you possess";
const char* const INDEX_MSG            = "To create or drop indexes";
const char* const INSERT_MSG           = "To insert data into tables";
const char* const PROCESS_MSG          = "To view the plain text of currently executing queries";
const char* const SELECT_MSG           = "To retrieve rows from table";
const char* const SHOW_DB_MSG          = "To see all databases with SHOW DATABASES";
const char* const SHOW_VIEW_MSG        = "To see views with SHOW CREATE VIEW";
const char* const SUPER_MSG            = "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc.";
const char* const UPDATE_MSG           = "To update existing rows";
const char* const USAGE_MSG            = "No privileges - allow connect only";
static const PrivilegeRow all_privileges[] =
{
  {"Alter", "Tables",  ALTER_TB_MSG},
  {"Create", "Databases,Tables,Indexes",  CREATE_DB_TB_MSG},
  {"Create view", "Tables",  CREATE_VIEW_MSG},
  {"Create user", "Server Admin",  CREATE_USER_MSG},
  {"Delete", "Tables",  DELETE_ROWS_MSG},
  {"Drop", "Databases,Tables", DROP_DB_TB_VIEWS_MSG},
  {"Grant option",  "Databases,Tables,Functions,Procedures", GRANT_OPTION_MSG},
  {"Index", "Tables",  INDEX_MSG},
  {"Insert", "Tables",  INSERT_MSG},
  {"Process", "Server Admin", PROCESS_MSG},
  {"Select", "Tables",  SELECT_MSG},
  {"Show databases","Server Admin", SHOW_DB_MSG},
  {"Show view","Tables", SHOW_VIEW_MSG},
  {"Super","Server Admin", SUPER_MSG},
  {"Update", "Tables",  UPDATE_MSG},
  {"Usage","Server Admin",USAGE_MSG},
  {NULL, NULL, NULL},
};

} // end namespace share
} // end namespace oceanbase
#endif /* _OB_INNER_TABLE_SCHEMA_CONSTANTS_H_ */
