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

#ifndef _OB_INNER_TABLE_SCHEMA_H_
#define _OB_INNER_TABLE_SCHEMA_H_

#include "lib/ob_define.h"
#include "ob_inner_table_schema_constants.h"

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

struct ALL_VIRTUAL_PLAN_STAT_CDE {
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    PLAN_ID,
    SQL_ID,
    TYPE,
    STATEMENT,
    SPECIAL_PARAMS,
    SYS_VARS,
    PLAN_HASH,
    FIRST_LOAD_TIME,
    SCHEMA_VERSION,
    MERGED_VERSION,
    LAST_ACTIVE_TIME,
    AVG_EXE_USEC,
    SLOWEST_EXE_TIME,
    SLOWEST_EXE_USEC,
    SLOW_COUNT,
    HIT_COUNT,
    PLAN_SIZE,
    EXECUTIONS,
    DISK_READS,
    DIRECT_WRITES,
    BUFFER_GETS,
    APPLICATION_WAIT_TIME,
    CONCURRENCY_WAIT_TIME,
    USER_IO_WAIT_TIME,
    ROWS_PROCESSED,
    ELAPSED_TIME,
    CPU_TIME,
    LARGE_QUERYS,
    DELAYED_LARGE_QUERYS,
    OUTLINE_VERSION,
    OUTLINE_ID,
    TABLE_SCAN
  };
};

class ObInnerTableSchema
{

public:
  static int all_core_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_root_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_schema(share::schema::ObTableSchema &table_schema);
  static int all_ddl_operation_schema(share::schema::ObTableSchema &table_schema);
  static int all_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_schema(share::schema::ObTableSchema &table_schema);
  static int all_user_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_schema(share::schema::ObTableSchema &table_schema);
  static int all_tablegroup_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_database_privilege_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_table_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_parameter_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_sys_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_column_statistic_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_schema(share::schema::ObTableSchema &table_schema);
  static int all_unit_config_schema(share::schema::ObTableSchema &table_schema);
  static int all_resource_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_tenant_resource_usage_schema(share::schema::ObTableSchema &table_schema);
  static int all_sequence_schema(share::schema::ObTableSchema &table_schema);
  static int all_charset_schema(share::schema::ObTableSchema &table_schema);
  static int all_collation_schema(share::schema::ObTableSchema &table_schema);
  static int help_topic_schema(share::schema::ObTableSchema &table_schema);
  static int help_category_schema(share::schema::ObTableSchema &table_schema);
  static int help_keyword_schema(share::schema::ObTableSchema &table_schema);
  static int help_relation_schema(share::schema::ObTableSchema &table_schema);
  static int all_local_index_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_dummy_schema(share::schema::ObTableSchema &table_schema);
  static int all_frozen_map_schema(share::schema::ObTableSchema &table_schema);
  static int all_clog_history_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_clog_history_info_v2_schema(share::schema::ObTableSchema &table_schema);
  static int all_rootservice_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_privilege_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_schema(share::schema::ObTableSchema &table_schema);
  static int all_outline_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_election_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_sub_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_part_info_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_schema(share::schema::ObTableSchema &table_schema);
  static int all_def_sub_part_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_server_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int all_time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_all_table_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_column_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_table_index_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_database_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_show_create_table_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_session_variable_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_privilege_grant_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_processlist_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_warning_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_current_tenant_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_database_status_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_tenant_status_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_interm_result_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_partition_stat_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_statname_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_event_name_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_global_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_meta_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_zone_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_mem_leak_checker_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_latch_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_kvcache_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_class_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_data_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_task_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_concurrency_object_pool_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_storage_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_disk_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_upgrade_inspection_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_mgr_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_mem_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trans_mem_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sstable_image_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_root_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_all_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_core_column_table_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_tenant_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_parameter_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_replay_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_clog_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_trace_log_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_engine_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_server_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sys_variable_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_schema_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_obrpc_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sstable_merge_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_outline_schema(share::schema::ObTableSchema &table_schema);
  static int tenant_virtual_concurrent_limit_sql_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_plan_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sstable_macro_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_partition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_sub_partition_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_proxy_route_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_tenant_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_unit_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_rebalance_replica_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_amplification_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_election_event_history_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_sorted_stores_info_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_leader_stat_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_partition_migration_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sys_task_status_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_macro_block_marker_status_schema(share::schema::ObTableSchema &table_schema);
  static int columns_schema(share::schema::ObTableSchema &table_schema);
  static int session_variables_schema(share::schema::ObTableSchema &table_schema);
  static int table_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int user_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int schema_privileges_schema(share::schema::ObTableSchema &table_schema);
  static int table_constraints_schema(share::schema::ObTableSchema &table_schema);
  static int global_status_schema(share::schema::ObTableSchema &table_schema);
  static int partitions_schema(share::schema::ObTableSchema &table_schema);
  static int session_status_schema(share::schema::ObTableSchema &table_schema);
  static int user_schema(share::schema::ObTableSchema &table_schema);
  static int db_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_server_memory_info_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int schemata_schema(share::schema::ObTableSchema &table_schema);
  static int character_sets_schema(share::schema::ObTableSchema &table_schema);
  static int global_variables_schema(share::schema::ObTableSchema &table_schema);
  static int statistics_schema(share::schema::ObTableSchema &table_schema);
  static int views_schema(share::schema::ObTableSchema &table_schema);
  static int tables_schema(share::schema::ObTableSchema &table_schema);
  static int collations_schema(share::schema::ObTableSchema &table_schema);
  static int collation_character_set_applicability_schema(share::schema::ObTableSchema &table_schema);
  static int processlist_schema(share::schema::ObTableSchema &table_schema);
  static int key_column_usage_schema(share::schema::ObTableSchema &table_schema);
  static int dba_outlines_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int gv_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int gv_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int v_statname_schema(share::schema::ObTableSchema &table_schema);
  static int v_event_name_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_event_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_schema(share::schema::ObTableSchema &table_schema);
  static int v_session_wait_history_schema(share::schema::ObTableSchema &table_schema);
  static int v_sesstat_schema(share::schema::ObTableSchema &table_schema);
  static int v_sysstat_schema(share::schema::ObTableSchema &table_schema);
  static int v_system_event_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int gv_latch_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memory_schema(share::schema::ObTableSchema &table_schema);
  static int v_memory_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int v_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int gv_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_memstore_info_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_stat_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_plan_stat_schema(share::schema::ObTableSchema &table_schema);
  static int gv_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int v_plan_cache_plan_explain_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_audit_schema(share::schema::ObTableSchema &table_schema);
  static int v_latch_schema(share::schema::ObTableSchema &table_schema);
  static int gv_obrpc_outgoing_schema(share::schema::ObTableSchema &table_schema);
  static int v_obrpc_outgoing_schema(share::schema::ObTableSchema &table_schema);
  static int gv_obrpc_incoming_schema(share::schema::ObTableSchema &table_schema);
  static int v_obrpc_incoming_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_monitor_schema(share::schema::ObTableSchema &table_schema);
  static int user_recyclebin_schema(share::schema::ObTableSchema &table_schema);
  static int gv_outline_schema(share::schema::ObTableSchema &table_schema);
  static int gv_concurrent_limit_sql_schema(share::schema::ObTableSchema &table_schema);
  static int gv_sql_plan_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int v_sql_plan_statistics_schema(share::schema::ObTableSchema &table_schema);
  static int gv_server_memstore_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_name_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_transition_schema(share::schema::ObTableSchema &table_schema);
  static int time_zone_transition_type_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_plan_cache_stat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_session_wait_history_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_system_event_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sesstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sysstat_i1_schema(share::schema::ObTableSchema &table_schema);
  static int all_virtual_sql_audit_i1_schema(share::schema::ObTableSchema &table_schema);

private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerTableSchema);
};

typedef int (*schema_create_func)(share::schema::ObTableSchema &table_schema);

const schema_create_func core_table_schema_creators [] = {
  ObInnerTableSchema::all_root_table_schema,
  ObInnerTableSchema::all_table_schema,
  ObInnerTableSchema::all_column_schema,
  ObInnerTableSchema::all_ddl_operation_schema,
  NULL,};

const schema_create_func sys_table_schema_creators [] = {
  ObInnerTableSchema::all_meta_table_schema,
  ObInnerTableSchema::all_user_schema,
  ObInnerTableSchema::all_user_history_schema,
  ObInnerTableSchema::all_database_schema,
  ObInnerTableSchema::all_database_history_schema,
  ObInnerTableSchema::all_tablegroup_schema,
  ObInnerTableSchema::all_tablegroup_history_schema,
  ObInnerTableSchema::all_tenant_schema,
  ObInnerTableSchema::all_tenant_history_schema,
  ObInnerTableSchema::all_table_privilege_schema,
  ObInnerTableSchema::all_table_privilege_history_schema,
  ObInnerTableSchema::all_database_privilege_schema,
  ObInnerTableSchema::all_database_privilege_history_schema,
  ObInnerTableSchema::all_table_history_schema,
  ObInnerTableSchema::all_column_history_schema,
  ObInnerTableSchema::all_zone_schema,
  ObInnerTableSchema::all_server_schema,
  ObInnerTableSchema::all_sys_parameter_schema,
  ObInnerTableSchema::all_sys_variable_schema,
  ObInnerTableSchema::all_sys_stat_schema,
  ObInnerTableSchema::all_column_statistic_schema,
  ObInnerTableSchema::all_unit_schema,
  ObInnerTableSchema::all_unit_config_schema,
  ObInnerTableSchema::all_resource_pool_schema,
  ObInnerTableSchema::all_tenant_resource_usage_schema,
  ObInnerTableSchema::all_sequence_schema,
  ObInnerTableSchema::all_charset_schema,
  ObInnerTableSchema::all_collation_schema,
  ObInnerTableSchema::help_topic_schema,
  ObInnerTableSchema::help_category_schema,
  ObInnerTableSchema::help_keyword_schema,
  ObInnerTableSchema::help_relation_schema,
  ObInnerTableSchema::all_local_index_status_schema,
  ObInnerTableSchema::all_dummy_schema,
  ObInnerTableSchema::all_frozen_map_schema,
  ObInnerTableSchema::all_clog_history_info_schema,
  ObInnerTableSchema::all_clog_history_info_v2_schema,
  ObInnerTableSchema::all_rootservice_event_history_schema,
  ObInnerTableSchema::all_privilege_schema,
  ObInnerTableSchema::all_outline_schema,
  ObInnerTableSchema::all_outline_history_schema,
  ObInnerTableSchema::all_election_event_history_schema,
  ObInnerTableSchema::all_recyclebin_schema,
  ObInnerTableSchema::all_part_schema,
  ObInnerTableSchema::all_part_history_schema,
  ObInnerTableSchema::all_sub_part_schema,
  ObInnerTableSchema::all_sub_part_history_schema,
  ObInnerTableSchema::all_part_info_schema,
  ObInnerTableSchema::all_part_info_history_schema,
  ObInnerTableSchema::all_def_sub_part_schema,
  ObInnerTableSchema::all_def_sub_part_history_schema,
  ObInnerTableSchema::all_server_event_history_schema,
  ObInnerTableSchema::all_time_zone_schema,
  ObInnerTableSchema::all_time_zone_name_schema,
  ObInnerTableSchema::all_time_zone_transition_schema,
  ObInnerTableSchema::all_time_zone_transition_type_schema,
  NULL,};

const schema_create_func virtual_table_schema_creators [] = {
  ObInnerTableSchema::tenant_virtual_all_table_schema,
  ObInnerTableSchema::tenant_virtual_table_column_schema,
  ObInnerTableSchema::tenant_virtual_table_index_schema,
  ObInnerTableSchema::tenant_virtual_show_create_database_schema,
  ObInnerTableSchema::tenant_virtual_show_create_table_schema,
  ObInnerTableSchema::tenant_virtual_session_variable_schema,
  ObInnerTableSchema::tenant_virtual_privilege_grant_schema,
  ObInnerTableSchema::all_virtual_processlist_schema,
  ObInnerTableSchema::tenant_virtual_warning_schema,
  ObInnerTableSchema::tenant_virtual_current_tenant_schema,
  ObInnerTableSchema::tenant_virtual_database_status_schema,
  ObInnerTableSchema::tenant_virtual_tenant_status_schema,
  ObInnerTableSchema::tenant_virtual_interm_result_schema,
  ObInnerTableSchema::tenant_virtual_partition_stat_schema,
  ObInnerTableSchema::tenant_virtual_statname_schema,
  ObInnerTableSchema::tenant_virtual_event_name_schema,
  ObInnerTableSchema::tenant_virtual_global_variable_schema,
  ObInnerTableSchema::all_virtual_core_meta_table_schema,
  ObInnerTableSchema::all_virtual_zone_stat_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_schema,
  ObInnerTableSchema::all_virtual_plan_stat_schema,
  ObInnerTableSchema::all_virtual_mem_leak_checker_info_schema,
  ObInnerTableSchema::all_virtual_latch_schema,
  ObInnerTableSchema::all_virtual_kvcache_info_schema,
  ObInnerTableSchema::all_virtual_data_type_class_schema,
  ObInnerTableSchema::all_virtual_data_type_schema,
  ObInnerTableSchema::all_virtual_server_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_task_stat_schema,
  ObInnerTableSchema::all_virtual_session_event_schema,
  ObInnerTableSchema::all_virtual_session_wait_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_schema,
  ObInnerTableSchema::all_virtual_system_event_schema,
  ObInnerTableSchema::all_virtual_tenant_memstore_info_schema,
  ObInnerTableSchema::all_virtual_concurrency_object_pool_schema,
  ObInnerTableSchema::all_virtual_sesstat_schema,
  ObInnerTableSchema::all_virtual_sysstat_schema,
  ObInnerTableSchema::all_virtual_storage_stat_schema,
  ObInnerTableSchema::all_virtual_disk_stat_schema,
  ObInnerTableSchema::all_virtual_memstore_info_schema,
  ObInnerTableSchema::all_virtual_partition_info_schema,
  ObInnerTableSchema::all_virtual_upgrade_inspection_schema,
  ObInnerTableSchema::all_virtual_trans_stat_schema,
  ObInnerTableSchema::all_virtual_trans_mgr_stat_schema,
  ObInnerTableSchema::all_virtual_election_info_schema,
  ObInnerTableSchema::all_virtual_election_mem_stat_schema,
  ObInnerTableSchema::all_virtual_sql_audit_schema,
  ObInnerTableSchema::all_virtual_trans_mem_stat_schema,
  ObInnerTableSchema::all_virtual_partition_sstable_image_info_schema,
  ObInnerTableSchema::all_virtual_core_root_table_schema,
  ObInnerTableSchema::all_virtual_core_all_table_schema,
  ObInnerTableSchema::all_virtual_core_column_table_schema,
  ObInnerTableSchema::all_virtual_memory_info_schema,
  ObInnerTableSchema::all_virtual_tenant_stat_schema,
  ObInnerTableSchema::all_virtual_sys_parameter_stat_schema,
  ObInnerTableSchema::all_virtual_partition_replay_status_schema,
  ObInnerTableSchema::all_virtual_clog_stat_schema,
  ObInnerTableSchema::all_virtual_trace_log_schema,
  ObInnerTableSchema::all_virtual_engine_schema,
  ObInnerTableSchema::all_virtual_proxy_server_stat_schema,
  ObInnerTableSchema::all_virtual_proxy_sys_variable_schema,
  ObInnerTableSchema::all_virtual_proxy_schema_schema,
  ObInnerTableSchema::all_virtual_plan_cache_plan_explain_schema,
  ObInnerTableSchema::all_virtual_obrpc_stat_schema,
  ObInnerTableSchema::all_virtual_sql_plan_monitor_schema,
  ObInnerTableSchema::all_virtual_partition_sstable_merge_info_schema,
  ObInnerTableSchema::all_virtual_sql_monitor_schema,
  ObInnerTableSchema::tenant_virtual_outline_schema,
  ObInnerTableSchema::tenant_virtual_concurrent_limit_sql_schema,
  ObInnerTableSchema::all_virtual_sql_plan_statistics_schema,
  ObInnerTableSchema::all_virtual_partition_sstable_macro_info_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_info_schema,
  ObInnerTableSchema::all_virtual_proxy_partition_schema,
  ObInnerTableSchema::all_virtual_proxy_sub_partition_schema,
  ObInnerTableSchema::all_virtual_proxy_route_schema,
  ObInnerTableSchema::all_virtual_rebalance_tenant_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_unit_stat_schema,
  ObInnerTableSchema::all_virtual_rebalance_replica_stat_schema,
  ObInnerTableSchema::all_virtual_partition_amplification_stat_schema,
  ObInnerTableSchema::all_virtual_election_event_history_schema,
  ObInnerTableSchema::all_virtual_partition_sorted_stores_info_schema,
  ObInnerTableSchema::all_virtual_leader_stat_schema,
  ObInnerTableSchema::all_virtual_partition_migration_status_schema,
  ObInnerTableSchema::all_virtual_sys_task_status_schema,
  ObInnerTableSchema::all_virtual_macro_block_marker_status_schema,
  ObInnerTableSchema::columns_schema,
  ObInnerTableSchema::session_variables_schema,
  ObInnerTableSchema::table_privileges_schema,
  ObInnerTableSchema::user_privileges_schema,
  ObInnerTableSchema::schema_privileges_schema,
  ObInnerTableSchema::table_constraints_schema,
  ObInnerTableSchema::global_status_schema,
  ObInnerTableSchema::partitions_schema,
  ObInnerTableSchema::session_status_schema,
  ObInnerTableSchema::user_schema,
  ObInnerTableSchema::db_schema,
  ObInnerTableSchema::all_virtual_server_memory_info_schema,
  ObInnerTableSchema::all_virtual_plan_cache_stat_i1_schema,
  ObInnerTableSchema::all_virtual_session_event_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_i1_schema,
  ObInnerTableSchema::all_virtual_session_wait_history_i1_schema,
  ObInnerTableSchema::all_virtual_system_event_i1_schema,
  ObInnerTableSchema::all_virtual_sesstat_i1_schema,
  ObInnerTableSchema::all_virtual_sysstat_i1_schema,
  ObInnerTableSchema::all_virtual_sql_audit_i1_schema,
  NULL,};

const schema_create_func sys_view_schema_creators [] = {
  ObInnerTableSchema::gv_plan_cache_stat_schema,
  ObInnerTableSchema::gv_plan_cache_plan_stat_schema,
  ObInnerTableSchema::schemata_schema,
  ObInnerTableSchema::character_sets_schema,
  ObInnerTableSchema::global_variables_schema,
  ObInnerTableSchema::statistics_schema,
  ObInnerTableSchema::views_schema,
  ObInnerTableSchema::tables_schema,
  ObInnerTableSchema::collations_schema,
  ObInnerTableSchema::collation_character_set_applicability_schema,
  ObInnerTableSchema::processlist_schema,
  ObInnerTableSchema::key_column_usage_schema,
  ObInnerTableSchema::dba_outlines_schema,
  ObInnerTableSchema::gv_session_event_schema,
  ObInnerTableSchema::gv_session_wait_schema,
  ObInnerTableSchema::gv_session_wait_history_schema,
  ObInnerTableSchema::gv_system_event_schema,
  ObInnerTableSchema::gv_sesstat_schema,
  ObInnerTableSchema::gv_sysstat_schema,
  ObInnerTableSchema::v_statname_schema,
  ObInnerTableSchema::v_event_name_schema,
  ObInnerTableSchema::v_session_event_schema,
  ObInnerTableSchema::v_session_wait_schema,
  ObInnerTableSchema::v_session_wait_history_schema,
  ObInnerTableSchema::v_sesstat_schema,
  ObInnerTableSchema::v_sysstat_schema,
  ObInnerTableSchema::v_system_event_schema,
  ObInnerTableSchema::gv_sql_audit_schema,
  ObInnerTableSchema::gv_latch_schema,
  ObInnerTableSchema::gv_memory_schema,
  ObInnerTableSchema::v_memory_schema,
  ObInnerTableSchema::gv_memstore_schema,
  ObInnerTableSchema::v_memstore_schema,
  ObInnerTableSchema::gv_memstore_info_schema,
  ObInnerTableSchema::v_memstore_info_schema,
  ObInnerTableSchema::v_plan_cache_stat_schema,
  ObInnerTableSchema::v_plan_cache_plan_stat_schema,
  ObInnerTableSchema::gv_plan_cache_plan_explain_schema,
  ObInnerTableSchema::v_plan_cache_plan_explain_schema,
  ObInnerTableSchema::v_sql_audit_schema,
  ObInnerTableSchema::v_latch_schema,
  ObInnerTableSchema::gv_obrpc_outgoing_schema,
  ObInnerTableSchema::v_obrpc_outgoing_schema,
  ObInnerTableSchema::gv_obrpc_incoming_schema,
  ObInnerTableSchema::v_obrpc_incoming_schema,
  ObInnerTableSchema::gv_sql_schema,
  ObInnerTableSchema::v_sql_schema,
  ObInnerTableSchema::gv_sql_monitor_schema,
  ObInnerTableSchema::v_sql_monitor_schema,
  ObInnerTableSchema::gv_sql_plan_monitor_schema,
  ObInnerTableSchema::v_sql_plan_monitor_schema,
  ObInnerTableSchema::user_recyclebin_schema,
  ObInnerTableSchema::gv_outline_schema,
  ObInnerTableSchema::gv_concurrent_limit_sql_schema,
  ObInnerTableSchema::gv_sql_plan_statistics_schema,
  ObInnerTableSchema::v_sql_plan_statistics_schema,
  ObInnerTableSchema::gv_server_memstore_schema,
  ObInnerTableSchema::time_zone_schema,
  ObInnerTableSchema::time_zone_name_schema,
  ObInnerTableSchema::time_zone_transition_schema,
  ObInnerTableSchema::time_zone_transition_type_schema,
  NULL,};

const schema_create_func information_schema_table_schema_creators[] = {
  NULL,};

const schema_create_func mysql_table_schema_creators[] = {
  NULL,};

const uint64_t tenant_space_tables [] = {
  OB_ALL_DUMMY_TID,
  OB_TENANT_VIRTUAL_ALL_TABLE_TID,
  OB_TENANT_VIRTUAL_TABLE_COLUMN_TID,
  OB_TENANT_VIRTUAL_TABLE_INDEX_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TID,
  OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TID,
  OB_TENANT_VIRTUAL_SESSION_VARIABLE_TID,
  OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TID,
  OB_TENANT_VIRTUAL_WARNING_TID,
  OB_TENANT_VIRTUAL_CURRENT_TENANT_TID,
  OB_TENANT_VIRTUAL_DATABASE_STATUS_TID,
  OB_TENANT_VIRTUAL_TENANT_STATUS_TID,
  OB_TENANT_VIRTUAL_INTERM_RESULT_TID,
  OB_TENANT_VIRTUAL_PARTITION_STAT_TID,
  OB_TENANT_VIRTUAL_STATNAME_TID,
  OB_TENANT_VIRTUAL_EVENT_NAME_TID,
  OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_CLASS_TID,
  OB_ALL_VIRTUAL_DATA_TYPE_TID,
  OB_ALL_VIRTUAL_TRACE_LOG_TID,
  OB_ALL_VIRTUAL_ENGINE_TID,
  OB_TENANT_VIRTUAL_OUTLINE_TID,
  OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_TID,
  OB_COLUMNS_TID,
  OB_SESSION_VARIABLES_TID,
  OB_TABLE_PRIVILEGES_TID,
  OB_USER_PRIVILEGES_TID,
  OB_SCHEMA_PRIVILEGES_TID,
  OB_TABLE_CONSTRAINTS_TID,
  OB_GLOBAL_STATUS_TID,
  OB_PARTITIONS_TID,
  OB_SESSION_STATUS_TID,
  OB_USER_TID,
  OB_DB_TID,
  OB_GV_PLAN_CACHE_STAT_TID,
  OB_GV_PLAN_CACHE_PLAN_STAT_TID,
  OB_SCHEMATA_TID,
  OB_CHARACTER_SETS_TID,
  OB_GLOBAL_VARIABLES_TID,
  OB_STATISTICS_TID,
  OB_VIEWS_TID,
  OB_TABLES_TID,
  OB_COLLATIONS_TID,
  OB_COLLATION_CHARACTER_SET_APPLICABILITY_TID,
  OB_PROCESSLIST_TID,
  OB_KEY_COLUMN_USAGE_TID,
  OB_DBA_OUTLINES_TID,
  OB_GV_SESSION_EVENT_TID,
  OB_GV_SESSION_WAIT_TID,
  OB_GV_SESSION_WAIT_HISTORY_TID,
  OB_GV_SYSTEM_EVENT_TID,
  OB_GV_SESSTAT_TID,
  OB_GV_SYSSTAT_TID,
  OB_V_STATNAME_TID,
  OB_V_EVENT_NAME_TID,
  OB_V_SESSION_EVENT_TID,
  OB_V_SESSION_WAIT_TID,
  OB_V_SESSION_WAIT_HISTORY_TID,
  OB_V_SESSTAT_TID,
  OB_V_SYSSTAT_TID,
  OB_V_SYSTEM_EVENT_TID,
  OB_GV_LATCH_TID,
  OB_GV_MEMORY_TID,
  OB_V_MEMORY_TID,
  OB_GV_MEMSTORE_TID,
  OB_V_MEMSTORE_TID,
  OB_GV_MEMSTORE_INFO_TID,
  OB_V_MEMSTORE_INFO_TID,
  OB_V_PLAN_CACHE_STAT_TID,
  OB_V_PLAN_CACHE_PLAN_STAT_TID,
  OB_GV_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_V_PLAN_CACHE_PLAN_EXPLAIN_TID,
  OB_V_LATCH_TID,
  OB_GV_OBRPC_OUTGOING_TID,
  OB_V_OBRPC_OUTGOING_TID,
  OB_GV_OBRPC_INCOMING_TID,
  OB_V_OBRPC_INCOMING_TID,
  OB_GV_SQL_TID,
  OB_V_SQL_TID,
  OB_GV_SQL_MONITOR_TID,
  OB_V_SQL_MONITOR_TID,
  OB_GV_SQL_PLAN_MONITOR_TID,
  OB_V_SQL_PLAN_MONITOR_TID,
  OB_USER_RECYCLEBIN_TID,
  OB_GV_OUTLINE_TID,
  OB_GV_CONCURRENT_LIMIT_SQL_TID,
  OB_GV_SQL_PLAN_STATISTICS_TID,
  OB_V_SQL_PLAN_STATISTICS_TID,
  OB_GV_SERVER_MEMSTORE_TID,
  OB_TIME_ZONE_TID,
  OB_TIME_ZONE_NAME_TID,
  OB_TIME_ZONE_TRANSITION_TID,
  OB_TIME_ZONE_TRANSITION_TYPE_TID,  };

const uint64_t only_rs_vtables [] = {
  OB_ALL_VIRTUAL_CORE_META_TABLE_TID,
  OB_ALL_VIRTUAL_ZONE_STAT_TID,
  OB_ALL_VIRTUAL_SERVER_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_TASK_STAT_TID,
  OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID,
  OB_ALL_VIRTUAL_CORE_ROOT_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_ALL_TABLE_TID,
  OB_ALL_VIRTUAL_CORE_COLUMN_TABLE_TID,
  OB_ALL_VIRTUAL_TENANT_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_TENANT_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_UNIT_STAT_TID,
  OB_ALL_VIRTUAL_REBALANCE_REPLICA_STAT_TID,
  OB_ALL_VIRTUAL_LEADER_STAT_TID,  };

static inline bool is_tenant_table(const uint64_t tid)
{
  bool in_tenant_space = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    if (common::extract_pure_id(tid) == tenant_space_tables[i]) {
      in_tenant_space = true;
    }
  }
  return in_tenant_space;
}

static inline bool is_global_virtual_table(const uint64_t tid)
{
  return common::is_virtual_table(tid) && !is_tenant_table(tid);
}

static inline bool is_tenant_virtual_table(const uint64_t tid)
{
  return common::is_virtual_table(tid) && is_tenant_table(tid);
}

static inline bool is_only_rs_virtual_table(const uint64_t tid)
{
  bool only_rs = false;
  for (int64_t i = 0; i < ARRAYSIZEOF(only_rs_vtables); ++i) {
    if (common::extract_pure_id(tid) == only_rs_vtables[i]) {
      only_rs = true;
    }
  }
  return only_rs;
}

const int64_t OB_CORE_TABLE_COUNT = 4;
const int64_t OB_SYS_TABLE_COUNT = 56;
const int64_t OB_VIRTUAL_TABLE_COUNT = 104;
const int64_t OB_SYS_VIEW_COUNT = 61;
const int64_t OB_SYS_TENANT_TABLE_COUNT = 226;
const int64_t OB_CORE_SCHEMA_VERSION = 1;
const int64_t OB_BOOTSTRAP_SCHEMA_VERSION = 229;

} // end namespace share
} // end namespace oceanbase
#endif /* _OB_INNER_TABLE_SCHEMA_H_ */
