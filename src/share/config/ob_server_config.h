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

#ifndef OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_
#define OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_

#include "ob_common_config.h"
#include "ob_system_config.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;

class ObServerConfig : public ObCommonConfig
{
public:
  int init(const ObSystemConfig &config);
  static ObServerConfig &get_instance();

  // read all config from system_config_
  virtual int read_config();

  // check if all config is validated
  virtual int check_all() const;
  // check some special settings strictly
  int strict_check_special() const;
  // print all config to log file
  void print() const;

  // server memory limit
  int64_t get_server_memory_limit();
  // server memory available for normal tenants
  int64_t get_server_memory_avail();
  // server momory reserved for internal usage.
  int64_t get_reserved_server_memory();

  virtual ObServerRole get_server_type() const { return common::OB_SERVER; }
  virtual bool is_debug_sync_enabled() const { return static_cast<int64_t>(debug_sync_timeout) > 0; }
  virtual bool is_manual_merge_enabled() { return enable_upgrade_mode || enable_manual_merge; }
  virtual bool is_rebalance_enabled() { return !enable_upgrade_mode && enable_rebalance; }
  virtual bool is_rereplication_enabled() { return !enable_upgrade_mode && enable_rereplication; }

  virtual double user_location_cpu_quota() const { return location_cache_cpu_quota; }
  virtual double sys_location_cpu_quota() const { return std::max(1., user_location_cpu_quota() / 2); }
  virtual double root_location_cpu_quota() const { return 1.; }
  virtual double core_location_cpu_quota() const { return 1.; }

  int64_t disk_actual_space_;

  ObAddr self_addr_;

  //// sstable config
  DEF_STR(data_dir, "store", "the directory for the data file", CFG_SECTION_SSTABLE);
  DEF_CAP(datafile_size, "0", "size of the data file. Range: [0, +∞)", CFG_SECTION_SSTABLE);
  DEF_INT(datafile_disk_percentage, "90", "[5,99]",
          "the percentage of disk space used by the data files. Range: [5,99] in integer",
          CFG_SECTION_SSTABLE);
  DEF_CAP(memory_reserved, "500M",
          "the size of the system memory reserved for emergency internal use. "
          "Range: [10M, total size of memory]",
          CFG_SECTION_SSTABLE)
  //// observer config
  DEF_STR_LIST(config_additional_dir, "etc2;etc3", "additional directories of configure file",
               CFG_SECTION_OBSERVER);
  DEF_STR(leak_mod_to_check, "NONE", "the name of the module under memory leak checks",
          CFG_SECTION_OBSERVER);
  DEF_INT(rpc_port, "2500", "(1024,65536)",
          "the port number for RPC protocol. Range: (1024. 65536) in integer",
          CFG_SECTION_OBSERVER);
  DEF_INT(mysql_port, "2880", "(1024,65536)",
          "port number for mysql connection. Range: (1024, 65536) in integer",
          CFG_SECTION_OBSERVER);
  DEF_STR(devname, "bond0", "name of network adapter", CFG_SECTION_OBSERVER);
  DEF_STR(zone, "", "specifies the zone name", CFG_SECTION_OBSERVER);
  DEF_TIME(internal_sql_execute_timeout, "30s",
           "the number of microseconds an internal DML request is permitted to "
           "execute before it is terminated. Range: [1000us, 10m]",
           CFG_SECTION_OBSERVER);
  DEF_INT(net_thread_count, "4", "[1,100]",
          "the number of I/O threads for Libeasy. Range: [1, 100] in integer",
          CFG_SECTION_OBSERVER);
  DEF_INT(tenant_task_queue_size, "65536", "[1024,]",
          "the size of the task queue for each tenant. Range: [1024,+∞)",
          CFG_SECTION_OBSERVER);
  DEF_CAP(memory_limit, "0",
          "the size of the memory reserved for internal use(for testing purpose)",
          CFG_SECTION_OBSERVER);
  DEF_INT(cpu_count, "0", "[0,]",
          "the number of CPU\\'s in the system. "
          "If this parameter is set to zero, the number will be set according to sysconf; "
          "otherwise, this parameter is used. Range: [0,+∞) in integer",
          CFG_SECTION_OBSERVER);
  DEF_INT(cpu_reserved, "2", "[0,15]",
          "the number of CPU\\'s reserved for system usage. Range: [0, 15] in integer",
          CFG_SECTION_OBSERVER);
  DEF_TIME(trace_log_sampling_interval, "10ms", "[0ms,]",
          "the time interval for periodically printing log info in trace log. "
          "When force_trace_log is set to FALSE, "
          "for each time interval specifies by sampling_trace_log_interval, "
          "logging info regarding ‘slow query’ and ‘white list’ will be printed out. "
          "Range: [0ms,+∞)",
          CFG_SECTION_OBSERVER);
  DEF_TIME(trace_log_slow_query_watermark, "100ms", "[1ms,]",
          "the threshold of execution time (in milliseconds) of a query beyond "
          "which it is considered to be a \\'slow query\\'. Range: [1ms,+∞)",
          CFG_SECTION_OBSERVER);
  DEF_BOOL(enable_record_trace_log, "False",
           "specifies whether to always record the trace log. The default value is False.",
           CFG_SECTION_OBSERVER);
  DEF_INT(system_trace_level, "1", "[0,2]",
          "system trace log level, 0:none, 1:standard, 2:debug. "
          "The default log level for trace log is 1",
          CFG_SECTION_OBSERVER);
  DEF_INT(max_string_print_length, "500", "[0,]",
          "truncate very long string when printing to log file",
          CFG_SECTION_OBSERVER);
  DEF_CAP(sql_audit_memory_limit, "3G", "[256M,]",
          "the maximum size of the memory used by SQL audit virtual table "
          "when the function is turned on. The default value is 3G. Range: [256M, +∞]",
          CFG_SECTION_OBSERVER);
  DEF_INT(sql_audit_queue_size, "10000000", "[10000,]",
          "the maximum number of the records in SQL audit virtual table. (don't use now)"
          "The default value is 10000000. Range: [10000, +∞], integer",
          CFG_SECTION_OBSERVER);
  DEF_BOOL(enable_sql_audit, "true",
           "specifies whether SQL audit is turned on. "
           "The default value is FALSE. Value: TRUE: turned on FALSE: turned off",
           CFG_SECTION_OBSERVER);

  DEF_TIME(debug_sync_timeout, "0", "[0,)",
           "Enable the debug sync facility and "
           "optionally specify a default wait timeout in micro seconds. "
           "A zero value keeps the facility disabled",
           CFG_SECTION_OBSERVER);
  DEF_BOOL(enable_perf_event, "False",
           "specifies whether to enable perf event feature. The default value is False.",
           CFG_SECTION_OBSERVER);
  DEF_BOOL(enable_upgrade_mode, "False",
           "specifies whether upgrade mode is turned on. "
           "If turned on, daily merger and balancer will be disabled. "
           "Value: True: turned on; False: turned off;",
           CFG_SECTION_OBSERVER);

  DEF_TIME(schema_history_expire_time, "7d",
            "the hour of expire time for schema history, from 1hour to 30days, "
            "with default 7days. Range: [1h, 30d]",
            CFG_SECTION_OBSERVER);

  // for observer, root_server_list in format ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2
  DEF_STR_LIST(rootservice_list, "",
               "a list of servers against which election candidate is checked for validation",
               CFG_SECTION_OBSERVER);
  DEF_STR(cluster, "obcluster", "Name of the cluster", CFG_SECTION_OBSERVER);
  DEF_INT(cluster_id, "0", "[1,4294967295]", "ID of the cluster", CFG_SECTION_OBSERVER);
  DEF_STR(obconfig_url, "", "URL for OBConfig service", CFG_SECTION_OBSERVER);
  DEF_LOG_LEVEL(syslog_level, "INFO",
                "specifies the current level of logging. There are DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR, six different log levels.",
                CFG_SECTION_OBSERVER);
  DEF_INT(max_syslog_file_count, "0", "[0,]",
          "specifies the maximum number of the log files "
          "that can co-exist before the log file recycling kicks in. "
          "Each log file can occupy at most 256MB disk space. "
          "When this value is set to 0, no log file will be removed. Range: [0, +∞) in integer",
          CFG_SECTION_OBSERVER);
  DEF_BOOL(enable_syslog_wf, "True",
           "specifies whether any log message with a log level higher than \\'WARN\\' "
           "would be printed into a separate file with a suffix of \\'wf\\'",
           CFG_SECTION_OBSERVER);
  DEF_BOOL(enable_syslog_recycle, "False",
           "specifies whether log file recycling is turned on. "
           "Value: True：turned on; False: turned off",
           CFG_SECTION_OBSERVER);
  DEF_INT(memory_limit_percentage, "80", "[10, 90]",
          "the size of the memory reserved for internal use(for testing purpose). Range: [0, +∞)",
          CFG_SECTION_OBSERVER);
  DEF_CAP(cache_wash_threshold, "4GB", "[0,]",
          "size of remaining memory at which cache eviction will be triggered. Range: [0,+∞)",
          CFG_SECTION_OBSERVER);

  //// tenant config
  DEF_DBL(tenant_cpu_variation_per_server, "50", "[0,100]",
          "the percentage variation for any tenant\\'s CPU quota allocation on each observer. "
          "The default value is 50(%). Range: [0, 500] in percentage",
          CFG_SECTION_TENANT);
  DEF_DBL(system_cpu_quota, "6", "[,10]",
          "the number of vCPUs allocated to the server tenant"
          "(a special internal tenant that exists on every observer). Range: [0, 10]",
          CFG_SECTION_TENANT);
  DEF_INT(system_memory_percentage,
          "20", "[0,100]",
          "the percentage of server memory limit that reserved for server tenant"
          "(a special internal tenant that exists on every observer). "
          "Range: [0,100]", CFG_SECTION_TENANT);
  DEF_DBL(election_cpu_quota, "3", "[,10]",
          "the number of vCPUs allocated to the \\'election\\' tenant. Range: [0,10] in integer",
          CFG_SECTION_TENANT);
  DEF_DBL(location_cache_cpu_quota, "5", "[,10]",
          "the number of vCPUs allocated for the requests regarding location "
          "info of the core tables. Range: [0,10] in integer",
          CFG_SECTION_TENANT);
  DEF_INT(workers_per_cpu_quota, "10", "[2,20]",
          "the ratio(integer) between the number of system allocated workers vs "
          "the maximum number of threads that can be scheduled concurrently. Range: [2, 20]",
          CFG_SECTION_TENANT);
  DEF_DBL(large_query_worker_percentage, "30", "[0,100]",
          "the percentage of the workers reserved to serve large query request. "
          "Range: [0, 100] in percentage",
          CFG_SECTION_TENANT)
  DEF_TIME(large_query_threshold, "5ms", "[1ms,]",
           "threshold for execution time beyond "
           "which a request may be paused and rescheduled as a \\'large request\\'",
           CFG_SECTION_TENANT);
  DEF_DBL(token_reserved_percentage,
          "30", "[0,100]",
          "specifies the amount of token increase allocated to a tenant based on "
          "his consumption from the last round (without exceeding his upper limit). "
          "Range: [0, 100] in percentage",
          CFG_SECTION_TENANT);
  // tenant memtable consumption related
  DEF_INT(memstore_limit_percentage, "50", "(0, 100)",
          "used in calculating the value of MEMSTORE_LIMIT parameter: "
          "memstore_limit_percentage = memstore_limit / min_memory，min_memory, "
          "where MIN_MEMORY is determined when the tenant is created. Range: (0, 100)",
          CFG_SECTION_TENANT);
  DEF_INT(freeze_trigger_percentage, "70", "(0, 100)",
          "the threshold of the size of the mem store when freeze will be triggered. Rang:(0，100)",
          CFG_SECTION_TENANT);
  DEF_CAP(plan_cache_high_watermark, "2000M",
          "(don't use now) memory usage at which plan cache eviction will be trigger immediately. Range: [0, +∞)",
          CFG_SECTION_TENANT);
  DEF_CAP(plan_cache_low_watermark, "1500M",
          "(don't use now) memory usage at which plan cache eviction will be stopped. "
          "Range: [0, plan_cache_high_watermark)",
          CFG_SECTION_TENANT);
  DEF_TIME(plan_cache_evict_interval, "30s",
           "time interval for periodic plan cache eviction. Range: [0, +∞)",
           CFG_SECTION_TENANT);

  ////  rootservice config
  DEF_BOOL(enable_rootservice_standalone, "False",
           "specifies whether the \\'SYS\\' tenant is allowed to occupy an observer exclusively, "
           "thus running in the \\'standalone\\' mode. Value:  True:turned on  False: turned off",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(lease_time, "10s",
           "Lease for current heartbeat. If the root server does not received any heartbeat "
           "from an observer in lease_time seconds, that observer is considered to be offline. "
           "Not recommended for modification. Range: [1s, 5m]",
           CFG_SECTION_ROOTSERVICE);
  DEF_INT(rootservice_async_task_thread_count, "1",
          "maximum of threads allowed for executing asynchronous task at rootserver. "
          "Range: [1, 10]",
          CFG_SECTION_ROOTSERVICE);
  DEF_INT(rootservice_async_task_queue_size, "16384", "[8,131072]",
          "the size of the queue for all asynchronous tasks at rootserver. "
          "Range: [8, 131072] in integer",
          CFG_SECTION_ROOTSERVICE);
  DEF_BOOL(enable_sys_table_ddl, "False",
           "specifies whether a \\'system\\' table is allowed be to created manually. "
           "Value: True: allowed; False: not allowed",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(auto_leader_switch_interval, "30s",
           "time interval for periodic leadership reorganization taking place. Range: [1s, +∞)",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(merger_switch_leader_duration_time, "3m", "[0s,30m]"
           "switch leader duration time for daily merge. Range: [0s,30m]",
           CFG_SECTION_ROOTSERVICE);
  // NOTE: server_temporary_offline_time is discarded.
  DEF_TIME(server_temporary_offline_time, "60s", "[15s,)",
           "the time interval between two heartbeats beyond "
           "which a server is considered to be \\'temporarily\\' offline. Range: [15s, +∞)",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(server_permanent_offline_time, "3600s", "[20s,)",
           "the time interval between any two heartbeats beyond "
           "which a server is considered to be \\'permanently\\' offline. Range: [20s,+∞)",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(migration_disable_time, "3600s", "[1s,)",
           "the duration in which the observer stays in the \\'block_migrate_in\\' status, "
           "which means no partition is allowed to migrate into the server. Range: [1s, +∞)",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(server_check_interval, "30s",
           "the time interval between schedules of a task "
           "that examines the __all_server table. Range: [1s, +∞)",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(rootservice_ready_check_interval, "3s",
           "the interval between the schedule of the task "
           "that checks on the status of the ZONE during restarting. Range: [100000us, 1m]",
           CFG_SECTION_ROOTSERVICE);
  DEF_INT(partition_table_scan_batch_count, "1024", "(0, 65536]",
          "the number of partition replication info "
          "that will be read by each request on the partition-related system tables "
          "during procedures such as load-balancing, daily merge, election and etc.",
          CFG_SECTION_ROOTSERVICE);
  DEF_BOOL(enable_auto_leader_switch, "True",
           "specifies whether partition leadership auto-switch is turned on. "
           "Value:  True:turned on;  False: turned off",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(replica_safe_remove_time, "2h", "[1m,)",
           "the time interval that replica not existed has not been modified beyond "
           "which a replica is considered can be safely removed",
           CFG_SECTION_ROOTSERVICE);
  DEF_TIME(partition_table_check_interval, "30m", "[1m,)",
           "the time interval that observer remove replica "
           "which not exist in observer from partition table",
           CFG_SECTION_ROOTSERVICE);
  DEF_INT(min_observer_major_version, "1", "[1,)",
           "the major version of current observer",
           CFG_SECTION_ROOTSERVICE);
  DEF_INT(min_observer_minor_version, "0", "[0,)",
           "the minor version of current observer",
           CFG_SECTION_ROOTSERVICE);
  DEF_INT(min_observer_patch_version, "0", "[0,)",
           "the patch version of current observer",
           CFG_SECTION_ROOTSERVICE);
  DEF_STR(min_observer_version, "1.0.0", "the min observer version", CFG_SECTION_ROOTSERVICE);

  //// load balance config
  DEF_INT(resource_soft_limit, "50", "(0, 10000]",
          "Used along with resource_hard_limit in unit allocation. "
          "If server utilization is less than resource_soft_limit, a policy of \\'best fit\\' "
          "will be used for unit allocation; "
          "otherwise, a \\'least load\\'policy will be employed. "
          "Ultimately,system utilization should not be large than resource_hard_limit. "
          "Range: (0,10000] in in",
          CFG_SECTION_LOAD_BALANCE);
  DEF_INT(resource_hard_limit, "100", "(0, 10000]",
          "Used along with resource_soft_limit in unit allocation. "
          "If server utilization is less than resource_soft_limit, "
          "a policy of \\'best fit\\' will be used for unit allocation; "
          "otherwise, a \\'least load\\' policy will be employed. "
          "Ultimately,system utilization should not be large than resource_hard_limit. "
          "Range: (0,10000] in integer",
          CFG_SECTION_LOAD_BALANCE);
  DEF_BOOL(enable_rereplication, "True",
           "specifies whether the partition auto-replication is turned on. "
           "Value:  True:turned on  False: turned off",
           CFG_SECTION_LOAD_BALANCE);
  DEF_BOOL(enable_rebalance, "True",
           "specifies whether the partition load-balancing is turned on. "
           "Value:  True:turned on  False: turned off",
           CFG_SECTION_LOAD_BALANCE);
  DEF_TIME(balancer_idle_time, "5m", "[10s,]",
           "the time interval between the schedules of the partition load-balancing task. "
           "Range: [10s, +∞)",
           CFG_SECTION_LOAD_BALANCE);
  DEF_INT(balancer_tolerance_percentage, "10", "[5, 100)",
          "specifies the tolerance (in percentage) of the unbalance of the disk space utilization "
          "among all units. The average disk space utilization is calculated by dividing "
          "the total space by the number of units. For example, say balancer_tolerance_percentage "
          "is set to 10 and a tenant has two units in the system, "
          "the average disk use for each unit should be about the same, "
          "thus 50% of the total value. Therefore, the system will start a rebalancing task "
          "when any unit\\'s disk space goes beyond +-10% of the average usage. "
          "Range: [5, 100) in percentage",
          CFG_SECTION_LOAD_BALANCE);
  DEF_INT(server_data_copy_in_concurrency, "2", "[1,)",
          "the maximum number of partitions allowed to migrate to the server. "
          "Range: [1,+∞), integer",
          CFG_SECTION_LOAD_BALANCE);
  DEF_INT(server_data_copy_out_concurrency, "2", "[1,)",
          "the maximum number of partitions allowed to migrate from the server. "
          "Range: [1,+∞), integer",
          CFG_SECTION_LOAD_BALANCE);
  DEF_INT(data_copy_concurrency, "20", "[1,)",
          "the maximum number of the data replication tasks. "
          "Range: [1,+∞) in integer",
          CFG_SECTION_LOAD_BALANCE);
  DEF_TIME(balancer_task_timeout, "20m",
           "the time to execute the load-balancing task before it is terminated. Range: [1s, +∞)",
           CFG_SECTION_LOAD_BALANCE);
  DEF_TIME(balancer_timeout_check_interval, "1m",
           "the time interval between the schedules of the task that checks "
           "whether the partition load balancing task has timed-out. Range: [1s, +∞)",
           CFG_SECTION_LOAD_BALANCE);
  DEF_TIME(balancer_log_interval, "1m",
           "the time interval between logging the load-balancing task\\'s statistics. "
           "Range: [1s, +∞)",
           CFG_SECTION_LOAD_BALANCE);

  //// daily merge  config
  // set to disable if don't want major freeze launch auto
  DEF_MOMENT(major_freeze_duty_time, "02:00",
             "the start time of system daily merge procedure. Range: [00:00, 24:00]",
             CFG_SECTION_DAILY_MERGE);
  DEF_BOOL(enable_manual_merge, "False",
           "specifies whether manual MERGE is turned on. Value: True:turned on  False: turned off",
           CFG_SECTION_DAILY_MERGE);
  DEF_INT(zone_merge_concurrency, "2", "(0,]",
          "the maximum number of zones "
          "which are allowed to be in the \\'MERGE\\' status concurrently. "
          "Range: [0,+∞) in integer",
          CFG_SECTION_DAILY_MERGE);
  DEF_BOOL(enable_merge_by_turn, "True",
           "specifies whether merge task can be performed on different zones "
           "in a alternating fashion. Value: True:turned on; False: turned off",
           CFG_SECTION_DAILY_MERGE);
  DEF_TIME(zone_merge_timeout, "3h",
           "the time for each zone to finish its merge process before "
           "the root service no longer consider it as in \\'MERGE\\' state. Range: [1s, +∞)",
           CFG_SECTION_DAILY_MERGE);
  DEF_TIME(merger_check_interval, "10m", "[10s, 60m]",
           "the time interval between the schedules of the task "
           "that checks on the progress of MERGE for each zone. Range: [10s, 60m]",
           CFG_SECTION_DAILY_MERGE);
  DEF_BOOL(ignore_replica_checksum_error, "False",
           "specifies whether error raised from the partition checksum validation can be ignored. "
           "Value: True:ignored; False: not ignored");
  DEF_INT(merger_completion_percentage, "95", "[5, 100]",
          "the merged partition count percentage and merged data size percentage "
          "when MERGE is completed",
          CFG_SECTION_DAILY_MERGE);

  //// transaction config
  DEF_TIME(trx_2pc_retry_interval, "100ms",
           "the time interval between the retries in case of failure "
           "during a transaction\\'s two-phase commit phase. Range: [1ms,1000ms]",
           CFG_SECTION_TRANS);
  DEF_TIME(clog_sync_time_warn_threshold, "10ms",
           "the time given to the commit log synchronization between a leader and its followers "
           "before a \\'warning\\' message is printed in the log file.  Range: [1ms,1000ms]",
           CFG_SECTION_TRANS);
  DEF_INT(row_purge_thread_count, "4",
          "maximum of threads allowed for executing row purge task. "
          "Range: [1, 64]",
          CFG_SECTION_TRANS);
  DEF_INT(row_compaction_update_limit, "6",
          "maximum update count before trigger row compaction. "
          "Range: [1, 64]",
          CFG_SECTION_TRANS);
  DEF_BOOL(ignore_replay_checksum_error, "False",
           "specifies whether error raised from the memtable replay checksum validation can be ignored. "
           "Value: True:ignored; False: not ignored",
           CFG_SECTION_TRANS);

  //// rpc config
  DEF_TIME(rpc_timeout, "2s",
           "the time during which a RPC request is permitted to execute before it is terminated",
           CFG_SECTION_RPC);

  //// location cache config
  DEF_INT(location_cache_priority, "1000", "[1,)",
          "priority of location cache among all system caching service. Range: [1, +∞) in integer",
          CFG_SECTION_LOCATION_CACHE);
  DEF_TIME(location_cache_expire_time, "600s",
           "the expiration time for a partition location info in partition location cache. "
           "Not recommended for modification. Range: [1s, +∞)",
           CFG_SECTION_LOCATION_CACHE);
  DEF_TIME(virtual_table_location_cache_expire_time, "8s",
           "expiration time for virtual table location info in partiton location cache. "
           "Range: [1s, +∞)",
           CFG_SECTION_LOCATION_CACHE);
  DEF_INT(location_refresh_thread_count, "4",
          "the number of threads "
          "that fetch the partition location information from the root service. Range: (1, 100)",
          CFG_SECTION_LOCATION_CACHE);
  DEF_INT(location_fetch_concurrency, "20",
          "the maximum number of the tasks "
          "which fetch the partition location information concurrently. Range: [1, 1000]",
          CFG_SECTION_LOCATION_CACHE);
  DEF_TIME(location_cache_refresh_min_interval, "100ms",
           "the time interval in which no request for location cache renewal will be executed. "
           "The default value is 100 milliseconds",
           CFG_SECTION_LOCATION_CACHE);

  //// cache config
  DEF_INT(clog_cache_priority, "1", "[1,)", "clog cache priority", CFG_SECTION_CACHE);
  DEF_INT(index_clog_cache_priority, "1", "[1,)", "index clog cache priority", CFG_SECTION_CACHE);
  DEF_INT(user_tab_col_stat_cache_priority, "1", "[1,)", "user tab col stat cache priority",
          CFG_SECTION_CACHE);
  DEF_INT(index_cache_priority, "10", "[1,)", "index cache priority", CFG_SECTION_CACHE);
  DEF_INT(index_info_block_cache_priority, "1", "[1,)", "index info block cache priority",
          CFG_SECTION_CACHE);
  DEF_INT(user_block_cache_priority, "1", "[1,)", "user block cache priority", CFG_SECTION_CACHE);
  DEF_INT(user_row_cache_priority, "1", "[1,)", "user row cache priority", CFG_SECTION_CACHE);
  DEF_INT(bf_cache_priority, "1", "[1,)", "bf cache priority", CFG_SECTION_CACHE);



protected:
  ObServerConfig();
  virtual ~ObServerConfig();
  const ObSystemConfig *system_config_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerConfig);
};
}
}

#define GCONF (::oceanbase::common::ObServerConfig::get_instance())

#endif // OCEANBASE_SHARE_CONFIG_OB_SERVER_CONFIG_H_
