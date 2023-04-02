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

#ifndef OBPROXY_CONFIG_H
#define OBPROXY_CONFIG_H

#include "lib/hash/ob_hashmap.h"
#include "share/config/ob_common_config.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObConfigStringKey;
}
namespace obproxy
{
namespace proxy
{
class ObMysqlResultHandler;
}
namespace obutils
{

enum ObServerRoutingMode
{
  OB_STANDARD_ROUTING_MODE = 0,
  OB_RANDOM_ROUTING_MODE,
  OB_MOCK_ROUTING_MODE,
  OB_MYSQL_ROUTING_MODE,
  // add others ...

  OB_MAX_ROUTING_MODE
};

enum ObProxyServiceMode
{
  OB_CLIENT_SERVICE_MODE = 0,
  OB_SERVER_SERVICE_MODE,
  // add others ...
  OB_MAX_SERVICE_MODE
};

enum ObProxyLocalCMDType
{
  OB_LOCAL_CMD_NONE = 0,
  OB_LOCAL_CMD_EXIT,
  OB_LOCAL_CMD_RESTART,
  OB_LOCAL_CMD_MAX
};

// Attention!! if you add one config item for using in mysql module,
// you must also add this config item to ObMysqlConfigParams;
class ObProxyConfig : public common::ObCommonConfig
{
public:
  // FIXME: here we use true value to init dump_config_res_, so that in start period,
  // if proxy fail to load config from local and ocp database,
  // we wont dump default config to local.
  ObProxyConfig() : with_config_server_(false), with_control_plane_(false), rwlock_()
  {
    original_value_str_[0] = '\0';
    set_app_name(app_name);
  }

  virtual ~ObProxyConfig() {}

  //print all proxy config to log file
  virtual void print() const;
  virtual void print_need_reboot_config() const;
  int init_need_reboot_config();
  int reset();
  void set_app_name(const common::ObConfigStringItem &app_name_item)
  {
    const int64_t length = static_cast<int64_t>(std::min(STRLEN(app_name_item.str()), sizeof(app_name_str_) - 1));
    MEMCPY(app_name_str_, app_name_item.str(), length);
    app_name_str_[length] = '\0';
  }

  //dump config to local file
  int dump_config_to_local();
  int update_config_item(const common::ObString &key_name, const common::ObString &value);
  int update_user_config_item(const common::ObString &key_name, const common::ObString &value);

  int get_old_config_value(const common::ObString &key_name, char *buf, const int64_t buf_size);

  // if level_flag = true, we will up sys log level
  // otherwise we will down sys log level
  void update_log_level(const bool level_flag);

  virtual int check_all() const;
  int check_proxy_serviceable() const;

  virtual common::ObServerRole get_server_type() const { return common::OB_PROXY; }

  //only if the value is set successfully and available, we update the latest value.
  //otherwise, do not change its old value
  //if allow_invalid_value is false, the value must be valid and set succeed
  //if allow_invalid_value is true, we can tolerate the invalid value
  int set_value_safe(common::ObConfigItem *item, const common::ObString &value,
                     const bool allow_invalid_value = true);
  int fill_proxy_config(proxy::ObMysqlResultHandler &result_handler);

  static common::ObString get_service_mode_string(const ObProxyServiceMode mode);
  static ObProxyServiceMode get_service_mode(const common::ObString &mode_string);
  static ObProxyServiceMode get_service_mode(const char *mode_str);
  bool is_client_service_mode() const;
  bool is_service_mode_available(ObProxyServiceMode &mode) const;

  static ObServerRoutingMode get_routing_mode(const common::ObString &mode_str);
  static ObServerRoutingMode get_routing_mode(const char *mode_str);
  static const char *get_routing_mode_str(ObServerRoutingMode mode);
  bool is_routing_mode_available(ObServerRoutingMode &mode) const;
  static bool is_local_cmd(const int64_t cmd);
  bool is_local_cmd();
  ObProxyLocalCMDType get_local_cmd_type() const;
  void reset_local_cmd();
  bool is_local_restart();

  bool is_metadb_used() const { return with_config_server_ && enable_metadb_used; }
  bool is_control_plane_used() const { return with_control_plane_; }

  bool with_config_server_;
  bool with_control_plane_;
  char app_name_str_[common::OB_MAX_APP_NAME_LENGTH + 1];

private:
  char original_value_str_[common::OB_MAX_CONFIG_VALUE_LEN];

  DISALLOW_COPY_AND_ASSIGN(ObProxyConfig);

public:
  // FIXME: DEF_INT can not compare with 0(<, >, >=, <=)
  mutable obsys::CRWLock rwlock_;
  // refresh local config
  DEF_BOOL(refresh_json_config, "false", "force update json info if refresh_json_config is true", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_MEMORY);
  DEF_BOOL(refresh_rslist, "false", "when refresh config server, update all rslist if refresh_rslist is true", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_MEMORY);
  DEF_BOOL(refresh_idc_list, "false", "when refresh config server, update all idc list if refresh_idc_list is true", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_MEMORY);
  DEF_BOOL(refresh_config, "false", "when table processor do check work, update all proxy config if refresh_config is true", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_MEMORY);

  //repeat task interval related
  DEF_TIME(proxy_info_check_interval, "60s", "[1s,1h]", "proxy info check task interval, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(proxy_hot_upgrade_check_interval, "5s", "[1s,1h]", "proxy info check task interval, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(cache_cleaner_clean_interval, "20s", "[1s, 1d]", "the interval for cache cleaner to clean cache, [1s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(server_state_refresh_interval, "20s", "[10ms, 1h]", "the interval to refresh server state for getting zone or server newest state, [10ms, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(metadb_server_state_refresh_interval, "60s", "[10ms, 1h]", "the interval to refresh metadb server state for getting zone or server newest state, [10ms, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(config_server_refresh_interval, "60s", "[10s,1d]", "config server info refresh task interval, [10s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(idc_list_refresh_interval, "2h", "[10s, 1d]", "the interval to refresh idc list for getting newest region-idc, [10s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(stat_table_sync_interval, "60s", "[0s,1d]", "update sync statistic to ob_all_proxy_stat table interval, [0s, 1d], 0 means disable, if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(stat_dump_interval, "6000s", "[0s,1d]", "dump statistic in log interval, [0s, 1d], 0 means disable, if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(cluster_count_high_water_mark, "256", "[2, 102400]", "if cluster count is greater than this water mark, cluser will be kicked out by LRU", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(cluster_expire_time, "1d", "[0,]", "cluster resource expire time, 0 means never expire,cluster will be deleted if it has not been accessed for more than the time,[0, ]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //proxy hot_upgrade related
  DEF_TIME(fetch_proxy_bin_random_time, "300s", "[1s,1h]", "max random waiting time of fetching proxy bin in hot upgrade, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(fetch_proxy_bin_timeout, "120s", "[1s,1200s]", "default hot upgrade fetch binary timeout, proxy will stop fetching after such long time, [1s, 1200s]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(hot_upgrade_failure_retries, "5", "[1,20]", "default hot upgrade failure retries, proxy will stop handle hot_upgrade command after such retries, [1, 20]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(hot_upgrade_rollback_timeout, "24h", "[1s,30d]", "default hot upgrade rollback timeout, proxy will do rollback if receive no rollback command in such long time, [1s, 30d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(hot_upgrade_exit_timeout, "30000000", "[-1,86400000000]", "graceful exit timeout, unit is us, default 30s, "
                                             "-1 means no timeout, 0 means quit now, > 0 means wait time", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(delay_exit_time, "100ms", "[100ms,500ms]", "delay exit time, [100ms,500ms]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //log cleanup related
  DEF_INT(log_file_percentage, "80", "[0, 100]", "max percentage of avail size occupied by proxy log file, [0, 90], 0 means ignore such limit", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(log_cleanup_interval, "1m","[5s,30d]", "log file clean up task schedule interval, set 1 day or longer, [5s, 30d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(log_dir_size_threshold, "64GB", "[256M,1T]", "max usable space size of log dir, used to decide whether should clean up log file, [256MB, 1T]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(max_syslog_file_time, "7d","[1s,]", "Maximum retention time of archive logs, 0 means ignore such limit", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(max_syslog_file_count, "0", "[0,]", "Maximum number of archive files to keep, 0 means ignore such limit", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_syslog_file_compress, "false", "Whether to enable archive log compression", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  //resource pool related
  DEF_BOOL(need_convert_vip_to_tname, "false", "convert vip to tenant name, which is useful in cloud", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_TIME(long_async_task_timeout, "60s", "[1s,1h]", "long async task timeout, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(short_async_task_timeout, "5s", "[1s,1h]", "short async task timeout, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR_LIST(username_separator, ":;-;.", "username separator", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  //client session related
  DEF_BOOL(enable_client_connection_lru_disconnect, "false",
        "if client connections reach throttle, true is that new connection will be accepted, and eliminate lru client connection, false is that new connection will disconnect, and err packet will be returned", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_INT(client_max_connections, "8192", "[0,]", "client max connections for one obproxy, [0, +∞]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(observer_query_timeout_delta, "20s", "[1s,30s]", "the delta value for @@ob_query_timeout, to cover net round trip time(proxy<->server) and task schedule time(server), [1s, 30s]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_cluster_checkout, "true", "if enable cluster checkout, proxy will send cluster name when login and server will check it", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_proxy_scramble, "false", "if enable proxy scramble, proxy will send client its variable scramble num, not support old observer", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_client_ip_checkout, "true", "if enabled, proxy send client ip when login", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //connection related
  DEF_INT(connect_observer_max_retries, "3", "[2,5]", "max retries to do connect", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //net related
  DEF_BOOL(frequent_accept, "true", "frequent accept", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(net_accept_threads, "2", "[0,8]", "net accept threads num, [0, 8]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(net_config_poll_timeout, "1ms", "[0,]", "not used, just for compatible", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(default_inactivity_timeout, "180000s", "[1s,30d]", "default inactivity timeout, [1s, 30d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(sock_recv_buffer_size_out, "0", "[0,8MB]", "sock param, recv buffer size, [0, 8MB], if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(sock_send_buffer_size_out, "0", "[0,8MB]", "sock param, send buffer size, [0, 8MB], if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(sock_option_flag_out, "3", "[0,]","sock param, option flag out, bit 1: NO_DELAY, bit 2: KEEP_ALIVE, bit 3: LINGER_ON", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(sock_packet_mark_out, "0", "[0,]","sock param, packet mark out, [0, 1]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(sock_packet_tos_out, "0", "[0,]","sock param, packet tos out, [0, 1]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(server_tcp_init_cwnd, "0", "[0,64]", "the initial tcp congestion window, [0, 64]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(server_tcp_keepidle, "5", "[0,7200]", "tcp keepalive idle time, unit is second, 0 means use default value by kernel", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(server_tcp_keepintvl, "5", "[0,75]", "tcp keepalive interval time, unit is second, 0 means use default value by kernel", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(server_tcp_keepcnt, "5", "[0,9]", "tcp keepalive probe count, 0 means use default value by kernel", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(server_tcp_user_timeout, "0", "[0,20]", "tcp user timeout, unit is s,  0 means no user timeout", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  DEF_INT(client_sock_option_flag_out, "2", "[0,]","client sock param, option flag out, bit 1: NO_DELAY, bit 2: KEEP_ALIVE", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(client_tcp_keepidle, "5", "[0,7200]", "client tcp keepalive idle time, unit is second, 0 means use default value by kernel", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(client_tcp_keepintvl, "5", "[0,75]", "client tcp keepalive interval time, unit is second, 0 means use default value by kernel", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(client_tcp_keepcnt, "5", "[0,9]", "client tcp keepalive probe count, 0 means use default value by kernel", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(client_tcp_user_timeout, "0", "[0,20]", "client tcp user timeout, unit is s,  0 means no user timeout", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //proxy init related
  DEF_CAP(proxy_mem_limited, "2G", "[100MB,100G]", "proxy memory limited, [100MB, 100G]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(stack_size, "1MB", "[1MB,10MB]", "stack size of one thread, [1MB, 10MB]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(routing_cache_mem_limited, "128MB", "[1KB,100G]", "max size of all proxy routing cache size, like table cache, location cache, etc. [1KB, 100G]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(work_thread_num, "128", "[1,128]", "proxy work thread num or max work thread num when automatic match, [1, 128]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(task_thread_num, "2", "[1,4]", "proxy task thread num, [1, 4]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(block_thread_num, "1", "[1,4]", "proxy block thread num, [1, 4]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(grpc_thread_num, "8", "[8,16]", "proxy grpc thread num, [8, 16]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(grpc_client_num, "9", "[9,16]", "proxy grpc client num, [9, 16]", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(automatic_match_work_thread, "true", "ignore work_thread_num configuration item, use the count of cpu for current proxy work thread num", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_strict_kernel_release, "true", "If is true, proxy only support 5u/6u/7u redhat. Otherwise no care kernel release, and proxy maybe unstable", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_CAP(max_log_file_size, "256MB", "[1MB,1G]", "max size of log file, [1MB, 1G]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //request&response transform related
  DEF_CAP(default_buffer_water_mark, "32KB", "[4B,64KB]", "default buffer water mark, [4B, 64KB]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(tunnel_request_size_threshold, "8KB", "(0,16MB]", "use tunnel to transfer request, [4KB, 16MB], if request bigger than the threshold, 0 disable", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(request_buffer_length, "4KB", "[1KB, 16MB]", "the max length of request buffer we will alloc for each reqeust", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(flow_high_water_mark, "64K", "[0,16MB]", "flow high water mark for flow control, [0, 16MB], if set a negative value, proxy treat it as 64K", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(flow_low_water_mark, "64K", "[0,16MB]", "flow low water mark for flow control, [0, 16MB], if set a negative value, proxy treat it as 64K", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(flow_consumer_reenable_threshold, "256", "[0,131072]", "consumer reenable threshold for flow control, [0, 131072], if set a negative value, proxy treat it as 256", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(flow_event_queue_threshold, "5", "[0,20]", "event queue threshold for flow control, [0, 20], if set a negative value, proxy treat it as 5", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_flow_control, "true", "whether flow control is enabled in a mysql tunnel, applied instantly in new created tunnels after updated", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //statistics related
  DEF_BOOL(enable_trans_detail_stats, "true", "enable mysql transaction detail stats", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_sync_all_stats, "true", "enable proxy sync all stats", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_mysqlsm_info, "true", "enable mysqlsm info, not used in proxy now", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_report_session_stats, "false", "enable report client session statistic table", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_strict_stat_time, "true", "enable strict statistic time, use gettimeofday or clock_gettime(CLOCK_REALTIME)", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_cpu_topology, "false", "enable cpu topology, work threads bind to cpu", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_trace_stats, "false", "enable mysql trace stats", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(slow_transaction_time_threshold, "1s", "[0s,30d]", "slow transaction time threshold, [0s, 30d], if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(slow_proxy_process_time_threshold, "2ms", "[0s,30d]", "slow proxy process time threshold, [0s, 30d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(query_digest_time_threshold, "100ms", "[0s,30d]", "digest time threshold, [0s, 30d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(slow_query_time_threshold, "500ms", "[0s,30d]", "slow query time threshold, [0s, 30d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //start up params, do not modify by manual
  DEF_BOOL(ignore_local_config, "true", "ignore all local cached files, start proxy with remote json", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_IP(local_bound_ip, "0.0.0.0", "local bound ip(any)", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_INT(listen_port, "6688", "(1024,65536)", "obproxy listen port", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(obproxy_config_server_url, "", "url of config info(rs list and so on)", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(proxy_service_mode, "client", "proxy deploy and service mode: 1.client(default); 2.server", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_INT(proxy_id, "0", "[0, 255]", "used to identify each obproxy, it can not be zero if proxy_service_mode is server", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(app_name, "undefined", "current application name which proxy works for, need defined, only modified when restart", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_metadb_used, "true", "use MetaDataBase when proxy run", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_get_rslist_remote, "false", "enable direct get rootservice list from remote", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // without config server url
  DEF_STR(rootservice_cluster_name, "undefined", "default cluster name for rootservice_list", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR_LIST(rootservice_list, "", "a list of servers against which election candidate is checked for validation, format ip1:sql_port1;ip2:sql_port2", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // default tenant
  DEF_STR(proxy_tenant_name, "", "default tenant name for cloud user", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  // congestion
  DEF_INT(congestion_failure_threshold, "5", "[0,]", "congestion failure threshold, [0, +∞], 0 means disable congestion", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(min_keep_congestion_interval, "20s", "[1s,1d]", "minimum keep congestion interval, [1s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(congestion_fail_window, "120s", "[1s,1h]", "congestion failure window size, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(congestion_retry_interval, "20s", "[1s,1h]", "congestion retry interval, [1s, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(min_congested_connect_timeout, "100ms", "[1ms,1h]", "if client connect timeout after the time, proxy set target server alive congested, [1ms, 1h]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_congestion, "true", "enable congestion feature or not", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  DEF_BOOL(enable_bad_route_reject, "false", "if enabled, bad route request will be rejected, e.g. first statement of transaction opened by BEGIN(or START TRANSACTION) without table name", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_partition_table_route, "true", "if enabled, partition table will be accurate routing", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_compression_protocol, "true", "if enabled, proxy will use compression protocol with server", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_ob_protocol_v2, "true", "if enabled, proxy will use oceanbase protocol 2.0 with server", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_reroute, "false", "if this and protocol_v2 enabled, proxy will reroute when routing error", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_pl_route, "true", "if enabled, pl will be accurate routing", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_cached_server, "true", "if enabled, use cached server session when no table entry", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // location rate limit
  DEF_INT(normal_pl_update_threshold, "100", "[0,]", "max partition location update task processing per second", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_INT(limited_pl_update_threshold, "10", "[0,]", "in merging state, max partition location update task processing per second", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // sqlaudit
  DEF_CAP(sqlaudit_mem_limited, "0", "[0,1G]", "sqlaudit memory limited, [0, 1GB]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_CAP(internal_cmd_mem_limited, "64K", "[0,64MB]", "internal cmd response memory limited, [0, 64MB], 0 means unlimited", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  //debug
  DEF_STR(test_server_addr, "", "proxy will choose this addr(if not empty) as observer addr forcibly, format ip1:sql_port1;ip2:sql_port2", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(server_routing_mode, "oceanbase", "server routing mode: 1.oceanbase(default mode); 2.random; 3.mock; 4.mysql", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_LOG_LEVEL(syslog_level, "INFO", "specifies the current level of logging: DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_LOG_LEVEL(monitor_log_level, "INFO", "specifies the current level of logging: DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_LOG_LEVEL(xflush_log_level, "INFO", "specifies the current level of logging: DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_async_log, "true", "if enabled, use async logging way, maybe lost some log when busy", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // proxy cmd
  DEF_INT(proxy_local_cmd, "0", "[0,2]", "proxy local cmd type: 0->none(default), 1->exit, 2->restart", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_MEMORY);

  //ldc
  DEF_STR(proxy_idc_name, "", "idc name for proxy ldc route. If is empty or invalid, treat as do not use ldc. User session vars 'proxy_session_ldc' can cover it", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  DEF_INT(current_local_config_version, "0", "[0,]", "local config version for current app", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_INT(local_vip_tenant_version, "0", "[0,]", "local vip tenant version", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_MEMORY);
  // unused now, keep it for test,deleted later
  DEF_INT(max_connections, "60000", "(128,65535]", "max fd proxy could use", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_CAP(client_max_memory_size, "8MB", "[8KB, 64MB]", "max dynamic alloc memory size of one client session", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // delay update table entry or partition entry
  DEF_TIME(delay_update_entry_interval, "5s", "[0s,1d]", "delay update table entry or partition entry interval, [0s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // tenant location valid time, if expred, will update all dummy
  DEF_TIME(tenant_location_valid_time, "1d", "[0s,100d]", "tenant location valid time, [0s, 100d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // monitor
  DEF_INT(monitor_item_limit, "3000", "[0,10000]", "obproxy monitor stat item/prometheus metric limit", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_TIME(monitor_item_max_idle_period, "30m", "[1m, 1d]", "monitor stat item in memory idle period. it will remove if timeout", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(monitor_cost_ms_unit, "false", "enable monitor cost unit is ms, default is us", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_TIME(monitor_stat_dump_interval, "1m", "[1s,1h]", "dump monitor statistic in log interval, [1s, 1h], 0 means disable, if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(monitor_stat_low_threshold, "30ms", "[0s, 5s]", "tenant stat time low threshold", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(monitor_stat_middle_threshold, "100ms", "[0s, 30s]", "tenant stat time middle threshold", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(monitor_stat_high_threshold, "500ms", "[0s, 1m]", "tenant stat time high threshold", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_monitor_stat, "true", "enable monitor stat or not", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // prometheus
  DEF_INT(prometheus_listen_port, "2884", "(1024,65536)", "obproxy prometheus listen port", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(prometheus_cost_ms_unit, "false", "enable prometheus cost unit is ms, default is us", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_TIME(prometheus_sync_interval, "1m", "[1s,1h]", "update sync metrics to prometheus exposer interval, [1s, 1h], 0 means disable, if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_prometheus, "true", "enable prometheus or not", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_extra_prometheus_metric, "false", "enable net and route prometheus merics or not", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  DEF_BOOL(enable_causal_order_read, "true", "if enabled, proxy will choose server by priority and sync safe snapshot version if need", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  // The following four parameters are related to the location cache at the table level.
  // Here, it is recommended to use partition_location_expire_relative_time in an emergency,
  // and set the active expiration recommendation location_expire_period_time
  // enable_qa_mode and location_expire_period will be discarded later
  DEF_INT(partition_location_expire_relative_time, "0", "[-36000000,36000000]", "the unit is ms, 0 means do not expire, others will expire partition location base on relative time", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_qa_mode, "false", "just for test, not recommended, if enabled, proxy can forcibly expire all location cache", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_INT(location_expire_period, "0", "[0,36000000]", "just for test, not recommended, the unit is ms, only work if qa_mode is set, it means location cache which has been created for more than this value will be expired", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_TIME(location_expire_period_time, "0d", "[0s, 30d]", "time for location expire period, values in [0s, 30d], 0 means no expire", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // in public cloud, will assign a vip addr to proxy. qa_mode_mock_slb_vip is a vip addr for testing
  DEF_STR(qa_mode_mock_public_cloud_slb_addr, "127.0.0.1:33045", "mock public cloud slb addr", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(qa_mode_mock_public_cloud_vid, "1", "[1,102400]", "mock public cloud vid", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(proxy_route_policy, "", "proxy route policy", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  DEF_STR(mysql_version, "5.6.25", "returned version for mysql mode, default value is 5.6.25. If set, proxy will send new version when user connect to proxy", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER)
  // sql table cache
  DEF_BOOL(enable_index_route, "false", "enable index route or not", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(sql_table_cache_expire_relative_time, "0", "[-36000000,36000000]", "the unit is ms, 0 means do not expire, others will expire sql table cache base on relative time", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_CAP(sql_table_cache_mem_limited, "128MB", "[1KB,100G]", "max size of proxy sql table cache size. [1KB, 100G]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_cloud_full_username, "false", "used for cloud user, if set false, treat all login user as username", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_full_username, "false", "used for non-cloud user, if set true, username must have tenant and cluster", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(skip_proxyro_check, "false", "used for proxro@sys, if set false, access denied", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  DEF_BOOL(skip_proxy_sys_private_check, "true", "skip_proxy_sys_private_check", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  // SSL related config
  DEF_BOOL(enable_client_ssl, "false", "if enabled, proxy will try best to connect client with ssl",
             CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_server_ssl, "false", "if enabled, proxy will try best to connect server whith ssl",
            CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // QOS
  DEF_BOOL(enable_qos, "false", "if enabled, proxy will be able to qos", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(qos_stat_clean_interval, "1m", "[1s,1h]", "clean qos stat interval, [1s, 1h], 0 means disable, if set a negative value, proxy treat it as 0", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(qos_stat_item_max_idle_period, "30m", "[1m, 1d]", "qos stat item in memory idle period. it will remove if timeout", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(qos_stat_item_limit, "3000", "[0,10000]", "obproxy qos stat item", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // standby
  DEF_BOOL(enable_standby, "true", "enable standby or not", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // primary zone
  DEF_STR(proxy_primary_zone_name, "", "primary zone name for proxy ldc route. If not empty, proxy only route to the zone", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  // LGD config
  DEF_BOOL(enable_ldg, "false", "enable proxy to support ldg", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_TIME(ldg_info_refresh_interval, "20s", "[10s,1d]", "config server info refresh task interval, [10s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  // location cache
  DEF_BOOL(check_tenant_locality_change, "true", "enable locality change trigger location cache dirty", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_async_pull_location_cache, "true", "enable async pull location cache when is dirty", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // sequence
  DEF_TIME(sequence_entry_expire_time, "1d", "[0s,1d]", "sequence entry valid time, [0s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(sequence_fail_retry_count, "2", "[0,100]", "the count sequence retry when fail, [0, 100]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_sequence_prefetch, "true", "if enabled, will prefetch sequence ", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(sequence_prefetch_threshold, "7000", "[0,10000]", "when cost reach threshold will prefetch, [0, 10000]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_mysql_proxy_pool, "true", "if enabled, will long conn for  sequence ", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // sidecar
  DEF_BOOL(enable_sharding, "false", "if enabled means use logic db", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(sidecar_node_id, "", "node id for dbmesh", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(dataplane_host, "", "dataplane address or hostname", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(use_local_dbconfig, "false", "if enabled, start dbmesh with local dbconfig", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_shard_authority, "false", "if enabled, check authority for sharding user", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(grpc_timeout, "30m", "[1s,1d]", "grpc client timeout, [1s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(env_tenant_name, "", "app tenant name", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(workspace_name, "", "app workspace name", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(server_zone, "", "app server location", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(pod_name, "", "sidecar pod name ", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(pod_namespace, "", "sidecar pod namespace", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(instance_ip, "", "ip addr string", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(runtime_env, "", "runtime env", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(mng_url, "", "ob sharding console url", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(cloud_instance_id, "", "ob sharding cloud instance id", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(auto_scan_all, "false", "if enabled, need scan all", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // session pool
  DEF_BOOL(is_pool_mode, "false", "if enabled means useing session pool", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_conn_precreate, "false", "if enabled means precreate conn for session pool", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_session_pool_for_no_sharding, "false", "if enabled can use session pool for no sharding", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(enable_no_sharding_skip_real_conn, "false", "if enabled no sharding will use saved password check to skip real conn", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(need_release_after_tx, "false", "if enabled means release server session after transaction complete", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(session_pool_retry_interval, "1ms", "[0s,1d]", "session_pool_retry_interval, [0s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(refresh_server_cont_num, "5", "[0,100]", "the num of refresh server cont, [0,1000]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(create_conn_cont_num, "10", "[0,100]", "the num of create  conn cont, [0, 1000]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(max_pending_num, "100000", "[0,1000000]", "the num of conn in pending list, [0, 100000]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(session_pool_cont_delay_interval, "5ms", "[0s,1d]", "session_pool_cont_delay_interval, [0s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(use_local_session_prop, "false", "if enabled means use_local_session prop", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(session_pool_default_min_conn, "0", "[0,10000]", "the num of min conn , [0, 100000]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(session_pool_default_max_conn, "20", "[0,100000]", "the num of max conn , [0, 100000]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(session_pool_default_idle_timeout, "1800s", "[0s,1d]", "session_pool_default_idle_timeout, [0s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(session_pool_default_blocking_timeout, "500ms", "[0ms,2s]", "session_pool_default_blocking_timeout, [0ms, 2s]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(session_pool_default_prefill, "false", "session_pool_default_prefill", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(session_pool_stat_log_ratio, "9000", "[0, 10000]", "the num when reach will log", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_TIME(session_pool_stat_log_interval, "1m", "[0s,1d]", "pool stat log interval, [0s, 1d]", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // beyond trust sdk
  DEF_STR(domain_name, "", "app domain name", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(bt_use_antvip, "true", "if enabled, will use ant vip for bt sdk", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(bt_server_addr, "", "beyond trust server address or hostname", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(bt_antvip_server_addr, "", "beyond trust antvip server", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(bt_server_antvip, "", "beyond trust server antvip", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_BOOL(bt_local_work_mode, "false", "true means LOCAL_CERT_MODE and false means CITADEL_CERT_MODE", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(bt_env_mode, "TENANT_MODE", "beyond trust env mode", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_STR(bt_instance_id, "", "beyond trust instance id for INSTANCEID env mode", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);
  DEF_INT(bt_retry_times, "3", "[0,100]", "beyond trust sdk retry times", CFG_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_USER);

  // proxyro@sys and root@proxysys passwd
  DEF_STR(inspector_password, "", "password for inspector user", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(obproxy_sys_password, "", "password for obproxy sys user", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(observer_sys_password, "", "password for observer sys user", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_STR(observer_sys_password1, "", "password for observer sys user", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  // performance_mode has two functions: competition and verification, it will turn off some auxiliary functions,
  // but these functions are required in production; another case is to verify whether the optimization method is effective
  DEF_BOOL(enable_performance_mode, "false", "if enabled, for performance situation", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_trace, "true", "if enabled, log will print trace info", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
  DEF_BOOL(enable_stat, "true", "if enabled, will collect stat info", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);

  DEF_INT(digest_sql_length, "1024", "0 means use default print sql len, otherwise is digest_sql_length", CFG_NO_NEED_REBOOT, CFG_SECTION_OBPROXY, CFG_VISIBLE_LEVEL_SYS);
};

ObProxyConfig &get_global_proxy_config();

inline common::ObString ObProxyConfig::get_service_mode_string(const ObProxyServiceMode mode)
{
  static const common::ObString service_mode_string_array[OB_MAX_SERVICE_MODE] =
  {
    common::ObString::make_string("client"),
    common::ObString::make_string("server")
  };
  common::ObString mode_string;
  if (OB_CLIENT_SERVICE_MODE <= mode && mode < OB_MAX_SERVICE_MODE) {
    mode_string = service_mode_string_array[mode];
  }
  return mode_string;
}

inline ObProxyServiceMode ObProxyConfig::get_service_mode(const common::ObString &mode_string)
{
  ObProxyServiceMode mode = OB_MAX_SERVICE_MODE;
  if (!mode_string.empty()) {
    if (0 == mode_string.case_compare(get_service_mode_string(OB_CLIENT_SERVICE_MODE))) {
      mode = OB_CLIENT_SERVICE_MODE;
    } else if (0 == mode_string.case_compare(get_service_mode_string(OB_SERVER_SERVICE_MODE))) {
      mode = OB_SERVER_SERVICE_MODE;
    }
  }
  return mode;
}

inline ObProxyServiceMode ObProxyConfig::get_service_mode(const char *mode_str)
{
  ObProxyServiceMode mode = OB_MAX_SERVICE_MODE;
  if (OB_LIKELY(NULL != mode_str)) {
    common::ObString mode_string(mode_str);
    mode = get_service_mode(mode_string);
  }
  return mode;
}

inline bool ObProxyConfig::is_client_service_mode() const
{
  bool bret = false;
  obsys::CRLockGuard guard(rwlock_);
  bret = (0 == get_service_mode_string(OB_CLIENT_SERVICE_MODE).case_compare(proxy_service_mode));
  return bret;
}

inline bool ObProxyConfig::is_service_mode_available(ObProxyServiceMode &mode) const
{
  bool bret = true;
  obsys::CRLockGuard guard(rwlock_);
  mode = get_service_mode(proxy_service_mode.str());
  if (OB_UNLIKELY(mode >= OB_MAX_SERVICE_MODE) || OB_UNLIKELY(mode < OB_CLIENT_SERVICE_MODE)) {
    bret = false;
  }
  return bret;
}

inline const char *ObProxyConfig::get_routing_mode_str(ObServerRoutingMode mode)
{
  const char *mode_str = NULL;
  static const char *routing_mode_str_array[OB_MAX_ROUTING_MODE] =
  {
    "oceanbase",
    "random",
    "mock",
    "mysql"
  };
  if (mode >= OB_STANDARD_ROUTING_MODE && mode < OB_MAX_ROUTING_MODE) {
    mode_str = routing_mode_str_array[mode];
  }
  return mode_str;
}

inline ObServerRoutingMode ObProxyConfig::get_routing_mode(const common::ObString &mode_str)
{
  ObServerRoutingMode mode = OB_MAX_ROUTING_MODE;

  if (!mode_str.empty()) {
    if (0 == mode_str.case_compare(get_routing_mode_str(OB_STANDARD_ROUTING_MODE))) {
      mode = OB_STANDARD_ROUTING_MODE;
    } else if (0 == mode_str.case_compare(get_routing_mode_str(OB_RANDOM_ROUTING_MODE))) {
      mode = OB_RANDOM_ROUTING_MODE;
    } else if (0 == mode_str.case_compare(get_routing_mode_str(OB_MOCK_ROUTING_MODE))) {
      mode = OB_MOCK_ROUTING_MODE;
    } else if (0 == mode_str.case_compare(get_routing_mode_str(OB_MYSQL_ROUTING_MODE))) {
      mode = OB_MYSQL_ROUTING_MODE;
    }
  }

  return mode;
}

inline ObServerRoutingMode ObProxyConfig::get_routing_mode(const char *mode_str)
{
  ObServerRoutingMode mode = OB_MAX_ROUTING_MODE;
  if (OB_LIKELY(NULL != mode_str)) {
    common::ObString tmp_mode_str(mode_str);
    mode = get_routing_mode(tmp_mode_str);
  }
  return mode;
}

inline bool ObProxyConfig::is_routing_mode_available(ObServerRoutingMode &mode) const
{
  bool bret = true;
  obsys::CRLockGuard guard(rwlock_);
  mode = get_routing_mode(server_routing_mode.str());
  if (OB_UNLIKELY(mode >= OB_MAX_ROUTING_MODE) || OB_UNLIKELY(mode < OB_STANDARD_ROUTING_MODE)) {
    bret = false;
  }
  return bret;
}

inline bool ObProxyConfig::is_local_cmd(const int64_t cmd)
{
  return (OB_LOCAL_CMD_NONE < cmd && cmd < OB_LOCAL_CMD_MAX);
}

inline bool ObProxyConfig::is_local_cmd()
{
  return is_local_cmd(get_local_cmd_type());
}

inline bool ObProxyConfig::is_local_restart()
{
  return (OB_LOCAL_CMD_RESTART == get_local_cmd_type());
}

inline ObProxyLocalCMDType ObProxyConfig::get_local_cmd_type() const
{
  return static_cast<ObProxyLocalCMDType>(proxy_local_cmd.get());
}

inline void ObProxyConfig::reset_local_cmd()
{
  proxy_local_cmd = 0;
}

}
}
}
#endif /* OBPROXY_CONFIG_H */
