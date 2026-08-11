/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OBPROXY_RPC_REDIS_STAT_H
#define OBPROXY_RPC_REDIS_STAT_H
#include <stdint.h>
#include "utils/ob_ref_hash_map.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "lib/ob_define.h"
#include "lib/time/ob_hrtime.h"
#include "lib/atomic/ob_atomic.h"
#include "stat/ob_mysql_stats.h"
#include "stat/ob_rpc_stats.h"
#include "stat/ob_rpc_req_stats.h"
#include "proxy/rpc/rpclib/ob_rpc_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define RUN_ID_LEN 40

class ObRpcRedisInfoStat{
public:
  ObRpcRedisInfoStat(): is_inited_(false), rpc_port_(0), start_time_us_(0),
                      cur_clients_(0), cur_monitor_clients_(0), client_recent_max_input_buffer_(0), client_recent_max_output_buffer_(0),
                      total_connections_received_(0), total_commands_processed_(0),
                      total_net_input_bytes_(0), total_net_output_bytes_(0), instantaneous_input_kbps_(0.0),
                      instantaneous_output_kbps_(0), rejected_connections_(0), hold_memory_(0), used_memory_(0), used_memory_peak_(0){
                        memset(run_id_, 0, sizeof(run_id_));
                      }
  ~ObRpcRedisInfoStat() {}

  // server
  void set_rpc_port(int32_t port) { rpc_port_ = port;}
  uint16_t get_rpc_port() { return rpc_port_; }
  void set_up_time(int64_t start_time_us) { start_time_us_ = start_time_us; }
  int64_t get_up_time_in_seconds() {
    int64_t cur_time_us = common::hrtime_to_usec(common::get_hrtime_internal());
    int64_t uptime_in_seconds = (cur_time_us - start_time_us_) / (1000 * 1000);
    return uptime_in_seconds;
  }
  void gen_run_id();
  char *get_run_id() { return run_id_; }

  // clients
  int64_t get_connected_clients() { RPC_READ_DYN_SUM(CURRENT_REDIS_CLIENT_CONNECTIONS, cur_clients_); return cur_clients_;}
  int64_t get_monitor_clients() { return cur_monitor_clients_;}
  void update_monitor_clients(int count){
    ATOMIC_AAF(&cur_monitor_clients_, count);
  }
  void set_max_input_buffer(int64_t len) {
    if (len > client_recent_max_input_buffer_) {
      client_recent_max_input_buffer_ = len;
    }
  }
  void set_max_output_buffer(int64_t len) {
    if (len > client_recent_max_output_buffer_) {
      client_recent_max_output_buffer_ = len;
    }
  }
  int64_t get_max_input_buffer() { return client_recent_max_input_buffer_;}
  int64_t get_max_output_buffer() { return client_recent_max_output_buffer_;}

  // stats
  int64_t get_total_connection_recevied() { RPC_READ_DYN_SUM(TOTAL_REDIS_CLIENT_CONNECTIONS, total_connections_received_); return total_connections_received_; }
  int64_t get_total_command_processed() { RPC_READ_DYN_SUM(REDIS_COMMAND_PROCESSED, total_commands_processed_); return total_commands_processed_; }
  int64_t get_total_net_input_bytes() { RPC_READ_DYN_SUM(REDIS_TOTAL_NET_INPUT_BYTES, total_net_input_bytes_); return total_net_input_bytes_; }
  int64_t get_total_net_output_bytes() { RPC_READ_DYN_SUM(REDIS_TOTAL_NET_OUTPUT_BYTES, total_net_output_bytes_); return total_net_output_bytes_; }
  void add_rejected_connections() { ATOMIC_AAF(&rejected_connections_, 1);}
  int64_t get_rejected_connections() { return rejected_connections_; }
  void set_instantaneous_input_kbps(double speed) { instantaneous_input_kbps_ = speed; }
  double get_instantaneous_input_kbps() { return instantaneous_input_kbps_; }
  void set_instantaneous_output_kbps(double speed) { instantaneous_output_kbps_ = speed; }
  double get_instantaneous_output_kbps() { return instantaneous_output_kbps_; }

  // memory
  void set_used_memory(int64_t cur_used_memory) {
    used_memory_ = cur_used_memory;
    if (cur_used_memory > used_memory_peak_) {
      used_memory_peak_ = cur_used_memory;
    }
  }
  void set_hold_memory(int64_t cur_hold_memory) { hold_memory_ = cur_hold_memory; }
  int64_t get_used_memory() { return used_memory_; }
  int64_t get_hold_memory() { return hold_memory_; }
  int64_t get_used_memory_peak() { return used_memory_peak_; }

private:
  bool is_inited_;
  // direct access
  // server
  uint16_t rpc_port_;
  int64_t start_time_us_;
  // clients
  int64_t cur_clients_;
  int64_t cur_monitor_clients_;
  int64_t client_recent_max_input_buffer_;
  int64_t client_recent_max_output_buffer_;
  // stats
  int64_t total_connections_received_;
  int64_t total_commands_processed_;
  int64_t total_net_input_bytes_;
  int64_t total_net_output_bytes_;
  double instantaneous_input_kbps_;
  double instantaneous_output_kbps_;
  int64_t rejected_connections_;

  // memory
  int64_t hold_memory_;
  int64_t used_memory_;
  int64_t used_memory_peak_;

  char run_id_[RUN_ID_LEN + 1];
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisInfoStat);
};
extern ObRpcRedisInfoStat &get_global_redis_info_stat();

class ObRpcRedisInfoFactory
{
public:
  ObRpcRedisInfoFactory() {}
  static int format_redis_server_info(char*buf, int64_t size, int64_t &pos);
  static int format_redis_clients_info(char *buf, int64_t size, int64_t &pos);
  static int format_redis_memory_info(char *buf, int64_t size, int64_t &pos);
  static int format_redis_persistence_info(char *buf, int64_t size, int64_t &pos);
  static int format_redis_stats_info(char *buf, int64_t size, int64_t &pos);
  static int format_redis_cpu_info(char *buf, int64_t size, int64_t &pos);
  static int format_redis_cluster_info(char *buf, int64_t size, int64_t &pos);

  static void bytesToHuman(char *s, int64_t memory);

};


} // proxy end
} // obproxy end
} //oceanbase end

#endif