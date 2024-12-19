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
#include <sys/resource.h>
#include "utils/ob_layout.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc/redis/ob_rpc_redis_stat.h"
#include "build_version.c"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
#define MAX_MEMORY_HUMAN_LEN 64

/*************************************ObRpcRedisInfoFactory*****************************************/
ObRpcRedisInfoStat &get_global_redis_info_stat()
{
  static ObRpcRedisInfoStat g_rpc_redis_info_stat;
  return g_rpc_redis_info_stat;
}

void ObRpcRedisInfoStat::gen_run_id()
{
  int ret = OB_SUCCESS;
  unsigned int len = 0;
  char charset[] = "0123456789abcdef";
  if (OB_FAIL(ObRandomNumUtils::get_random_bytes((unsigned char *)run_id_, len))) {
    LOG_WDIAG("gen_run_id failed", K(ret));
  } else {
    for (int i = 0; i < len && i < RUN_ID_LEN; i++) {
      run_id_[i] = charset[run_id_[i] & 0x0F];
    }
  }
}

int ObRpcRedisInfoFactory::format_redis_server_info(char*buf, int64_t size, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    int64_t up_time_in_seconds = get_global_redis_info_stat().get_up_time_in_seconds();
    int32_t rpc_port = get_global_redis_info_stat().get_rpc_port();
    if (rpc_port == 0) {
      rpc_port = obutils::get_global_proxy_config().rpc_listen_port;
    }
    int len = snprintf(buf + pos, size,
            "# Server\r\n"
            "obproxy_version:obproxy (%s)\r\n"
            "obproxy_git_sha1:%s\r\n"
            "obproxy_git_dirty:%s\r\n"
            "obproxy_build_id:%s\r\n"
            "redis_mode:%s\r\n"
            "process_id:%ld\r\n"
            "process_supervised:%s\r\n"
            "run_id:%s\r\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%ld\r\n"
            "executable:%s\r\n",
            PACKAGE_STRING,
            "00000000",
            "0",
            build_version(),
            "standalone",
            (int64_t) getpid(),
            "no",
            get_global_redis_info_stat().get_run_id(),
            rpc_port,
            up_time_in_seconds,
            get_global_layout().get_bin_dir()
            );
    if (len < 0) {
      pos += 0;
      ret = OB_ERR_SYS;
      LOG_DEBUG("build server info failed", K(up_time_in_seconds));
    } else {
      pos += len;
    }
  }
  return ret;
}

int ObRpcRedisInfoFactory::format_redis_clients_info(char*buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    int maxclients = obutils::get_global_proxy_config().client_max_connections;
    int64_t connected_clients = get_global_redis_info_stat().get_connected_clients();
    int64_t max_input_buffer = get_global_redis_info_stat().get_max_input_buffer();
    int64_t max_output_buffer = get_global_redis_info_stat().get_max_output_buffer();
    int len = snprintf(buf + pos, size,
            "# Clients\r\n"
            "connected_clients:%ld\r\n"
            "maxclients:%d\r\n"
            "client_recent_max_input_buffer:%ld\r\n"
            "client_recent_max_output_buffer:%ld\r\n",
            connected_clients,
            maxclients,
            max_input_buffer,
            max_output_buffer
            );
    if (len < 0) {
      ret = OB_ERR_SYS;
      LOG_WDIAG("build clients info failed", K(len));
    } else {
      pos += len;
    }
  }
  return ret;
}

int ObRpcRedisInfoFactory::format_redis_memory_info(char *buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    int64_t hold_memory = proxy::get_global_redis_info_stat().get_hold_memory() + 70 * 1024 * 1024; // add OTHER MEMORY
    int64_t used_memory = get_global_redis_info_stat().get_used_memory();
    int64_t used_memory_peak = get_global_redis_info_stat().get_used_memory_peak();
    int64_t maxmemory = obutils::get_global_proxy_config().proxy_mem_limited;
    char hold_memory_human[MAX_MEMORY_HUMAN_LEN] = { 0 };
    char used_memory_human[MAX_MEMORY_HUMAN_LEN] = { 0 };
    char used_memory_peak_human[MAX_MEMORY_HUMAN_LEN] = { 0 };
    char maxmemory_human[MAX_MEMORY_HUMAN_LEN]= { 0 };
    float used_memory_peak_perc = 0.0;
    if (OB_LIKELY(0 != used_memory_peak)) {
      used_memory_peak_perc = (static_cast<float>(used_memory) / static_cast<float>(used_memory_peak)) * 100.0f;
    }

    bytesToHuman(hold_memory_human, hold_memory);
    bytesToHuman(used_memory_human, used_memory);
    bytesToHuman(used_memory_peak_human, used_memory_peak);
    bytesToHuman(maxmemory_human, maxmemory);

    int len = snprintf(buf + pos, size,
            "# Memory\r\n"
            "used_memory:%ld\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%ld\r\n"
            "used_memory_rss_human:%s\r\n"
            "used_memory_peak:%ld\r\n"
            "used_memory_peak_human:%s\r\n"
            "used_memory_peak_perc:%.2f%%\r\n"
            "maxmemory:%ld\r\n"
            "maxmemory_human:%s\r\n",
            hold_memory,
            hold_memory_human,
            used_memory,
            used_memory_human,
            used_memory_peak,
            used_memory_peak_human,
            used_memory_peak_perc,
            maxmemory,
            maxmemory_human
            );
    if (len < 0) {
      ret = OB_ERR_SYS;
      LOG_WDIAG("build memory info failed", K(hold_memory), K(used_memory), K(used_memory_peak), K(used_memory_peak_perc));
    } else {
      pos += len;
    }
  }
  return ret;
}

int ObRpcRedisInfoFactory::format_redis_persistence_info(char *buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    int len = snprintf(buf + pos, size,
            "# Persistence\r\n"
            "backend:%s\r\n",
            "obkv"
            );
    if (len < 0) {
      pos += 0;
      ret = OB_ERR_SYS;
      LOG_WDIAG("build persistence info failed", K(len));
    } else {
      pos += len;
    }
  }
  return ret;
}

int ObRpcRedisInfoFactory::format_redis_stats_info(char *buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    int len = snprintf(buf + pos, size,
            "# Stats\r\n"
            "total_connections_received:%ld\r\n"
            "total_commands_processed:%ld\r\n"
            "total_net_input_bytes:%ld\r\n"
            "total_net_output_bytes:%ld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%ld\r\n",
            get_global_redis_info_stat().get_total_connection_recevied(),
            get_global_redis_info_stat().get_total_command_processed(),
            get_global_redis_info_stat().get_total_net_input_bytes(),
            get_global_redis_info_stat().get_total_net_output_bytes(),
            get_global_redis_info_stat().get_instantaneous_input_kbps(),
            get_global_redis_info_stat().get_instantaneous_output_kbps(),
            get_global_redis_info_stat().get_rejected_connections()
            );
    if (len < 0) {
      ret = OB_ERR_SYS;
      LOG_WDIAG("build stats info failed", K(len));
    } else {
      pos += len;
    }
  }
  return ret;
}

int ObRpcRedisInfoFactory::format_redis_cpu_info(char *buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    struct rusage self_ru, child_ru;
    if (getrusage(RUSAGE_SELF, &self_ru) == -1) {
      LOG_WDIAG("fail to get RUSAGE_SELF", K(buf));
    }
    if (getrusage(RUSAGE_CHILDREN, &child_ru) == -1) {
      LOG_WDIAG("fail to get RUSAGE_CHILDREN", K(buf));
    }
    int len = snprintf( buf + pos, size,
            "# CPU\r\n"
            "used_cpu_sys:%ld.%06ld\r\n"
            "used_cpu_user:%ld.%06ld\r\n"
            "used_cpu_sys_childen:%ld.%06ld\r\n"
            "used_cpu_user_childen:%ld.%06ld\r\n",
            (long)self_ru.ru_stime.tv_sec, (long)self_ru.ru_stime.tv_usec,
            (long)self_ru.ru_utime.tv_sec, (long)self_ru.ru_utime.tv_usec,
            (long)child_ru.ru_stime.tv_sec, (long)child_ru.ru_stime.tv_usec,
            (long)child_ru.ru_utime.tv_sec, (long)child_ru.ru_utime.tv_usec);
    if (len < 0) {
      ret = OB_ERR_SYS;
      LOG_WDIAG("build persistence info failed", K(self_ru.ru_stime.tv_sec), K(self_ru.ru_stime.tv_usec),
                K(self_ru.ru_utime.tv_sec), K(self_ru.ru_utime.tv_usec));
    } else {
      pos += len;
    }
  }
  return ret;
}

int ObRpcRedisInfoFactory::format_redis_cluster_info(char *buf, int64_t size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis inner msg is null", K(ret));
  } else {
    int len = snprintf(buf + pos, size,
            "# Cluster\r\n"
            "cluster_enabled:%d\r\n",
            0
            );
    if (len < 0) {
      pos += 0;
      ret = OB_ERR_SYS;
      LOG_WDIAG("build cluster info failed", K(len));
    } else {
      pos += len;
    }
  }
  return ret;
}

void ObRpcRedisInfoFactory::bytesToHuman(char *memory_human, int64_t memory)
{
  double new_memory;
  if (memory < 1024) {
    sprintf(memory_human, "%ldB", memory);
  } else if (memory < (1024 * 1024)) {
    new_memory = static_cast<double>(memory / 1024);
    sprintf(memory_human, "%.2fK", new_memory);
  } else if (memory < (1024LL * 1024 *1024)) {
    new_memory = static_cast<double> (memory/ (1024 *1024));
    sprintf(memory_human, "%.2fM", new_memory);
  } else if (memory < (1024LL * 1024 *1024 *1024)) {
    new_memory = static_cast<double> (memory/ (1024LL * 1024 * 1024));
    sprintf(memory_human, "%.2fG", new_memory);
  } else {
    // will not come here
    sprintf(memory_human, "%ldB", memory);
  }
}

}
}
}
