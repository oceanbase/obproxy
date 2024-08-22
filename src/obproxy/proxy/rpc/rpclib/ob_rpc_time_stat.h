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

#ifndef OBPROXY_RPC_TIME_STAT_H
#define OBPROXY_RPC_TIME_STAT_H

#include "lib/time/ob_hrtime.h"
#include "lib/utility/ob_print_utils.h"
#include "utils/ob_proxy_lib.h"
#include "iocore/net/ob_inet.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

struct ObRpcClientMilestones
{
  ObRpcClientMilestones() { reset(); }
  ~ObRpcClientMilestones() { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObRpcClientMilestones));
  }

  TO_STRING_KV(K_(client_begin), K_(client_read_end), K_(client_write_begin), K_(client_end));

  // client :
  // client_begin represents the time this transaction started.
  // If this is the first transaction in a connection, then client_begin is set to accept time.
  // otherwise it is set to first read time.
  ObHRTime client_begin_;
  ObHRTime client_read_end_;
  ObHRTime client_write_begin_;
  ObHRTime client_end_;
};

struct ObRpcServerMilestones
{
  ObRpcServerMilestones() { reset(); }
  ~ObRpcServerMilestones() { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObRpcServerMilestones));
  }

  TO_STRING_KV(K_(server_write_begin), K_(server_write_end), K_(server_read_begin), K_(server_read_end));

  // server
  ObHRTime server_write_begin_;
  ObHRTime server_write_end_;
  ObHRTime server_read_begin_;
  ObHRTime server_read_end_;
};

struct ObRpcMilestones
{
  ObRpcMilestones()
      : server_first_write_begin_(0), pl_lookup_begin_(0), pl_lookup_end_(0),
        pl_process_begin_(0), pl_process_end_(0), congestion_control_begin_(0),
        congestion_control_end_(0), congestion_process_begin_(0), congestion_process_end_(0),
        cluster_resource_create_begin_(0), cluster_resource_create_end_(0)
  { }
  ~ObRpcMilestones() { }

  void trans_reset()
  {
    MEMSET(this, 0, sizeof(ObRpcMilestones));
  }

  TO_STRING_KV(K_(analyze_request_begin), K_(analyze_request_end), K_(ctx_lookup_begin), K_(ctx_lookup_end),
               K_(query_async_lookup_begin), K_(query_async_lookup_end), K_(index_entry_lookup_begin), K_(index_entry_lookup_end),
               K_(table_group_lookup_begin), K_(table_group_lookup_end), K_(pl_lookup_begin), K_(pl_lookup_end));

  ObHRTime server_first_write_begin_;

  //analyzer
  ObHRTime analyze_request_begin_;
  ObHRTime analyze_request_end_;

  // ObRpcReqCtx lookup
  ObHRTime ctx_lookup_begin_;
  ObHRTime ctx_lookup_end_;

  // QueryAsyncInfo lookup
  ObHRTime query_async_lookup_begin_;
  ObHRTime query_async_lookup_end_;

  // IndexEntry lookup
  ObHRTime index_entry_lookup_begin_;
  ObHRTime index_entry_lookup_end_;

  // TableGroup lookup
  ObHRTime table_group_lookup_begin_;
  ObHRTime table_group_lookup_end_;

  // partition location lookup
  ObHRTime pl_lookup_begin_;
  ObHRTime pl_lookup_end_;

  // pl process
  ObHRTime pl_process_begin_;
  ObHRTime pl_process_end_;

  // congestion_control_lookup
  ObHRTime congestion_control_begin_;
  ObHRTime congestion_control_end_;

  // congestion process
  ObHRTime congestion_process_begin_;
  ObHRTime congestion_process_end_;

  // init cluster resource
  ObHRTime cluster_resource_create_begin_;
  ObHRTime cluster_resource_create_end_;
};

struct ObRpcReqCmdTimeStat
{
  ObRpcReqCmdTimeStat()
  {
    reset();
  }
  ~ObRpcReqCmdTimeStat () { }

  void reset()
  {
    MEMSET(this, 0, sizeof(ObRpcReqCmdTimeStat));
  }

  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObHRTime client_request_read_time_;

  ObHRTime client_request_analyze_time_;
  ObHRTime cluster_resource_create_time_;
  ObHRTime pl_lookup_time_;
  ObHRTime pl_process_time_;
  ObHRTime query_async_lookup_time_;
  ObHRTime index_entry_lookup_time_;
  ObHRTime tablegroup_entry_lookup_time_;
  ObHRTime rpc_ctx_lookup_time_;

  ObHRTime congestion_control_time_;
  ObHRTime congestion_process_time_;

  ObHRTime build_server_request_time_;
  ObHRTime prepare_send_request_to_server_time_;
  ObHRTime server_request_write_time_;

  ObHRTime server_process_request_time_;
  ObHRTime server_response_read_time_;
  ObHRTime server_response_analyze_time_;

  ObHRTime client_response_write_time_;

  ObHRTime request_total_time_;
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OBPROXY_MYSQL_SM_TIME_STAT_H
