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

#ifndef OBPROXY_DB_CONFIG_PROCESSOR_H
#define OBPROXY_DB_CONFIG_PROCESSOR_H

#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/grpc/ob_proxy_grpc_client_pool.h"

namespace google
{
namespace protobuf
{
class Message;
} // end namespace protobuf
} // end namespace google

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

class ObDbConfigProcessor
{
public:
  ObDbConfigProcessor();
  ~ObDbConfigProcessor() {}

  int init(const int64_t client_count, int64_t startup_time_us);
  int start();
  int init_sharding_config();
  int start_watch_parent_crd();
  bool is_config_inited() const { return is_config_inited_; }

  ObGrpcClientPool &get_grpc_client_pool() { return gc_pool_; }
  common::ObString get_startup_time_str() { return startup_time_str_; }
  int set_startup_time(int64_t startup_time_us);
  int sync_fetch_tenant_config(const google::protobuf::Message &message, bool start_mode=false);
  int handle_bt_sdk();
  void reset_bt_update_flag() { is_bt_updated_ = false; }

private:
  bool is_inited_;
  bool is_config_inited_;
  bool is_client_avail_;
  bool is_bt_updated_;
  common::ObString startup_time_str_;
  char startup_time_buf_[common::OB_MAX_TIMESTAMP_LENGTH];
  ObGrpcClientPool gc_pool_;
};

ObDbConfigProcessor &get_global_db_config_processor();

} // end dbconfig
} // end obproxy
} // end oceanbase
#endif /* OBPROXY_DB_CONFIG_PROCESSOR_H */
