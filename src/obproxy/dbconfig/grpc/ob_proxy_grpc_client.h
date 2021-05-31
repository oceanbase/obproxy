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

#ifndef OBPROXY_GRPC_CLIENT_H
#define OBPROXY_GRPC_CLIENT_H

#include <grpcpp/channel.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/list/ob_intrusive_list.h"
#include "obproxy/dbconfig/protobuf/envoy/service/discovery/v2/ads.grpc.pb.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{
class ObGrpcClient
{
public:
  ObGrpcClient() : is_inited_(false), channel_(), stub_(), context_(nullptr)
  {
    memset(dataplane_host_, 0, sizeof(dataplane_host_));
    memset(node_id_, 0, sizeof(node_id_));
  }
  ~ObGrpcClient() {}

  static int alloc(ObGrpcClient *&grpc_client);

  bool sync_write(envoy::api::v2::DiscoveryRequest &request);
  bool sync_read(envoy::api::v2::DiscoveryResponse &response);
  void finish_rpc();

  int init_stream();
  int check_and_reconnect();
  bool is_avail() const
  {
    return NULL != channel_ && GRPC_CHANNEL_READY == channel_->GetState(false);
  }

private:
  int fill_request_node(envoy::api::v2::DiscoveryRequest &request);
  int to_json(common::ObIArray<obmysql::ObStringKV>& labels, char *json, int length);
  int getip_from_nodeid(char *ipstr, int64_t length);
  int fill_labels_values(const char *ipstr, common::ObIArray<obmysql::ObStringKV>& labels);
  int build_label(const common::ObString& key, const common::ObString& value, common::ObIArray<obmysql::ObStringKV>& labels);
public:
  static const char *get_channel_state(grpc_connectivity_state state);

public:
  static const int64_t OBPROXY_MAX_HOST_NAME_LENGTH = 128;
  static const int64_t OBPROXY_MAX_NODE_ID_LENGTH = 512;
  LINK(ObGrpcClient, link_);

private:
  bool is_inited_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<envoy::service::discovery::v2::AggregatedDiscoveryService::Stub> stub_;
  grpc::ClientContext *context_;
  std::shared_ptr<grpc::ClientReaderWriter<envoy::api::v2::DiscoveryRequest, envoy::api::v2::DiscoveryResponse>> stream_;
  char dataplane_host_[OBPROXY_MAX_HOST_NAME_LENGTH];
  char node_id_[OBPROXY_MAX_NODE_ID_LENGTH];
  static const int64_t MAX_REQUEST_LABELS_LENGTH = 1024;
  static const int64_t LABELS_NUMS = 10;
};

} // end dbcobfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_GRPC_CLIENT_H */
