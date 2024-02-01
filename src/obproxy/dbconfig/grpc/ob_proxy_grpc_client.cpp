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

#include <grpcpp/grpcpp.h>
#include "dbconfig/grpc/ob_proxy_grpc_client.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "obutils/ob_proxy_config.h"
#include "common/ob_version.h"

using namespace grpc;
using namespace envoy::service::discovery::v2;
using namespace envoy::api::v2;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obmysql;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

int ObGrpcClient::alloc(ObGrpcClient *&grpc_client)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(grpc_client = op_alloc(ObGrpcClient))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for grpc client", K(ret));
  } else if (OB_FAIL(grpc_client->init_stream())) {
    LOG_WDIAG("fail to init stream", K(ret));
  }
  return ret;
}

int ObGrpcClient::check_and_reconnect()
{
  int ret = OB_SUCCESS;
  grpc_connectivity_state state = channel_->GetState(true);
  LOG_DEBUG("grpc channel state", "state", get_channel_state(state));
  if (GRPC_CHANNEL_READY != state) {
    LOG_INFO("grpc channel is not avail, will reconnect", "state", get_channel_state(state));
    finish_rpc();
    if (OB_FAIL(init_stream())) {
      LOG_WDIAG("fail to init stream", K(ret));
    }
  }
  return ret;
}

int ObGrpcClient::init_stream()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(ret));
  } else {
    ObProxyConfig &proxy_config = get_global_proxy_config();
    {
      obsys::CRLockGuard guard(proxy_config.rwlock_);
      int64_t str_len = static_cast<int64_t>(strlen(proxy_config.dataplane_host));
      if (OB_UNLIKELY(str_len) <= 0 || OB_UNLIKELY(str_len >= OB_MAX_HOST_NAME_LENGTH)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid dataplane host", K(ret));
      } else {
        memcpy(dataplane_host_, proxy_config.dataplane_host, str_len);
        str_len = static_cast<int64_t>(strlen(proxy_config.sidecar_node_id));
        if (OB_UNLIKELY(str_len) <= 0 || OB_UNLIKELY(str_len >= (OBPROXY_MAX_NODE_ID_LENGTH - OB_MAX_TIMESTAMP_LENGTH))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WDIAG("invalid node_id", K(ret));
        } else {
          int64_t pos = 0;
          memcpy(node_id_, proxy_config.sidecar_node_id, str_len);
          pos += str_len;
          node_id_[pos++] = '~';
          ObString startup_time_str = get_global_db_config_processor().get_startup_time_str();
          memcpy(node_id_ + pos, startup_time_str.ptr(), startup_time_str.length());
          pos += startup_time_str.length();
          node_id_[pos] = '\0';
          LOG_INFO("succ to set node id", "node_ip", node_id_);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr != context_) {
        delete context_;
        context_ = NULL;
      }
      if (nullptr == (context_ = new (std::nothrow) ClientContext())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to alloc memory for client context", K(ret));
      } else {
        int64_t grpc_timeout_ms = obutils::get_global_proxy_config().grpc_timeout / 1000; // default 30 min
        std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(grpc_timeout_ms);
        LOG_DEBUG("will set timeout for grpc client", K(grpc_timeout_ms));
        context_->set_deadline(deadline);
        context_->set_wait_for_ready(false); // fail fast
        LOG_DEBUG("finish set context");
        grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_DNS_MIN_TIME_BETWEEN_RESOLUTIONS_MS, 10);
        channel_ = grpc::CreateCustomChannel(dataplane_host_, grpc::InsecureChannelCredentials(), args);
        stub_ = envoy::service::discovery::v2::AggregatedDiscoveryService::NewStub(channel_);
        stream_ = stub_->StreamAggregatedResources(context_);
        grpc_connectivity_state state = channel_->GetState(false);
        LOG_INFO("FINISH init grpc stream", "state", get_channel_state(state));
        is_inited_ = true;
      }
    }
  }
  return ret;
}

bool ObGrpcClient::sync_write(DiscoveryRequest &request)
{
  bool status = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_and_reconnect())) {
    LOG_WDIAG("fail to check grpc state", K(ret));
  } else if (OB_FAIL(fill_request_node(request))) {
    LOG_WDIAG("fail to fill request node info", K(ret));
  } else {
    status = stream_->Write(request);
  }
  if (!status) {
    LOG_INFO("the stream has been closed, will rebuild the grpc client stream");
    finish_rpc();
    if (OB_FAIL(init_stream())) {
      LOG_WDIAG("fail to init stream", K(ret));
    }
  }
  return status;
}

int ObGrpcClient::fill_request_node(DiscoveryRequest &request)
{
  int ret = OB_SUCCESS;

  core::Node *node = request.mutable_node();
  if (OB_ISNULL(node) || OB_ISNULL(node_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("can not get node from DiscoveryRequest or node id is null", K(ret));
  } else {
    node->set_id(node_id_);
    LOG_INFO("success to set node id to request", "node_id_", node_id_);

    const char *is_sidecar = getenv("IS_SIDECAR");
    if (OB_NOT_NULL(is_sidecar) && OB_UNLIKELY(strncmp(is_sidecar, "false", strlen(is_sidecar)) == 0)) {// app is not in sidecar
      char ipstr[MAX_IP_ADDR_LENGTH];
      MEMSET(ipstr, 0, MAX_IP_ADDR_LENGTH);

      ObSEArray<ObStringKV, LABELS_NUMS> labels;

      char json[MAX_REQUEST_LABELS_LENGTH];
      MEMSET(json, 0, sizeof(json));

      if (OB_FAIL(getip_from_nodeid(ipstr, MAX_IP_ADDR_LENGTH))) {
        LOG_WDIAG("can not get ip from node id", K(ret));
      } else if (OB_UNLIKELY(strlen(ipstr) == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ipstr can not be empty", K(ret));
      } else if (OB_FAIL(fill_labels_values(ipstr, labels))) {
        LOG_WDIAG("can not fill labels values", K(ret));
      } else if (OB_FAIL(to_json(labels, json, MAX_REQUEST_LABELS_LENGTH))) {
        LOG_WDIAG("fail to make json for pilot grpc request", K(ret));
      } else if (OB_UNLIKELY(strlen(json) == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("json can not be empty", K(ret));
      } else {
        ::google::protobuf::Struct *meta = node->mutable_metadata();
        ::google::protobuf::Map<::std::string, ::google::protobuf::Value> *fields = meta->mutable_fields();
        ::google::protobuf::Value v;
        v.set_string_value(json);
        (*fields)["LABELS"] = v;

        LOG_INFO("success to set pilot request LABELS value", "value", json);
      }
    } else {
      // sidecar do node need LABELS in DiscoveryRequest Node
    }
  }

  return ret;
}

int ObGrpcClient::getip_from_nodeid(char *ipstr, int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node_id_) || OB_ISNULL(ipstr) || OB_UNLIKELY(length == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invaild node id or ip string", KP(node_id_), KP(ipstr), K(length), K(ret));
  } else {
    ObString ip = ObString::make_string(node_id_);
    ip = ip.after('~');
    if (OB_UNLIKELY(ip.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("can not find first ~ in node id", "node_id", node_id_, K(ret));
    }

    const char *trailing;
    if (OB_SUCC(ret)) {
      trailing = ip.find('~');
      if (OB_ISNULL(trailing)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("can not find second ~ in node id", "node_id", node_id_, K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ip.clip(trailing);
      if (OB_UNLIKELY(ip.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("can not find ip in node id", "node_id", node_id_, K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(length < ip.length())) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WDIAG("IP string in node id is longer than MAX_IP_ADDR_LENGTH", K(length), K(ip.length()), K(ret));
      } else {
        memcpy(ipstr, ip.ptr(), ip.length());
        ipstr[ip.length()] = '\0';
      }
    }
  }

  return ret;
}

int ObGrpcClient::fill_labels_values(const char *ipstr, ObIArray<obmysql::ObStringKV>& labels)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ipstr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invaild arg in ObGrpcClient::fill_labels_values func", KP(ipstr), K(ret));
  // need by alipay main site
  } else if (OB_FAIL(build_label("odp-version", build_version(), labels))) {
    LOG_WDIAG("can not set odp-version to labels", K(ret));
  } else if (OB_FAIL(build_label("cloudmesh.workload/ip", ipstr, labels))) {
    LOG_WDIAG("can not set ip to labels", K(ret));
  } else if (OB_FAIL(build_label("sigma.ali/app-name", get_global_proxy_config().app_name.str(), labels))) {
    LOG_WDIAG("can not set app-name to labels", K(ret));
  } else if (OB_FAIL(build_label("cloudmesh.workload.type", "odp", labels))) {
    LOG_WDIAG("can not set type to labels", K(ret));
  } else if (OB_FAIL(build_label("cloudmesh.cluster/env", get_global_proxy_config().workspace_name.str(), labels))) {
    LOG_WDIAG("can not set env to labels", K(ret));
  } else if (OB_FAIL(build_label("VMMODE", "true", labels))) {
    LOG_WDIAG("can not set VMMODE to labels", K(ret));
  // need by E-Commerce Bank
  } else if (OB_FAIL(build_label("app.kubernetes.io/name", get_global_proxy_config().app_name.str(), labels))) {
    LOG_WDIAG("can not set app.kubernetes.io/name to labels", K(ret));
  } else if (OB_FAIL(build_label("cafe.sofastack.io/tenant", get_global_proxy_config().env_tenant_name.str(), labels))) {
    LOG_WDIAG("can not set cafe.sofastack.io/tenant to labels", K(ret));
  } else if (OB_FAIL(build_label("cafe.sofastack.io/workspace", get_global_proxy_config().workspace_name.str(), labels))) {
    LOG_WDIAG("can not set cafe.sofastack.io/workspace to labels", K(ret));
  } else if (OB_FAIL(build_label("scheduling.sigma.ali/pod-group-namespace", get_global_proxy_config().pod_namespace.str(), labels))) {
    LOG_WDIAG("can not set scheduling.sigma.ali/pod-group-namespace to labels", K(ret));
  }

  return ret;
}

int ObGrpcClient::build_label(const ObString& key, const ObString& value, ObIArray<obmysql::ObStringKV>& labels)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(key.empty()) || OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invaild arg in ObGrpcClient::build_label func", K(key), K(value), K(ret));
  } else {
    ObStringKV kv;
    kv.key_ = key;
    kv.value_ = value;

    if (OB_FAIL(labels.push_back(kv))) {
      LOG_WDIAG("can not set kv to labels", K(ret));
    }
  }

  return ret;
}

int ObGrpcClient::to_json(ObIArray<obmysql::ObStringKV>& labels, char *json, int length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(labels.empty()) || OB_ISNULL(json) || OB_UNLIKELY(length <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invaild arg in ObGrpcClient::to_json func", KP(json), K(length), K(ret));
  } else {
    int64_t pos = 0;
    json[pos] = '{';
    pos++;

    int64_t i = 0;
    for (i = 0; OB_SUCC(ret) && (i < labels.count()); i++) {
      ObStringKV& kv = labels.at(i);

      if (OB_FAIL(databuff_printf(json, length, pos, "\"%.*s\":\"%.*s\",", kv.key_.length(), kv.key_.ptr(), kv.value_.length(), kv.value_.ptr()))) {
        LOG_WDIAG("json string length is not enough", K(length), K(pos), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      json[pos - 1] = '}';
      json[pos] = '\0';
    }
  }

  return ret;
}

bool ObGrpcClient::sync_read(DiscoveryResponse &response)
{
  bool status = false;
  int ret = OB_SUCCESS;
  status = stream_->Read(&response);
  if (!status) {
    LOG_INFO("the stream has been closed, will rebuild the grpc client stream");
    finish_rpc();
    if (OB_FAIL(init_stream())) {
      LOG_WDIAG("fail to init stream", K(ret));
    }
  }
  return status;
}

void ObGrpcClient::finish_rpc()
{
  Status status = stream_->Finish();
  if (!status.ok()) {
    LOG_WDIAG("rpc failed", "error code:", status.error_code(),
             ", error message:", status.error_message().c_str());
  } else {
    LOG_DEBUG("finish stream");
  }
  is_inited_ = false;
}

const char *ObGrpcClient::get_channel_state(grpc_connectivity_state state)
{
  const char *str_ret = "";
  switch (state) {
    case GRPC_CHANNEL_IDLE:
      str_ret = "GRPC_CHANNEL_IDLE";
      break;
    case GRPC_CHANNEL_CONNECTING:
      str_ret = "GRPC_CHANNEL_CONNECTING";
      break;
    case GRPC_CHANNEL_READY :
      str_ret = "GRPC_CHANNEL_READY";
      break;
    case GRPC_CHANNEL_TRANSIENT_FAILURE:
      str_ret = "GRPC_CHANNEL_TRANSIENT_FAILURE";
      break;
    case GRPC_CHANNEL_SHUTDOWN:
      str_ret = "GRPC_CHANNEL_SHUTDOWN";
      break;
    default:
      break;
  }
  return str_ret;
}

} // end namespace dbconfig
} // end namespace obproxy
} // end namespace oceanbase
