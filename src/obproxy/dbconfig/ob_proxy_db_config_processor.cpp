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
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/ob_proxy_inotify_processor.h"
#include "utils/ob_proxy_utils.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"
#include "dbconfig/ob_dbconfig_tenant_cont.h"
#include "dbconfig/ob_proxy_db_config_task.h"
#include "iocore/eventsystem/ob_shard_watch_task.h"

using namespace grpc;
using namespace google::protobuf;
using namespace envoy::service::discovery::v2;
using namespace envoy::api::v2;
using namespace envoy::api::v2::core;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

ObDbConfigProcessor &get_global_db_config_processor()
{
  static ObDbConfigProcessor db_config_processor;
  return db_config_processor;
}

//------ ObDbConfigProcessor------
ObDbConfigProcessor::ObDbConfigProcessor()
  : is_inited_(false), is_config_inited_(false), is_client_avail_(false), is_bt_updated_(false), startup_time_str_(), gc_pool_()
{
  startup_time_buf_[0] = '\0';
}

int ObDbConfigProcessor::init(const int64_t client_count, int64_t startup_time_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(set_startup_time(startup_time_us))) {
    LOG_WARN("fail to set startup time", K(startup_time_us), K(ret));
  } else if (!get_global_proxy_config().use_local_dbconfig && OB_FAIL(gc_pool_.init(client_count, is_client_avail_))) {
    LOG_WARN("fail to init grpc client pool", K(client_count), K_(is_client_avail), K(ret));
  } else if (get_global_proxy_config().use_local_dbconfig && OB_FAIL(get_global_inotify_processor().init())) {
    LOG_WARN("fail to init inotify processor", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObDbConfigProcessor::set_startup_time(int64_t startup_time_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(convert_timestamp_to_version(startup_time_us, startup_time_buf_, OB_MAX_TIMESTAMP_LENGTH))) {
    LOG_WARN("fail to format startup time to str", K(startup_time_us), K(ret));
  } else {
    startup_time_str_.assign_ptr(startup_time_buf_, static_cast<int32_t>(strlen(startup_time_buf_)));
  }
  return ret;
}

int ObDbConfigProcessor::start()
{
  int ret = OB_SUCCESS;
  if (!get_global_proxy_config().use_local_dbconfig) {
    if (!is_client_avail_) {
      LOG_INFO("grpc client pool is not avail, will load local sharding config");
      if (OB_FAIL(get_global_dbconfig_cache().load_local_dbconfig())) {
        LOG_WARN("fail to load local dbconfig, we can fetch from dataplane later", K(ret));
      } else {
        is_config_inited_ = true;
        LOG_INFO("succ to load local dbconfig");
      }
    } else if (OB_FAIL(init_sharding_config())) {
      LOG_ERROR("fail to init sharding config", K(ret));
    }

    if (OB_SUCC(ret) && get_global_proxy_config().is_control_plane_used()) {
      if (OB_FAIL(start_watch_parent_crd())) {
        LOG_WARN("fail to start watch parent crd", K(ret));
      }
    }
  } else {
    if (OB_FAIL(get_global_dbconfig_cache().load_local_dbconfig())) {
      LOG_WARN("fail to load local dbconfig when use_local_dbconfig", K(ret));
    } else {
      is_config_inited_ = true;
      LOG_INFO("succ to load local dbconfig");
      if (OB_FAIL(get_global_inotify_processor().start_watch_sharding_config())) {
        LOG_WARN("fail to start inotify watch sharding config", K(ret));
      }
    }
  }

  return ret;
}

int ObDbConfigProcessor::start_watch_parent_crd()
{
  int ret = OB_SUCCESS;
  ObWatchParentCont *cont = NULL;
  if (OB_FAIL(ObWatchParentCont::alloc_watch_parent_cont(cont, TYPE_DATABASE))) {
    LOG_WARN("fail to alloc fetch task for watch parent crd", K(ret));
  } else if (OB_ISNULL(cont)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cont is null", K(ret));
  } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_SHARD_WATCH))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to schedule fetch crd cont for watch parent", K(ret));
  } else {
    LOG_INFO("succ to schedule fetch crt task for watch parent");
  }
  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
    }
  }
  return ret;
}

int ObDbConfigProcessor::init_sharding_config()
{
  int ret = OB_SUCCESS;
  ObGrpcClient *grpc_client = NULL;
  if (OB_ISNULL(grpc_client = get_grpc_client_pool().acquire_grpc_client())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to acquire grpc client", K(ret));
  } else {
    DiscoveryRequest request;
    DiscoveryResponse response;
    request.set_version_info("");
    request.set_type_url(get_type_url(TYPE_DATABASE));
    if (grpc_client->sync_write(request)
        && grpc_client->sync_read(response)) {
      const std::string& type_url = response.type_url();
      LOG_INFO("Received gRPC message", "type_url", type_url.c_str(),
                "version", response.version_info().c_str());
      const google::rpc::Status &status = response.error_detail();
      if (OB_UNLIKELY(0 != status.code())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rpc failed", "error code:", status.code(), ", error message:", status.message().c_str(), K(ret));
      } else if (OB_UNLIKELY(response.resources_size() == 0)) {
        LOG_INFO("empty resources, maybe no logic databases", K(ret));
      } else if (OB_UNLIKELY(type_url.compare(get_type_url(TYPE_DATABASE)) != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to watch database cr, invalid response url", "url", type_url.c_str(), K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to watch database cr, maybe connection has been closed", K(ret));
    }

    get_grpc_client_pool().release_grpc_client(grpc_client);
    grpc_client = NULL;
    if (OB_SUCC(ret) && !is_config_inited_ && response.resources_size() > 0) {
      bool start_mode = true;
      if (OB_FAIL(sync_fetch_tenant_config(response, start_mode))) {
        LOG_WARN("fail to fetch tenant config, will try to load local config", K(ret));
      } else {
        is_config_inited_ = true;
      }
    }
  }
  if (!is_config_inited_) {
    LOG_INFO("will try to load local dbconfig");
    if (OB_FAIL(get_global_dbconfig_cache().load_local_dbconfig())) {
      LOG_WARN("fail to load local dbconfig", K(ret));
    } else {
      is_config_inited_ = true;
      LOG_INFO("succ to load local dbconfig");
    }
  }
  LOG_INFO("finish init sharding config", K(ret));

  return ret;
}

int ObDbConfigProcessor::sync_fetch_tenant_config(const google::protobuf::Message &message, bool start_mode/*false*/)
{
  int ret = OB_SUCCESS;
  ObDbConfigTenantCont *cont = NULL;
  ObDbConfigTenantContWrapper wrapper;
  reset_bt_update_flag();
  if (OB_FAIL(ObDbConfigFetchCont::alloc_fetch_cont(reinterpret_cast<ObDbConfigFetchCont *&>(cont), NULL, 0, TYPE_TENANT))) {
    LOG_WARN("fail to alloc fetch tenant config cont", K(ret));
  } else if (OB_FAIL(wrapper.init())) {
    LOG_WARN("fail to init wrapper", K(ret));
  } else if (OB_FAIL(wrapper.run_cond_.lock())) {
    LOG_WARN("fail to lock", K(ret));
  } else if (OB_FAIL(cont->do_fetch_tenant_config(wrapper, message, start_mode))) {
    LOG_WARN("fail to do fetch tenant config", K(ret));
  }
  if (OB_FAIL(ret)) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
    }
  } else {
    LOG_INFO("wait for fetching tenant config");
    while (!wrapper.is_fetch_complete_) {
      if (OB_FAIL(wrapper.run_cond_.wait())) {
        LOG_WARN("fail to wait", K(ret));
      }
    }
  }
  if (!wrapper.is_fetch_succ_) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("fail to fetch tenant config", K(ret));
  } else {
    LOG_INFO("succ to fetch tenant config");
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = wrapper.run_cond_.unlock()))) {
    LOG_WARN("fail to unlock", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }

  return ret;
}

int ObDbConfigProcessor::handle_bt_sdk()
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_ERROR("not support beyond trust password", K(ret));
  return ret;
}

} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
