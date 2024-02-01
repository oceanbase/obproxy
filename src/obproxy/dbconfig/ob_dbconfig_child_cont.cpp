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
#include "dbconfig/ob_dbconfig_child_cont.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "utils/ob_proxy_blowfish.h"
#include "utils/ob_proxy_utils.h"
#include "obutils/ob_proxy_config_utils.h"
#include "dbconfig/protobuf/dds-api/database.pb.h"
#include "dbconfig/protobuf/dds-api/databaseAuthorities.pb.h"
#include "dbconfig/protobuf/dds-api/databaseVariables.pb.h"
#include "dbconfig/protobuf/dds-api/databaseProperties.pb.h"
#include "dbconfig/protobuf/dds-api/shardsTopology.pb.h"
#include "dbconfig/protobuf/dds-api/shardsDistribute.pb.h"
#include "dbconfig/protobuf/dds-api/shardsRouter.pb.h"
#include "dbconfig/protobuf/dds-api/shardsConnector.pb.h"
#include "dbconfig/protobuf/dds-api/shardsProperties.pb.h"
#include <google/protobuf/any.pb.h>
#include "lib/json/ob_json.h"

using namespace grpc;
using namespace com::alipay::dds::api::v1;
using namespace google::protobuf;
using namespace envoy::service::discovery::v2;
using namespace envoy::api::v2;
using namespace envoy::api::v2::core;
using namespace oceanbase::common;
using namespace oceanbase::json;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace dbconfig
{

//-------ObDbConfigChildCont------
ObDbConfigChildCont::ObDbConfigChildCont(ObProxyMutex *m, ObContinuation *cb_cont, ObEThread *cb_thread, const ObDDSCrdType type)
  : ObDbConfigFetchCont(m, cb_cont, cb_thread, type),
    fetch_faliure_count_(0), db_info_(NULL), resource_array_()
{
}

void ObDbConfigChildCont::destroy()
{
  fetch_faliure_count_ = 0;
  resource_array_.clear();
  set_db_info(NULL);
  ObDbConfigFetchCont::destroy();
}

int ObDbConfigChildCont::init_task()
{
  int ret = OB_SUCCESS;
  bool need_reschedule = false;
  bool fetch_succ = false;
  ObGrpcClient *grpc_client = get_global_db_config_processor().get_grpc_client_pool().acquire_grpc_client();
  if (NULL != grpc_client) {
    LOG_DEBUG("begin to do dbconfig fetch task", "task_name", get_type_task_name(type_), K_(db_info_key));
    // send fetch crd request
    DiscoveryRequest request;
    DiscoveryResponse response;
    request.set_version_info("");
    request.set_type_url(get_type_url(type_));
    for (int64_t i = 0; i < resource_array_.size(); ++i) {
      const std::string &str = resource_array_[i];
      request.add_resource_names(str);
    }
    if (grpc_client->sync_write(request)
        && grpc_client->sync_read(response)) {
      const std::string& type_url = response.type_url();
      LOG_DEBUG("Received gRPC message", "type_url", type_url.c_str(),
                "version", response.version_info().c_str(),
                "target url", get_type_url(type_),
                "resource size", response.resources_size());
      const google::rpc::Status &status = response.error_detail();
      if (0 != status.code()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("rpc failed",  K(need_reschedule),
                 ", error code:", status.code(), ", error message:", status.message().c_str(),
                 "type", get_type_task_name(type_), K(ret));
      } else if (type_url.compare(get_type_url(type_)) == 0) {
        LOG_DEBUG("begin to parse resources", "type", get_type_task_name(type_),
                  "resources size", response.resources_size(),
                  K_(db_info_key));
        for (int i = 0; OB_SUCC(ret) && i < response.resources_size(); ++i) {
          const Any& res = response.resources(i);
          if (OB_FAIL(parse_child_resource(res))) {
            LOG_WDIAG("fail to parse child resource", "resource type", get_type_task_name(type_), K(ret));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("type url and target url is mismatched", "type_url", type_url.c_str(), "target url", get_type_url(type_), K(ret));
      }
      if (OB_SUCC(ret)) {
        fetch_succ = true;
      } else {
        ++fetch_faliure_count_;
      }
    } else {
      // read or write failed means grpc connection meet some exception
      ++fetch_faliure_count_;
      need_reschedule = true;
    }
    LOG_DEBUG("finish fetch crd task", "type", get_type_task_name(type_),
              K(need_reschedule), K(fetch_succ), K_(fetch_faliure_count));
  }
  if (NULL != grpc_client) {
    get_global_db_config_processor().get_grpc_client_pool().release_grpc_client(grpc_client);
    grpc_client = NULL;
  }
  if (!fetch_succ && fetch_faliure_count_ >= MAX_FETCH_RETRY_TIMES) {
    need_reschedule = false;
    LOG_WDIAG("fail to fetch child config more than 3 times, no need to fetch",
             "type", get_type_task_name(type_), K(need_reschedule), K(fetch_succ), K_(fetch_faliure_count));
  }
  if (need_reschedule) {
    // reschedule
    if (OB_ISNULL(self_ethread().schedule_in(this, HRTIME_MSECONDS(RETRY_INTERVAL_MS), EVENT_IMMEDIATE))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to reschedule fetch child cr task", "type", get_type_task_name(type_), K(ret));
    } else {
      ret = OB_SUCCESS;
      LOG_DEBUG("succ to reschedule fetch child cr task", "type", get_type_task_name(type_), K(ret));
    }
  } else {
    result_.fetch_result_ = fetch_succ;
    if (NULL != cb_cont_) {
      need_callback_ = true;
    } else {
      terminate_ = true;
    }
  }
  return ret;
}

int ObDbConfigChildCont::parse_child_resource(const Any &resource)
{
  int ret = OB_SUCCESS;
  switch (type_) {
    case TYPE_DATABASE_AUTH:
      ret = parse_database_auth(resource);
      break;
    case TYPE_DATABASE_VAR:
      ret = parse_database_var(resource);
      break;
    case TYPE_DATABASE_PROP:
      ret = parse_database_prop(resource);
      break;
    case TYPE_SHARDS_TPO:
      ret = parse_shard_tpo(resource);
      break;
    case TYPE_SHARDS_ROUTER:
      ret = parse_shard_router(resource);
      break;
    case TYPE_SHARDS_DIST:
      ret = parse_shard_dist(resource);
      break;
    case TYPE_SHARDS_CONNECTOR:
      ret = parse_shard_connector(resource);
      break;
    case TYPE_SHARDS_PROP:
      ret = parse_shard_prop(resource);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("invalid type", K(type_));
      break;
  }
  return ret;
}

int ObDbConfigChildCont::parse_database_auth(const Any &res)
{
  int ret = OB_SUCCESS;
  ObDataBaseAuth *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<DatabaseAuthorities>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not DatabaseAuthorities", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObDataBaseAuth))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObDataBaseAuth", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    DatabaseAuthorities message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    if (OB_FAIL(child_info->init(name, db_info_key_))) {
      LOG_WDIAG("fail to init ObDataBaseAuth", "name", name.c_str(), K(ret));
    } else {
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
      }
      ObShardUserPrivInfo up_info;
      for (int i = 0; OB_SUCC(ret) && i < message.users_size(); ++i) {
        up_info.reset();
        const DatabaseUser &db_user = message.users(i);
        up_info.set_username(db_user.user().c_str(), db_user.user().length());
        up_info.set_host(db_user.host().c_str(), db_user.host().length());
        up_info.set_user_priv(db_user.alter_priv().c_str(), db_user.alter_priv().length(), OB_PRIV_ALTER_SHIFT);
        up_info.set_user_priv(db_user.create_priv().c_str(), db_user.create_priv().length(), OB_PRIV_CREATE_SHIFT);
        up_info.set_user_priv(db_user.delete_priv().c_str(), db_user.delete_priv().length(), OB_PRIV_DELETE_SHIFT);
        up_info.set_user_priv(db_user.drop_priv().c_str(), db_user.drop_priv().length(), OB_PRIV_DROP_SHIFT);
        up_info.set_user_priv(db_user.insert_priv().c_str(), db_user.insert_priv().length(), OB_PRIV_INSERT_SHIFT);
        up_info.set_user_priv(db_user.update_priv().c_str(), db_user.update_priv().length(), OB_PRIV_UPDATE_SHIFT);
        up_info.set_user_priv(db_user.select_priv().c_str(), db_user.select_priv().length(), OB_PRIV_SELECT_SHIFT);
        up_info.set_user_priv(db_user.index_priv().c_str(), db_user.index_priv().length(), OB_PRIV_INDEX_SHIFT);
        if (OB_FAIL(up_info.set_password(db_user.password().c_str(), db_user.password().length()))) {
          LOG_WDIAG("fail to set auth password", "user", db_user.user().c_str(),K(ret));
        } else if (OB_FAIL(child_info->up_array_.push_back(up_info))) {
          LOG_WDIAG("fail to put ObShardUserPrivInfo", K(up_info), K(ret));
        }
      } // end for users
      if (OB_SUCC(ret)) {
        // no need acquire write lock. Concurrent Task use different array and map. one array only use by one Task
        ObDataBaseAuth *old_child_info = db_info_->da_array_.ccr_map_.remove(child_info->name_.config_string_);
        if (NULL != old_child_info) {
          if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                     KPC(old_child_info), KPC(child_info), K(ret));
          }
          old_child_info->set_deleting_state();
          old_child_info->dec_ref();
          old_child_info = NULL;
        }
        if (OB_SUCC(ret)) {
          child_info->set_avail_state();
          child_info->set_need_dump_config(true);
          child_info->inc_ref();
          db_info_->da_array_.ccr_map_.unique_set(child_info);
          LOG_DEBUG("succ to put ObDataBaseAuth", KPC(child_info));
        }
      }
    }
  }
  if (NULL != child_info) {
    // already inc_ref after alloc child info
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_database_var(const Any &res)
{
  int ret = OB_SUCCESS;
  ObDataBaseVar *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<DatabaseVariables>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not DatabaseVariables", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObDataBaseVar))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObDataBaseVar", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    DatabaseVariables message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    if (OB_FAIL(child_info->init(name, db_info_key_))) {
      LOG_WDIAG("fail to init ObDataBaseVar", "name", name.c_str(), K(ret));
    } else {
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
      }
      ObDataBaseVar *old_child_info = db_info_->dv_array_.ccr_map_.remove(child_info->name_.config_string_);
      if (NULL != old_child_info) {
        if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                   KPC(old_child_info), KPC(child_info), K(ret));
        }
        old_child_info->set_deleting_state();
        old_child_info->dec_ref();
        old_child_info = NULL;
      }
      if (OB_SUCC(ret)) {
        child_info->set_avail_state();
        child_info->set_need_dump_config(true);
        child_info->inc_ref();
        db_info_->dv_array_.ccr_map_.unique_set(child_info);
        LOG_DEBUG("succ to put ObDataBaseVar", KPC(child_info));
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_database_prop(const Any &res)
{
  int ret = OB_SUCCESS;
  ObDataBaseProp *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<DatabaseProperties>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not DatabaseProperties", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObDataBaseProp))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObDataBaseProp", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    DatabaseProperties message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    if (OB_FAIL(child_info->init(name, db_info_key_))) {
      LOG_WDIAG("fail to init ObDataBaseProp", "name", name.c_str(), K(ret));
    } else {
      const std::string &prop_rule = message.properties_rule();
      const std::string &prop_name = message.properties_name();
      child_info->prop_name_.set_value(prop_name.length(), prop_name.c_str());
      child_info->prop_rule_.set_value(prop_rule.length(), prop_rule.c_str());
      const Map<std::string, std::string >& kv_map = message.variables();
      if (OB_FAIL(ObProxyPbUtils::parse_database_prop_rule(prop_rule, *child_info))) {
        LOG_WDIAG("fail to parse database prop rule", "prop name", prop_name.c_str(),
                 "prop rule", prop_rule.c_str(), K(ret));
      } else {
        for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
          child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
        }
        ObDataBaseProp *old_child_info = db_info_->dp_array_.ccr_map_.remove(child_info->name_.config_string_);
        if (NULL != old_child_info) {
          if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                     KPC(old_child_info), KPC(child_info), K(ret));
          }
          old_child_info->set_deleting_state();
          old_child_info->dec_ref();
          old_child_info = NULL;
        }
        if (OB_SUCC(ret)) {
          child_info->set_avail_state();
          child_info->set_need_dump_config(true);
          child_info->inc_ref();
          db_info_->dp_array_.ccr_map_.unique_set(child_info);
          LOG_DEBUG("succ to put ObDataBaseProp", KPC(child_info));
        }
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_shard_tpo(const Any &res)
{
  int ret = OB_SUCCESS;
  ObShardTpo *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<ShardsTopology>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not ShardsTopology", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObShardTpo))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObShardTpo", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    ShardsTopology message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    bool with_db_info_key = false;
    if (OB_FAIL(child_info->init(name, db_info_key_, with_db_info_key))) {
      LOG_WDIAG("fail to init ObShardTpo", "name", name.c_str(), K(ret));
    } else {
      const std::string &tpo_name = message.topology_name();
      const std::string &arch = message.architecture();
      const std::string &specification = message.specification();
      child_info->tpo_name_.set_value(tpo_name.length(), tpo_name.c_str());
      child_info->arch_.set_value(arch.length(), arch.c_str());
      child_info->specification_.set_value(specification.length(), specification.c_str());
      child_info->parse_tpo_specification(child_info->specification_.config_string_);
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
      }
      const Map<std::string, std::string >& tpo_map = message.specific_layer();
      ObGroupCluster *gc_info = NULL;
      for (auto it = tpo_map.cbegin(); OB_SUCC(ret) && it != tpo_map.cend(); ++it) {
        gc_info = NULL;
        if (OB_ISNULL(gc_info = op_alloc(ObGroupCluster))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc memory for ObGroupCluster", K(ret), K_(db_info_key));
        } else if (OB_FAIL(ObProxyPbUtils::parse_group_cluster(it->first, it->second, *gc_info))) {
          LOG_WDIAG("fail to parse gc info", K(ret));
        } else {
          gc_info->calc_total_read_weight();
          child_info->gc_map_.unique_set(gc_info);
        }
        if (OB_FAIL(ret) && NULL != gc_info) {
          op_free(gc_info);
          gc_info = NULL;
        }
      }
      if (OB_SUCC(ret)) {
        ObShardTpo *old_child_info = db_info_->st_array_.ccr_map_.remove(child_info->name_.config_string_);
        if (NULL != old_child_info) {
          if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                     KPC(old_child_info), KPC(child_info), K(ret));
          }
          old_child_info->set_deleting_state();
          old_child_info->dec_ref();
          old_child_info = NULL;
        }
        if (OB_SUCC(ret)) {
          child_info->set_avail_state();
          child_info->set_need_dump_config(true);
          child_info->inc_ref();
          db_info_->st_array_.ccr_map_.unique_set(child_info);
          LOG_DEBUG("succ to put ObShardTpo", KPC(child_info));
        }
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_shard_router(const Any &res)
{
  int ret = OB_SUCCESS;
  ObShardRouter *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<ShardsRouter>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not ShardsRouter", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObShardRouter))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObShardRouter", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    ShardsRouter message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    if (OB_FAIL(child_info->init(name, db_info_key_))) {
      LOG_WDIAG("fail to init ObShardRouter", "name", name.c_str(), K(ret));
    } else {
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
      }
      ObShardRule *rule_info = NULL;
      for (int i = 0; OB_SUCC(ret) && i < message.routers_size(); ++i) {
        rule_info = NULL;
        const MarkedRouter &marked_router = message.routers(i);
        const std::string &table_name = marked_router.mark();
        string_to_upper_case(const_cast<char *>(table_name.c_str()), static_cast<int32_t>(table_name.length()));
        const Router &router = marked_router.router();
        if (OB_ISNULL(rule_info = op_alloc(ObShardRule))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc memory for ObShardRule", K(ret), K_(db_info_key));
        } else if (OB_FAIL(rule_info->init(table_name))) {
          LOG_WDIAG("fail to init rule info", "table_name", table_name.c_str(), K(ret));
        } else {
          if (marked_router.sequence()) {
            child_info->set_sequence_table(table_name);
            rule_info->set_is_sequence();
            LOG_INFO("succ to set sequence table name", "sequence table", table_name.c_str());
          }
          const Map<std::string, std::string >& rule_map = router.rules();
          for (auto it = rule_map.cbegin(); OB_SUCC(ret) && it != rule_map.cend(); ++it) {
            if (OB_FAIL(ObProxyPbUtils::parse_shard_rule(it->first, it->second, *rule_info))) {
              LOG_WDIAG("fail to parse rule info", "rule_name", it->first.c_str(),
                       "rule_value", it->second.c_str());
            }
          } // end for rule map
          if (OB_SUCC(ret)) {
            LOG_DEBUG("succ to set db_rule_info", "table_name", table_name.c_str(),
                      KPC(rule_info));
            child_info->mr_map_.unique_set(rule_info);
          }
        }
        if (OB_FAIL(ret) && NULL != rule_info) {
          op_free(rule_info);
          rule_info = NULL;
        }
      } // end for routers
      if (OB_SUCC(ret)) {
        ObShardRouter *old_child_info = db_info_->sr_array_.ccr_map_.remove(child_info->name_.config_string_);
        if (NULL != old_child_info) {
          if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                     KPC(old_child_info), KPC(child_info), K(ret));
          }
          old_child_info->set_deleting_state();
          old_child_info->dec_ref();
          old_child_info = NULL;
        }
        if (OB_SUCC(ret)) {
          child_info->set_avail_state();
          child_info->set_need_dump_config(true);
          child_info->inc_ref();
          db_info_->sr_array_.ccr_map_.unique_set(child_info);
          LOG_DEBUG("succ to put ObShardRouter", KPC(child_info));
        }
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_shard_dist(const Any &res)
{
  int ret = OB_SUCCESS;
  ObShardDist *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<ShardsDistribute>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not ShardsDistribute", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObShardDist))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObShardDist", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    ShardsDistribute message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    bool with_db_info_key = false;
    if (OB_FAIL(child_info->init(name, db_info_key_, with_db_info_key))) {
      LOG_WDIAG("fail to init ObShardDist", "name", name.c_str(), K(ret));
    } else {
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
      }
      ObMarkedDist *dist_info = NULL;
      for (int i = 0; OB_SUCC(ret) && i < message.distributions_size(); ++i) {
        dist_info = NULL;
        const MarkedDistribution &marked_dist = message.distributions(i);
        if (OB_ISNULL(dist_info = op_alloc(ObMarkedDist))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc memory for ObMarkedDist", K(ret), K_(db_info_key));
        } else if (OB_FAIL(dist_info->init(marked_dist.mark(), marked_dist.distribution()))) {
          LOG_WDIAG("fail to init marked dist info", "dist name", name.c_str(), K(ret));
        } else {
          child_info->md_map_.unique_set(dist_info);
        }
        if (OB_FAIL(ret) && NULL != dist_info) {
          op_free(dist_info);
          dist_info = NULL;
        }
      }
      if (OB_SUCC(ret)) {
        ObShardDist *old_child_info = db_info_->sd_array_.ccr_map_.remove(child_info->name_.config_string_);
        if (NULL != old_child_info) {
          if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                     KPC(old_child_info), KPC(child_info), K(ret));
          }
          old_child_info->set_deleting_state();
          old_child_info->dec_ref();
          old_child_info = NULL;
        }
        if (OB_SUCC(ret)) {
          child_info->set_avail_state();
          child_info->set_need_dump_config(true);
          child_info->inc_ref();
          db_info_->sd_array_.ccr_map_.unique_set(child_info);
          LOG_DEBUG("succ to put ObShardDist", KPC(child_info));
        }
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_shard_connector(const Any &res)
{
  int ret = OB_SUCCESS;
  ObShardConnector *child_info = NULL;
  if (OB_UNLIKELY(!res.Is<ShardsConnector>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not ShardsConnector", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObShardConnector))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObShardConnector", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    ShardsConnector message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    const std::string &shard_name = message.shards_name();
    const std::string &shard_type = message.shards_type();
    const std::string &shard_url = message.shards_connector();
    bool with_db_info_key = false;
    if (OB_FAIL(child_info->init(name, db_info_key_, with_db_info_key))) {
      LOG_WDIAG("fail to init ObShardConnector", "name", name.c_str(), K(ret));
    } else if (OB_FAIL(child_info->set_shard_type(shard_type))) {
      LOG_WDIAG("fail to set shard type", "shard_type", shard_type.c_str(), K(ret));
    } else if (OB_FAIL(ObProxyPbUtils::parse_shard_url(shard_url, *child_info))) {
      LOG_WDIAG("fail parse shard url", "shard_url", shard_url.c_str(), K(ret));
    } else {
      child_info->shard_name_.set_value(shard_name.length(), shard_name.c_str());
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
      }
      const Map<std::string, std::string >& shard_auth_map = message.shards_authority();
      for (auto it = shard_auth_map.cbegin(); it != shard_auth_map.cend(); ++it) {
        if (it->first.compare("user") == 0) {
          child_info->full_username_.set_value(it->second.length(), it->second.c_str());
          ObProxyPbUtils::parse_shard_auth_user(*child_info);
        } else if (it->first.compare("password") == 0) {
          child_info->org_password_.set_value(it->second.length(), it->second.c_str());
        } else if (it->first.compare("encType") == 0) {
          child_info->enc_type_ = get_enc_type(it->second.c_str(), it->second.length());
        }
      }
      char pwd_buf[OB_MAX_PASSWORD_LENGTH];
      memset(pwd_buf, 0, sizeof(pwd_buf));
      if (child_info->is_enc_beyond_trust()) {
        db_info_->set_need_update_bt();
      } else if (OB_FAIL(ObBlowFish::decode(child_info->org_password_.ptr(), child_info->org_password_.length(), pwd_buf, OB_MAX_PASSWORD_LENGTH))) {
        LOG_WDIAG("fail to decode encrypted password", K_(child_info->org_password), K(ret));
      } else {
        child_info->password_.set_value(strlen(pwd_buf), pwd_buf);
      }

      if (OB_SUCC(ret)) {
        ObShardConnector *old_child_info = db_info_->sc_array_.ccr_map_.remove(child_info->name_.config_string_);
        if (NULL != old_child_info) {
          if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                     KPC(old_child_info), KPC(child_info), K(ret));
          }
          old_child_info->set_deleting_state();
          old_child_info->dec_ref();
          old_child_info = NULL;
        }
        if (OB_SUCC(ret)) {
          child_info->set_avail_state();
          child_info->set_need_dump_config(true);
          child_info->inc_ref();
          db_info_->sc_array_.ccr_map_.unique_set(child_info);
          LOG_DEBUG("succ to put ObShardConnector", KPC(child_info));
        }
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

int ObDbConfigChildCont::parse_shard_prop(const Any &res)
{
  int ret = OB_SUCCESS;
  ObShardProp*child_info = NULL;
  if (OB_UNLIKELY(!res.Is<ShardsProperties>())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("resource type is not ShardsProperties", K(ret));
  } else if (OB_ISNULL(child_info = op_alloc(ObShardProp))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc memory for ObShardProp", K(ret), K_(db_info_key));
  } else {
    child_info->inc_ref();
    ShardsProperties message;
    res.UnpackTo(&message);
    const MetaData& meta = message.metadata();
    const std::string &name = meta.name();
    const std::string &shard_name = message.shards_name();
    if (OB_FAIL(child_info->init(name, db_info_key_))) {
      LOG_WDIAG("fail to init ObShardProp", "name", name.c_str(), K(ret));
    } else {
      child_info->shard_name_.set_value(shard_name.length(), shard_name.c_str());
      const Map<std::string, std::string >& kv_map = message.variables();
      for (auto it = kv_map.cbegin(); it != kv_map.cend(); ++it) {
        ObString tmp_str = ObString::make_string(it->second.c_str()).trim();
        if (!tmp_str.prefix_match("{") && !tmp_str.prefix_match("]")) {
          child_info->kv_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
        } else {
          child_info->k_obj_map_.insert(std::pair<std::string, std::string>(it->first, it->second));
          ObArenaAllocator allocator(ObModIds::OB_JSON_PARSER);
          Parser parser;
          json::Value *json_root = NULL;
          if (OB_FAIL(parser.init(&allocator))) {
            LOG_WDIAG("json parser init failed", K(ret));
          } else if (OB_FAIL(parser.parse(tmp_str.ptr(), tmp_str.length(), json_root))) {
            LOG_WDIAG("parse json failed", K(tmp_str), K(ret));
          } else if (OB_ISNULL(json_root)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(child_info->do_handle_json_value(json_root, it->first))){
            LOG_DEBUG("fail to handle json value", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ret = child_info->set_prop_values_from_conf();
      }
      ObShardProp *old_child_info = db_info_->sp_array_.ccr_map_.remove(child_info->name_.config_string_);
      if (NULL != old_child_info) {
        if (OB_UNLIKELY(old_child_info->version_ != child_info->version_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("invalid child info, building child info version is not equal with new child info",
                   KPC(old_child_info), KPC(child_info), K(ret));
        }
        old_child_info->set_deleting_state();
        old_child_info->dec_ref();
        old_child_info = NULL;
      }
      if (OB_SUCC(ret)) {
        child_info->set_avail_state();
        child_info->set_need_dump_config(true);
        child_info->inc_ref();
        db_info_->sp_array_.ccr_map_.unique_set(child_info);
        LOG_DEBUG("succ to put ObShardProp", KPC(child_info));
      }
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  return ret;
}

} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
