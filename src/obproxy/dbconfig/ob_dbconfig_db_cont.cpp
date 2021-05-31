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
#include "dbconfig/ob_dbconfig_db_cont.h"
#include "dbconfig/ob_dbconfig_child_cont.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "iocore/eventsystem/ob_grpc_task.h"

using namespace google::protobuf;
using namespace com::alipay::dds::api::v1;
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

//-------ObDbConfigDbCont------
ObDbConfigDbCont::ObDbConfigDbCont(ObProxyMutex *m, ObContinuation *cb_cont, ObEThread *cb_thread, const ObDDSCrdType type)
  : ObDbConfigFetchCont(m, cb_cont, cb_thread, type), target_count_(0), db_info_(NULL), database_()
{
    memset(child_action_array_, 0, sizeof(ObAction *) * TYPE_MAX);
}

void ObDbConfigDbCont::destroy()
{
  cancel_all_pending_action();
  set_db_info(NULL);
  ObDbConfigFetchCont::destroy();
}

int ObDbConfigDbCont::init_task()
{
  int ret = OB_SUCCESS;
  ObSEArray<obutils::ObProxyConfigString, 3> resource_array;
  if (OB_SUCC(ret) && database_.has_database_authorities()) {
    const DatabaseAuthoritiesReference &da_ref = database_.database_authorities();
    ret = parse_child_reference(da_ref, TYPE_DATABASE_AUTH, db_info_->da_array_, child_action_array_[TYPE_DATABASE_AUTH]);
  }
  if (OB_SUCC(ret) && database_.has_database_variables()) {
    const DatabaseVariablesReference &dv_ref = database_.database_variables();
    ret = parse_child_reference(dv_ref, TYPE_DATABASE_VAR, db_info_->dv_array_, child_action_array_[TYPE_DATABASE_VAR]);
  }
  if (OB_SUCC(ret) && database_.has_database_properties()) {
    const DatabasePropertiesReference &dp_ref = database_.database_properties();
    ret = parse_child_reference(dp_ref, TYPE_DATABASE_PROP, db_info_->dp_array_, child_action_array_[TYPE_DATABASE_PROP]);
  }
  if (OB_SUCC(ret) && database_.has_shards_topology()) {
    const ShardsTopologyReference &st_ref = database_.shards_topology();
    ret = parse_child_reference(st_ref, TYPE_SHARDS_TPO, db_info_->st_array_, child_action_array_[TYPE_SHARDS_TPO]);
  }
  if (OB_SUCC(ret) && database_.has_shards_router()) {
    const ShardsRouterReference &sr_ref = database_.shards_router();
    ret = parse_child_reference(sr_ref, TYPE_SHARDS_ROUTER, db_info_->sr_array_, child_action_array_[TYPE_SHARDS_ROUTER]);
  }
  if (OB_SUCC(ret) && database_.has_shards_distribute()) {
    const ShardsDistributeReference &sd_ref = database_.shards_distribute();
    ret = parse_child_reference(sd_ref, TYPE_SHARDS_DIST, db_info_->sd_array_, child_action_array_[TYPE_SHARDS_DIST]);
  }
  if (OB_SUCC(ret) && database_.has_shards_connector()) {
    const ShardsConnectorReference &sc_ref = database_.shards_connector();
    ret = parse_child_reference(sc_ref, TYPE_SHARDS_CONNECTOR, db_info_->sc_array_, child_action_array_[TYPE_SHARDS_CONNECTOR]);
  }
  if (OB_SUCC(ret) && database_.has_shards_properties()) {
    const ShardsPropertiesReference &sp_ref = database_.shards_properties();
    ret = parse_child_reference(sp_ref, TYPE_SHARDS_PROP, db_info_->sp_array_, child_action_array_[TYPE_SHARDS_PROP]);
  }

  if (OB_FAIL(ret)) {
    cancel_all_pending_action();
    need_callback_ = true;
  }
  if (0 == target_count_ && OB_SUCC(ret)) {
    // no child cr changed
    result_.fetch_result_ = true;
    need_callback_ = true;
  }
  return ret;
}

int ObDbConfigDbCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;
  --target_count_;
  terminate_ = false;
  need_callback_ = false;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch result is null", K(ret));
  } else {
    ObDbConfigFetchContResult result = *(static_cast<ObDbConfigFetchContResult *>(data));
    if (OB_UNLIKELY(result.cont_index_ < 0) || OB_UNLIKELY(result.cont_index_ >= TYPE_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cont result", K_(result.cont_index), K(ret));
    } else {
      child_action_array_[result.cont_index_] = NULL;
      result_.fetch_result_ = result.fetch_result_;
      LOG_INFO("fetch child cont complete", K_(db_info_key),
               "cont index", result.cont_index_,
               "child type", get_type_task_name(static_cast<ObDDSCrdType>(result.cont_index_)),
               "child cont result", result.fetch_result_);
      if (!result_.fetch_result_) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("fetch child config failed", K_(db_info_key), K(ret));
      }
    }
  }

  if (0 == target_count_ || OB_FAIL(ret)) {
    cancel_all_pending_action();
    need_callback_ = true;
    LOG_INFO("finish fetch db config", K_(db_info_key), K_(result_.fetch_result));
  }

  return ret;
}

void ObDbConfigDbCont::cancel_all_pending_action()
{
  for (int64_t i = 0; i < TYPE_MAX; ++i) {
    if (NULL != child_action_array_[i]) {
      child_action_array_[i]->cancel();
      child_action_array_[i] = NULL;
    }
  }
}

template<typename T, typename L>
int ObDbConfigDbCont::parse_child_reference(const T& ref,
                                            const ObDDSCrdType type,
                                            ObDbConfigChildArrayInfo<L> &cr_array,
                                            ObAction *&action)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("begin to parse child reference", "type", get_type_task_name(type), K_(db_info_key));
  ObDbConfigChildCont *cont = NULL;
  bool need_fetch_resource = false;
  bool is_new_version = false;
  if (OB_FAIL(ObDbConfigFetchCont::alloc_fetch_cont(reinterpret_cast<ObDbConfigFetchCont *&>(cont), this, type, type))) {
    LOG_WARN("fail to alloc fetch task", "task_name", get_type_task_name(type), K(ret));
  } else if (OB_ISNULL(cont)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cont is null", K(ret));
  } else {
    cont->set_db_info_key(db_info_key_);
    cont->set_db_info(db_info_);
    const std::string &parent = ref.parent();
    const std::string &dds_namespace = ref.namespace_();
    const std::string &kind = ref.kind();
    std::string resource_name;
    std::string meta;
    LOG_DEBUG("child reference", "parent", parent.c_str(),
              "namespace", dds_namespace.c_str(),
              "ref size", ref.reference_size());
    cr_array.ref_info_.parent_.set_value(parent.length(), parent.c_str());
    cr_array.ref_info_.namespace_.set_value(dds_namespace.length(), dds_namespace.c_str());
    cr_array.ref_info_.kind_.set_value(kind.length(), kind.c_str());
    for (int i = 0; OB_SUCC(ret) && i < ref.reference_size(); ++i) {
      const std::string &reference = ref.reference(i);
      meta = parent.length() > 0 ? parent + "." + reference
                                 : reference;
      if (OB_FAIL(handle_child_ref(type, reference, cr_array, is_new_version))) {
        LOG_WARN("fail to handle child ref", K_(db_info_key), K(ret));
      } else if (is_new_version) {
        need_fetch_resource = true;
        resource_name = parent.length() > 0 ? parent + "." + reference + "." + dds_namespace
                                            : reference + "." + dds_namespace;
        cont->add_resource_name(resource_name);
      }
    }
    if (OB_SUCC(ret)) {
      if (TYPE_SHARDS_TPO == type) {
        const ShardsTopologyReference *st_ref = reinterpret_cast<const ShardsTopologyReference *>(&ref);
        ObString tmp_str(static_cast<int64_t>(st_ref->specific_mode().length()), st_ref->specific_mode().c_str());
        if (tmp_str.case_compare("strict") == 0) {
          db_info_->set_is_strict_spec_mode();
        }
      } else if (TYPE_SHARDS_CONNECTOR == type) {
        const ShardsConnectorReference *sc_ref = reinterpret_cast<const ShardsConnectorReference *>(&ref);
        const std::string &testload_prefix = sc_ref->test_load_prefix();
        if (testload_prefix.length() > 0) {
          LOG_INFO("testload prefix", "prefix", testload_prefix.c_str());
          db_info_->set_testload_prefix(testload_prefix);
        }
        for (int i = 0; OB_SUCC(ret) && i < sc_ref->test_load_reference_size(); ++i) {
          const std::string &reference = sc_ref->test_load_reference(i);
          meta = parent.length() > 0 ? parent + "." + reference
                                     : reference;
          if (OB_FAIL(handle_child_ref(type, reference, cr_array, is_new_version))) {
            LOG_WARN("fail to handle child ref", K_(db_info_key), K(ret));
          } else if (is_new_version) {
            need_fetch_resource = true;
            resource_name = parent.length() > 0 ? parent + "." + reference + "." + dds_namespace
                                                : reference + "." + dds_namespace;
            cont->add_resource_name(resource_name);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && need_fetch_resource) {
    if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_GRPC))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule fetch crd cont", K(ret));
    } else {
      action = &cont->get_action();
      ++target_count_;
      LOG_DEBUG("succ to schedule fetch child cr task", "task", get_type_task_name(type), K_(db_info_key));
    }
  }
  if (OB_FAIL(ret) || !need_fetch_resource) {
    if (NULL != cont) {
      cont->destroy();
      cont = NULL;
    }
  }
  return ret;
}

template<typename L>
int ObDbConfigDbCont::handle_child_ref(const ObDDSCrdType type,
                                       const std::string &reference,
                                       ObDbConfigChildArrayInfo<L> &cr_array,
                                       bool &is_new_child_version)
{
  int ret = OB_SUCCESS;
  is_new_child_version = false;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  ObDbConfigChild *child_info = NULL;
  ObDbConfigChild *new_child_info = NULL;
  ObString name;
  ObString version;
  // reference format: ref_name.version
  size_t pos = std::string::npos;
  if (OB_UNLIKELY(std::string::npos == (pos = reference.rfind(".")))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid child reference", "reference string", reference.c_str());
  } else {
    name.assign_ptr(reference.c_str(), static_cast<ObString::obstr_size_t>(pos));
    version.assign_ptr(reference.c_str() + pos + 1, static_cast<ObString::obstr_size_t>(reference.length() - pos - 1));
    child_info = dbconfig_cache.get_child_info(db_info_key_, name, type);
    if (NULL == child_info) {
      is_new_child_version = true;
    } else if (child_info->is_version_changed(version)) {
      is_new_child_version = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDbConfigChild::alloc_child_info(db_info_key_, type, name, version, new_child_info))) {
      LOG_WARN("fail to alloc child info", "type", get_type_task_name(type), K(name), K(version), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (!is_new_child_version) {
      if (OB_FAIL(new_child_info->assign(*child_info))) {
        LOG_WARN("fail to copy child info", K(db_info_key_), K(name), K(version), K(ret));
      } else {
        new_child_info->set_avail_state();
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cr_array.ccr_map_.unique_set(reinterpret_cast<L *>(new_child_info)))) {
      LOG_WARN("fail to add child info into cr array", K(db_info_key_), K(name), K(version), K(ret));
    }
  }
  if (NULL != child_info) {
    child_info->dec_ref();
    child_info = NULL;
  }
  if (OB_FAIL(ret) && NULL != new_child_info) {
    new_child_info->dec_ref();
    new_child_info = NULL;
  }
  return ret;
}


} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
