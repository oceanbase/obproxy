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
#include "dbconfig/ob_dbconfig_tenant_cont.h"
#include "dbconfig/ob_dbconfig_db_cont.h"
#include "dbconfig/ob_proxy_db_config_processor.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"
#include "dbconfig/ob_proxy_pb_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "dbconfig/protobuf/dds-api/database.pb.h"
#include "lib/time/ob_hrtime.h"
#include "iocore/eventsystem/ob_grpc_task.h"
#include "utils/ob_proxy_utils.h"

using namespace grpc;
using namespace google::protobuf;
using namespace envoy::service::discovery::v2;
using namespace envoy::api::v2;
using namespace envoy::api::v2::core;
using namespace com::alipay::dds::api::v1;
using namespace obsys;
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

int ObDbConfigTenantContWrapper::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(run_cond_.init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("fail to init run cond", K(ret));
  }
  return ret;
}

void ObDbConfigTenantContWrapper::destroy()
{
  run_cond_.destroy();
  is_fetch_complete_ = false;
  is_fetch_succ_ = false;
}

//-------ObDbConfigTenantCont------
void ObDbConfigTenantCont::destroy()
{
  LOG_INFO("tenant config fetch cont will be destroyed", KP(this));
  cancel_timeout_action();
  cancel_all_pending_action();
  destroy_all_db_info();
  if (NULL != array_buf_ && buf_size_ > 0) {
    op_fixed_mem_free(array_buf_, buf_size_);
  }
  array_buf_ = NULL;
  buf_size_ = 0;
  db_action_array_ = NULL;
  db_info_array_ = NULL;
  ObDbConfigFetchCont::destroy();
}

void ObDbConfigTenantCont::cancel_timeout_action()
{
  if (NULL != timeout_action_) {
    timeout_action_->cancel();
    timeout_action_ = NULL;
  }
}

void ObDbConfigTenantCont::cancel_all_pending_action()
{
  if (NULL != db_action_array_) {
    for (int64_t i = 0; i < db_count_; ++i) {
      if (NULL != db_action_array_[i]) {
        db_action_array_[i]->cancel();
        db_action_array_[i] = NULL;
      }
    }
  }
}

inline int ObDbConfigTenantCont::schedule_timeout_action()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL != timeout_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("timeout action must be NULL", K_(timeout_action), K(ret));
  } else if (OB_ISNULL(timeout_action_ = self_ethread().schedule_in(this, HRTIME_MSECONDS(OB_DBCONFIG_FETCH_TIMEOUT_MS)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule timeout", K_(timeout_action), K(ret));
  }
  return ret;
}

inline void ObDbConfigTenantCont::handle_timeout_action()
{
  timeout_action_ = NULL;
  LOG_INFO("timeout to fetch tenant db config", KP(this));
  if (start_mode_) {
    result_.fetch_result_ = false;
  }
  cancel_all_pending_action();
  need_callback_ = true;
}

void ObDbConfigTenantCont::destroy_all_db_info()
{
  if (NULL != db_info_array_) {
    for (int64_t i = 0; i < db_count_; ++i) {
      if (NULL != db_info_array_[i]) {
        db_info_array_[i]->dec_ref();
        db_info_array_[i] = NULL;
      }
    }
  }
}

int ObDbConfigTenantCont::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  int ret_event = EVENT_CONT;
  LOG_INFO("[ObDbConfigTenantCont::main_handler]", K(event), K(this));

  if (OB_UNLIKELY(this_ethread() != mutex_->thread_holding_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    switch (event) {
      case EVENT_IMMEDIATE:
        if (OB_FAIL(schedule_timeout_action())) {
          LOG_WDIAG("fail to schedule timeout action", K(ret));
        } else if (OB_FAIL(handle_fetch_db_config())) {
          LOG_WDIAG("fail to handle fetch db config", K(ret));
        }
        break;
      case ASYNC_PROCESS_DONE_EVENT:
        if (OB_FAIL(handle_fetch_db_complete(data))) {
          LOG_WDIAG("fail to handle fetch db complete", K(ret));
        }
        break;
      case EVENT_INTERVAL:
        handle_timeout_action();
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown event", K(event), K(ret));
        break;
    }
  }

  if (OB_FAIL(ret)) {
    cancel_timeout_action();
    cancel_all_pending_action();
    need_callback_ = true;
  }
  
  if (0 == target_count_ && result_.fetch_result_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = dump_config_to_file())) {
      LOG_WDIAG("fail to dump tenant config into file", K(tmp_ret));
    }
  }

  if (0 == target_count_ || need_callback_) {
    if (OB_FAIL(notify_caller())) {
      LOG_WDIAG("fail to notify caller", K(ret));
    }
  }

  if (terminate_) {
    destroy();
    ret_event = EVENT_DONE;
  }
  return ret_event;
}

int ObDbConfigTenantCont::handle_fetch_db_config()
{
  int ret = OB_SUCCESS;
  LOG_INFO("handle fetch db config");
  for (int64_t i = 0; OB_SUCC(ret) && i < discovery_resp_.resources_size(); ++i) {
    const Any& res = discovery_resp_.resources(static_cast<int>(i));
    if (OB_UNLIKELY(!res.Is<Database>())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("resource is not database", K(ret));
    } else {
      Database message;
      res.UnpackTo(&message);
      const std::string& cluster_name = message.cluster_name();
      const std::string& tenant_name = message.tenant_name();
      const std::string& database_name = message.database_name();
      const std::string &database_mode = message.database_mode();
      const std::string &database_type = message.database_type();
      const std::string &meta = message.metadata().name();
      LOG_INFO("db info from remote",
               "tenant_name", tenant_name.c_str(),
               "db_name", database_name.c_str(),
               "db_meta", meta.c_str());
      db_info_key_.tenant_name_.set_value(tenant_name.length(), tenant_name.c_str());
      db_info_key_.database_name_.set_value(database_name.length(), database_name.c_str());
      if (tenant_name_.empty()) {
        tenant_name_.set_value(tenant_name.length(), tenant_name.c_str());
        if (OB_FAIL(add_logic_tenant(tenant_name_.config_string_))) {
          LOG_WDIAG("fail to add logic tenant info", K(tenant_name_), K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        bool is_new_db_version = false;
        if (OB_FAIL(add_logic_database(db_info_key_, meta, is_new_db_version, db_info_array_[i]))) {
          LOG_WDIAG("fail to add logic database", K_(db_info_key), K(is_new_db_version), K(i), K(ret));
        } else if (is_new_db_version) {
          db_info_array_[i]->db_cluster_.set_value(cluster_name.length(), cluster_name.c_str());
          db_info_array_[i]->db_type_.set_value(database_type.length(), database_type.c_str());
          db_info_array_[i]->db_mode_.set_value(database_mode.length(), database_mode.c_str());
          ObDbConfigDbCont *cont = NULL;
          if (OB_FAIL(ObDbConfigFetchCont::alloc_fetch_cont(reinterpret_cast<ObDbConfigFetchCont *&>(cont), this, i, TYPE_DATABASE))) {
            LOG_WDIAG("fail to alloc fetch db cont", "database", database_name.c_str(), K(ret));
          } else if (FALSE_IT(cont->set_database(message))) {
            // do nothing
          } else if (FALSE_IT(cont->set_db_info_key(db_info_key_))) {
            // do nothing
          } else if (FALSE_IT(cont->set_db_info(db_info_array_[i]))) {
            // do nothing
          } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_GRPC))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule db config fetch cont", "database", database_name.c_str(), K(ret));
          } else {
            ++target_count_;
            db_action_array_[i] = &cont->get_action();
            LOG_INFO("succ to schedule fetch db task", "database", database_name.c_str(), "cont index", i);
          }
        }
      }
    }
  } // end for
  if (OB_SUCC(ret) && 0 == target_count_) {
    // no need fetch any db config, set result to succ
    result_.fetch_result_ = true;
  }
  return ret;
}

int ObDbConfigTenantCont::notify_caller()
{
  int ret = OB_SUCCESS;
  cancel_timeout_action();
  if (OB_FAIL(wrapper_->run_cond_.lock())) {
    LOG_WDIAG("fail to lock", K(ret));
  } else {
    wrapper_->is_fetch_complete_ = true;
    wrapper_->is_fetch_succ_ = result_.fetch_result_;
    if (OB_FAIL(wrapper_->run_cond_.signal())) {
      LOG_WDIAG("fail to signal", K(ret));
    } else if (OB_FAIL(wrapper_->run_cond_.unlock())) {
      LOG_WDIAG("fail to unlock", K(ret));
    }
  }
  terminate_ = true;
  return ret;
}

int ObDbConfigTenantCont::do_fetch_tenant_config(ObDbConfigTenantContWrapper &wrapper,
                                                 const google::protobuf::Message &message,
                                                 bool start_mode)
{
  int ret = OB_SUCCESS;
  discovery_resp_ = static_cast<const DiscoveryResponse &>(message);
  if (OB_UNLIKELY(discovery_resp_.resources_size() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("resources is empty", K(ret));
  } else {
    db_count_ = discovery_resp_.resources_size();
    int64_t action_size = sizeof(ObAction *) * db_count_;
    int64_t db_info_size = sizeof(ObDbConfigChild *) * db_count_;
    buf_size_ = action_size + db_info_size;
    if (OB_UNLIKELY(NULL != array_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("array buf is not null", K(array_buf_), K(ret));
    } else if (OB_ISNULL(array_buf_ = static_cast<char *>(op_fixed_mem_alloc(buf_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K(buf_size_), K(ret));
    } else {
      db_action_array_ = new (array_buf_) ObAction *[db_count_];
      db_info_array_ = new (array_buf_ + action_size) ObDbConfigLogicDb*[db_count_];
      memset(array_buf_, 0, buf_size_);
      wrapper_ = &wrapper;
      start_mode_ = start_mode;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(g_event_processor.schedule_imm(this, ET_GRPC))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("fail to schedule blocking tenant config fetch task", K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to schedule ObDbConfigTenantCont", KP(this));
  }
  return ret;
}

int ObDbConfigTenantCont::handle_fetch_db_complete(void *data)
{
  int ret = OB_SUCCESS;
  --target_count_;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("fetch result is null", K(ret));
  } else {
    ObDbConfigFetchContResult result = *(static_cast<ObDbConfigFetchContResult *>(data));
    if (OB_UNLIKELY(result.cont_index_ < 0) || OB_UNLIKELY(result.cont_index_ >= db_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("unexpected cont result", K_(result.cont_index), K(ret));
    } else {
      db_action_array_[result.cont_index_] = NULL;
      ObDbConfigLogicDb *db_info = db_info_array_[result.cont_index_];
      if (result.fetch_result_) {
        if (OB_FAIL(get_global_dbconfig_cache().handle_new_db_info(*db_info))) {
          result.fetch_result_ = false;
          LOG_WDIAG("fail handle new db info", K_(start_mode), KPC(db_info), K(ret));
        } else {
          LOG_DEBUG("succ to add logic db info", KPC(db_info));
        }
      }
      if (start_mode_) {
        result_.fetch_result_ = result.fetch_result_;
      } else {
        result_.fetch_result_ = result.fetch_result_ || result_.fetch_result_;
        if (OB_FAIL(ret)) {
          // allow some logic db success, some fail. failed logic db will no save to config file
          ret = OB_SUCCESS;
        }
      }
      LOG_INFO("fetch db cont complete", "cont index", result.cont_index_,
               "db cont result", result.fetch_result_, "tenant cont result", result_.fetch_result_);
      if (!result_.fetch_result_ && start_mode_) {
        ret = OB_INVALID_CONFIG;
        LOG_WDIAG("fetch database config failed", K(ret));
        need_callback_ = true;
      }
    }
  }
  
  return ret;
}

// no need acquire lock when dump config file
// here need use db info of cache to dump
int ObDbConfigTenantCont::dump_config_to_file()
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicTenant *tenant_info = NULL;
  const ObString &tenant_name = tenant_name_.config_string_;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  int64_t dump_db_count = 0;
  if (OB_ISNULL(tenant_info = dbconfig_cache.get_exist_tenant(tenant_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("logic tenant does not exist", K(tenant_name), K(ret));
  } else {
    ObDbConfigLogicTenant::LDHashMap &map = const_cast<ObDbConfigLogicTenant::LDHashMap &>(tenant_info->ld_map_);
    for (ObDbConfigLogicTenant::LDHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
      if (!it->need_dump_config()) {
        // do nothing
      } else if (OB_FAIL(ObProxyPbUtils::dump_database_info_to_file(*it))) {
        LOG_WDIAG("fail to dump db info into file", K(ret));
      } else {
        it->set_need_dump_config(false);
        ++dump_db_count;
      }
    }
  }
  if (OB_SUCC(ret) && dump_db_count > 0) {
    if (OB_FAIL(ObProxyPbUtils::dump_tenant_info_to_file(*tenant_info))) {
      LOG_WDIAG("fail to dump tenant info into file", K(tenant_name), K(ret));
    } else if (OB_FAIL(dump_config_to_log(*tenant_info))) {
      LOG_WDIAG("fail to dump tenant info into config", K(tenant_name), K(ret));
    }
  }
  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

int ObDbConfigTenantCont::dump_config_to_log(ObDbConfigLogicTenant &tenant_info)
{
  int ret = OB_SUCCESS;
  const ObString &tenant_name = tenant_info.tenant_name_.config_string_;
  OBPROXY_CONFIG_LOG_ROTATE(INFO, tenant_info.get_version().ptr());
  ObSqlString start_buf;
  ObSqlString end_buf;
  ObSqlString version_info_str;
  const char * app_name = (get_global_proxy_config().app_name) == NULL ? "" : get_global_proxy_config().app_name;
  if (OB_FAIL(version_info_str.append_fmt("%.*s_%.*s_%.*s",
                                          static_cast<int>(STRLEN(app_name)), app_name,
                                          tenant_name.length(), tenant_name.ptr(),
                                          tenant_info.get_version().length(), tenant_info.get_version().ptr()))) {
    LOG_WDIAG("fail to dump tenant info into version", K(tenant_name), K(ret));
  } else if (OB_FAIL(ObProxyPbUtils::dump_config_to_buf(start_buf, version_info_str.ptr(), static_cast<int>(version_info_str.length()),
                                                        tenant_name.ptr(), tenant_name.length(),
                                                        "", 0, ObProxyPbUtils::ODP_CONFIG_LOG_START,
                                                        "", 0))) {
    LOG_WDIAG("fail to dump tenant info into start flag", K(tenant_name), K(ret));
  } else if (OB_FAIL(ObProxyPbUtils::dump_config_to_buf(end_buf, version_info_str.ptr(), static_cast<int>(version_info_str.length()),
                                                        tenant_name.ptr(), tenant_name.length(),
                                                        "", 0, ObProxyPbUtils::ODP_CONFIG_LOG_END,
                                                        "", 0))) {
    LOG_WDIAG("fail to dump tenant info into end flag", K(tenant_name), K(ret));
  } else {
    LOG_DEBUG("CRD log start flag", K(ret), K(start_buf));
    _OBPROXY_CONFIG_LOG(INFO, "%.*s", static_cast<int>(start_buf.length()), start_buf.ptr());
  }

  if (OB_SUCC(ret)) {
    if(OB_FAIL(ObProxyPbUtils::dump_tenant_info_to_log(tenant_info))) {
      LOG_WDIAG("fail to dump tenant info into file", K(tenant_name), K(ret));
    } else {
      ObDbConfigLogicTenant::LDHashMap &map = const_cast<ObDbConfigLogicTenant::LDHashMap &>(tenant_info.ld_map_);
      for (ObDbConfigLogicTenant::LDHashMap::iterator it = map.begin(); OB_SUCC(ret) && it != map.end(); ++it) {
        if (OB_FAIL(ObProxyPbUtils::dump_database_info_to_log(*it))) {
          LOG_WDIAG("fail to dump db info into file", K(ret));
        }
      }
    }
  }
  // force print end flag not matter success or failed
  LOG_DEBUG("CRD log end flag", K(ret), K(end_buf));
  _OBPROXY_CONFIG_LOG(INFO, "%.*s", static_cast<int>(end_buf.length()), end_buf.ptr());

  return ret;
}

int ObDbConfigTenantCont::add_logic_tenant(const ObString &tenant_name)
{
  int ret = OB_SUCCESS;
  ObDbConfigLogicTenant *tenant_info = NULL;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  char cur_timestamp[OB_MAX_TIMESTAMP_LENGTH];
  if (OB_FAIL(convert_timestamp_to_version(hrtime_to_usec(get_hrtime_internal()), cur_timestamp, OB_MAX_TIMESTAMP_LENGTH))) {
    LOG_WDIAG("fail to convert current timestamp to version", K(ret));
  } else {
    tenant_version_.set_value(static_cast<int64_t>(strlen(cur_timestamp)), cur_timestamp);
  }
  if (OB_FAIL(ret)) {
    //  dp nothing
  } else if (OB_ISNULL(tenant_info = dbconfig_cache.get_exist_tenant(tenant_name))) {
    if (OB_ISNULL(tenant_info = op_alloc(ObDbConfigLogicTenant))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc memory for ObDbConfigLogicTenant", K(tenant_name), K(ret));
    } else {
      tenant_info->inc_ref();
      tenant_info->set_tenant_name(tenant_name);
      tenant_info->set_version(tenant_version_.config_string_);
      // dec_ref will be called in the end of this function, so we need add ref before push into map
      tenant_info->inc_ref();
      CWLockGuard guard(dbconfig_cache.rwlock_);
      if (OB_FAIL(dbconfig_cache.lt_map_.unique_set(tenant_info))) {
        LOG_WDIAG("fail to add tenant info", K(db_info_key_), K(ret));
      } else {
        LOG_DEBUG("succ to add logic tenant info", KPC(tenant_info));
      }
    }
  }
  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

int ObDbConfigTenantCont::add_logic_database(const ObDataBaseKey &db_info_key, const std::string &meta,
                                             bool &is_new_db_version, ObDbConfigLogicDb *&new_db_info)
{
  int ret = OB_SUCCESS;
  ObDbConfigCache &dbconfig_cache = get_global_dbconfig_cache();
  ObDbConfigLogicTenant *tenant_info = NULL;
  ObDbConfigLogicDb *db_info = NULL;
  bool is_new_db = false;
  const ObString &tenant_name = db_info_key.tenant_name_.config_string_;
  const ObString &database_name = db_info_key.database_name_.config_string_;
  if (OB_ISNULL(tenant_info = dbconfig_cache.get_exist_tenant(tenant_name))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("logic tenant does not exist", K(tenant_name), K(database_name), K(ret));
  } else if (OB_ISNULL(db_info = tenant_info->get_exist_db(database_name))) {
    is_new_db_version = true;
    is_new_db = true;
  } else {
    // meta format: cluster.tenant.database.version
    size_t pos = meta.rfind(".");
    ObString db_version;
    if (OB_UNLIKELY(std::string::npos == pos)) {
      ret = OB_INVALID_CONFIG;
      LOG_INFO("invalid db version", "meta", meta.c_str());
    } else if (FALSE_IT(db_version.assign_ptr(meta.c_str() + pos + 1, static_cast<ObString::obstr_size_t>(meta.length() - pos - 1)))) {
      // do nothing
    } else if (db_info->is_version_changed(db_version)) {
      is_new_db_version = true;
    } else {
      // do nothing, wait to be add into building tenant info
    }
  }
  if (OB_SUCC(ret)) {
    if (is_new_db_version) {
      if (OB_ISNULL(new_db_info = op_alloc(ObDbConfigLogicDb))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc memory for ObDbConfigLogicDb", K(ret), K(db_info_key));
      } else if (OB_FAIL(new_db_info->init(meta, db_info_key))) {
        LOG_WDIAG("fail to init ObDbConfigLogicDb", "meta", meta.c_str(), K(ret), K(db_info_key));
      } else {
        new_db_info->set_db_name(database_name);
        new_db_info->inc_ref();
      }
    }
  }

  if (OB_SUCC(ret) && NULL != (new_db_info)) {
    if (is_new_db) {
      new_db_info->set_building_state();
    } else {
      new_db_info->set_updating_state();
    }
  }
  
  if (NULL != db_info) {
    db_info->dec_ref();
    db_info = NULL;
  }
  if (NULL != tenant_info) {
    tenant_info->dec_ref();
    tenant_info = NULL;
  }
  return ret;
}

} // end namespace dbconfig
} // end namespace proxy
} // end namespace oceanbase
