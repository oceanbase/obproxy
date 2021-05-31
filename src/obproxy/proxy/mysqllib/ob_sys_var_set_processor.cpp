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

#include "obutils/ob_resource_pool_processor.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "proxy/mysqllib/ob_sys_var_set_processor.h"
#include "proxy/mysqllib/ob_session_field_mgr.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
int ObSysVarFetchCont::init_task()
{
  int ret = OB_SUCCESS;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  int64_t len = snprintf(sql, OB_SHORT_SQL_LENGTH, "SELECT /*+READ_CONSISTENCY(WEAK)*/ "
                         "name, data_type, value, modified_time, flags "
                         "FROM %s LIMIT %ld", // defense for @@session.sql_select_limit
                         OB_ALL_VIRTUAL_PROXY_SYS_VARIABLE_TNAME, INT64_MAX);
  if (OB_UNLIKELY(len <= 0) || OB_UNLIKELY(len >= OB_SHORT_SQL_LENGTH)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill sql", K(len), K(sql), K(OB_SHORT_SQL_LENGTH), K(ret));
  } else if (OB_ISNULL(cr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cluster resource can not be null here", K_(cr), K(ret));
  } else if (OB_FAIL(cr_->mysql_proxy_.async_read(this, sql, pending_action_))) {
    LOG_WARN("fail to asyanc read sysvar set", K(ret));
  } else if (OB_ISNULL(pending_action_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending_action can not be NULL", K_(pending_action), K(ret));
  }

  return ret;
}

int ObSysVarFetchCont::finish_task(void *data)
{
  int ret = OB_SUCCESS;
  ObDefaultSysVarSet *var_set = NULL;
  if (OB_ISNULL(data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data", K(data), K(ret));
  } else if (OB_ISNULL(var_set = new (std::nothrow) ObDefaultSysVarSet())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem for default sysvar set", K(ret));
  } else if (FALSE_IT(var_set->inc_ref())) {
    // impossible
  } else if (OB_FAIL(var_set->init())) {
    LOG_WARN("fail to init var set", K(ret));
  } else {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObMysqlResultHandler handler;
    handler.set_resp(resp);
    if (OB_FAIL(var_set->load_system_variable_snapshot(handler))) {
      LOG_WARN("fail to load system variable snapshot", K(ret));
    } else if (OB_FAIL(cr_->sys_var_set_processor_.swap(var_set))) {
      LOG_WARN("fail to set new system variable set", K(ret));
    } else {
      fetch_result_ = true;
    }
  }

  if (OB_LIKELY(NULL != var_set)) {
    var_set->dec_ref();
    var_set = NULL;
  }
  return ret;
}

inline void *ObSysVarFetchCont::get_callback_data()
{
  return static_cast<void *>(&fetch_result_);
}

void ObSysVarFetchCont::destroy()
{
  if (NULL != cr_) {
    LOG_DEBUG("ObSysVarFetchCont will kill self", K(this), "cluster_info", cr_->cluster_info_key_);
    cr_->dec_ref();
    cr_ = NULL;
  } else {
    LOG_ERROR("cluster resource can not be NULL", K_(cr), K(this));
  }
  ObAsyncCommonTask::destroy();
}

//------------------------ObSysVarSetProcessor-----------------------------//
int ObSysVarSetProcessor::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSysVarSetProcessor::destroy()
{
  if (is_inited_) {
    DRWLock::WRLockGuard lock(set_lock_);
    release(sys_var_set_);
    sys_var_set_ = NULL;
    is_inited_ = false;
  }
}

int ObSysVarSetProcessor::add_sys_var_renew_task(ObClusterResource &cr)
{
  int ret = OB_SUCCESS;
  ObSysVarFetchCont *cont = NULL;
  ObProxyMutex *mutex = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem for proxymutex", K(ret));
  } else {
    cr.inc_ref();
    if (OB_ISNULL(cont = new (std::nothrow) ObSysVarFetchCont(&cr, mutex))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc mem for ObSysVarFetchCont", K(ret));
      if (NULL != mutex) {
        mutex->free();
        mutex = NULL;
      }
      cr.dec_ref();
    } else if (OB_ISNULL(g_event_processor.schedule_imm(cont, ET_CALL))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schedule sysvar fetch task error", K(ret));
    }

    if (OB_FAIL(ret)) {
      if(OB_LIKELY(NULL != cont)) {
        cont->destroy();
        cont = NULL;
      }
    }
  }

  return ret;
}

int ObSysVarSetProcessor::renew_sys_var_set(ObDefaultSysVarSet *var_set)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_FAIL(swap(var_set))) {
    LOG_WARN("fail to set new system variable set", K(ret));
  }
  return ret;
}

int ObSysVarSetProcessor::swap(ObDefaultSysVarSet *sys_var_set)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(sys_var_set)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(sys_var_set), K(ret));
  } else {
    // new objects must start with a zero refcount. The sys var set
    // processor holds it's own refcount. We should be the only
    // refcount holder at this point.
    sys_var_set->inc_ref();
    ObDefaultSysVarSet *old_set = NULL;
    {
      DRWLock::WRLockGuard lock(set_lock_);
      old_set = sys_var_set_;
      sys_var_set_ = sys_var_set;
    }
    if (OB_LIKELY(NULL != old_set)) {
      release(old_set);
    }
  }
  return ret;
}

ObDefaultSysVarSet *ObSysVarSetProcessor::acquire()
{
  // hand out a refcount to the caller. We should still have out
  // own refcount, so it should be at least 2.
  ObDefaultSysVarSet *var_set = NULL;
  if (OB_LIKELY(is_inited_)) {
    DRWLock::RDLockGuard lock(set_lock_);
    if (OB_ISNULL(var_set = sys_var_set_)) {
      LOG_ERROR("current system variable set is NULL");
    } else {
      var_set->inc_ref();
    }
  }
  return var_set;
}

void ObSysVarSetProcessor::release(ObDefaultSysVarSet *var_set)
{
  if (OB_LIKELY(NULL != var_set)) {
    var_set->dec_ref();
    var_set = NULL;
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
