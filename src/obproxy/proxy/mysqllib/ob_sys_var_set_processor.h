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

#ifndef OBPROXY_SYS_VAR_SET_PROCESSOR
#define OBPROXY_SYS_VAR_SET_PROCESSOR
#include "lib/ob_define.h"
#include "lib/ptr/ob_ptr.h"
#include "lib/lock/ob_drw_lock.h"
#include "obutils/ob_async_common_task.h"

namespace oceanbase
{
namespace obproxy
{
class ObDefaultSysVarSet;
namespace obutils
{
class ObClusterResource;
}
namespace proxy
{
class ObMysqlProxy;
class ObSysVarSetProcessor;
class ObSysVarFetchCont : public obutils::ObAsyncCommonTask
{
public:
  ObSysVarFetchCont(obutils::ObClusterResource *cr, event::ObProxyMutex *m,
                    event::ObContinuation *cb_cont = NULL, event::ObEThread *submit_thread = NULL)
    : obutils::ObAsyncCommonTask(m, "sysvar_fetch_task", cb_cont, submit_thread),
      fetch_result_(false), cr_(cr) {}
  virtual ~ObSysVarFetchCont() {}

  virtual int init_task();
  virtual int finish_task(void *data);
  virtual void *get_callback_data();
  virtual void destroy();

private:
  bool fetch_result_;
  obutils::ObClusterResource *cr_;
  DISALLOW_COPY_AND_ASSIGN(ObSysVarFetchCont);
};

class ObSysVarSetProcessor
{
public:
  ObSysVarSetProcessor() : is_inited_(false), sys_var_set_(NULL) {}
  ~ObSysVarSetProcessor() { destroy(); }
  void destroy();

  int init();

  int add_sys_var_renew_task(obutils::ObClusterResource &cr);
  int renew_sys_var_set(obproxy::ObDefaultSysVarSet *set);

  // acquire and release must be used in pairs
  ObDefaultSysVarSet *acquire();
  void release(obproxy::ObDefaultSysVarSet *set);
  int swap(obproxy::ObDefaultSysVarSet *sys_var_set);
  TO_STRING_KV(K_(is_inited));

private:
  bool is_inited_;
  common::DRWLock set_lock_;
  ObDefaultSysVarSet *sys_var_set_;
  DISALLOW_COPY_AND_ASSIGN(ObSysVarSetProcessor);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase

#endif /* OBPROXY_SYS_VAR_SET_PROCESSOR */
