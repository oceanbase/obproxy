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

#ifndef OBPROXY_DB_CONFIG_TENANT_CONT_H
#define OBPROXY_DB_CONFIG_TENANT_CONT_H

#include "dbconfig/ob_dbconfig_fetch_cont.h"
#include "lib/lock/ob_thread_cond.h"
#include "obutils/ob_async_common_task.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "iocore/eventsystem/ob_event.h"
#include "dbconfig/grpc/ob_proxy_grpc_client.h"

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
namespace event
{
class ObProxyMutex;
class ObContinuation;
class ObEThread;
}
namespace event
{
class ObProxyMutex;
}
namespace dbconfig
{

struct ObDbConfigTenantContWrapper
{
public:
  ObDbConfigTenantContWrapper()
    : is_fetch_complete_(false), is_fetch_succ_(false), run_cond_() {}
  ~ObDbConfigTenantContWrapper() { destroy(); }

  int init();

private:
  void destroy();

public:
  bool is_fetch_complete_;
  bool is_fetch_succ_;
  common::ObThreadCond run_cond_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigTenantContWrapper);
};

class ObDbConfigTenantCont : public ObDbConfigFetchCont
{
public:
  ObDbConfigTenantCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *cb_thread, const ObDDSCrdType type)
    : ObDbConfigFetchCont(m, cb_cont, cb_thread, type),
      start_mode_(false), target_count_(0), db_count_(0),
      array_buf_(NULL), buf_size_(0),
      db_action_array_(NULL), db_info_array_(NULL),
      wrapper_(NULL), discovery_resp_(),
      tenant_name_(), tenant_version_() {}
  virtual ~ObDbConfigTenantCont() {}

  virtual void destroy();

  virtual int main_handler(int event, void *data);

  void cancel_all_pending_action();
  void cancel_timeout_action();
  void destroy_all_db_info();
  int do_fetch_tenant_config(ObDbConfigTenantContWrapper &wrapper,
                             const google::protobuf::Message &message,
                             bool start_mode);

private:
  int schedule_timeout_action();
  void handle_timeout_action();
  int handle_fetch_db_complete(void *data);
  int handle_fetch_db_config();
  int notify_caller();

  int add_logic_database(const ObDataBaseKey &db_info_key, const std::string &meta,
                         bool &is_new_db_version, ObDbConfigLogicDb *&new_db_info);
  int add_logic_tenant(const common::ObString &tenant_name);

  int dump_config_to_file();
  int dump_config_to_log(oceanbase::obproxy::dbconfig::ObDbConfigLogicTenant &tenant_info);

private:
  static const int64_t OB_DBCONFIG_FETCH_TIMEOUT_MS = 5000;

private:
  bool start_mode_;
  int64_t target_count_;
  int64_t db_count_;
  char *array_buf_;
  int64_t buf_size_;
  event::ObAction **db_action_array_;
  ObDbConfigLogicDb **db_info_array_;
  ObDbConfigTenantContWrapper *wrapper_;
  envoy::api::v2::DiscoveryResponse discovery_resp_;
  obutils::ObProxyConfigString tenant_name_;
  obutils::ObProxyConfigString tenant_version_;
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigTenantCont);
};


} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_DB_CONFIG_TENANT_CONT_H */
