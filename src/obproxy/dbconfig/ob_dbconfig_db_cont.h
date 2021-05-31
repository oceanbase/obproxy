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

#ifndef OBPROXY_DB_CONFIG_DB_CONT_H
#define OBPROXY_DB_CONFIG_DB_CONT_H

#include "dbconfig/ob_dbconfig_fetch_cont.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "obproxy/iocore/eventsystem/ob_event.h"
#include "obproxy/dbconfig/protobuf/dds-api/database.pb.h"

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
namespace dbconfig
{

class ObDbConfigDbCont : public ObDbConfigFetchCont
{
public:
  ObDbConfigDbCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *cb_thread, const ObDDSCrdType type);
  virtual ~ObDbConfigDbCont() {}

  virtual void destroy();
  virtual int init_task();
  virtual int finish_task(void *data);
  void set_database(com::alipay::dds::api::v1::Database &database) { database_ = database; }

  void set_db_info(ObDbConfigLogicDb *db_info)
  {
    if (NULL != db_info_) {
      db_info_->dec_ref();
      db_info_ = NULL;
    }
    if (NULL != db_info) {
      db_info->inc_ref();
      db_info_ = db_info;
    }
  }

  void cancel_all_pending_action();

private:
  template<typename T, typename L>
  int parse_child_reference(const T &ref,
                            const ObDDSCrdType type,
                            ObDbConfigChildArrayInfo<L> &cr_array,
                            event::ObAction *&action);

  template<typename L>
  int handle_child_ref(const ObDDSCrdType type,
                       const std::string &reference,
                       ObDbConfigChildArrayInfo<L> &cr_array,
                       bool &is_new_child_version);


private:
  int64_t target_count_;
  ObDbConfigLogicDb *db_info_;
  com::alipay::dds::api::v1::Database database_;
  event::ObAction *child_action_array_[TYPE_MAX];
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigDbCont);
};


} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_DB_CONFIG_DB_CONT_H */
