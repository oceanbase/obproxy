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

#ifndef OBPROXY_WATCH_PARENT_CONT_H
#define OBPROXY_WATCH_PARENT_CONT_H

#include "obutils/ob_async_common_task.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{
class ObProxyMutex;
}
namespace dbconfig
{

class ObWatchParentCont : public obutils::ObAsyncCommonTask
{
public:
  ObWatchParentCont(event::ObProxyMutex *m, const ObDDSCrdType type)
    : obutils::ObAsyncCommonTask(m, get_type_task_name(type)),
      type_(type) {}
  virtual ~ObWatchParentCont() {}

  virtual void destroy();

  virtual int init_task();
  virtual int finish_task(void *data)
  {
    UNUSED(data);
    return common::OB_SUCCESS;
  }
  virtual void *get_callback_data() { return NULL; }

  static int alloc_watch_parent_cont(ObWatchParentCont *&cont, const ObDDSCrdType type);

private:
  static const int64_t RETRY_INTERVAL_MS = 100;

private:
  ObDDSCrdType type_;
  DISALLOW_COPY_AND_ASSIGN(ObWatchParentCont);
};


} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_DB_CONFIG_TASK_H */
