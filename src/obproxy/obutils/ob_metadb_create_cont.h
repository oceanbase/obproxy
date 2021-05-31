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

#ifndef OPROXY_METADB_CREATE_CONT_H
#define OPROXY_METADB_CREATE_CONT_H

#include "iocore/eventsystem/ob_event_system.h"
#include "obutils/ob_async_common_task.h"

#define METADB_CREATE_START_EVENT (METADB_CREATE_EVENT_EVENTS_START + 1)

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObProxyTableProcessor;
class ObMetadbCreateCont : public obutils::ObAsyncCommonTask
{
public:
  ObMetadbCreateCont(event::ObProxyMutex *m, proxy::ObMysqlProxy *meta_proxy)
    : obutils::ObAsyncCommonTask(m, "metadb_create_task"), reentrancy_count_(0), retry_count_(INT32_MAX),
      meta_proxy_(meta_proxy) {}
  virtual ~ObMetadbCreateCont() {}

  virtual int main_handler(int event, void *data);
  // before create metadb, must confirm old metadb has been deleted
  static int create_metadb(proxy::ObMysqlProxy *meta_proxy = NULL);

private:
  int handle_create_meta_db();
  int handle_create_complete(void *data);

private:
  static const int64_t RETRY_INTERVAL_MS = 1000; // 1s

  int32_t reentrancy_count_;
  int32_t retry_count_; // if create fail, retry count, default is INT32_MAX;
  proxy::ObMysqlProxy *meta_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObMetadbCreateCont);
};

} // end of namespace obutils
} // end of namespace obproxy
} // end of namespace oceanbase

#endif // OPROXY_METADB_CREATE_CONT_H
