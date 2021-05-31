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

#ifndef OBPROXY_DB_CONFIG_FETCH_CONT_H
#define OBPROXY_DB_CONFIG_FETCH_CONT_H

#include "obutils/ob_async_common_task.h"
#include "dbconfig/grpc/ob_proxy_grpc_utils.h"
#include "dbconfig/ob_proxy_db_config_info.h"
#include "obproxy/iocore/eventsystem/ob_event.h"

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

struct ObDbConfigFetchContResult
{
public:
  ObDbConfigFetchContResult() : fetch_result_(false), cont_index_(-1) {}
  ~ObDbConfigFetchContResult() {}

public:
  bool fetch_result_;
  int64_t cont_index_;
};

class ObDbConfigFetchCont : public obutils::ObAsyncCommonTask
{
public:
  ObDbConfigFetchCont(event::ObProxyMutex *m, event::ObContinuation *cb_cont, event::ObEThread *cb_thread, const ObDDSCrdType type);
  virtual ~ObDbConfigFetchCont() {}

  void init();
  virtual void destroy();
  virtual void *get_callback_data() { return static_cast<void *>(&result_); }
  
  void set_db_info_key(const ObDataBaseKey &key);

  static int alloc_fetch_cont(ObDbConfigFetchCont *&cont, event::ObContinuation *cb_cont,
                              const int64_t index, const ObDDSCrdType type);

public:
  ObDDSCrdType type_;
  ObDbConfigFetchContResult result_;
  ObDataBaseKey db_info_key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDbConfigFetchCont);
};

inline void ObDbConfigFetchCont::set_db_info_key(const ObDataBaseKey &key)
{
  db_info_key_.assign(key);
}




} // end dbconfig
} // end obproxy
} // end oceanbase

#endif /* OBPROXY_DB_CONFIG_DB_CONT_H */
