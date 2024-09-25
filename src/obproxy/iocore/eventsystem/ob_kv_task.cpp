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

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_kv_task.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "proxy/mysql/ob_mysql_proxy_server_main.h"
#include "iocore/net/ob_unix_net_processor.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObEventThreadType ET_OBKV = ET_CALL;
ObKvTaskProcessor g_obkv_task_processor;

int ObKvTaskProcessor::start(const int64_t obkv_task_all_threads, const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread_pool_event_queue_ = new (std::nothrow) ObProtectedQueueThreadPool())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to new ObProtectedQueueThreadPool", K(ret));
  } else if (OB_FAIL(thread_pool_event_queue_->init())) {
    LOG_WDIAG("fail to init thread_pool_event_queue", K(ret));
  } else if (OB_FAIL(g_event_processor.spawn_event_threads(obkv_task_all_threads, "ET_OBKV", stacksize, ET_OBKV))) {
    LOG_WDIAG("fail to spawn event threads for ET_OBKV", K(ret));
  } else {
    int64_t event_thread_conut = g_event_processor.thread_count_for_type_[ET_OBKV];
    for (int i = 0; i < event_thread_conut && OB_SUCC(ret); i++) {
      ObEThread **threads = g_event_processor.event_thread_[ET_OBKV];
      if (OB_ISNULL(threads) || OB_ISNULL(threads[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("unexpected obkv task treads", K(ret));
      } else if (OB_FAIL(net::initialize_thread_for_net(threads[i]))) {
        LOG_WDIAG("fail to init thread for net", K(ret));
      } else if (OB_FAIL(proxy::init_cache_map_for_one_thread(threads[i]))) {
        LOG_WDIAG("fail to init cache map for thread", K(i), K(threads[i]), K(ret));
      }
    }
    LOG_INFO("succ to start ObKvTaskProcessor", K(obkv_task_all_threads), K(stacksize), K(ET_OBKV));
  }

  return ret;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase