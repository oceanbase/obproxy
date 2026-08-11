/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY_EVENT

#include "iocore/eventsystem/ob_session_pool_event_processor.h"
#include "iocore/eventsystem/ob_event_system.h"
#include "iocore/net/ob_unix_net.h"
#include "obutils/ob_async_common_task.h"
#include "obutils/ob_session_pool_processor.h"
#include "obutils/ob_proxy_config.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace event
{
ObEventThreadType ET_SESS_POOL = ET_NET;
ObSessionPoolEventProcessor g_session_pool_event_processor;



int ObSessionPoolEventProcessor::start(const int64_t threads, const int64_t stacksize)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(g_event_processor.spawn_event_threads(threads, "ET_SESS_POOL", stacksize, ET_SESS_POOL))) {
    LOG_WDIAG("fail to spawn event threads for ET_SHARD_WATCH", K(ret));
  } else {
    int64_t net_thread_count = g_event_processor.thread_count_for_type_[ET_SESS_POOL];
    ObEThread **ethreads = g_event_processor.event_thread_[ET_SESS_POOL];
    for (int64_t i = 0; i < net_thread_count && OB_SUCC(ret); ++i) {
      if (OB_FAIL(net::initialize_thread_for_net(ethreads[i],
          reinterpret_cast<net::NetContHandler>(&net::ObNetHandler::start_session_pool_event)))) {
        PROXY_NET_LOG(EDIAG, "fail to initialize thread for session pool", K(i), K(ret));
      // 在 ET_SESS_POOL 线程上启动重置会话的异步任务
      // 因为 reset connection 操作会对 session socket fd 进行读写操作
      // 必须由监听 session socket fd 事件的线程执行
      // 所以需要在每一个 ET_SESS_POOL 上都启动一个周期任务
      } else if (OB_FAIL(ObSessionPoolProcessor::start_reset_conn_task(ethreads[i]))) {
        PROXY_NET_LOG(EDIAG, "fail to initialize reset connection task for ET_SESS_POOL thread", K(i), K(ret));
      }
    }
    LOG_INFO("succ to start session pool threads", K(threads));
  }

  return ret;
}
} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
