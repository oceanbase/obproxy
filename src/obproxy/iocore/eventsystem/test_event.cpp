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

#include "ob_event_system.h"

namespace oceanbase
{
namespace obproxy
{
namespace event
{

#define TEST_TIME_SECOND 60
#define TEST_THREADS     2

int count;
#define DIAGS_LOG_FILE "diags.log"

//////////////////////////////////////////////////////////////////////////////
//
//      void reconfigure_diags()
//
//      This function extracts the current diags configuration settings from
//      records.config, and rebuilds the Diags data structures.
//
//////////////////////////////////////////////////////////////////////////////

struct alarm_printer:public ObContinuation
{
  alarm_printer(ObProxyMutex * m):ObContinuation(m)
  {
    SET_HANDLER(&alarm_printer::dummy_function);
  }
  int dummy_function(int /* event */, ObEvent * /* e */)
  {
    (void)ATOMIC_FAA((int *) &count, 1);
    printf("Count = %d\n", count);
    return 0;
  }
};
struct process_killer:public ObContinuation
{
  process_killer(ObProxyMutex * m):ObContinuation(m)
  {
    SET_HANDLER(&process_killer::kill_function);
  }
  int kill_function(int /* event */, ObEvent * /* e */)
  {
    printf("Count is %d \n", count);
    if (count <= 0)
      exit(1);
    if (count > TEST_TIME_SECOND * TEST_THREADS)
      exit(1);
    exit(0);
    return 0;
  }
};

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase

using namespace oceanbase;
using namespace obproxy;
using namespace event;

int main(int /* argc */, const char */* argv */[])
{
  count = 0;

  init_event_system(EVENT_SYSTEM_MODULE_VERSION);
  g_event_processor.start(TEST_THREADS, 1048576); // Hardcoded stacksize at 1MB

  alarm_printer *alrm = new alarm_printer(new_proxy_mutex());
  process_killer *killer = new process_killer(new_proxy_mutex());
  //g_event_processor.schedule_in(killer, HRTIME_SECONDS(10));
  g_event_processor.schedule_every(alrm, HRTIME_SECONDS(1));
  ((ObEThread*)(this_thread()))->schedule_in_local(killer, HRTIME_SECONDS(10));
  this_thread()->execute();
  return 0;
}
