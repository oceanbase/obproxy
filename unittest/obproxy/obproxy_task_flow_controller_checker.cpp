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

#include "obproxy_task_flow_controller_checker.h"
#include <stdlib.h>
#include "obutils/ob_task_flow_controller.h"
#include "obproxy/utils/ob_proxy_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace test
{
// Table Location
MockTableLocation::MockTableLocation(int64_t table_count)
{
  if (table_count < 0) {
    table_count_ = DEFAULT_TABLE_COUNT;
  } else {
    table_count_ = table_count;
  }
  entrys_ = new MockTableEntry[table_count_];
}

MockTableLocation::~MockTableLocation()
{
  if (NULL != entrys_) {
    delete [] entrys_;
    entrys_ = NULL;
  }
}

void MockTableLocation::handle_request(const int64_t table_idx)
{
  if (table_idx < table_count_) {
    ATOMIC_INC(&g_pl_stat.cur_request_count_);
    if (DIRTY == entrys_[table_idx].state_) {
      if (get_pl_task_flow_controller().can_deliver_task()) {
        if (ATOMIC_BCAS(&entrys_[table_idx].state_, DIRTY, AVAIL)) {
          ATOMIC_INC(&g_pl_stat.cur_pl_update_delivery_count_);
          ATOMIC_DEC(&g_pl_stat.total_dirty_count_);
          ATOMIC_SET(&entrys_[table_idx].version_, g_mock_server.version_);
        }
      }
    }
  }
}

void MockTableLocation::handle_responce(const int64_t table_idx)
{
  if (table_idx < table_count_) {
    if (entrys_[table_idx].version_ < g_mock_server.version_) {
      ATOMIC_INC(&g_pl_stat.cur_pl_miss_count_);
      if (ATOMIC_BCAS(&entrys_[table_idx].state_, AVAIL, DIRTY)) {
        ATOMIC_INC(&g_pl_stat.cur_dirty_count_);
        ATOMIC_INC(&g_pl_stat.total_dirty_count_);
        get_pl_task_flow_controller().handle_new_task();
      }
    }
  }
}

// Thread Function
void *worker_thread(void *params)
{
  MockWorker *p_worker = static_cast<MockWorker *>(params);
  while (!is_exit) {
    int64_t table_idx = 0;
    if (OB_SUCCESS != ObRandomNumUtils::get_random_num(0,
                                                       p_worker->table_location_.get_table_count(),
                                                       table_idx)) {
      // unexpected;
      fprintf(stderr, "error to get random num\n");
    } else {
      p_worker->table_location_.handle_request(table_idx);
      usleep(static_cast<uint32_t>(ONE_SECOND / p_worker->query_per_second_));
      p_worker->table_location_.handle_responce(table_idx);
    }
  }
  return NULL;
}

void *server_thread(void *params)
{
  MockServer *p_mock_server = static_cast<MockServer *>(params);
  sleep(2);
  while (!is_exit) {
    p_mock_server->version_++;
    printf("--BEGIN MERGING\n");
    sleep(p_mock_server->merging_time_);
    printf("--FINISH MERGING\n");
    sleep(p_mock_server->merging_interval_);
  }
  return NULL;
}

void *stat_thread(void *params)
{
  UNUSED(params);
  printf("----------RUN INFO-----------------------------------------------\n");
  printf("--idx--|--req--|--pl_miss--|--dirty--|--dirty[all]--|--delivery--\n");

  int64_t idx = 0;
  while (!is_exit) {
    idx++;
    sleep(1);

    printf("%7ld|%7ld|%11ld|%9ld|%14ld|%12ld\n", idx,
                                                 g_pl_stat.cur_request_count_,
                                                 g_pl_stat.cur_pl_miss_count_,
                                                 g_pl_stat.cur_dirty_count_,
                                                 g_pl_stat.total_dirty_count_,
                                                 g_pl_stat.cur_pl_update_delivery_count_);


    g_pl_stat.cur_request_count_ = 0;
    g_pl_stat.cur_pl_miss_count_ = 0;
    g_pl_stat.cur_pl_update_delivery_count_ = 0;
    g_pl_stat.cur_dirty_count_ = 0;
  }

  return NULL;
}

void run_test(const int64_t table_count, const int64_t worker_count, const int64_t worker_qps,
              const int64_t timeout)
{
  MockTableLocation table_location(table_count);
  MockWorker worker(worker_qps, table_location);

  pthread_t *worker_tids = new pthread_t [worker_count];
  pthread_t server_merge_tid;
  pthread_t stat_tid;

  printf("Begin to run test case: \n");
  printf("server : merging time = %u, merging interval = %u\n",
         g_mock_server.merging_time_, g_mock_server.merging_interval_);
  printf("worker : table_count = %ld, worker_count = %ld, worker_qps = %ld\n",
         table_count, worker_count, worker_qps);
  is_exit = false;

  // create threads
  for (int i = 0; i < worker_count; ++i) {
    pthread_create(&worker_tids[i], NULL, worker_thread, &worker);
  }
  pthread_create(&server_merge_tid, NULL, server_thread, &g_mock_server);
  pthread_create(&stat_tid, NULL, stat_thread, NULL);

  // sleep timeout and set thread exit
  sleep(static_cast<unsigned int>(timeout));
  is_exit = true;

  // join threads
  for (int i = 0; i < worker_count; ++i) {
    pthread_join(worker_tids[i], NULL);
  }
  pthread_join(server_merge_tid, NULL);
  pthread_join(stat_tid, NULL);

  // free resouce
  delete [] worker_tids;
}

} // enf of test
} // end of obproxy
} // end of oceanbase

using namespace oceanbase::obproxy::test;

int main() {
  // flow controller conf
  get_pl_task_flow_controller().set_normal_threshold(100);
  get_pl_task_flow_controller().set_limited_threshold(2);
  get_pl_task_flow_controller().set_limited_period(HRTIME_SECONDS(10));
  // server conf
  g_mock_server.merging_interval_ = 10;
  g_mock_server.merging_time_ = 5;
  // worker conf
  int64_t table_count = 200;
  int64_t worker_count = 8;
  int64_t worker_qps = 2000;
  int64_t timeout = 200;
  run_test(table_count, worker_count, worker_qps, timeout);

  return 0;
}
