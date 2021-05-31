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

#define private public
#define protected public
#include <gtest/gtest.h>
#include <pthread.h>
#include "test_eventsystem_api.h"
#include "ob_priority_event_queue.h"

namespace oceanbase
{
namespace obproxy
{
using namespace proxy;
using namespace common;
using namespace event;

enum TestEnqueueType {
  TEST_BOUNDARY_OF_EACH = 0,
  TEST_MIDDLE_OF_EACH,
  TEST_ALL_IN_MIN,
  TEST_ALL_IN_MAX,
  TEST_TOTAL_ENQUEUE_TYPE,
  TEST_MAX_SECOND,
  TEST_MAX_SECOND_LARGE,
  TEST_MAX_MINUTES,
  TEST_MAX_MINUTES_LARGE,
  TEST_MAX_TOTAL_ENQUEUE_TYPE,
};

#define TEST_DELTA_TIME (PQ_BUCKET_TIME(i))

class TestPriorityEventQueue : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown();
  void clear_priority_event_queue(ObPriorityEventQueue *priority_queue);
  void clear_event_arrays(ObEvent *event_arrays_ptr[ObPriorityEventQueue::PQ_LIST_COUNT]);
  void check_enqueue(TestEnqueueType type, bool cancelled = false);
  void common_check_enqueue_specific(int64_t site, ObHRTime base_time = 0);
  ObEvent *common_check_enqueue(int64_t site, ObHRTime delta_time);
  void check_enqueue_result(int64_t site, ObEvent *tmp_event);
  void check_remove();
  void check_dequeue_ready();
  void check_earliest_timeout();
  void check_check_ready(ObHRTime delta_time, bool test_increase = false, bool test_empty = false);
  void check_check_ready_result(ObHRTime now_time, int64_t site_array[], bool test_empty = false);
  void check_check_ready_execute();

public:
  ObPriorityEventQueue priority_queue_;
  ObHRTime cur_time_;
  ObEvent *event_arrays_ptr_[ObPriorityEventQueue::PQ_LIST_COUNT];
};

void save_event_site_in_queue(ObEvent *event_arrays_ptr[], int64_t site_array[]);

void TestPriorityEventQueue::SetUp()
{
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    event_arrays_ptr_[i] = NULL;
  }
  cur_time_ = get_hrtime_internal();
}

void TestPriorityEventQueue::TearDown()
{
  clear_priority_event_queue(&priority_queue_);
  clear_event_arrays(event_arrays_ptr_);
}

void TestPriorityEventQueue::clear_priority_event_queue(ObPriorityEventQueue *priority_queue)
{
  ASSERT_TRUE(NULL != priority_queue);
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    ObEvent *event;
    Que(ObEvent, link_) q = priority_queue->after_queue_[i];
    priority_queue->after_queue_[i].reset();
    while (NULL != (event = q.dequeue())) {
      event->cancelled_ = true;
      event->in_the_priority_queue_ = 0;
      event->free();
    }
  }
}

void TestPriorityEventQueue::clear_event_arrays(ObEvent *event_arrays_ptr[ObPriorityEventQueue::PQ_LIST_COUNT])
{
  ASSERT_TRUE(NULL != event_arrays_ptr);
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    if (NULL != event_arrays_ptr[i]
        && !event_arrays_ptr[i]->cancelled_
        && 0 == event_arrays_ptr[i]->in_the_priority_queue_) {
      event_arrays_ptr[i]->free();
      event_arrays_ptr[i] = NULL;
    }
  }
}

void TestPriorityEventQueue::common_check_enqueue_specific(int64_t site, ObHRTime base_time)
{
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    event_arrays_ptr_[i] = common_check_enqueue(site, base_time + HRTIME_MSECONDS(i) % PQ_BUCKET_TIME(0));
  }
}

void TestPriorityEventQueue::check_enqueue(TestEnqueueType type, bool cancelled)
{
  switch (type) {
  case TEST_BOUNDARY_OF_EACH: {
    for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {//boundary of each
      event_arrays_ptr_[i] = common_check_enqueue(i, PQ_BUCKET_TIME(i));
    }
    break;
  }
  case TEST_MIDDLE_OF_EACH: {
    event_arrays_ptr_[0] = common_check_enqueue(0, PQ_BUCKET_TIME(0) >> 1);
    for (int64_t i = 1; i < ObPriorityEventQueue::PQ_LIST_COUNT ; ++i) {//middle of each
      event_arrays_ptr_[i] = common_check_enqueue(i,
          (PQ_BUCKET_TIME(i - 1) + PQ_BUCKET_TIME(i)) >> 1);
    }
    break;
  }
  case TEST_ALL_IN_MIN: {
    common_check_enqueue_specific(0);//all in min
    break;
  }
  case TEST_ALL_IN_MAX: {
    common_check_enqueue_specific(9, PQ_BUCKET_TIME(8) + PQ_BUCKET_TIME(0));//all in max
    break;
  }
  case TEST_MAX_SECOND: {
    common_check_enqueue_specific(9, HRTIME_SECONDS(1));
    break;
  }
  case TEST_MAX_SECOND_LARGE: {
    common_check_enqueue_specific(9, HRTIME_SECONDS(30));
    break;
  }
  case TEST_MAX_MINUTES: {
    common_check_enqueue_specific(9, HRTIME_MINUTES(5));
    break;
  }
  case TEST_MAX_MINUTES_LARGE: {
    common_check_enqueue_specific(9, HRTIME_MINUTES(30));
    break;
  }
  case TEST_TOTAL_ENQUEUE_TYPE:
  case TEST_MAX_TOTAL_ENQUEUE_TYPE: {
    break;
  }
  default: {
    LOG_ERROR("check_enqueue, invalid enqueue type");
    break;
  }
  }

  if (cancelled) {
    for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
      event_arrays_ptr_[i]->cancelled_ = true;
    }
  } else {}
}

ObEvent *TestPriorityEventQueue::common_check_enqueue(int64_t site, ObHRTime delta_time)
{
  ObEvent *tmp_event = NULL;
  if (NULL == (tmp_event = op_reclaim_alloc(ObEvent))) {//destroyed in teardown if fail
    LOG_ERROR("fail to alloc mem for ObEvent");
  } else {
    tmp_event->timeout_at_ = cur_time_ + delta_time;
    priority_queue_.enqueue(tmp_event, cur_time_);
    check_enqueue_result(site, tmp_event);
  }
  return tmp_event;
}

void TestPriorityEventQueue::check_enqueue_result(int64_t site, ObEvent *tmp_event)
{
  ASSERT_TRUE(NULL != tmp_event);
  ASSERT_EQ(1, tmp_event->in_the_priority_queue_);
  ASSERT_EQ(site, tmp_event->in_heap_);
  ASSERT_FALSE(priority_queue_.after_queue_[site].empty());
  ASSERT_TRUE(priority_queue_.after_queue_[site].in(tmp_event));
}

void TestPriorityEventQueue::check_remove()
{
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {//remove
    ASSERT_TRUE(priority_queue_.after_queue_[event_arrays_ptr_[i]->in_heap_].in(event_arrays_ptr_[i]));
    priority_queue_.remove(event_arrays_ptr_[i]);//event_arrays_ptr_[i] destroy in teardown
    ASSERT_EQ(0, event_arrays_ptr_[i]->in_the_priority_queue_);
    ASSERT_FALSE(priority_queue_.after_queue_[event_arrays_ptr_[i]->in_heap_].in(event_arrays_ptr_[i]));
  }
  clear_event_arrays(event_arrays_ptr_);
}

void TestPriorityEventQueue::check_dequeue_ready()
{
  ObEvent *event = NULL;
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {//destroyed in teardown if fail
    event = priority_queue_.dequeue_ready();
    ASSERT_TRUE(event == event_arrays_ptr_[i]);
    ASSERT_EQ(0, event->in_the_priority_queue_);
    ASSERT_FALSE(priority_queue_.after_queue_[0].in(event));
  }
  clear_event_arrays(event_arrays_ptr_);
}

void TestPriorityEventQueue::check_earliest_timeout()
{
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {//not empty
    ASSERT_EQ(priority_queue_.earliest_timeout(),
        cur_time_ + (PQ_BUCKET_TIME(event_arrays_ptr_[i]->in_heap_) >> 1));
    ASSERT_TRUE(event_arrays_ptr_[i] ==
        priority_queue_.after_queue_[event_arrays_ptr_[i]->in_heap_].dequeue());
    event_arrays_ptr_[i]->in_the_priority_queue_ = 0;
  }
  clear_event_arrays(event_arrays_ptr_);
}

void save_event_site_in_queue(ObEvent *event_arrays_ptr[], int64_t site_array[])
{
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    site_array[i] = event_arrays_ptr[i]->in_heap_;
  }
}

void TestPriorityEventQueue::check_check_ready_result(ObHRTime now_time, int64_t site_array[],
    bool test_empty)
{
  uint64_t check_buckets = (uint64_t)(now_time / PQ_BUCKET_TIME(0));
  uint64_t todo_buckets = check_buckets ^ priority_queue_.last_check_buckets_;//pass N times of 5ms
  priority_queue_.check_ready(now_time);
  ASSERT_EQ(priority_queue_.last_check_time_, now_time);
  ASSERT_EQ(priority_queue_.last_check_buckets_, check_buckets);

  todo_buckets &= ((1 << (ObPriorityEventQueue::PQ_LIST_COUNT - 1)) - 1);
  int64_t k = 0;
  while (todo_buckets) {
    k++;
    todo_buckets >>= 1;
  }
  if (test_empty){
    for (int64_t i = 1; i <= k; ++i) {
      ASSERT_TRUE(priority_queue_.after_queue_[i].empty());
    }
  } else {
    for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
      if (site_array[i] >= 1 && site_array[i] <= k) {
        ObHRTime diff_time = event_arrays_ptr_[i]->timeout_at_ - now_time;
        int64_t j = 0;
        for (j = site_array[i]; j > 0 && diff_time <= PQ_BUCKET_TIME(j - 1);) {
          j--;
        }
        ASSERT_EQ(1, event_arrays_ptr_[i]->in_the_priority_queue_);
        ASSERT_TRUE(priority_queue_.after_queue_[j].in(event_arrays_ptr_[i]));
      } else {}
    }
  }
}

void TestPriorityEventQueue::check_check_ready(ObHRTime delta_time, bool test_increase,
    bool test_empty)
{
  ObHRTime now_time = cur_time_;
  ObHRTime tmp_delta_time = 0;
  priority_queue_.last_check_time_ = cur_time_;
  int64_t site_array[ObPriorityEventQueue::PQ_LIST_COUNT] = {0};
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    save_event_site_in_queue(event_arrays_ptr_, site_array);
    tmp_delta_time = (test_increase * TEST_DELTA_TIME) + delta_time;
    now_time = now_time + tmp_delta_time;
    check_check_ready_result(now_time, site_array, test_empty);
  }
  clear_priority_event_queue(&priority_queue_);
  clear_event_arrays(event_arrays_ptr_);
}

void TestPriorityEventQueue::check_check_ready_execute()
{
  bool test_finish = false;
  bool done_one = false;
  int finish_num = 0;
  ObEvent *event = NULL;
  ObHRTime now_time = cur_time_;
  ObHRTime next_time = 0;
  priority_queue_.last_check_time_ = cur_time_;

  while (!test_finish) {
    do {
      done_one = false;
      priority_queue_.check_ready(now_time);
      while ((event = priority_queue_.dequeue_ready())) {
        ASSERT_TRUE(NULL != event);
        done_one = true;
        finish_num++;
        ASSERT_TRUE((now_time - event->timeout_at_) < HRTIME_MSECONDS(30));
        ASSERT_TRUE((event->timeout_at_ - now_time) <= HRTIME_MSECONDS(1));
      }
    } while (done_one);

    if (finish_num >= ObPriorityEventQueue::PQ_LIST_COUNT) {
      test_finish = true;
    } else {
      next_time = priority_queue_.earliest_timeout();
      if (next_time > (now_time + 30 * HRTIME_MSECOND)) {
        next_time = now_time + 30 * HRTIME_MSECOND;
      }
      now_time = next_time;
    }
  }

  clear_priority_event_queue(&priority_queue_);
  clear_event_arrays(event_arrays_ptr_);
}

TEST_F(TestPriorityEventQueue, test_ObPriorityEventQueue)
{
  LOG_DEBUG("test_ObPriorityEventQueue");//check ObPriorityEventQueue()
  ObPriorityEventQueue tmp_priority_queue;
  cur_time_ = get_hrtime_internal();
  ASSERT_LE(tmp_priority_queue.last_check_time_, cur_time_);
  ASSERT_GE(tmp_priority_queue.last_check_time_ + PQ_BUCKET_TIME(0), cur_time_);
  ASSERT_EQ(tmp_priority_queue.last_check_buckets_, cur_time_/PQ_BUCKET_TIME(0));
  for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
    ASSERT_TRUE(tmp_priority_queue.after_queue_[i].empty());
  }
}

TEST_F(TestPriorityEventQueue, test_enqueue)
{
  LOG_DEBUG("test_enqueue");
  for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i);
  }
}

TEST_F(TestPriorityEventQueue, test_remove)
{
  LOG_DEBUG("test_remove");
  for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i);
    check_remove();
  }
}

TEST_F(TestPriorityEventQueue, test_remove_not_in_queue)
{
  LOG_DEBUG("test_remove not in queue in fact");
  if (NULL == (event_arrays_ptr_[0] = op_reclaim_alloc(ObEvent))) {//destroyed in teardown if fail
    LOG_ERROR("fail to alloc mem for ObEvent");
    ASSERT_TRUE(false);
  } else {
    for (int64_t i = 0; i < ObPriorityEventQueue::PQ_LIST_COUNT; ++i) {
      ASSERT_FALSE(priority_queue_.after_queue_[i].in(event_arrays_ptr_[0]));
      event_arrays_ptr_[0]->in_the_priority_queue_ = 1;//it is not in queue in fact
      priority_queue_.remove(event_arrays_ptr_[0]);
      ASSERT_EQ(0, event_arrays_ptr_[0]->in_the_priority_queue_);
      ASSERT_FALSE(priority_queue_.after_queue_[i].in(event_arrays_ptr_[0]));
    }
  }
}

TEST_F(TestPriorityEventQueue, test_dequeue_ready)
{
  LOG_DEBUG("test_dequeue_ready");
  ASSERT_TRUE(priority_queue_.after_queue_[0].empty());//empty
  ASSERT_TRUE(NULL == priority_queue_.dequeue_ready());
  check_enqueue(TEST_ALL_IN_MIN);//not empty
  check_dequeue_ready();
}

TEST_F(TestPriorityEventQueue, test_earliest_timeout)
{
  LOG_DEBUG("test_earliest_timeout");
  priority_queue_.last_check_time_ =  cur_time_;

  ASSERT_EQ(priority_queue_.earliest_timeout(), cur_time_ + HRTIME_FOREVER);//empty

  for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i);//not empty
    check_earliest_timeout();
  }
}

TEST_F(TestPriorityEventQueue, test_check_ready_small_increment)
{
  LOG_DEBUG("test_check_ready_small_increment");
  ObHRTime delta[2] = {PQ_BUCKET_TIME(0) >> 1, (PQ_BUCKET_TIME(1) + PQ_BUCKET_TIME(0)) >> 1};
  for (int64_t j = 0; j < 2; ++j) {
    for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
      check_enqueue((TestEnqueueType)i);
      check_check_ready(delta[j]);
    }
  }
}

TEST_F(TestPriorityEventQueue, test_check_ready_gradually_increasing)
{
  LOG_DEBUG("test_check_ready_gradually_increasing");
  ObHRTime delta[2] = {PQ_BUCKET_TIME(0) >> 1, (PQ_BUCKET_TIME(1) + PQ_BUCKET_TIME(0)) >> 1};

  for (int64_t j = 0; j < 2; ++j) {
    for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
      check_enqueue((TestEnqueueType)i);
      check_check_ready(delta[j], true);
    }
  }
}

TEST_F(TestPriorityEventQueue, test_check_ready_cancelled)
{
  LOG_DEBUG("test_check_ready_cancelled");
  //delta == PQ_BUCKET_TIME(0) >> 1
  for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i, true);
    check_check_ready(PQ_BUCKET_TIME(0) >> 1, false, true);
  }
  //delta == PQ_BUCKET_TIME(k) + ((PQ_BUCKET_TIME(1) + PQ_BUCKET_TIME(0)) >> 1)
  for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i, true);
    check_check_ready((PQ_BUCKET_TIME(1) + PQ_BUCKET_TIME(0)) >> 1, true, true);
  }
}

TEST_F(TestPriorityEventQueue, test_check_ready_execute_simulation)
{
  LOG_DEBUG("test_check_ready_execute_simulation");
  for (int64_t i = 0; i < TEST_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i);
    check_check_ready_execute();
  }

  for (int64_t i = TEST_TOTAL_ENQUEUE_TYPE + 1; i < TEST_MAX_TOTAL_ENQUEUE_TYPE; ++i) {
    check_enqueue((TestEnqueueType)i);
    check_check_ready_execute();
  }
}

} // end of namespace obproxy
} // end of namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
