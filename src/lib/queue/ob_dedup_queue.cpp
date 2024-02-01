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

#include "lib/queue/ob_dedup_queue.h"

namespace oceanbase
{
namespace common
{
#define ERASE_FROM_LIST(head, tail, node) \
  do \
  { \
    if (NULL == node->get_prev()) \
    { \
      head = node->get_next(); \
    } \
    else \
    { \
      node->get_prev()->set_next(node->get_next()); \
    } \
    if (NULL == node->get_next()) \
    { \
      tail = node->get_prev(); \
    } \
    else \
    { \
      node->get_next()->set_prev(node->get_prev()); \
    } \
    node->set_prev(NULL); \
    node->set_next(NULL); \
  } while(0)

ObDedupQueue::ObDedupQueue() : is_inited_(false),
                               thread_num_(DEFAULT_THREAD_NUM),
                               thread_dead_threshold_(DEFALT_THREAD_DEAD_THRESHOLD),
                               hash_allocator_(allocator_),
                               gc_queue_head_(NULL),
                               gc_queue_tail_(NULL)
{
}

ObDedupQueue::~ObDedupQueue()
{
  destroy();
}

int ObDedupQueue::init(int32_t thread_num /*= DEFAULT_THREAD_NUM*/,
                       const int64_t queue_size /*= TASK_QUEUE_SIZE*/,
                       const int64_t task_map_size /*= TASK_MAP_SIZE*/,
                       const int64_t total_mem_limit /*= TOTAL_LIMIT*/,
                       const int64_t hold_mem_limit /*= HOLD_LIMIT*/,
                       const int64_t page_size /*= PAGE_SIZE*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (thread_num <= 0 || thread_num > MAX_THREAD_NUM
             || total_mem_limit <= 0 || hold_mem_limit <= 0
             || page_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(task_queue_sync_.init(ObWaitEventIds::DEDUP_QUEUE_COND_WAIT))) {
    COMMON_LOG(WDIAG, "fail to init task queue sync cond, ", K(ret));
  } else {
    thread_num_ = thread_num;
    setThreadCount(thread_num);
    if (OB_SUCCESS != (ret = allocator_.init(total_mem_limit, hold_mem_limit, page_size))) {
      COMMON_LOG(WDIAG, "allocator init fail", K(total_mem_limit), K(hold_mem_limit), K(page_size),
                 K(ret));
    } else if (OB_SUCCESS != (ret = task_map_.create(task_map_size, &hash_allocator_,
                                                     ObModIds::OB_HASH_BUCKET_TASK_MAP))) {
      COMMON_LOG(WDIAG, "task_map create fail", K(ret));
    } else if (OB_SUCCESS != (ret = task_queue_.init(queue_size))) {
      COMMON_LOG(WDIAG, "task_queue init fail", K(ret));
    } else if (thread_num_ != start()) {
      COMMON_LOG(WDIAG, "start thread fail");
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_inited_ = true;
      _stop = false;
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void ObDedupQueue::destroy()
{
  stop();
  wait();
  // add by yubai
  if (NULL != _thread) {
    delete[] _thread;
    _thread = NULL;
  }
  IObDedupTask *iter = gc_queue_head_;
  while (NULL != iter) {
    IObDedupTask *next = iter->get_next();
    destroy_task_(iter);
    iter = next;
  }
  gc_queue_head_ = NULL;
  gc_queue_tail_ = NULL;
  while (OB_SUCCESS == task_queue_.pop(iter)) {
    destroy_task_(iter);
    iter = NULL;
  }
  task_queue_.destroy();
  task_map_.destroy();
  allocator_.destroy();
  task_queue_sync_.destroy();
  is_inited_ = false;
}

int ObDedupQueue::set_thread_dead_threshold(const int64_t thread_dead_threshold)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (0 > thread_dead_threshold) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "thread_dead_threshold is not valid", K(ret));
  } else {
    thread_dead_threshold_ = thread_dead_threshold;
  }
  return ret;
}

int ObDedupQueue::add_task(const IObDedupTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    MapFunction func(*this, task);
    int hash_ret = task_map_.atomic_refactored(&task, func);
    if (OB_SUCCESS == hash_ret) {
      ret = func.result_code_;
    } else if (OB_HASH_NOT_EXIST == hash_ret) {
      ret = add_task_(task);
    } else {
      COMMON_LOG(WDIAG, "unexpected hash_ret", K(hash_ret));
      ret = hash_ret;
    }
    if (OB_SUCC(ret)) {
      task_queue_sync_.lock();
      task_queue_sync_.signal();
      task_queue_sync_.unlock();
    }
    if (REACH_TIME_INTERVAL(THREAD_CHECK_INTERVAL)) {
      for (int64_t i = 0; i < thread_num_; i++) {
        if (thread_metas_[i].check_dead(thread_dead_threshold_)) {
          COMMON_LOG(WDIAG, "thread maybe dead", K(i), K(thread_metas_[i]));
        }
      }
    }
  }
  return ret;
}

IObDedupTask *ObDedupQueue::copy_task_(const IObDedupTask &task)
{
  IObDedupTask *ret = NULL;
  if (IS_NOT_INIT) {
    COMMON_LOG(WDIAG, "ObDedupQueue is not inited");
  } else {
    int64_t deep_copy_size = task.get_deep_copy_size();
    char *memory = NULL;
    if (NULL == (memory = (char *)allocator_.alloc(deep_copy_size))) {
      COMMON_LOG(WDIAG, "alloc memory fail", K(deep_copy_size));
    } else if (NULL == (ret = task.deep_copy(memory, deep_copy_size))) {
      COMMON_LOG(WDIAG, "deep copy task object fail", K(deep_copy_size), KP(memory));
    } else {
      COMMON_LOG(DEBUG, "deep copy task succ", K(ret), KP(memory), K(deep_copy_size));
      ret->set_memory_ptr(memory);
    }
    if (NULL == ret) {
      if (NULL != memory) {
        allocator_.free(memory);
        memory = NULL;
      }
    }
  }
  return ret;
}

void ObDedupQueue::destroy_task_(IObDedupTask *task)
{
  if (IS_NOT_INIT) {
    COMMON_LOG(WDIAG, "ObDedupQueue is not inited");
  } else if (OB_ISNULL(task)) {
    COMMON_LOG(WDIAG, "invalid argument");
  } else {
    char *memory = task->get_memory_ptr();
    task->~IObDedupTask();
    if (NULL != memory) {
      allocator_.free(memory);
      memory = NULL;
    }
  }
}

int ObDedupQueue::map_callback_(const IObDedupTask &task, TaskMapKVPair &kvpair)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    IObDedupTask *task2remove = NULL;
    IObDedupTask *task2add = NULL;
    if (kvpair.first != kvpair.second
        || NULL == (task2remove = kvpair.second)) {
      COMMON_LOG(WDIAG, "unexpected key null pointer", K(kvpair.first), K(kvpair.second));
      ret = OB_ERR_UNEXPECTED;
    } else if (0 != task2remove->trylock()) {
      ret = OB_EAGAIN;
    } else if (!task2remove->is_process_done()
               || ::oceanbase::common::ObTimeUtility::current_time() < task2remove->get_abs_expired_time()) {
      task2remove->unlock();
      ret = OB_EAGAIN;
    } else if (NULL == (task2add = copy_task_(task))) {
      task2remove->unlock();
      COMMON_LOG(WDIAG, "copy task fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_SUCCESS != (ret = task_queue_.push(task2add))) {
      task2remove->unlock();
      COMMON_LOG(WDIAG, "push task to queue fail", K(ret));
      destroy_task_(task2add);
      task2add = NULL;
    } else {
      gc_queue_sync_.lock();
      ERASE_FROM_LIST(gc_queue_head_, gc_queue_tail_, task2remove);
      gc_queue_sync_.unlock();
      kvpair.first = task2add;
      kvpair.second = task2add;
      task2remove->unlock();
      destroy_task_(task2remove);
      task2remove = NULL;
    }
  }
  return ret;
}

int ObDedupQueue::add_task_(const IObDedupTask &task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else {
    IObDedupTask *task2add = NULL;
    if (NULL == (task2add = copy_task_(task))) {
      COMMON_LOG(WDIAG, "copy task fail");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      // lockstat is to aviod that job not insert task_queue was deleted from map by other thread.
      task2add->lock();
      int hash_ret = task_map_.set_refactored(task2add, task2add);
      if (OB_SUCCESS != hash_ret) {
        COMMON_LOG(WDIAG, "set to task map fail", K(hash_ret));
        task2add->unlock();
        destroy_task_(task2add);
        task2add = NULL;
        ret = OB_EAGAIN;
      } else if (OB_SUCCESS != (ret = task_queue_.push(task2add))) {
        COMMON_LOG(WDIAG, "push task to queue fail", K(ret));
        if (OB_SUCCESS != (hash_ret = task_map_.erase_refactored(task2add))) {
          task2add->unlock();
          COMMON_LOG(WDIAG, "unexpected erase from task_map fail", K(hash_ret));
        } else {
          task2add->unlock();
          destroy_task_(task2add);
          task2add = NULL;
        }
      } else {
        task2add->unlock();
      }
    }
  }
  return ret;
}

bool ObDedupQueue::gc_()
{
  bool bret = false;
  IObDedupTask *task_list[GC_BATCH_NUM];
  int64_t task_list_size = 0;
  if (0 == gc_queue_sync_.trylock()) {
    bret = true;
    IObDedupTask *iter = gc_queue_head_;
    while (NULL != iter
           && GC_BATCH_NUM > task_list_size) {
      IObDedupTask *next = iter->get_next();
      if (iter->is_process_done()
          && ::oceanbase::common::ObTimeUtility::current_time() > iter->get_abs_expired_time()
          && 0 == iter->trylock()) {
        task_list[task_list_size++] = iter;
        ERASE_FROM_LIST(gc_queue_head_, gc_queue_tail_, iter);
      }
      iter = next;
    }
    gc_queue_sync_.unlock();
  }
  for (int64_t i = 0; i < task_list_size; i++) {
    if (NULL == task_list[i]) {
      continue;
    }
    int hash_ret = task_map_.erase_refactored(task_list[i]);
    if (OB_SUCCESS != hash_ret) {
      task_list[i]->unlock();
      COMMON_LOG(WDIAG, "unexpected erase from task_map fail", K(hash_ret));
    } else {
      task_list[i]->unlock();
      destroy_task_(task_list[i]);
      task_list[i] = NULL;
    }
  }
  return bret;
}

void ObDedupQueue::run(obsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  int tmp_ret = OB_SUCCESS;
  int64_t thread_pos = (int64_t)arg;
  ThreadMeta &thread_meta = thread_metas_[thread_pos];
  thread_meta.init();
  while (!_stop) {
    IObDedupTask *task2process = NULL;
    if (OB_SUCCESS != (tmp_ret = task_queue_.pop(task2process)) && OB_UNLIKELY(tmp_ret != OB_ENTRY_NOT_EXIST)) {
      COMMON_LOG(WDIAG, "task_queue_.pop error", K(tmp_ret), K(task2process));
    } else if (NULL != task2process) {
      thread_meta.on_process_start(task2process);
      task2process->process();
      thread_meta.on_process_end();

      gc_queue_sync_.lock();
      task2process->set_next(NULL);
      if (NULL == gc_queue_tail_) {
        task2process->set_prev(NULL);
        gc_queue_head_ = task2process;
        gc_queue_tail_ = task2process;
      } else {
        task2process->set_prev(gc_queue_tail_);
        gc_queue_tail_->set_next(task2process);
        gc_queue_tail_ = task2process;
      }
      gc_queue_sync_.unlock();
      task2process->set_process_done();
    }

    thread_meta.on_gc_start();
    bool gc_done = gc_();
    thread_meta.on_gc_end();

    if ((NULL != gc_queue_head_ && gc_done)
        || NULL != task2process) {
      // need not wait
    } else {
      task_queue_sync_.lock();
      if (0 == task_queue_.get_total()) {
        task_queue_sync_.wait(QUEUE_WAIT_TIME_MS);
      }
      task_queue_sync_.unlock();
    }
  }
}
} // namespace common
} // namespace oceanbase
