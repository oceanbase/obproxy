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

#include "ob_io_manager.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/wait_event/ob_wait_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/thread_local/ob_tsi_factory.h"

namespace oceanbase
{
namespace common
{
ObIOInfo::ObIOInfo()
  : fd_(0),
    offset_(0),
    size_(0),
    io_desc_()
{
}

ObIOInfo::~ObIOInfo()
{
}

ObIOController::ObIOController()
  : buf_(NULL),
    data_offset_(0),
    io_desc_(),
    callback_(NULL),
    req_submit_time_(0),
    io_submit_time_(0),
    io_finish_time_(0),
    wait_begin_time_(0),
    wait_end_time_(0),
    has_finished_(false),
    is_success_(false),
    need_submit_(true),
    ref_cnt_(0)
{
  memset(&iocb_, 0, sizeof(iocb));
  cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT);
}

ObIOController::~ObIOController()
{
  if (NULL != callback_) {
    callback_->~ObIOCallback();
  }
}


ObIOHandle::ObIOHandle()
  : control_(NULL),
    processor_(NULL),
    inited_(false)
{
}

ObIOHandle::~ObIOHandle()
{
  reset();
}

ObIOHandle::ObIOHandle(const ObIOHandle &other)
  : control_(NULL),
    processor_(NULL),
    inited_(false)
{
  *this = other;
}

ObIOHandle &ObIOHandle::operator=(const ObIOHandle &other)
{
  if (this != &other) {
    reset();
    if (NULL != other.control_) {
      ObIOManager::get_instance().inc_ref(other.control_);
    }
    control_ = other.control_;
    processor_ = other.processor_;
    inited_ = other.inited_;
  }
  return *this;
}


int ObIOHandle::init(ObIOController *control, ObIOProcessor *processor)
{
  int ret = OB_SUCCESS;
  if (NULL == control || NULL == processor) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WDIAG, "The inner control_ is not NULL, ", K(ret));
  } else {
    ObIOManager::get_instance().inc_ref(control);
    control_ = control;
    processor_ = processor;
    inited_ = true;
  }
  return ret;
}

int ObIOHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOHandle has not been inited, ", K(ret));
  } else if (timeout_ms < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", K(timeout_ms), K(ret));
  } else {
    if (!control_->has_finished_) {
      ObWaitEventGuard wait_guard(
        control_->io_desc_.wait_event_no_,
        timeout_ms,
        control_->iocb_.aio_fildes,
        control_->iocb_.u.c.offset,
        control_->iocb_.u.c.nbytes);
      int wait_timeout = MAX_IO_WAIT_TIME_MS;
      if (timeout_ms > 0 && timeout_ms <= MAX_IO_WAIT_TIME_MS) {
        wait_timeout = (int) timeout_ms;
      }

      control_->cond_.lock();
      if (!control_->has_finished_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = processor_->record_wait_begin(*control_))) {
          COMMON_LOG(WDIAG, "fail to record wait begin, ", K(tmp_ret));
        }
        if (OB_FAIL(control_->cond_.wait(wait_timeout))) {
          COMMON_LOG(WDIAG, "fail to wait cond, ", K(ret));
        }
        if (OB_SUCCESS == tmp_ret) {
          if (OB_SUCCESS != (tmp_ret = processor_->record_wait_end(*control_))) {
            COMMON_LOG(WDIAG, "fail to record wait end, ", K(tmp_ret));
          }
        }

        if (OB_TIMEOUT == ret) {
          COMMON_LOG(WDIAG, "IO wait timeout, ", K(timeout_ms), K(ret), K(get_wait_time_us()),
              K(get_queue_wait_time_us()), K(get_disk_wait_time_us()));
        }
      }
      control_->cond_.unlock();
    }

    if (OB_SUCC(ret)) {
      if (!control_->is_success_) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WDIAG, "IO error, ", K(ret));
      }
    }

    if (IO_CMD_PREAD == control_->iocb_.aio_lio_opcode) {
      EVENT_INC(ObStatEventIds::IO_READ_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_BYTES, control_->iocb_.u.c.nbytes);
      EVENT_ADD(ObStatEventIds::IO_READ_DELAY, control_->io_finish_time_ - control_->io_submit_time_);
    } else {
      EVENT_INC(ObStatEventIds::IO_WRITE_COUNT);
      EVENT_ADD(ObStatEventIds::IO_WRITE_BYTES, control_->iocb_.u.c.nbytes);
      EVENT_ADD(ObStatEventIds::IO_WRITE_DELAY, control_->io_finish_time_ - control_->io_submit_time_);
    }
  }

  return ret;
}

const char *ObIOHandle::get_buffer()
{
  const char *buf = NULL;
  if (NULL != control_ && NULL != control_->buf_) {
    if (control_->has_finished_ && control_->is_success_ && NULL == control_->callback_) {
      buf = control_->buf_ + control_->data_offset_;
    }
  }
  return buf;
}

int64_t ObIOHandle::get_wait_time_us()
{
  int64_t wait_time = 0;
  if (NULL != control_) {
    wait_time = control_->wait_end_time_ - control_->wait_begin_time_;
  }
  return wait_time;
}

int64_t ObIOHandle::get_queue_wait_time_us()
{
  int64_t wait_time = 0;
  if (NULL != control_) {
    wait_time = control_->io_submit_time_ - control_->wait_begin_time_;
    if (wait_time < 0) {
      wait_time = 0;
    }
  }
  return wait_time;
}

int64_t ObIOHandle::get_disk_wait_time_us()
{
  int64_t wait_time = 0;
  if (NULL != control_) {
    wait_time = control_->wait_end_time_ - control_->io_submit_time_;
    if (wait_time < 0) {
      wait_time = 0;
    }
  }
  return wait_time;
}

void ObIOHandle::reset()
{
  if (NULL != control_) {
    control_->need_submit_ = false;
    ObIOManager::get_instance().dec_ref(control_);
    control_ = NULL;
  }
  processor_ = NULL;
  inited_ = false;
}

int64_t ObIOHandle::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", KP(buf), K(buf_len), K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOHandle has not been inited, ", K(ret));
  } else {
    const int64_t ref_cnt = control_->ref_cnt_;
    J_KV("data_offset_", control_->data_offset_,
        "io_desc_", control_->io_desc_,
        "req_submit_time_", control_->req_submit_time_,
        "io_submit_time_", control_->io_submit_time_,
        "io_finish_time_", control_->io_finish_time_,
        "wait_begin_time_", control_->wait_begin_time_,
        "wait_end_time_", control_->wait_end_time_,
        "has_finished_", control_->has_finished_,
        "is_success_", control_->is_success_,
        "need_submit_", control_->need_submit_,
        "ref_cnt_", ref_cnt);
  }
  return pos;
}

ObIOQueue::ObIOQueue()
  : item_array_(NULL),
    array_size_(0),
    producer_(0),
    consumer_(0),
    wait_cnt_(0),
    wait_begin_time_sum_(0),
    min_queue_hold_(0),
    io_weight_(0),
    wait_score_(0),
    inited_(false)
{
}

ObIOQueue::~ObIOQueue()
{
}

int ObIOQueue::init(ObIOController **item_array, const int64_t array_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WDIAG, "The ObIOQueue has been inited, ", K(ret));
  } else if (NULL == item_array || array_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", K(item_array), K(array_size), K(ret));
  } else {
    item_array_ = item_array;
    array_size_ = array_size;
    producer_ = 0;
    consumer_ = 0;
    wait_cnt_ = 0;
    min_queue_hold_ = 0;
    io_weight_ = 1;
    wait_score_ = 0;
    inited_ = true;
  }
  return ret;
}

void ObIOQueue::destroy()
{
  item_array_ = NULL;
  array_size_ = 0;
  producer_ = 0;
  consumer_ = 0;
  wait_cnt_ = 0;
  min_queue_hold_ = 0;
  io_weight_ = 0;
  wait_score_ = 0;
  inited_ = false;
}

int ObIOQueue::push(ObIOController *item)
{
  int ret = OB_SUCCESS;
  if (NULL == item) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", KP(item), K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOQueue has not been inited, ", K(ret));
  } else {
    item->req_submit_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    ObSpinLockGuard guard(lock_);
    if (producer_ > array_size_ && consumer_ > array_size_) {
      producer_ -= array_size_;
      consumer_ -= array_size_;
    }
    if (producer_ >= consumer_ + array_size_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      item_array_[producer_ % array_size_] = item;
      ++producer_;
    }
  }
  return ret;
}

int ObIOQueue::pop(ObIOController *&item)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOQueue has not been inited, ", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    if (consumer_ >= producer_) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(WDIAG, "The entry not exist, ", K(ret));
    } else {
      item = item_array_[consumer_ % array_size_];
      ++consumer_;
      item->io_submit_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    }
  }
  return ret;
}

int ObIOQueue::record_wait_begin(ObIOController *item)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOQueue has not been inited, ", K(ret));
  } else if (NULL == item) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", KP(item), K(ret));
  } else {
    item->wait_begin_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    ObSpinLockGuard guard(lock_);
    ++wait_cnt_;
    wait_begin_time_sum_ += item->wait_begin_time_;
  }
  return ret;
}

int ObIOQueue::record_wait_end(ObIOController *item)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOQueue has not been inited, ", K(ret));
  } else if (NULL == item) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", KP(item), K(ret));
  } else {
    item->wait_end_time_ = ::oceanbase::common::ObTimeUtility::current_time();
    ObSpinLockGuard guard(lock_);
    --wait_cnt_;
    wait_begin_time_sum_ -= item->wait_begin_time_;
  }
  return ret;
}

void ObIOQueue::refresh()
{
  int64_t cur_time = ::oceanbase::common::ObTimeUtility::current_time();
  ObSpinLockGuard guard(lock_);
  if (wait_cnt_ > 0) {
    wait_score_ = (cur_time - wait_begin_time_sum_ / wait_cnt_) * io_weight_;
  } else {
    wait_score_ = 0;
  }
}

int ObIOQueue::can_pop(bool &bool_ret)
{
  int ret = OB_SUCCESS;
  bool_ret = false;
  int64_t cur_time = ::oceanbase::common::ObTimeUtility::current_time();
 if (!inited_) {
  ret = OB_NOT_INIT;
  COMMON_LOG(WDIAG, "The ObIOQueue has not been inited, ", K(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    if (producer_ - consumer_ > min_queue_hold_) {
      bool_ret = true;
    } else if (producer_ > consumer_) {
      ObIOController *item = item_array_[consumer_ % array_size_];
      if(!item->need_submit_){
        bool_ret = true;
      } else if (item->req_submit_time_ > 0 && cur_time - item->req_submit_time_ > 1000L * 1000L) {
        bool_ret = true;
      }
    }
  }
  return ret;
}

ObIOProcessor::ObIOProcessor()
  : event_thread_cnt_(1), allocator_(ObModIds::OB_SSTABLE_AIO, common::OB_MALLOC_BIG_BLOCK_SIZE), inited_(false)
{
  memset(&context_, 0, sizeof(context_));
  memset(&submited_cnt_, 0, sizeof(submited_cnt_));
}

ObIOProcessor::~ObIOProcessor()
{
  destroy();
}

int ObIOProcessor::init(const int64_t event_thread_cnt)
{
  int ret = OB_SUCCESS;
  if (event_thread_cnt <= 0 || event_thread_cnt > MAX_THREAD_CNT ) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", K(event_thread_cnt), K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WDIAG, "The ObIOProcessor has been inited, ", K(ret));
  } else if (OB_FAIL(queue_cond_.init(ObWaitEventIds::IO_PROCESSOR_COND_WAIT))) {
    COMMON_LOG(WDIAG, "Fai to init queue cond, ", K(ret));
  } else {
    memset(&context_, 0, sizeof(context_));
    memset(&submited_cnt_, 0, sizeof(submited_cnt_));
    ObIOController **item_array = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < IO_QUEUE_CNT; ++i) {
      if (NULL == (item_array = (ObIOController**) allocator_.alloc(sizeof(ObIOController*) * MAX_IO_QUEUE_DEPTH))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WDIAG, "Fail to allocate memory, ", K(ret));
      } else if (OB_FAIL(queue_[i].init(item_array, MAX_IO_QUEUE_DEPTH))) {
        COMMON_LOG(WDIAG, "Fail to init queue, ", K(i), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < event_thread_cnt; ++i) {
      if (0 != io_setup(MAX_AIO_EVENT_CNT, &context_[i])) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WDIAG, "Fail to setup aio context, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      queue_[0].set_weight(8192);
      queue_[1].set_weight(8192);
      queue_[2].set_weight(1);
      queue_[3].set_weight(1);
      event_thread_cnt_ = event_thread_cnt;
      inited_ = true;
      setThreadCount((int) (event_thread_cnt + 1));
      if ((event_thread_cnt + 1) != start()) {
        inited_ = false;
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WDIAG, "The started thread num is not enough, ", K(event_thread_cnt), K(ret));
      }
    }
  }

  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObIOProcessor::destroy()
{
  inited_ = false;
  stop();
  wait();
  for (int64_t i = 0; i < event_thread_cnt_; ++i) {
    io_destroy(context_[i]);
  }
  for (int64_t i = 0; i < IO_QUEUE_CNT; ++i) {
    queue_[i].destroy();
  }
  queue_cond_.destroy();
  allocator_.reset();
}

int ObIOProcessor::submit(ObIOController &control)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOProcessor has not been inited, ", K(ret));
  } else {
    ObIOManager::get_instance().inc_ref(&control);
    int64_t queue_idx = get_queue_idx(control);
    if (OB_FAIL(queue_[queue_idx].push(&control))) {
      control.is_success_ = false;
      if (NULL != control.callback_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = control.callback_->process(control.is_success_))) {
          COMMON_LOG(WDIAG, "Fail to callback, ", K(tmp_ret));
        }
      }
      control.has_finished_ = true;
      ObIOManager::get_instance().dec_ref(&control);
      ret = OB_IO_ERROR;
      COMMON_LOG(WDIAG, "Fail to submit io request to queue, ", K(ret));
    } else {
      queue_cond_.lock();
      queue_cond_.signal();
      queue_cond_.unlock();
    }
  }
  return ret;
}

int ObIOProcessor::record_wait_begin(ObIOController &control)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOProcessor has not been inited, ", K(ret));
  } else {
    if (OB_FAIL(queue_[get_queue_idx(control)].record_wait_begin(&control))) {
      COMMON_LOG(WDIAG, "Fail to record wait begin, ", K(ret));
    }
  }
  return ret;
}

int ObIOProcessor::record_wait_end(ObIOController &control)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOProcessor has not been inited, ", K(ret));
  } else {
    if (OB_FAIL(queue_[get_queue_idx(control)].record_wait_end(&control))) {
      COMMON_LOG(WDIAG, "Fail to record wait end, ", K(ret));
    }
  }
  return ret;
}

void ObIOProcessor::balance()
{
  if (!inited_) {
    int ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOProcessor has not been inited, ", K(ret));
  } else {
    int64_t score = 0;
    int64_t max_idx = -1;
    for (int64_t i = 0; i < IO_QUEUE_CNT; ++i) {
      queue_[i].refresh();
      score = queue_[i].get_wait_score();
      if (0 == score) {
        //do nothing
      } else {
        if (-1 == max_idx || queue_[max_idx].get_wait_score() < score) {
          max_idx = i;
        }
      }
    }

    if (-1 != max_idx) {
      queue_[max_idx].dec_queue_hold();
      for (int64_t i = 0; i < IO_QUEUE_CNT; ++i) {
        if (i != max_idx && 0 != queue_[i].get_wait_score()) {
          queue_[i].inc_queue_hold();
        }
      }
      int tmp_ret = OB_SUCCESS;
      bool can_pop = false;
      if (OB_SUCCESS != (tmp_ret = queue_[max_idx].can_pop(can_pop))) {
        COMMON_LOG(WDIAG, "Fail to test can pop, ", K(tmp_ret));
      } else if (can_pop) {
        queue_cond_.lock();
        queue_cond_.signal();
        queue_cond_.unlock();
      }
    }
  }
}


void ObIOProcessor::run(obsys::CThread *thread, void *arg)
{
  UNUSED(thread);
  int64_t thread_id = ((int64_t) arg) % _threadCount;
  if (0 == thread_id) {
    inner_submit();
  } else {
    inner_get_events(thread_id - 1);
  }
}

void ObIOProcessor::inner_submit()
{
  int ret = OB_SUCCESS;
  ObIOController *control = NULL;
  struct iocb *iocbp = NULL;
  struct iocb **iocbpp = &iocbp;
  int io_ret = 0;
  bool queue_empty[IO_QUEUE_CNT];
  memset(queue_empty, 0, sizeof(queue_empty));
  int queue_cond_wait_ms = 100;
  int64_t queue_idx = 0;
  int64_t thread_idx = 0;
  int64_t i = 0;
  bool can_pop = false;
  bool is_need_wait = true;
  bool is_busy = true;
  while (!_stop) {
    if (OB_FAIL(need_wait(is_need_wait))) {
      COMMON_LOG(WDIAG, "Fail to test need wait, ", K(ret));
    } else if (is_need_wait) {
      queue_cond_.lock();
      if (OB_FAIL(need_wait(is_need_wait))) {
        COMMON_LOG(WDIAG, "Fail to test need wait, ", K(ret));
      } else if (is_need_wait) {
        queue_cond_.wait(queue_cond_wait_ms);
      }
      queue_cond_.unlock();
    }
    is_busy = false;
    while (OB_SUCCESS == ret && !is_busy) {
      if (OB_FAIL(is_all_thread_busy(is_busy))) {
        COMMON_LOG(WDIAG, "Fail to test can pop, ", K(ret));
      } else if (!is_busy) {
        if (OB_FAIL(queue_[queue_idx].can_pop(can_pop))) {
          COMMON_LOG(WDIAG, "Fail to test can pop, ", K(ret));
        } else if (can_pop) {
          if (OB_SUCC(queue_[queue_idx].pop(control))) {
            queue_empty[queue_idx] = false;
            if(!control->need_submit_){//no need to submit IO to OS
              ObIOManager::get_instance().dec_ref(control);
            } else {
              iocbp = &(control->iocb_);
              for (i = 0; i < event_thread_cnt_; ++i) {
                thread_idx = (thread_idx + 1) % event_thread_cnt_;
                if (ATOMIC_LOAD(&submited_cnt_[thread_idx]) < MAX_AIO_EVENT_CNT
                    && 1 == (io_ret = io_submit(context_[thread_idx], 1, iocbpp))) {
                  ATOMIC_INC(&submited_cnt_[thread_idx]);
                  break;
                }
              }

              if (i == event_thread_cnt_) {
                //fail to submit request
                ret = OB_IO_ERROR;
                COMMON_LOG(WDIAG, "Fail to submit io request to os, ", K(io_ret), K(ret));
                control->is_success_ = false;
                notify(*control);
              }
            }
          } else {
            if (OB_ENTRY_NOT_EXIST == ret) {
              queue_empty[queue_idx] = true;
              ret = OB_SUCCESS;
            }
          }
        } else {
          queue_empty[queue_idx] = true;
        }
        queue_idx = (queue_idx + 1) % IO_QUEUE_CNT;

        int64_t j = 0;
        for (j = 0; j < IO_QUEUE_CNT; ++j) {
          if (!queue_empty[j]) {
            break;
          }
        }
        if (IO_QUEUE_CNT == j) {
          break;
        }
      }
    }
  }
}

//TODO: can_pop variable change should in queue_cond_
int ObIOProcessor::need_wait(bool &bool_ret)
{
  int ret = OB_SUCCESS;
  bool_ret = true;
  int tmp_ret = OB_SUCCESS;
  bool can_pop = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOProcessor has not been inited, ", K(ret));
  } else {
    for (int64_t i = 0; i < IO_QUEUE_CNT; ++i) {
      if (OB_SUCCESS != (tmp_ret = queue_[i].can_pop(can_pop))) {
        COMMON_LOG(WDIAG, "Fail to test can pop, ", K(tmp_ret));
      } else if (can_pop) {
        bool_ret = false;
        break;
      }
    }
  }
  return ret;
}

void ObIOProcessor::inner_get_events(const int64_t thread_idx)
{
  int64_t event_cnt = 0;
  ObIOController *control = NULL;
  struct timespec timeout;
  timeout.tv_sec = 0;
  timeout.tv_nsec = 100000000L;
  struct io_event events[MAX_AIO_EVENT_CNT];
  memset(events, 0, sizeof(events));
  while (!_stop) {
    if (0 < (event_cnt = io_getevents(context_[thread_idx], 1, MAX_AIO_EVENT_CNT, events, &timeout))) {
      for (int64_t i = 0; i < event_cnt; ++i) {
        control = reinterpret_cast<ObIOController*> (events[i].data);
        if (NULL != control) {
          control->io_finish_time_ = ::oceanbase::common::ObTimeUtility::current_time();
          if (0 != events[i].res2) {
            control->is_success_ = false;
          } else {
            control->is_success_ = true;
          }
          notify(*control);
        }
      }
      (void)ATOMIC_FAS(&submited_cnt_[thread_idx], event_cnt);
      queue_cond_.lock();
      queue_cond_.signal();
      queue_cond_.unlock();
    }
  }
}

void ObIOProcessor::notify(ObIOController &control)
{
  int ret = OB_SUCCESS;
  if (NULL != control.callback_) {
    if (OB_FAIL(control.callback_->process(control.is_success_))) {
      COMMON_LOG(WDIAG, "Fail to callback, ", K(ret));
    }
  }
  control.cond_.lock();
  control.has_finished_ = true;
  control.cond_.signal();
  control.cond_.unlock();
  ObIOManager::get_instance().dec_ref(&control);
}

int ObIOProcessor::is_all_thread_busy(bool &bool_ret)
{
  int ret = OB_SUCCESS;
  bool_ret = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOProcessor has not been inited, ", K(ret));
  } else {
    for (int64_t i = 0; bool_ret && i < event_thread_cnt_; ++i) {
      if (ATOMIC_LOAD(&submited_cnt_[i]) < MAX_AIO_EVENT_CNT) {
        bool_ret = false;
      }
    }
  }
  return ret;
}

ObIOManager::ObIOManager()
  : inner_allocator_(ObModIds::OB_SSTABLE_AIO),
    thread_cnt_per_disk_(1),
    inited_(false)
{
}

ObIOManager::~ObIOManager()
{
  destroy();
}

ObIOManager &ObIOManager::get_instance()
{
  static ObIOManager instance_;
  return instance_;
}

int ObIOManager::init(const int64_t mem_limit, const int64_t thread_cnt_per_disk)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WDIAG, "The ObIOManager has been inited, ", K(ret));
  } else if (mem_limit < OB_MALLOC_BIG_BLOCK_SIZE || thread_cnt_per_disk <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "The mem limit is too small, ", K(mem_limit), K(thread_cnt_per_disk), K(ret));
  } else if (OB_FAIL(processor_map_.create(MAX_DISK_NUM, ObModIds::OB_SSTABLE_AIO))) {
    COMMON_LOG(WDIAG, "Fail to init processor map, ", K(ret));
  } else if (OB_FAIL(io_allocator_.init(mem_limit, OB_MALLOC_BIG_BLOCK_SIZE, OB_MALLOC_BIG_BLOCK_SIZE))) {
    COMMON_LOG(WDIAG, "Fail to init io allocator, ", K(ret));
  } else if (OB_FAIL(timer_.init())) {
    COMMON_LOG(WDIAG, "Fail to init timer, ", K(ret));
  } else if (OB_FAIL(timer_.schedule(inner_task_, BALANCE_INTERVAL_US, true))) {
    COMMON_LOG(WDIAG, "Fail to schedule balance task, ", K(ret));
  } else {
    io_allocator_.set_mod_id(ObModIds::OB_SSTABLE_AIO);
    thread_cnt_per_disk_ = thread_cnt_per_disk;
    inited_ = true;
    COMMON_LOG(INFO, "Success to init ObIOManager!");
  }

  if (!inited_) {
    destroy();
  }
  return ret;
}

void ObIOManager::destroy()
{
  timer_.destroy();
  ObIOProcessor *processor = NULL;
  hash::ObHashMap<__dev_t, ObIOProcessor*>::iterator iter;
  for (iter = processor_map_.begin(); iter != processor_map_.end(); ++iter) {
    processor = iter->second;
    processor->destroy();
  }
  processor_map_.destroy();
  io_allocator_.destroy();
  inner_allocator_.reset();
  inited_ = false;
}


int ObIOManager::read(const ObIOInfo &info, ObIOHandle &handle, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "the argument is invalid, ", K(info), K(ret));
  } else if (OB_FAIL(aio_read(info, handle))) {
    COMMON_LOG(WDIAG, "Fail to submit aio read request, ", K(ret));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    COMMON_LOG(WDIAG, "Fail to wait io finish, ", K(ret));
  }
  return ret;
}

int ObIOManager::write(const ObIOInfo &info, const char *data, const uint64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  ObIOHandle handle;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else if (!info.is_valid() || NULL == data) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "the argument is invalid, ", K(info), KP(data), K(ret));
  } else if (OB_FAIL(aio_write(info, data, handle))) {
    COMMON_LOG(WDIAG, "Fail to submit aio write request, ", K(ret));
  } else if (OB_FAIL(handle.wait(timeout_ms))) {
    COMMON_LOG(WDIAG, "Fail to wait io finish, ", K(ret));
  }
  return ret;
}

int ObIOManager::aio_read(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObIOProcessor *processor = NULL;
  char *buf = NULL;
  ObIOController *control = NULL;
  int64_t offset = 0;
  int64_t size = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "the argument is invalid, ", K(info), K(ret));
  } else {
    handle.reset();
    align_offset_size(info.offset_, info.size_, offset, size);
    if (OB_SUCCESS != (ret = get_processor(info, processor))) {
      COMMON_LOG(WDIAG, "Fail to get io processor, ", K(ret));
    } else if (NULL == (buf = (char*)io_allocator_.alloc(sizeof(ObIOController)
        + size + DIO_ALIGN_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WDIAG, "Fail to allocate memory, ", K(ret));
    } else {
      control = new (buf) ObIOController();
      control->buf_ = (char*) upper_align((int64_t)buf + sizeof(ObIOController), DIO_ALIGN_SIZE);
      control->data_offset_ = info.offset_ - offset;
      control->io_desc_ = info.io_desc_;
      io_prep_pread(&control->iocb_, info.fd_, control->buf_, size, offset);
      control->iocb_.data = control;
      inc_ref(control);

      if (OB_SUCCESS != (ret = handle.init(control, processor))) {
        COMMON_LOG(WDIAG, "Fail to init read handle, ", K(ret));
      } else if (OB_SUCCESS != (ret = processor->submit(*control))) {
        COMMON_LOG(WDIAG, "Fail to submit aio read request, ", K(ret));
      }

      dec_ref(control);
    }
  }
  return ret;
}

int ObIOManager::aio_read(const ObIOInfo &info, char *buf, ObIOCallback &callback, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObIOProcessor *processor = NULL;
  ObIOController *control = NULL;
  char *tmp = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else if (!info.is_valid() || NULL == buf) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "the argument is invalid, ", K(info), KP(buf), K(ret));
  } else if (OB_FAIL(get_processor(info, processor))) {
    COMMON_LOG(WDIAG, "Fail to get io processor, ", K(ret));
  } else if (!info.is_aligned()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "The offset or size is not aligned, ", K(ret));
  } else if (0 != (int64_t) buf % DIO_ALIGN_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "The read buffer is not aligned, ", K(ret));
  } else if (NULL == (tmp = (char*) (io_allocator_.alloc(sizeof(ObIOController) + callback.size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WDIAG, "Fail to allocate memory, ", K(ret));
  } else {
    control = new (tmp) ObIOController();
    control->buf_ = buf;
    control->data_offset_ = 0;
    control->io_desc_ = info.io_desc_;
    io_prep_pread(&control->iocb_, info.fd_, control->buf_, info.size_, info.offset_);
    control->iocb_.data = control;
    inc_ref(control);

    if (OB_FAIL(callback.deep_copy(tmp + sizeof(ObIOController),
        callback.size(), control->callback_))) {
      COMMON_LOG(WDIAG, "Fail to deep copy callback function, ", K(ret));
    } else if (OB_FAIL(handle.init(control, processor))) {
      COMMON_LOG(WDIAG, "Fail to init read handle, ", K(ret));
    } else if (OB_FAIL(processor->submit(*control))) {
      COMMON_LOG(WDIAG, "Fail to submit aio read request, ", K(ret));
    }

    dec_ref(control);
  }
  return ret;
}


int ObIOManager::aio_write(const ObIOInfo &info, const char *data, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObIOProcessor *processor = NULL;
  char *buf = NULL;
  ObIOController *control = NULL;

  handle.reset();
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else if (!info.is_valid() || NULL == data) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "the argument is invalid, ", K(info), KP(data), K(ret));
  } else if (OB_FAIL(get_processor(info, processor))) {
    COMMON_LOG(WDIAG, "Fail to get io processor, ", K(ret));
  } else if (!info.is_aligned()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "The write position is not aligned, ", K(ret));
  } else if (NULL == (buf = (char*) io_allocator_.alloc(sizeof(ObIOController)
      + info.size_ + DIO_ALIGN_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WDIAG, "Fail to allocate memory, ", K(ret));
  } else {
    control = new (buf) ObIOController();
    control->buf_ = (char*)(upper_align((int64_t) buf + sizeof(ObIOController), DIO_ALIGN_SIZE));
    MEMCPY(control->buf_, data, info.size_);
    control->data_offset_ = 0;
    control->io_desc_ = info.io_desc_;
    io_prep_pwrite(&control->iocb_, info.fd_, control->buf_, info.size_, info.offset_);
    control->iocb_.data = control;
    inc_ref(control);

    if (OB_FAIL(handle.init(control, processor))) {
      COMMON_LOG(WDIAG, "Fail to init write handle, ", K(ret));
    } else if (OB_FAIL(processor->submit(*control))) {
      COMMON_LOG(WDIAG, "Fail to submit aio write request, ", K(ret));
    }

    dec_ref(control);
  }
  return ret;
}


int ObIOManager::get_processor(const ObIOInfo &info, ObIOProcessor *&processor)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  struct stat stat;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WDIAG, "Invalid argument, ", K(ret));
  } else if (0 != fstat(info.fd_, &stat)) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WDIAG, "Fail to get file stat info, ", K(ret));
  } else {
    int hash_ret = OB_HASH_NOT_EXIST;
    {
      DRWLock::RDLockGuard guard(lock_);
      if (OB_SUCCESS == (hash_ret = processor_map_.get_refactored(stat.st_dev, processor))) {
        //success found, do nothing
      } else if (OB_HASH_NOT_EXIST != hash_ret) {
        ret = hash_ret;
        COMMON_LOG(WDIAG, "Unexpected error, ", K(hash_ret), K(ret));
      }
    }

    if (OB_HASH_NOT_EXIST == hash_ret) {
      DRWLock::WRLockGuard guard(lock_);
      if (OB_SUCCESS == (hash_ret = processor_map_.get_refactored(stat.st_dev, processor))) {
        //double check, has exist, do nothing
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        if (NULL == (buf = inner_allocator_.alloc(sizeof(ObIOProcessor)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WDIAG, "Fail to allocate memory, ", K(ret));
        } else {
          processor = new (buf) ObIOProcessor();
          if (OB_FAIL(processor->init(thread_cnt_per_disk_))) {
            COMMON_LOG(WDIAG, "Fail to init io processor, ", K(ret));
          } else if (OB_SUCCESS != (hash_ret = processor_map_.set_refactored(stat.st_dev, processor))) {
            ret = hash_ret;
            COMMON_LOG(WDIAG, "Fail to set processor to map, ", K(ret));
          }

          if (OB_FAIL(ret)) {
            processor->destroy();
            inner_allocator_.free(processor);
            processor = NULL;
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WDIAG, "Unexpected error, ", K(hash_ret), K(ret));
      }
    }
  }
  return ret;
}


void ObIOManager::balance()
{
  if (!inited_) {
    int ret = OB_NOT_INIT;
    COMMON_LOG(WDIAG, "The ObIOManager has not been inited, ", K(ret));
  } else {
    DRWLock::RDLockGuard guard(lock_);
    hash::ObHashMap<__dev_t, ObIOProcessor*>::iterator iter;
    ObIOProcessor *processor = NULL;
    for (iter = processor_map_.begin(); iter != processor_map_.end(); ++iter) {
      processor = iter->second;
      processor->balance();
    }
  }
}

void align_offset_size(const int64_t offset, const int64_t size,  int64_t &align_offset, int64_t &align_size)
{
  align_offset = lower_align(offset, DIO_ALIGN_SIZE);
  align_size = upper_align(size + offset - align_offset, DIO_ALIGN_SIZE);
}

} /* namespace common */
} /* namespace oceanbase */
