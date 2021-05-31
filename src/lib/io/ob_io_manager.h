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

#ifndef OB_IO_MANAGER_H_
#define OB_IO_MANAGER_H_
#include <libaio.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/task/ob_timer.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/wait_event/ob_wait_event.h"

namespace oceanbase
{
namespace common
{

const int64_t MAX_IO_WAIT_TIME_MS = 5000;          // wait 5 second for io operations.

enum ObIOCategory
{
  USER_IO = 0,
  SYS_IO = 1,
  MAX_IO_CATEGORY = 2
};

struct ObIODesc
{
  ObIODesc() : category_(USER_IO), wait_event_no_(0){}
  enum ObIOCategory category_;
  int64_t wait_event_no_;
  bool is_valid() const
  {
    return category_ >= USER_IO && category_ < MAX_IO_CATEGORY && wait_event_no_ >= 0;
  }
  TO_STRING_KV(K_(category), K_(wait_event_no));
};

struct ObIOInfo
{
  ObIOInfo();
  virtual ~ObIOInfo();
  inline bool is_valid() const;
  inline bool is_aligned() const;
  int fd_;
  int64_t offset_;
  int64_t size_;
  ObIODesc io_desc_;
  TO_STRING_KV(K_(fd), K_(offset), K_(size), K_(io_desc));
};

class ObIOCallback
{
public:
  ObIOCallback() {}
  virtual ~ObIOCallback() {}
  virtual int64_t size() const = 0;
  virtual int process(const bool is_success) = 0;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIOCallback *&callback) const = 0;
};

struct ObIOController
{
  ObIOController();
  virtual ~ObIOController();
  struct iocb iocb_;
  ObThreadCond cond_;
  char *buf_;
  int64_t data_offset_;
  ObIODesc io_desc_;
  ObIOCallback *callback_;
  int64_t req_submit_time_;
  int64_t io_submit_time_;
  int64_t io_finish_time_;
  int64_t wait_begin_time_;
  int64_t wait_end_time_;
  bool has_finished_;
  bool is_success_;
  bool need_submit_;
  volatile int64_t ref_cnt_;
};

class ObIOQueue {
public:
  ObIOQueue();
  virtual ~ObIOQueue();
  int init(ObIOController **item_array, const int64_t array_size);
  void destroy();
  int push(ObIOController *item);
  int pop(ObIOController *&item);
  int record_wait_begin(ObIOController *item);
  int record_wait_end(ObIOController *item);
  void refresh();
  int can_pop(bool &bool_ret);
  inline void set_weight(const int64_t weight);
  inline void inc_queue_hold();
  inline void dec_queue_hold();
  inline int64_t get_wait_score();
  TO_STRING_KV(K_(min_queue_hold), K_(io_weight), K_(wait_score));
private:
  ObSpinLock lock_;
  ObIOController **item_array_;
  int64_t array_size_;
  int64_t producer_;
  int64_t consumer_;
  int64_t wait_cnt_;
  int64_t wait_begin_time_sum_;
  int64_t min_queue_hold_;
  int64_t io_weight_;
  int64_t wait_score_;
  bool inited_;
};

class ObIOProcessor : public obsys::CDefaultRunnable
{
public:
  ObIOProcessor();
  virtual ~ObIOProcessor();
  int init(const int64_t event_thread_cnt);
  void destroy();
  int submit(ObIOController &control);
  int record_wait_begin(ObIOController &control);
  int record_wait_end(ObIOController &control);
  void balance();
  virtual void run(obsys::CThread *thread, void *arg);
private:
  void inner_submit();
  int need_wait(bool &bool_ret);
  void inner_get_events(const int64_t thread_idx);
  void notify(ObIOController &control);
  int is_all_thread_busy(bool &bool_ret);
  inline int64_t get_queue_idx(ObIOController &control);
  static const int64_t MAX_AIO_EVENT_CNT = 512;
  static const int64_t MAX_THREAD_CNT = 16;
  static const int64_t MAX_IO_QUEUE_DEPTH = 100000;
  static const int64_t IO_QUEUE_CNT = 4; //user write; user read; sys write; sys read
  int64_t event_thread_cnt_;
  io_context_t context_[MAX_THREAD_CNT];
  int64_t submited_cnt_[MAX_THREAD_CNT];
  ObIOQueue queue_[IO_QUEUE_CNT];
  ObThreadCond queue_cond_;
  ObArenaAllocator allocator_;
  bool inited_;
};

class ObIOHandle
{
public:
  ObIOHandle();
  virtual ~ObIOHandle();
  ObIOHandle(const ObIOHandle &other);
  ObIOHandle &operator=(const ObIOHandle &other);
  int init(ObIOController *control, ObIOProcessor *processor);
  int wait(const int64_t timeout_ms = MAX_IO_WAIT_TIME_MS);
  const char *get_buffer();
  int64_t get_wait_time_us();
  int64_t get_queue_wait_time_us();
  int64_t get_disk_wait_time_us();
  void reset();
  inline bool is_empty() { return NULL == control_; }
  inline bool is_valid() const { return NULL != control_ && NULL != control_->buf_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  ObIOController *control_;
  ObIOProcessor *processor_;
  bool inited_;
};

class ObIOManager
{
public:
  static ObIOManager &get_instance();
  int init(const int64_t mem_limit, const int64_t thread_cnt_per_disk = 1);
  void destroy();
  //synchronously read
  //@param info: input, include fd, offset, size
  //@param handle: output, it will hold the memory of IO until the handle is reset or destructed.
  //@param timeout_ms: input, this function will block at most timeout_ms
  //@return OB_SUCCESS or other error code
  int read(const ObIOInfo &info, ObIOHandle &handle, const uint64_t timeout_ms = MAX_IO_WAIT_TIME_MS);
  //synchronously write
  //@param info: input, include fd, offset, size, NOTE that the offset and size MUST be aligned.
  //@param data: input, the data need to write
  //@param timeout_ms: input, this function will block at most timeout_ms
  //@return OB_SUCCESS or other error code
  int write(const ObIOInfo &info, const char *data, const uint64_t timeout_ms = MAX_IO_WAIT_TIME_MS);
  //asynchronously read
  //@param info: input, include fd, offset, size
  //@param handle: output, it will hold the memory of IO until the handle is reset or destructed.
  //@return OB_SUCCESS or other error code
  int aio_read(const ObIOInfo &info, ObIOHandle &handle);
  //asynchronously read
  //@param info: input, include fd, offset, size. NOTE that the offset and size MUST be aligned.
  //@param buf: input, the memory to store read data, NOTE that the buf MUST be aligned.
  //@param callback: input
  //@param handle: output, can invoke handle.wait() to wait io finish
  //@return OB_SUCCESS or other error code
  int aio_read(const ObIOInfo &info, char *buf, ObIOCallback &callback, ObIOHandle &handle);
  //asynchronously write
  //@param info: input, include fd, offset, size, NOTE that the offset and size MUST be aligned.
  //@param data: input, the data need to write. The memory can be freed after this function return.
  //@param handle: output, can invoke handle.wait() to wait io finish
  //@return OB_SUCCESS or other error code
  int aio_write(const ObIOInfo &info, const char *data, ObIOHandle &handle);
public:
  inline void inc_ref(ObIOController *control);
  inline void dec_ref(ObIOController *control);
private:
  class BalanceTask : public ObTimerTask
  {
    virtual void runTimerTask()
    {
      ObIOManager::get_instance().balance();
    }
  };
  ObIOManager();
  virtual ~ObIOManager();
  int get_processor(const ObIOInfo &info, ObIOProcessor *&processor);
  void balance();
  static const int64_t MAX_DISK_NUM = 16;
  static const int64_t BALANCE_INTERVAL_US = 1000; // 1ms
  DRWLock lock_;
  ObArenaAllocator inner_allocator_;
  hash::ObHashMap<__dev_t, ObIOProcessor*> processor_map_;
  ObConcurrentFIFOAllocator io_allocator_;
  int64_t thread_cnt_per_disk_;
  BalanceTask inner_task_;
  ObTimer timer_;
  bool inited_;
};


void align_offset_size(const int64_t offset, const int64_t size,  int64_t &align_offset, int64_t &align_size);

inline bool ObIOInfo::is_valid() const
{
  return fd_ > 0 && offset_ >= 0 && size_ > 0;
}

inline bool ObIOInfo::is_aligned() const
{
  return 0 == offset_ % DIO_ALIGN_SIZE && 0 == size_ % DIO_ALIGN_SIZE;
}

inline void ObIOQueue::set_weight(const int64_t weight)
{
  io_weight_ = weight;
}

inline void ObIOQueue::inc_queue_hold()
{
  ObSpinLockGuard guard(lock_);
  if (min_queue_hold_ <= producer_ - consumer_) {
    ++min_queue_hold_;
  }
}

inline void ObIOQueue::dec_queue_hold()
{
  if (min_queue_hold_ > 0) {
    --min_queue_hold_;
  }
}

inline int64_t ObIOQueue::get_wait_score()
{
  return wait_score_;
}

inline int64_t ObIOProcessor::get_queue_idx(ObIOController &control)
{
  return control.io_desc_.category_ * 2 + ((control.iocb_.aio_lio_opcode == IO_CMD_PREAD) ? 1 : 0);
}


inline void ObIOManager::inc_ref(ObIOController *control)
{
  if (NULL != control) {
    ATOMIC_INC(&control->ref_cnt_);
  }
}

inline void ObIOManager::dec_ref(ObIOController *control)
{
  if (NULL != control) {
    if (0 == ATOMIC_SAF(&control->ref_cnt_, 1)) {
      control->~ObIOController();
      io_allocator_.free(control);
      control = NULL;
    }
  }
}

} /* namespace common */
} /* namespace oceanbase */

#endif /* OB_IO_MANAGER_H_ */
