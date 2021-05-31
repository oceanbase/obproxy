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

#ifndef OCEANBASE_ALLOCATOR_OB_RETIRE_STATION_H_
#define OCEANBASE_ALLOCATOR_OB_RETIRE_STATION_H_
#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"
#include "lib/thread_local/ob_tsi_utils.h"

namespace oceanbase
{
namespace common
{
class QClock
{
public:
  struct ThreadClock
  {
    ThreadClock(): clock_(UINT64_MAX) {}
    ~ThreadClock() {}
    uint64_t clock_ CACHE_ALIGNED;
    // void record_bt() { snprintf(bt_, sizeof(bt_), "%s", lbt()); }
    // char bt_[256];
  };
  QClock(): clock_(1), qclock_(0) {}
  virtual ~QClock() {}
  void enter_critical() {
    int64_t tid = get_itid();
    if (tid < OB_MAX_THREAD_NUM) {
      ThreadClock* tclock = thread_clock_ + tid;
      WEAK_BARRIER();
      if (NULL != tclock) {
        //assert(UINT64_MAX == tclock->clock_);
        //tclock->record_bt();
        tclock->clock_ = get_clock();
      }
    }
  }
  void leave_critical() {
    int64_t tid = get_itid();
    if (tid < OB_MAX_THREAD_NUM) {
      ThreadClock* tclock = thread_clock_ + tid;
      WEAK_BARRIER();
      if (NULL != tclock) {
        //assert(UINT64_MAX != tclock->clock_);
        tclock->clock_ = UINT64_MAX;
      }
    }
  }

  uint64_t wait_quiescent(uint64_t clock) {
    uint64_t ret_clock = get_clock();
    //assert(thread_clock_[get_itid()].clock_ > clock);
    while(!is_quiescent(clock)) {
      PAUSE();
    }
    inc_clock();
    return ret_clock;
  }

  bool is_quiescent(uint64_t clock) {
    WEAK_BARRIER();
    uint64_t cur_clock = get_clock();
    return clock < cur_clock && (clock < qclock_ || clock < (qclock_ = get_quiescent_clock(cur_clock)));
  }

  uint64_t inc_clock() { return ATOMIC_AAF(&clock_, 1); }
  uint64_t get_clock() { return ATOMIC_LOAD(&clock_); }

private:
  void update_cur_thread_clock() {
    int64_t tid = get_itid();
    if (tid < OB_MAX_THREAD_NUM) {
      ThreadClock* tclock = thread_clock_ + tid;
      WEAK_BARRIER();
      if (NULL != tclock && UINT64_MAX != tclock->clock_) {
        tclock->clock_ = get_clock();
      }
    }
  }
  uint64_t get_quiescent_clock(uint64_t cur_clock) {
    WEAK_BARRIER();
    uint64_t qclock = cur_clock;
    for(int64_t i = 0; i < get_max_itid(); i++){
      uint64_t tclock = thread_clock_[i].clock_;
      if (tclock < qclock) {
        qclock = tclock;
      }
    }
    return qclock;
  }
private:
  uint64_t clock_ CACHE_ALIGNED;
  uint64_t qclock_ CACHE_ALIGNED;
  ThreadClock thread_clock_[OB_MAX_THREAD_NUM];
};

inline QClock& get_global_qclock()
{
  static QClock __global_qclock;
  return __global_qclock;
}

class HazardList
{
public:
  typedef ObLink Link;
  HazardList(): size_(0), head_(NULL), tail_(NULL) {}
  virtual ~HazardList() {}

public:
  int64_t size() { return size_; }

  void move_to(HazardList& target) {
    target.concat(*this);
    this->clear_all();
  }

  Link* pop() {
    Link* p = NULL;
    if (NULL != head_) {
      p = head_;
      head_ = head_->next_;
      if (NULL == head_) {
        tail_ = NULL;
      }
    }
    if (NULL != p) {
      size_--;
    }
    return p;
  }
  void push(Link* p) {
    if (NULL != p) {
      p->next_ = NULL;
      if (NULL == tail_) {
        head_ = tail_ = p;
      } else {
        tail_->next_ = p;
        tail_ = p;
      }
      size_++;
    }
  }

private:
  Link* get_head() { return head_; }
  Link* get_tail() { return tail_; }

  void concat(HazardList& that) {
    if (NULL == tail_) {
      head_ = that.get_head();
      tail_ = that.get_tail();
    } else if (NULL != that.get_tail()) {
      tail_->next_ = that.get_head();
      tail_ = that.get_tail();
    }
    size_ += that.size();
  }

  void clear_all() {
    size_ = 0;
    head_ = NULL;
    tail_ = NULL;
  }

private:
  int64_t size_;
  Link* head_;
  Link* tail_;
};

class RetireStation
{
public:
  typedef HazardList List;

  struct LockGuard
  {
    explicit LockGuard(uint64_t& lock): lock_(lock) {
      while(1 == ATOMIC_TAS(&lock_, 1)) {
        PAUSE();
      }
    }
    ~LockGuard() {
      ATOMIC_STORE(&lock_, 0);
    }
    uint64_t& lock_;
  };

  struct RetireList
  {
    RetireList(): lock_(0), retire_clock_(0) {}
    ~RetireList() {}
    void retire(List& reclaim_list, List& retire_list, int64_t limit, QClock& qclock) {
      LockGuard lock_guard(lock_);
      retire_list.move_to(prepare_list_);
      if (prepare_list_.size() > limit) {
        retire_clock_ = qclock.wait_quiescent(retire_clock_);
        retire_list_.move_to(reclaim_list);
        prepare_list_.move_to(retire_list_);
      }
    }
    uint64_t lock_;
    uint64_t retire_clock_;
    List retire_list_;
    List prepare_list_;
  };

  RetireStation(QClock& qclock=get_global_qclock()): qclock_(qclock) {}
  virtual ~RetireStation() {}

  void retire(List& reclaim_list, List& retire_list, int64_t limit) {
    get_retire_list().retire(reclaim_list, retire_list, limit, qclock_);
  }

  void purge(List& reclaim_list) {
    List retire_list;
    for(int i = 0; i < 2; i++) {
      for(int64_t tid = 0; tid < get_max_itid(); tid++) {
        retire_list_[tid].retire(reclaim_list, retire_list, -1, qclock_);
      }
    }
  }

private:
  RetireList& get_retire_list() {
    int64_t tid = get_itid();
    return tid < OB_MAX_THREAD_NUM ? retire_list_[tid] : retire_list_[0];
  }
private:
  QClock& qclock_;
  RetireList retire_list_[OB_MAX_THREAD_NUM];
};

class QClockGuard
{
public:
  explicit QClockGuard(QClock& qclock=get_global_qclock()): qclock_(qclock) { qclock_.enter_critical(); }
  ~QClockGuard() { qclock_.leave_critical(); }
private:
  QClock& qclock_;
};

class IFreeHandler
{
public:
  IFreeHandler() {}
  virtual ~IFreeHandler() {}
  virtual void free(void* p) = 0;
};

class RetireWrapper
{
public:
  RetireWrapper(RetireStation& station, IFreeHandler& free_handler): retire_station_(station), free_handler_(free_handler) {}
  ~RetireWrapper() { sync(); }
  void retire(ObLink* retire_link, int64_t retire_limit)
  {
    HazardList retire_list;
    HazardList reclaim_list;
    retire_list.push(retire_link);
    retire_station_.retire(reclaim_list, retire_list, retire_limit);
    destroy_nodes(reclaim_list);
  }
  void sync()
  {
    HazardList reclaim_list;
    retire_station_.purge(reclaim_list);
    destroy_nodes(reclaim_list);
  }
private:
  void destroy_nodes(HazardList& reclaim_list) {
    ObLink* p = NULL;
    while(NULL != (p = reclaim_list.pop())) {
      free_handler_.free(p);
    }
  }
private:
  RetireStation& retire_station_;
  IFreeHandler& free_handler_;
};

}; // end namespace allocator
}; // end namespace oceanbase

#endif /* OCEANBASE_ALLOCATOR_OB_RETIRE_STATION_H_ */
