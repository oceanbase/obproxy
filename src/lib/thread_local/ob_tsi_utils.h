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

#ifndef OCEANBASE_COMMON_OB_TSI_UTILS_
#define OCEANBASE_COMMON_OB_TSI_UTILS_
#include <errno.h>
#include <unistd.h>
#include <sys/syscall.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
volatile int64_t __next_tid __attribute__((weak));
inline int64_t get_itid()
{
  static __thread int64_t tid = -1;
  return tid < 0 ? (tid = ATOMIC_FAA(&__next_tid, 1)) : tid;
}
inline int64_t get_max_itid()
{
#if OB_GCC_VERSION > 40704
  return __atomic_load_n(&__next_tid, __ATOMIC_SEQ_CST);
#else
  asm volatile("" ::: "memory");
  return __next_tid;
#endif
}

#ifndef HAVE_SCHED_GETCPU
inline int sched_getcpu(void) { return get_itid() & (64 - 1); }
#endif

inline int64_t icpu_id()
{
  return sched_getcpu();
}

inline int icore_id()
{
  return sched_getcpu();
}

struct TCValue
{
public:
  TCValue() : items_() { memset(items_, 0, sizeof(items_)); }
  ~TCValue() {}
  int64_t &get() { return *(int64_t *)(items_ + get_itid()); }
private:
  char items_[OB_MAX_THREAD_NUM][CACHE_ALIGN_SIZE];
};

struct TCCounter
{
  struct Item
  {
    uint64_t value_;
  } CACHE_ALIGNED;
  TCCounter() : items_() { memset(items_, 0, sizeof(items_)); }
  ~TCCounter() {}
  int64_t inc(int64_t delta = 1)
  {
    return ATOMIC_FAA(&items_[get_itid() % OB_MAX_THREAD_NUM].value_, delta);
  }
  int64_t value() const
  {
    int64_t sum = 0;
    int64_t thread_count = get_max_itid();
    int64_t slot_count = (thread_count < OB_MAX_THREAD_NUM) ? thread_count
                                                            : OB_MAX_THREAD_NUM;
    for (int64_t i = 0; i < slot_count; i++) {
      sum += items_[i].value_;
    }
    return sum;
  }
  Item items_[OB_MAX_THREAD_NUM];
};

class SimpleItem
{
public:
  SimpleItem(): value_(0) {}
  ~SimpleItem() {}
  inline void reset() {value_ = 0;}
  inline SimpleItem &operator+=(const SimpleItem &item)
  {
    this->value_ += item.value_;
    return *this;
  }
  int64_t value_;
};

template <int64_t MAX_ELEMENTS_SIZE = 1,
          class T = int64_t,
          int64_t MAX_THREAD_NUM = OB_MAX_THREAD_NUM>
class ObTsiHpCounter
{
public:
  ObTsiHpCounter() : items_()
  {
    memset(items_, 0, sizeof(items_));
  }
  ~ObTsiHpCounter() {}
public:
  inline void get_value(T &result, int64_t idx = 0) const
  {
    if (idx < 0 || idx >= MAX_ELEMENTS_SIZE) {
      _LIB_LOG(EDIAG, "get_value param is out of range, idx[0,%ld) = %ld",
                MAX_ELEMENTS_SIZE, idx);
    } else {
      int64_t thread_count = get_max_itid();
      int64_t slot_count = (thread_count < MAX_THREAD_NUM) ? thread_count
                                                           : MAX_THREAD_NUM;
      for (int64_t i = 0; i < slot_count; i++) {
        result += items_[i][idx];
      }
    }
  }

  inline void inc(const int64_t idx, const T &item)
  {
    int64_t tid = get_itid();
    if (tid >= MAX_THREAD_NUM || idx < 0 || idx >= MAX_ELEMENTS_SIZE) {
      _LIB_LOG(EDIAG, "inc param is out of range, thread num[0,%ld) = %ld, idx[0,%ld) = %ld",
                      MAX_THREAD_NUM, tid, MAX_ELEMENTS_SIZE, idx);
    } else {
      items_[tid][idx] += item;
    }
  }
private:
  T items_[MAX_THREAD_NUM][MAX_ELEMENTS_SIZE];
};

struct ExpStat
{
  struct ExpStatItem
  {
  public:
    ExpStatItem(): count_(0), value_(0) {}
    inline void clear() {count_ = 0; value_ = 0;}
    inline ExpStatItem &operator+= (const ExpStatItem &item)
    {
      this->count_ += item.count_;
      this->value_ += item.value_;
      return *this;
    }
    int64_t count_;
    int64_t value_;
  };
  ExpStat(const char *metric_name) : stat_(), metric_name_(NULL)
  {
    memset(this, 0, sizeof(*this));
    metric_name_ = metric_name;
  }
  ~ExpStat() {}
  inline static int64_t get_idx(const int64_t x)
  {
    return (0 != x) ? (64 - __builtin_clzl(x)) : 0;
  }
  inline int64_t count() const
  {
    int64_t total_count = 0;
    for (int64_t i = 0; i < TSI_HP_COUNTER_ELEMENTS_SIZE; i++) {
      ExpStatItem item;
      stat_.get_value(item, i);
      total_count += item.count_;
    }
    return total_count;
  }
  inline int64_t value() const
  {
    int64_t total_value = 0;
    for (int64_t i = 0; i < TSI_HP_COUNTER_ELEMENTS_SIZE; i++) {
      ExpStatItem item;
      stat_.get_value(item, i);
      total_value += item.value_;
    }
    return total_value;
  }
  inline void add(int64_t x)
  {
    ExpStatItem item;
    int64_t idx = get_idx(x);
    item.count_ = 1;
    item.value_ = x;
    stat_.inc(idx, item);
  }
  int64_t to_string(char *buf, const int64_t len) const;
  static const int64_t TSI_HP_COUNTER_ELEMENTS_SIZE = 64;
  ObTsiHpCounter<TSI_HP_COUNTER_ELEMENTS_SIZE, ExpStatItem> stat_;
  const char *metric_name_;
};
} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_TSI_UTILS_
