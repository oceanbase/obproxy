/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * **************************************************************
 *
 * This file contain:
 * SpLinkQueue: a queue based list, support multi-thread push, single thread pop.
 * LinkQueue: a queue based list, support multi-thread push, multi-thread pop.
 * 1. implement based list, no capacity limit.
 * 2. For ease of use, LinkQueue is not based on hazard, and pop is not lock-free so that free can be as soon as possible.
 * return:
 * push() always success.
 * pop() if NULL return OB_EAGAIN;
 * top() if NULL return OB_EAGAIN;
 */

#ifndef OCEANBASE_QUEUE_OB_LINK_QUEUE_
#define OCEANBASE_QUEUE_OB_LINK_QUEUE_
#include "lib/ob_define.h"
#include "lib/queue/ob_link.h"  // ObLink

namespace oceanbase
{
namespace common
{
typedef ObLink QLink;

class ObSpLinkQueue
{
public:
  typedef QLink Link;
  ObSpLinkQueue() : head_(&dummy_), tail_(&dummy_), dummy_() {}
  ~ObSpLinkQueue() {}
public:
  int pop(Link *&p);
  int push(Link *p);
  bool is_empty() const;
private:
  int do_pop(Link *&p);
private:
  Link *head_ CACHE_ALIGNED;
  Link *tail_ CACHE_ALIGNED;
  Link dummy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSpLinkQueue);
};

class ObLinkQueue
{
public:
  typedef QLink Link;
  enum { QUEUE_COUNT = 4096 };
  ObLinkQueue() : queue_(), push_(0), pop_(0)
  {}
  ~ObLinkQueue() {}
  int push(Link *p);
  int pop(Link *&p);
  int64_t size() const;
private:
  static int64_t idx(int64_t x) { return x & (QUEUE_COUNT - 1); }
private:
  ObSpLinkQueue queue_[QUEUE_COUNT];
  uint64_t push_ CACHE_ALIGNED;
  uint64_t pop_ CACHE_ALIGNED;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLinkQueue);
};

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_QUEUE_OB_LINK_QUEUE_
