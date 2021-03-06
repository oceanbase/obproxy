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

#include "ob_link_queue.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{
bool ObSpLinkQueue::is_empty() const
{
  return ATOMIC_LOAD(&head_) == ATOMIC_LOAD(&tail_);
}

int ObSpLinkQueue::pop(Link *&p)
{
  int ret = OB_SUCCESS;
  while(OB_SUCC(ret) && OB_SUCC(do_pop(p)) && p == &dummy_) {
    ret = push(p);
  }
  return ret;
}

int ObSpLinkQueue::push(Link *p)
{
  int ret = OB_SUCCESS;
  Link *tail = NULL;
  if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    p->next_ = NULL;
    tail = ATOMIC_TAS(&tail_, p);
    ATOMIC_STORE(&tail->next_, p);
  }
  return ret;
}

int ObSpLinkQueue::do_pop(Link *&p)
{
  int ret = OB_SUCCESS;
  Link *head = NULL;
  while(NULL == (head = ATOMIC_TAS(&head_, NULL))) {
    PAUSE();
  }
  if (head == tail_) {
    ret = OB_EAGAIN;
    ATOMIC_STORE(&head_, head);
  } else {
    while(NULL == ATOMIC_LOAD(&head->next_)) {
      PAUSE();
    }
    ATOMIC_STORE(&head_, head->next_);
    p = head;
  }
  return ret;
}

int64_t ObLinkQueue::size() const
{
  uint64_t pop = ATOMIC_LOAD(&pop_);
  uint64_t push = ATOMIC_LOAD(&push_);
  return (int64_t)(push - pop);
}

static uint64_t faa_bounded(uint64_t *addr, uint64_t *limit_addr, uint64_t &limit)
{
  uint64_t ov = 0;
  uint64_t nv = ATOMIC_LOAD(addr);
  while (((ov = nv) < limit || ov < (limit = ATOMIC_LOAD(limit_addr)))
          && ov != (nv = ATOMIC_CAS(addr, ov, ov + 1))) {
    PAUSE();
  }
  return nv;
}

int ObLinkQueue::push(Link *p)
{
  int ret = OB_SUCCESS;
  if (NULL == p) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    uint64_t push_idx = ATOMIC_FAA(&push_, 1);
    ObSpLinkQueue *pqueue = queue_ + idx(push_idx);
    while(OB_SUCCESS != pqueue->push(p)) {
      ;
    }
  }
  return ret;
}

int ObLinkQueue::pop(Link *&p)
{
  int ret = OB_SUCCESS;
  uint64_t pop_limit = 0;
  uint64_t pop_idx = faa_bounded(&pop_, &push_, pop_limit);
  if (pop_idx >= pop_limit) {
    ret = OB_EAGAIN;
    p = NULL;
  } else {
    ObSpLinkQueue *pqueue = queue_ + idx(pop_idx);
    while(OB_SUCCESS != pqueue->pop(p)) {
      ;
    }
  }
  return ret;
}

} // end namespace common
} // end namespace oceanbase
