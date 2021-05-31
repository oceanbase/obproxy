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

#include "iocore/eventsystem/ob_event_system.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace event
{

inline int ObEvent::check_schedule_common()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("it was not inited, it should not happened", K(is_inited_), K(ret));
  } else if (OB_ISNULL(ethread_) || OB_UNLIKELY(ethread_->id_ != self_ethread().id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ethread_ is not curr ethread, it should not happened", K(ethread_), K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObEvent::schedule_imm(const int32_t acallback_event/*EVENT_IMMEDIATE*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_schedule_common())) {
    LOG_WARN("fail to check schedule common", K(ret));
  } else {
    if (in_the_priority_queue_) {
      ethread_->event_queue_.remove(this);
    }
    callback_event_ = acallback_event;
    timeout_at_ = 0;
    period_ = 0;
    mutex_ = continuation_->mutex_;
    if (!in_the_prot_queue_) {
      ethread_->event_queue_external_.enqueue_local(this);
    }
  }
  return ret;
}

int ObEvent::schedule_at(const ObHRTime atimeout_at, const int32_t acallback_event/*EVENT_INTERVAL*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_schedule_common())) {
    LOG_WARN("fail to check schedule common", K(ret));
  } else if (OB_UNLIKELY(atimeout_at <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("atimeout_at need bigger than zero", K(atimeout_at), K(ret));
  } else {
    if (in_the_priority_queue_) {
      ethread_->event_queue_.remove(this);
    }
    callback_event_ = acallback_event;
    timeout_at_ = atimeout_at;
    period_ = 0;
    mutex_ = continuation_->mutex_;
    if (!in_the_prot_queue_) {
      ethread_->event_queue_external_.enqueue_local(this);
    }
  }
  return ret;
}

int ObEvent::schedule_in(const ObHRTime atimeout_in, const int32_t acallback_event/*EVENT_INTERVAL*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_schedule_common())) {
    LOG_WARN("fail to check schedule common", K(ret));
  } else {
    if (in_the_priority_queue_) {
      ethread_->event_queue_.remove(this);
    }
    callback_event_ = acallback_event;
    timeout_at_ = get_hrtime() + atimeout_in;
    period_ = 0;
    mutex_ = continuation_->mutex_;
    if (!in_the_prot_queue_) {
      ethread_->event_queue_external_.enqueue_local(this);
    }
  }
  return ret;
}

int ObEvent::schedule_every(const ObHRTime aperiod, const int32_t acallback_event/*EVENT_INTERVAL*/)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_schedule_common())) {
    LOG_WARN("fail to check schedule common", K(ret));
  } else if (OB_UNLIKELY(0 == aperiod)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aperiod need not zero", K(aperiod), K(ret));
  } else {
    if (in_the_priority_queue_) {
      ethread_->event_queue_.remove(this);
    }
    if (aperiod < 0) {
      timeout_at_ = aperiod;
    } else {
      timeout_at_ = get_hrtime() + aperiod;
    }
    callback_event_ = acallback_event;
    period_ = aperiod;
    mutex_ = continuation_->mutex_;
    if (!in_the_prot_queue_) {
      ethread_->event_queue_external_.enqueue_local(this);
    }
  }
  return ret;
}

int64_t ObEvent::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), K_(in_the_prot_queue), K_(in_the_priority_queue),
       K_(in_heap), K_(callback_event), K_(timeout_at), K_(period)
#ifdef ENABLE_TIME_TRACE
       , K_(start_time)
#endif
       );
  databuff_printf(buf, buf_len, pos, ", ethread=%p", ethread_);
  databuff_printf(buf, buf_len, pos, ", continuation=%p", continuation_);
  databuff_printf(buf, buf_len, pos, ", cookie=%p", cookie_);
  databuff_printf(buf, buf_len, pos, ", mutex=%p", mutex_.ptr_);
  J_OBJ_END();
  return pos;
}

} // end of namespace event
} // end of namespace obproxy
} // end of namespace oceanbase
