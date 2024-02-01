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

#include "lib/task/ob_timer.h"
#include "lib/time/ob_time_utility.h"

namespace oceanbase
{
namespace common
{
using namespace tbutil;

const int32_t ObTimer::MAX_TASK_NUM;

int ObTimer::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else {
    if (0 != pthread_create(&tid_, NULL, ObTimer::run_thread, this)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      is_inited_ = true;
      is_destroyed_ = false;
      is_stopped_ = false;
      has_running_repeat_task_ = false;
    }
  }
  return ret;
}

ObTimer::~ObTimer()
{
  destroy();
}

int ObTimer::start()
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  is_stopped_ = false;
  monitor_.notifyAll();
  OB_LOG(INFO, "ObTimer start success");
  return ret;
}

int ObTimer::stop()
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  is_stopped_ = true;
  monitor_.notifyAll();
  OB_LOG(INFO, "ObTimer stop success");
  return ret;
}

int ObTimer::wait()
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  while (has_running_task_) {
    static const int64_t WAIT_INTERVAL_US = 2000000; // 2s
    (void)monitor_.timedWait(Time(WAIT_INTERVAL_US));
  }
  return ret;
}

void ObTimer::destroy()
{
  if (!is_destroyed_ && is_inited_) {
    Monitor<Mutex>::Lock guard(monitor_);
    is_stopped_ = true;
    is_destroyed_ = true;
    is_inited_ = false;
    has_running_repeat_task_=false;
    monitor_.notifyAll();
    tasks_num_ = 0;
    OB_LOG(INFO, "ObTimer destroy");
  }
  if (tid_ != 0) {
    (void)pthread_join(tid_, NULL);
    tid_ = 0;
  }
}

bool ObTimer::task_exist(const ObTimerTask &task)
{
  bool ret = false;
  Monitor<Mutex>::Lock guard(monitor_);
  for (int pos = 0; pos < tasks_num_; ++pos) {
    if (tokens_[pos].task == &task) {
      ret = true;
      break;
    }
  }
  return ret;
}

int ObTimer::schedule(ObTimerTask &task, const int64_t delay, const bool repeate /*=false*/)
{
  int ret = OB_SUCCESS;
  const bool schedule_immediately = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = schedule_task(task, delay, repeate, schedule_immediately);
  }
  return ret;
}

int ObTimer::schedule_repeate_task_immediately(ObTimerTask &task, const int64_t delay)
{
  int ret = OB_SUCCESS;
  const bool schedule_immediately = true;
  const bool repeate = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    ret = schedule_task(task, delay, repeate, schedule_immediately);
  }
  return ret;
}

int ObTimer::schedule_task(ObTimerTask &task, const int64_t delay, const bool repeate,
    const bool is_scheduled_immediately)
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (delay < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WDIAG, "invalid argument", K(ret), K(delay));
  } else if (tasks_num_ >= MAX_TASK_NUM) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WDIAG, "too much timer task", K(ret), K_(tasks_num), "max_task_num", MAX_TASK_NUM);
  } else {
    int64_t time = Time::now(Time::Monotonic).toMicroSeconds();
    if(!is_scheduled_immediately) {
      time += delay;
    }
    if (is_stopped_) {
      ret = OB_CANCELED;
      OB_LOG(WDIAG, "schedule task on stopped timer", K(ret), K(task));
    } else {
      ret = insert_token(Token(time, repeate ? delay : 0, &task));
      if (OB_SUCC(ret)) {
        if (0 == wakeup_time_ || wakeup_time_ >= time) {
          monitor_.notify();
        }
      } else {
        OB_LOG(WDIAG, "insert token failed", K(ret), K(task));
      }
    }
  }
  return ret;
}

int ObTimer::insert_token(const Token &token)
{
  int ret = OB_SUCCESS;
  int32_t max_task_num= MAX_TASK_NUM;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    if (has_running_repeat_task_) {
      max_task_num = MAX_TASK_NUM - 1;
    }
    if (tasks_num_ >= max_task_num) {
      OB_LOG(WDIAG, "tasks_num_ exceed max_task_num", K_(tasks_num), "max_task_num", MAX_TASK_NUM);
      ret = OB_ERR_UNEXPECTED;
    } else {
      int64_t pos = 0;
      for (pos = 0; pos < tasks_num_; ++pos) {
        if (token.scheduled_time <= tokens_[pos].scheduled_time) {
          break;
        }
      }
      for (int64_t i = tasks_num_; i > pos; --i) {
        tokens_[i] = tokens_[i - 1];
      }
      tokens_[pos] = token;
      ++tasks_num_;
    }
  }
  return ret;
}

int ObTimer::cancel(const ObTimerTask &task)
{
  int ret = OB_SUCCESS;
  Monitor<Mutex>::Lock guard(monitor_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    int64_t pos = -1;
    for (int64_t i = 0; i < tasks_num_; ++i) {
      if (&task == tokens_[i].task) {
        pos = i;
        break;
      }
    }
    if (pos != -1) {
      memmove(&tokens_[pos], &tokens_[pos + 1],
              sizeof(tokens_[0]) * (tasks_num_ - pos - 1));
      --tasks_num_;
    }
  }
  return ret;
}

void ObTimer::cancel_all()
{
  Monitor<Mutex>::Lock guard(monitor_);
  tasks_num_ = 0;
}

void *ObTimer::run_thread(void *arg)
{
  if (NULL == arg) {
    OB_LOG(EDIAG, "timer thread failed to start", "tid", GETTID());
  } else {
    ObTimer *t = reinterpret_cast<ObTimer *>(arg);
    t->run();
  }
  return NULL;
}

void ObTimer::run()
{
  int tmp_ret = OB_SUCCESS;
  Token token(0, 0, NULL);
  OB_LOG(INFO, "timer thread started", "tid", syscall(__NR_gettid));
  while (true) {
    {
      Monitor<Mutex>::Lock guard(monitor_);
      if (is_destroyed_) {
        break;
      }
      //add repeated task to tasks_ again
      if (token.delay != 0) {
        has_running_repeat_task_ = false;
        token.scheduled_time = Time::now(Time::Monotonic).toMicroSeconds() + token.delay;
        if (OB_SUCCESS != (tmp_ret = insert_token(
            Token(token.scheduled_time, token.delay, token.task)))) {
          OB_LOG(WDIAG, "insert token error", K(tmp_ret), K(token));
        }
      }
      has_running_task_ = false;
      if (is_stopped_) {
        monitor_.notifyAll();
      }
      while (!is_destroyed_ && is_stopped_) {
        monitor_.wait();
      }
      if (is_destroyed_) {
        break;
      }
      if (0 == tasks_num_) {
        wakeup_time_ = 0;
        monitor_.wait();
      }
      if (is_stopped_) {
        continue;
      }
      if (is_destroyed_) {
        break;
      }
      while (tasks_num_ > 0 && !is_destroyed_ && !is_stopped_) {
        const int64_t now = Time::now(Time::Monotonic).toMicroSeconds();
        if (tokens_[0].scheduled_time <= now) {
          has_running_task_ = true;
          token = tokens_[0];
          memmove(tokens_, tokens_ + 1, (tasks_num_ - 1) * sizeof(tokens_[0]));
          --tasks_num_;
          if (token.delay != 0) {
            has_running_repeat_task_ = true;
          }
          break;
        }
        if (is_stopped_) {
          continue;
        } else if (is_destroyed_) {
          break;
        } else {
          wakeup_time_ = tokens_[0].scheduled_time;
          {
            const int64_t rt1 = ObTimeUtility::current_time();
            const int64_t rt2 = ObTimeUtility::current_time_coarse();
            const int64_t delta = rt1 > rt2 ? (rt1 - rt2) : (rt2 - rt1);
            static const int64_t MAX_REALTIME_DELTA1 = 20000; // 20ms
            static const int64_t MAX_REALTIME_DELTA2 = 500000; // 500ms
            if (delta > MAX_REALTIME_DELTA1) {
              OB_LOG(WDIAG, "Hardware clock skew", K(rt1), K(rt2), K_(wakeup_time), K(now));
            } else if (delta > MAX_REALTIME_DELTA2) {
              OB_LOG(EDIAG, "Hardware clock error", K(rt1), K(rt2), K_(wakeup_time), K(now));
            }
          }
          monitor_.timedWait(Time(wakeup_time_ - now));
        }
      }
    }
    if (token.task != NULL && !is_destroyed_ && !is_stopped_) {
      const int64_t start_time = ::oceanbase::common::ObTimeUtility::current_time();
      token.task->runTimerTask();
      const int64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
      if (end_time - start_time > 1000 * 1000) {
        OB_LOG(WDIAG, "timer task cost too much time", "task", to_cstring(*token.task),
            K(start_time), K(end_time), "used", end_time - start_time);
      }
    }
  }
  OB_LOG(INFO, "timer thread exit");
}

void ObTimer::dump() const
{
  for (int32_t i = 0; i < tasks_num_; ++i) {
    printf("%d : %ld %ld %p\n", i, tokens_[i].scheduled_time, tokens_[i].delay, tokens_[i].task);
  }
}

bool ObTimer::inited() const
{
  return is_inited_;
}

} /* common */
} /* chunkserver */
