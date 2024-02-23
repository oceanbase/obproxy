/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


#ifndef OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
#define OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/list/ob_dlist.h"
#include "lib/oblog/ob_log.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/list/ob_dlist.h"
#include "lib/coro/co_var.h"
#include "common/ob_clock_generator.h"
#include "lib/atomic/ob_atomic.h"

bool &get_tp_switch()
{
  RLOCAL(bool, turn_off);
  return turn_off;
}

#define INJECT_EVENT_CALL(event_no, ...) ({ \
      EventItem &item = ::oceanbase::common::EventTable::instance().get_event(event_no); \
      item.call(SELECT(1, ##__VA_ARGS__)); })

// to check if a certain tracepoint is set
// example: if (E(50) OB_SUCCESS) {...}
// you can also specify condition:
// if (E(50, session_id) OB_SUCCESS) { ... }
// which means:
//   check whether event 50 of session_id was raised

#ifdef ERRSIM 
#define OB_E(event_no, ...)  \
  INJECT_EVENT_CALL(event_no, ##__VA_ARGS__)?:
#else
#define OB_E(...)
#endif



// to set a particular tracepoint
// example: TP_SET_EVENT(50, 4016, 1, 1)
// specify condition: TP_SET_EVENT(50, 4016, 1, 1, 3302201)
// which means:
//   when session id is 3302201, trigger event 50 with error -4016
#ifdef ERRSIM
#define TP_SET_EVENT(id, error_in, occur, trigger_freq, ...)         \
  {                                                                  \
    EventItem item;                                                  \
    item.error_code_ = error_in;                                     \
    item.occur_ = occur;                                             \
    item.trigger_freq_ = trigger_freq;                               \
    item.cond_ = SELECT(1, ##__VA_ARGS__, 0);                        \
    ::oceanbase::common::EventTable::instance().set_event(id, item); \
  }

#define TP_RESET()                                         \
  { ::oceanbase::common::EventTable::instance().reset(); } \

#else
#define TP_SET_EVENT(...)
#define TP_RESET()
#endif

namespace oceanbase
{
namespace common
{

struct EventItem
{
  int64_t occur_;            // number of occurrences
  int64_t trigger_freq_;         // trigger frequency
  int64_t error_code_;        // error code to return
  int64_t cond_;

  EventItem()
    : occur_(0),
      trigger_freq_(0),
      error_code_(0),
      cond_(0) {}

  int call(const int64_t v) { return cond_ == v ? call() : 0; }
  int call()
  {
    int ret = 0;
    int64_t trigger_freq = trigger_freq_;
    if (occur_ > 0) {
      do {
        int64_t occur = occur_;
        if (occur > 0) {
          if (ATOMIC_VCAS(&occur_, occur, occur - 1)) {
            ret = static_cast<int>(error_code_);
            COMMON_LOG(WDIAG, "[ERRSIM] sim error", K(ret), K(occur_));
            break;
          }
        } else {
          ret = 0;
          break;
        }
      } while (true);
    } else if (OB_LIKELY(trigger_freq == 0)) {
      ret = 0;
    } else if (get_tp_switch()) { // true means skip errsim
      ret = 0;
    } else if (trigger_freq == 1) {
      ret = static_cast<int>(error_code_);
      COMMON_LOG(WDIAG, "[ERRSIM] sim error", K(ret));
    } else {
      if (rand() % trigger_freq == 0) {
        ret = static_cast<int>(error_code_);
        COMMON_LOG(WDIAG, "[ERRSIM] sim error", K(ret), K_(error_code), K(trigger_freq));
      } else {
        ret = 0;
      }
    }
    return ret;
  }

};

struct NamedEventItem : public ObDLinkBase<NamedEventItem>
{
  NamedEventItem(const char *name, ObDList<NamedEventItem> &l) : name_(name)
  {
    l.add_last(this);
  }
  operator int(void) { return item_.call(); }

  const char *name_;
  EventItem item_;
};

class EventTable
{
  public:
    EventTable() {
      for (int64_t i = 0; i < EVENT_TABLE_MAX; ++i) {
        memset(&(event_table_[i]), 0, sizeof(EventItem));
      }
    }
    virtual ~EventTable() {}

    // All tracepoints should be defined here before they can be used
    enum {
      EVENT_TABLE_INVALID = 0,
      EN_HANDLE_REQUEST_FAIL = 1,
      EN_TUNNEL_CLIENT_TIMEOUT = 2,
      EN_DUMMY_ENTRY_ERR = 3,
      EN_ORA_FATAL = 4,
      EN_CLIENT_INTERNAL_CMD_TIMEOUT = 5,
      EN_CLIENT_CONNECT_TIMEOUT = 6,
      EN_CLIENT_NET_READ_TIMEOUT = 7,
      EN_CLIENT_NET_WRITE_TIMEOUT = 8,
      EN_CLIENT_EXECUTE_PLAN_TIMEOUT = 9,
      EN_CLIENT_WAIT_TIMEOUT = 10,
      EN_SERVER_QUERY_TIMEOUT = 11,
      EN_SERVER_TRX_TIMEOUT = 12,
      EN_SERVER_WAIT_TIMEOUT = 13,
      EN_FORCE_SETUP_CLUSTER_RESOURCE_FORCE = 14,
      EN_CHECK_CLUSTER_VERSION_FAIL = 15,
      EN_FETCH_RSLIST_FAIL = 16,
      EN_SYNC_DATABASE_FAIL = 17,
      EN_CHECK_TENANT_MAX_CONNECTION_FAIL = 18,
      EN_HANDLE_PL_LOOKUP_FAIL = 19,
      EN_SYNC_SYS_VAR_FAIL = 20,
      EN_SYNC_USER_VAR_FAIL = 21,
      EN_SYNC_START_TRANS_FAIL = 22,
      EN_COM_LOGIN_FAIL = 23,
      EN_COM_HANDSHAKE_FAIL = 24,
      EN_COM_QUIT_FAIL = 25,
      EN_TRANSACTION_COORDINATOR_INVALID = 26,
      EN_COM_STMT_CLOSE_STACK_OVERFLOW = 27,
      EN_SERVER_OPEN_FAIL = 28,
      EVENT_TABLE_MAX 
    };

    /* get an event value */
    inline EventItem &get_event(int64_t index)
    { return (index >= 0 && index < EVENT_TABLE_MAX) ? event_table_[index] : event_table_[0]; }

    /* set an event value */
    inline void set_event(int64_t index, const EventItem &item)
    {
      if (index >= 0 && index < EVENT_TABLE_MAX) {
         event_table_[index] = item;
      }
    }
    inline void reset()
    {
      for (int64_t i = 0; i < EVENT_TABLE_MAX; ++i) {
        memset(&(event_table_[i]), 0, sizeof(EventItem));
      }
    }

    static inline void set_event(const char *name, const EventItem &item)
    {
      for (NamedEventItem *curr = global_item_list().get_first(); curr != global_item_list().get_header() && curr != NULL; curr = curr->get_next()) {
        if (NULL != curr->name_ && NULL != name && strcmp(curr->name_, name) == 0) {
          curr->item_ = item;
        }
      }
    }

    static ObDList<NamedEventItem> &global_item_list()
    {
      static ObDList<NamedEventItem> g_list;
      return g_list;
    }

    static EventTable &instance()
    {
      static EventTable et;
      return et;
    }

  private:
    /*
       Array of error codes for all tracepoints.
       For normal error code generation, the value should be the error code itself.
     */
    EventItem event_table_[EVENT_TABLE_MAX];
};

}
}

#endif //OCEANBASE_LIB_UTILITY_OB_TRACEPOINT_
