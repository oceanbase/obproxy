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

#include "proxy/api/ob_api_utils_internal.h"
#include "proxy/mysql/ob_mysql_sm.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
using namespace common;
using namespace event;

// This is the highest txn arg that can be used, we choose this
// value to minimize the likelihood of it causing any problems.
const int TRANSACTION_STORAGE_INDEX = MYSQL_SSN_TXN_MAX_USER_ARG - 1;

static inline void invoke_plugin_for_event(ObApiPlugin *plugin, ObMysqlSM *sm, ObEventType event)
{
  ObApiTransaction *transaction = ObApiUtilsInternal::get_api_transaction(sm);
  if (NULL != transaction) {
    switch (event) {
      case OB_EVENT_MYSQL_TXN_START:
        plugin->handle_txn_start(*transaction);
        break;

      case OB_EVENT_MYSQL_READ_REQUEST:
        plugin->handle_read_request(*transaction);
        break;

      case OB_EVENT_MYSQL_SEND_REQUEST:
        plugin->handle_send_request(*transaction);
        break;

      case OB_EVENT_MYSQL_READ_RESPONSE:
        plugin->handle_read_response(*transaction);
        break;

      case OB_EVENT_MYSQL_SEND_RESPONSE:
        plugin->handle_send_response(*transaction);
        break;

      case OB_EVENT_MYSQL_OBSERVER_PL:
        plugin->handle_observer_partition_location(*transaction);
        break;

      case OB_EVENT_MYSQL_CMD_COMPLETE:
        plugin->handle_cmd_complete(*transaction);
        break;

      case OB_EVENT_MYSQL_TXN_CLOSE:
        plugin->handle_txn_close(*transaction);
        break;

      default:
        ERROR_API("invalid event type"); // we should never get here
        break;
    }
  }
}

static inline void invoke_plugin_for_event(
    ObApiPlugin *plugin, ObProxyClientSession *ssn, ObEventType event)
{
  switch (event) {
    case OB_EVENT_MYSQL_SSN_START:
      plugin->handle_ssn_start(ssn);
      break;

    case OB_EVENT_MYSQL_SSN_CLOSE:
      plugin->handle_ssn_close(ssn);
      break;

    default:
      ERROR_API("invalid event type"); // we should never get here
      break;
  }
}

ObApiTransaction *ObApiUtilsInternal::get_api_transaction(ObMysqlSM *sm, const bool create)
{
  ObApiTransaction *transaction = static_cast<ObApiTransaction *>(
      sm->trans_state_.user_args_[TRANSACTION_STORAGE_INDEX]);
  if (NULL == transaction && create) {
    transaction = ObApiTransaction::alloc(sm);
    if (NULL != transaction) {
      DEBUG_API("Created new transaction object at %p for pointer %p", transaction, sm);
      sm->trans_state_.user_args_[TRANSACTION_STORAGE_INDEX] = transaction;
    } else {
      ERROR_API("failed to allocate memory for ObApiTransaction");
    }
  }

  return transaction;
}

ObPtr<ObProxyMutex> &ObApiUtilsInternal::get_transaction_plugin_mutex(
    ObTransactionPlugin &transaction_plugin)
{
  return transaction_plugin.get_mutex();
}

ObMysqlHookID ObApiUtilsInternal::convert_internal_hook(ObApiPlugin::ObHookType hooktype)
{
  ObMysqlHookID ret = OB_MYSQL_LAST_HOOK;
  switch (hooktype) {
    case ObApiPlugin::HOOK_SSN_START:
      ret = OB_MYSQL_SSN_START_HOOK;
      break;

    case ObApiPlugin::HOOK_TXN_START:
      ret = OB_MYSQL_TXN_START_HOOK;
      break;

    case ObApiPlugin::HOOK_READ_REQUEST:
      ret = OB_MYSQL_READ_REQUEST_HOOK;
      break;

    case ObApiPlugin::HOOK_READ_RESPONSE:
      ret = OB_MYSQL_READ_RESPONSE_HOOK;
      break;

    case ObApiPlugin::HOOK_SEND_REQUEST:
      ret = OB_MYSQL_SEND_REQUEST_HOOK;
      break;

    case ObApiPlugin::HOOK_SEND_RESPONSE:
      ret = OB_MYSQL_SEND_RESPONSE_HOOK;
      break;

    case ObApiPlugin::HOOK_OBSERVER_PL:
      ret = OB_MYSQL_OBSERVER_PL_HOOK;
      break;

    case ObApiPlugin::HOOK_CMD_COMPLETE:
      ret = OB_MYSQL_CMD_COMPLETE_HOOK;
      break;

    case ObApiPlugin::HOOK_TXN_CLOSE:
      ret = OB_MYSQL_TXN_CLOSE_HOOK;
      break;

    case ObApiPlugin::HOOK_SSN_CLOSE:
      ret = OB_MYSQL_SSN_CLOSE_HOOK;
      break;

    default:
      ERROR_API("invalid hook type"); // shouldn't happen, let's catch it early
      break;
  }

  return ret;
}

ObMysqlHookID ObApiUtilsInternal::convert_internal_transformation_type(
    ObTransformationPlugin::ObTransformType type)
{
  ObMysqlHookID ret = OB_MYSQL_LAST_HOOK;
  switch (type) {
    case ObTransformationPlugin::REQUEST_TRANSFORMATION:
      ret = OB_MYSQL_REQUEST_TRANSFORM_HOOK;
      break;

    case ObTransformationPlugin::RESPONSE_TRANSFORMATION:
      ret = OB_MYSQL_RESPONSE_TRANSFORM_HOOK;
      break;

    default:
      ERROR_API("invalid transformation type"); // shouldn't happen, let's catch it early
      break;
  }

  return ret;
}

void ObApiUtilsInternal::invoke_plugin_for_event(
    ObTransactionPlugin *plugin, ObMysqlSM *sm, ObEventType event)
{
  if (NULL != plugin) {
    MUTEX_LOCK(lock, plugin->get_mutex(), this_ethread());
    proxy::invoke_plugin_for_event(plugin, sm, event);
  } else {
    mysqlsm_reenable(sm, OB_EVENT_MYSQL_CONTINUE);
  }
}

void ObApiUtilsInternal::invoke_plugin_for_event(
    ObSessionPlugin *plugin, ObProxyClientSession *ssn, ObEventType event)
{
  ob_assert(OB_EVENT_MYSQL_SSN_START == event || OB_EVENT_MYSQL_SSN_CLOSE == event);
  if (NULL != plugin) {
    proxy::invoke_plugin_for_event(plugin, ssn, event);
  } else {
    ssn_reenable(ssn, OB_EVENT_MYSQL_CONTINUE);
  }
}

void ObApiUtilsInternal::invoke_plugin_for_event(
    ObGlobalPlugin *plugin, void *data, ObEventType event)
{
  if (OB_EVENT_MYSQL_SSN_START == event || OB_EVENT_MYSQL_SSN_CLOSE == event) {
    if (NULL != plugin) {
      proxy::invoke_plugin_for_event(plugin, static_cast<ObProxyClientSession *>(data), event);
    } else {
      ssn_reenable(static_cast<ObProxyClientSession *>(data), OB_EVENT_MYSQL_CONTINUE);
    }
  } else {
    if (NULL != plugin) {
      proxy::invoke_plugin_for_event(plugin, static_cast<ObMysqlSM *>(data), event);
    } else {
      mysqlsm_reenable(static_cast<ObMysqlSM *>(data), OB_EVENT_MYSQL_CONTINUE);
    }
  }
}

ObAction *ObApiUtilsInternal::cont_schedule(
    ObContInternal *cont, ObHRTime timeout, ObThreadPool tp)
{
  ObAction *action = NULL;
  ObEventThreadType etype = ET_TASK;

  if (ATOMIC_FAA((int *) &cont->event_count_, 1) < 0) {
    ob_assert (!"not reached");
  }

  switch (tp) {
  case OB_THREAD_POOL_NET:
  case OB_THREAD_POOL_DEFAULT:
    etype = ET_NET;
    break;

  case OB_THREAD_POOL_TASK:
    etype = ET_TASK;
    break;

  default:
    etype = ET_NET;
    break;
  }

  if (0 == timeout) {
    if (ET_NET == etype && REGULAR == this_ethread()->tt_) {
      action = this_ethread()->schedule_imm(cont);
    } else {
      action = g_event_processor.schedule_imm(cont, etype);
    }
  } else {
    if (ET_NET == etype && REGULAR == this_ethread()->tt_) {
      action = this_ethread()->schedule_in(cont, HRTIME_MSECONDS(timeout));
    } else {
      action = g_event_processor.schedule_in(cont, HRTIME_MSECONDS(timeout), etype);
    }
  }

  // This is a hack. SHould be handled in ink_types
  action = (ObAction *) ((uintptr_t) action | 0x1);
  return action;
}

ObAction *ObApiUtilsInternal::cont_schedule_every(
    ObContInternal *cont, ObHRTime every, ObThreadPool tp)
{
  ObAction *action = NULL;
  ObEventThreadType etype = ET_TASK;

  if (ATOMIC_FAA((int *)&cont->event_count_, 1) < 0) {
    ob_assert (!"not reached");
  }

  switch (tp) {
  case OB_THREAD_POOL_NET:
  case OB_THREAD_POOL_DEFAULT:
    etype = ET_NET;
    break;

  case OB_THREAD_POOL_TASK:
    etype = ET_TASK;
    break;

  default:
    etype = ET_TASK;
    break;
  }

  if (ET_NET == etype && REGULAR == this_ethread()->tt_) {
    action = this_ethread()->schedule_every(cont, HRTIME_MSECONDS(every));
  } else {
    action = g_event_processor.schedule_every(cont, HRTIME_MSECONDS(every), etype);
  }

  // This is a hack. SHould be handled in types
  action = (ObAction *) ((uintptr_t) action | 0x1);
  return action;
}

ObAction *ObApiUtilsInternal::mysql_schedule(ObContInternal *cont, ObMysqlSM *sm, ObHRTime timeout)
{
  ObAction *action = NULL;
  if (ATOMIC_FAA((int *) &cont->event_count_, 1) < 0) {
    ob_assert (!"not reached");
  }

  sm->api_.set_mysql_schedule(cont);

  if (0 == timeout) {
    if (REGULAR == this_ethread()->tt_) {
      action = this_ethread()->schedule_imm(sm);
    } else {
      action = g_event_processor.schedule_imm(sm, ET_NET);
    }
  } else {
    if (REGULAR == this_ethread()->tt_) {
      action = this_ethread()->schedule_in(sm, HRTIME_MSECONDS(timeout));
    } else {
      action = g_event_processor.schedule_in(sm, HRTIME_MSECONDS(timeout), ET_NET);
    }
  }

  action = (ObAction *) ((uintptr_t) action | 0x1);
  return action;
}

void ObApiUtilsInternal::action_cancel(ObAction *action)
{
  ObAction *a = NULL;
  ObContInternal *cont = NULL;

  // This is a hack. SHould be handled in ink_types
  if (0 != ((uintptr_t) action & 0x1)) {
    a = (ObAction *) ((uintptr_t) action - 1);
    cont = (ObContInternal *) a->continuation_;
    cont->handle_event_count(EVENT_IMMEDIATE);
  } else {
    a = action;
  }

  a->cancel();
}

class ObMysqlSMCallback : public ObContinuation
{
public:
  ObMysqlSMCallback(ObMysqlSM *sm, ObEventType event)
      : ObContinuation(sm->mutex_), sm_(sm), event_(event)
  {
    SET_HANDLER(&ObMysqlSMCallback::event_handler);
  }
  virtual ~ObMysqlSMCallback() {}

  static ObMysqlSMCallback *alloc(ObMysqlSM *sm, ObEventType event)
  {
    return op_reclaim_alloc_args(ObMysqlSMCallback, sm, event);
  }

  void destroy()
  {
    mutex_.release(); // decrease the reference count of mutex
  }

  int event_handler(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    sm_->state_api_callback((int)event_, 0);
    destroy();
    return 0;
  }

private:
  ObMysqlSM *sm_;
  ObEventType event_;
};


void ObApiUtilsInternal::mysqlsm_reenable(ObMysqlSM *sm, ObEventType event)
{
  ob_assert(NULL != sm);
  ObEThread *eth = this_ethread();

  // If this function is being executed on a thread which was not
  // created using the ObEThread API, eth will be NULL, and the
  // continuation needs to be called back on a REGULAR thread.
  //
  // If this function is being executed on a thread created by the API
  // which is DEDICATED, the continuation needs to be called back on a
  // REGULAR thread.
  if (NULL == eth || REGULAR != eth->tt_) {
    g_event_processor.schedule_imm(ObMysqlSMCallback::alloc(sm, event), ET_NET);
  } else {
    MUTEX_TRY_LOCK(trylock, sm->mutex_, eth);
    if (!trylock.is_locked()) {
      eth->schedule_imm(ObMysqlSMCallback::alloc(sm, event));
    } else {
      sm->state_api_callback((int)event, 0);
    }
  }
}

class ObMysqlSsnCallback : public ObContinuation
{
public:
  ObMysqlSsnCallback(ObProxyClientSession *ssn, ObEventType event)
      : ObContinuation(ssn->mutex_), ssn_(ssn), event_(event)
  {
    SET_HANDLER(&ObMysqlSsnCallback::event_handler);
  }
  virtual ~ObMysqlSsnCallback() {}

  static ObMysqlSsnCallback *alloc(ObProxyClientSession *ssn, ObEventType event)
  {
    return op_reclaim_alloc_args(ObMysqlSsnCallback, ssn, event);
  }

  void destroy()
  {
    mutex_.release(); // decrease the reference count of mutex
  }

  int event_handler(int event, void *data)
  {
    UNUSED(event);
    UNUSED(data);
    ssn_->handle_event((int)event_, 0);
    destroy();
    return 0;
  }

private:
  ObProxyClientSession *ssn_;
  ObEventType event_;
};

void ObApiUtilsInternal::ssn_reenable(ObProxyClientSession *ssn, ObEventType event)
{
  ObEThread *eth = this_ethread();

  // If this function is being executed on a thread created by the API
  // which is DEDICATED, the continuation needs to be called back on a
  // REGULAR thread.
  if (REGULAR != eth->tt_) {
    g_event_processor.schedule_imm(ObMysqlSsnCallback::alloc(ssn, event), ET_NET);
  } else {
    MUTEX_TRY_LOCK(trylock, ssn->mutex_, eth);
    if (!trylock.is_locked()) {
      eth->schedule_imm(ObMysqlSsnCallback::alloc(ssn, event));
    } else {
      ssn->handle_event((int)event, 0);
    }
  }
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
