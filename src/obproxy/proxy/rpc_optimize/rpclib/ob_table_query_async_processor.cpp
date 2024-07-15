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

#define USING_LOG_PREFIX PROXY

#include "proxy/rpc_optimize/rpclib/ob_table_query_async_processor.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObTableQueryAsyncEntryCont : public ObContinuation
{
public:
  ObTableQueryAsyncEntryCont();
  virtual ~ObTableQueryAsyncEntryCont() {}

  int main_handler(int event, void *data);
  int init(ObTableQueryAsyncParam &param);
  ObAction *get_action() { return &action_; }
  static const char *get_event_name(const int64_t event);
  void kill_this();
  void set_need_notify(const bool need_notify) { need_notify_ = need_notify; }

private:
  int start_lookup_table_query_async_entry();
  int lookup_entry_in_cache();
  int handle_lookup_cache_done();
  int notify_caller();

private:
  uint32_t magic_;
  ObTableQueryAsyncParam param_;

  ObAction *pending_action_;
  ObAction action_;

  ObTableQueryAsyncEntry *gcached_entry_; // the entry from global cache

  bool kill_self_;
  bool need_notify_;
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncEntryCont);
};

ObTableQueryAsyncEntryCont::ObTableQueryAsyncEntryCont()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE),
    param_(), pending_action_(NULL), action_(), gcached_entry_(NULL),
    kill_self_(false), need_notify_(true)
{
  SET_HANDLER(&ObTableQueryAsyncEntryCont::main_handler);
}

inline int ObTableQueryAsyncEntryCont::init(ObTableQueryAsyncParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid table query async param", K(param), K(ret));
  } else {
    param_.deep_copy(param);
    action_.set_continuation(param.cont_);
    mutex_ = param.cont_->mutex_;
  }

  return ret;
}

void ObTableQueryAsyncEntryCont::kill_this()
{
  param_.reset();
  int ret = OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending action", K_(pending_action), K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  if (NULL != gcached_entry_) {
    gcached_entry_->dec_ref();
    gcached_entry_ = NULL;
  }

  action_.set_continuation(NULL);
  magic_ = OB_CONT_MAGIC_DEAD;
  mutex_.release();

  op_free(this);
}

const char *ObTableQueryAsyncEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT: {
      name = "RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT";
      break;
    }
    case RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE: {
      name = "RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE";
      break;
    }
    case RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_EVENT: {
      name = "RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

int ObTableQueryAsyncEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObTableQueryAsyncEntryCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_CONT_MAGIC_ALIVE != magic_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this table query async entry cont is dead", K_(magic), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT: {
        if (OB_FAIL(start_lookup_table_query_async_entry())) {
          LOG_WDIAG("fail to start lookup table query async entry", K(ret));
        }
        break;
      }
      case RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_EVENT: {
        if (OB_FAIL(lookup_entry_in_cache())) {
          LOG_WDIAG("fail to lookup enty in cache", K(ret));
        }
        break;
      }
      case RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE: {
        if (OB_FAIL(handle_lookup_cache_done())) {
          LOG_WDIAG("fail to handle lookup cache done", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknow event", K(event), K(data), K(ret));
        break;
      }
    }
  }

  // if failed, just inform out
  if (OB_FAIL(ret)) {
    if (NULL != (param_.result_.target_entry_)) {
      param_.result_.target_entry_->dec_ref();
      param_.result_.target_entry_ = NULL;
    }
    if (OB_FAIL(notify_caller())) {
      LOG_WDIAG("fail to notify caller result", K(ret));
    }
  }

  if (kill_self_) {
    kill_this();
    he_ret = EVENT_DONE;
  }
  return he_ret;
}

int ObTableQueryAsyncEntryCont::start_lookup_table_query_async_entry()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lookup_entry_in_cache())) {
    LOG_WDIAG("fail to lookup enty in cache", K(ret));
  }

  return ret;
}

int ObTableQueryAsyncEntryCont::handle_lookup_cache_done()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gcached_entry_)) {
    // do nothing
    LOG_DEBUG("handle_lookup_cache_done get nothing", KP_(gcached_entry));
  } else if (!gcached_entry_->is_avail_state()) {
    LOG_INFO("this table query async entry is not in avail state", KPC_(gcached_entry));
    gcached_entry_->dec_ref();
    gcached_entry_ = NULL;
  }

  if (OB_SUCC(ret)) {
    param_.result_.target_entry_ = gcached_entry_;
    gcached_entry_ = NULL;
    if (OB_FAIL(notify_caller())) { // notify_caller
      LOG_WDIAG("fail to notify caller", K(ret));
    }
  }

  return ret;
}

int ObTableQueryAsyncEntryCont::lookup_entry_in_cache()
{
  int ret = OB_SUCCESS;
  uint64_t key = param_.key_;
  ObAction *action = NULL;
  ret = get_global_table_query_async_cache().get_table_query_async_entry(this, key, &gcached_entry_, action);
  if (OB_SUCC(ret)) {
    if (NULL != action) {
      pending_action_ = action;
    } else if (OB_FAIL(handle_lookup_cache_done())) {
      LOG_WDIAG("fail to handle lookup cache done", K(ret));
    }
  } else {
    LOG_WDIAG("fail to get table query async loaction entry", K_(param), K(ret));
  }

  return ret;
}

int ObTableQueryAsyncEntryCont::notify_caller()
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncEntry *&entry = param_.result_.target_entry_;
  if (NULL != entry && entry->is_avail_state()) {
    ObTableQueryAsyncRefHashMap &query_async_map = self_ethread().get_table_query_async_map();
    if (OB_FAIL(query_async_map.set(entry))) {
      LOG_WDIAG("fail to set table query async map", KPC(entry), K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }

  if (action_.cancelled_) {
    // when cancelled, do no forget free the table entry
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
    LOG_INFO("ObTableQueryAsyncEntryCont has been cancelled", K_(param));
  } else if (need_notify_) {
    action_.continuation_->handle_event(RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE, &param_.result_);
    param_.result_.reset();
  } else {
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
    param_.result_.reset();
  }

  if (OB_SUCC(ret)) {
    kill_self_ = true;
  }
  return ret;
}

int64_t ObTableQueryAsyncParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(new_async_query),
       K_(key),
       K_(result));
  J_OBJ_END();
  return pos;
}

inline void ObTableQueryAsyncParam::deep_copy(ObTableQueryAsyncParam &other)
{
  if (this != &other) {
    cont_ = other.cont_;
    key_ = other.key_;
    new_async_query_ = other.new_async_query_;
  }
}

uint64_t ObTableQueryAsyncProcessor::RpcQuerySessionIdGenerator::rpc_query_sessionid_generator_ = 0;

uint64_t ObTableQueryAsyncProcessor::get_new_query_session_id()
{
  uint64_t query_session_id = 0;
  while(0 == query_session_id) {
    query_session_id = ATOMIC_AAF(&(RpcQuerySessionIdGenerator::rpc_query_sessionid_generator_), 1);
  }
  return query_session_id;
}

int ObTableQueryAsyncProcessor::alloc_new_table_query_async_entry(ObTableQueryAsyncParam &param)
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncEntry *entry = NULL;
  uint64_t query_session_id = 0;

  if (OB_FAIL(ObTableQueryAsyncEntry::allocate(entry))) {
    LOG_WDIAG("fail to alloc ObTableQueryAsyncEntry", K(ret));
  } else {
    query_session_id = get_new_query_session_id();

    if (OB_FAIL(entry->init(query_session_id))) {
      LOG_WDIAG("fail to init ObTableQueryAsyncEntry", K(ret));
    } else {
      LOG_DEBUG("succ to alloc new ObTableQueryAsyncEntry", K(query_session_id), KPC(entry));
      param.key_ = query_session_id;
      param.result_.target_entry_ = entry;

      entry->inc_ref();   //inc before add to cache
      entry->set_building_state();
      if (OB_FAIL(get_global_table_query_async_cache().add_table_query_async_entry(*entry, false))) {
        LOG_WDIAG("fail to add table query async entry", KPC(entry), K(ret));
        entry->dec_ref();
      } else {
        LOG_DEBUG("succ to add table query async entry into global cache", KPC(entry));
      }
    }

    if (OB_FAIL(ret)) {
      entry->dec_ref();
      entry = NULL;
    }
  }

  return ret;
}

int ObTableQueryAsyncProcessor::get_table_query_async_entry(ObTableQueryAsyncParam &param, ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObTableQueryAsyncEntry *tmp_entry = NULL;
  ObTableQueryAsyncEntryCont *cont = NULL;
  if (param.new_async_query_) {
    // alloc new Async Entry
    if (OB_FAIL(alloc_new_table_query_async_entry(param))) {
      LOG_WDIAG("fail to call alloc_new_table_query_async_entry", K(ret));
    } else if (OB_NOT_NULL(param.result_.target_entry_)) {
      ObTableQueryAsyncRefHashMap &query_async_map = self_ethread().get_table_query_async_map();
      if (OB_FAIL(query_async_map.set(param.result_.target_entry_))) {
        LOG_WDIAG("fail to set table query async map", KPC_(param.result_.target_entry), K(ret));
        ret = OB_SUCCESS; // ignore ret
      }
    }
  } else if (OB_FAIL(get_table_query_async_entry_from_thread_cache(param, tmp_entry))) {
    LOG_WDIAG("fail to get table query async entry in thread cache", K(param), K(ret));
  } else if (NULL != tmp_entry) { // thread cache hit
    // hand over ref
    param.result_.target_entry_ = tmp_entry;
    tmp_entry = NULL;
  } else {
    // 2. find table query async entry global cache
    cont = op_alloc(ObTableQueryAsyncEntryCont);
    if (OB_ISNULL(cont)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc ObTableQueryAsyncEntryCont", K(ret));
    } else if (OB_FAIL(cont->init(param))) {
      LOG_WDIAG("fail to init table query async entry cont", K(ret));
    } else {
      action = cont->get_action();
      if (OB_ISNULL(self_ethread().schedule_imm(cont, RPC_QUERY_ASYNC_ENTRY_LOOKUP_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule imm", K(ret));
      }
    }

    if (OB_FAIL(ret) && (NULL != cont)) {
      cont->kill_this();
      cont = NULL;
    }
  }

  if (OB_FAIL(ret)) {
    action = NULL;
  }

  return ret;
}

int ObTableQueryAsyncProcessor::get_table_query_async_entry_from_thread_cache(
    ObTableQueryAsyncParam &param,
    ObTableQueryAsyncEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  ObTableQueryAsyncRefHashMap &table_query_async_map = self_ethread().get_table_query_async_map();
  ObTableQueryAsyncEntry *tmp_entry = NULL;
  uint64_t key = param.key_;

  tmp_entry = table_query_async_map.get(key); // get will inc entry's ref
  if (NULL != tmp_entry) {
    bool find_succ = false;
    if (tmp_entry->is_deleted_state()) {
      LOG_DEBUG("this table query async entry has deleted", KPC(tmp_entry));
    } else if (get_global_table_query_async_cache().is_table_query_async_entry_expired(*tmp_entry)) {
      // table query async entry has expired
      LOG_DEBUG("the table query async entry is expired",
                "expire_time_us", get_global_table_query_async_cache().get_cache_expire_time_us(),
                KPC(tmp_entry), K(param));
    } else if (tmp_entry->is_avail_state()) { // avail
      find_succ = true;
    } else {
      LOG_EDIAG("building state table query async entry can not in thread cache", KPC(tmp_entry));
    }

    if (!find_succ) {
      tmp_entry->dec_ref();
      tmp_entry = NULL;
    } else {
      LOG_DEBUG("get table query async entry from thread cache succ", KPC(tmp_entry));
    }
  }
  entry = tmp_entry;

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
