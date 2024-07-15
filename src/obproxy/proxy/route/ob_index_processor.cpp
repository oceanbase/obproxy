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
#include "proxy/route/ob_index_processor.h"
#include "proxy/route/ob_index_cache.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "obutils/ob_task_flow_controller.h"
#include "stat/ob_processor_stats.h"
#include "prometheus/ob_route_prometheus.h"


using namespace oceanbase::common;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::prometheus;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

class ObIndexEntryCont : public ObContinuation
{
public:
  ObIndexEntryCont();
  virtual ~ObIndexEntryCont() {}

  int main_handler(int event, void *data);
  int init(ObIndexParam &param);
  ObAction *get_action() { return &action_; }
  static const char *get_event_name(const int64_t event);
  void kill_this();
  void set_need_notify(const bool need_notify) { need_notify_ = need_notify; }

private:
  int start_lookup_index_entry();
  int lookup_entry_in_cache();
  int handle_lookup_cache_done();
  int handle_checking_lookup_cache_done();
  int notify_caller();

private:
  uint32_t magic_;
  ObIndexParam param_;

  ObAction *pending_action_;
  ObAction action_;

  ObIndexEntry *updating_entry_;
  ObIndexEntry *gcached_entry_; // the entry from global cache

  bool kill_self_;
  bool need_notify_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexEntryCont);
};

ObIndexEntryCont::ObIndexEntryCont()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE),
    param_(), pending_action_(NULL), action_(),
    updating_entry_(NULL), gcached_entry_(NULL),
    kill_self_(false), need_notify_(true)
{
  SET_HANDLER(&ObIndexEntryCont::main_handler);
}

inline int ObIndexEntryCont::init(ObIndexParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid index param", K(param), K(ret));
  } else {
    param_.deep_copy(param);
    action_.set_continuation(param.cont_);
    mutex_ = param.cont_->mutex_;
  }

  return ret;
}

void ObIndexEntryCont::kill_this()
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

  if (NULL != updating_entry_) {
    updating_entry_->dec_ref();
    updating_entry_ = NULL;
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

const char *ObIndexEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case INDEX_ENTRY_LOOKUP_START_EVENT: {
      name = "INDEX_ENTRY_LOOKUP_START_EVENT";
      break;
    }
    case INDEX_ENTRY_LOOKUP_CACHE_EVENT: {
      name = "INDEX_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    }
    case INDEX_ENTRY_LOOKUP_CACHE_DONE: {
      name = "INDEX_ENTRY_LOOKUP_CACHE_DONE";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

int ObIndexEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObIndexEntryCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_CONT_MAGIC_ALIVE != magic_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this index entry cont is dead", K_(magic), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case INDEX_ENTRY_LOOKUP_START_EVENT: {
        if (OB_FAIL(start_lookup_index_entry())) {
          LOG_WDIAG("fail to start lookup index entry", K(ret));
        }
        break;
      }
      case INDEX_ENTRY_LOOKUP_CACHE_EVENT: {
        if (OB_FAIL(lookup_entry_in_cache())) {
          LOG_WDIAG("fail to lookup enty in cache", K(ret));
        }
        break;
      }
      case INDEX_ENTRY_LOOKUP_CACHE_DONE: {
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
    param_.result_.is_from_remote_ = false;
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

int ObIndexEntryCont::start_lookup_index_entry()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(lookup_entry_in_cache())) {
    LOG_WDIAG("fail to lookup enty in cache", K(ret));
  }

  return ret;
}

int ObIndexEntryCont::handle_lookup_cache_done()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(gcached_entry_)) {
    // do nothing
    LOG_DEBUG("handle_lookup_cache_done get nothing", KP_(gcached_entry));
  } else if (!gcached_entry_->is_avail_state()) {
    LOG_INFO("this index entry is not in avail state", KPC_(gcached_entry));
    // just inform out, treat as no found index location
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

int ObIndexEntryCont::lookup_entry_in_cache()
{
  int ret = OB_SUCCESS;
  ObIndexEntryKey key(param_.name_,
                      param_.cluster_version_,
                      param_.cluster_id_);

  ObAction *action = NULL;
  ret = get_global_index_cache().get_index_entry(this, key, &gcached_entry_, action);
  if (OB_SUCC(ret)) {
    if (NULL != action) {
      pending_action_ = action;
    } else if (OB_FAIL(handle_lookup_cache_done())) {
      LOG_WDIAG("fail to handle lookup cache done", K(ret));
    }
  } else {
    LOG_WDIAG("fail to get index loaction entry", K_(param), K(ret));
  }

  return ret;
}

int ObIndexEntryCont::notify_caller()
{
  int ret = OB_SUCCESS;
  ObIndexEntry *&entry = param_.result_.target_entry_;
  if (NULL != entry) {
    entry->renew_last_access_time();
  }

  // update thread cache index entry
  if (NULL != entry && entry->is_avail_state()) {
    ObIndexRefHashMap &part_map = self_ethread().get_index_map();
    if (OB_FAIL(part_map.set(entry))) {
      LOG_WDIAG("fail to set index map", KPC(entry), K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }

  if (action_.cancelled_) {
    // when cancelled, do no forget free the table entry
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
    LOG_INFO("ObIndexEntryCont has been cancelled", K_(param));
  } else if (need_notify_) {
    action_.continuation_->handle_event(INDEX_ENTRY_LOOKUP_CACHE_DONE, &param_.result_);
    param_.result_.reset();
  } else {
    // enable async pull index entry, need dec_ref
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

int64_t ObIndexParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(cluster_version),
       K_(cluster_id),
       K_(name));
  J_OBJ_END();
  return pos;
}

inline void ObIndexParam::deep_copy(ObIndexParam &other)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    cont_ = other.cont_;

    cluster_version_ = other.cluster_version_;
    cluster_id_ = other.cluster_id_;

    if (OB_NOT_NULL(name_buf_) && name_buf_len_ > 0) {
      op_fixed_mem_free(name_buf_, name_buf_len_);
      name_buf_ = NULL;
      name_buf_len_ = 0;
    }
    name_buf_len_ = other.name_.get_total_str_len();
    name_buf_ = static_cast<char *>(op_fixed_mem_alloc(name_buf_len_));
    if (OB_ISNULL(name_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc mem", K_(name_buf_len), K(ret));
    } else if (OB_FAIL(name_.deep_copy(other.name_, name_buf_, name_buf_len_))) {
      LOG_WDIAG("fail to deep copy index entry name", K(ret));
    }

    if (OB_FAIL(ret) && (NULL != name_buf_)) {
      op_fixed_mem_free(name_buf_, name_buf_len_);
      name_buf_ = NULL;
      name_buf_len_ = 0;
      name_.reset();
    }
  }
}

int ObIndexProcessor::get_index_entry(ObIndexParam &param, ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObIndexEntry *tmp_entry = NULL;
  ObIndexEntryCont *cont = NULL;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(param), K(ret));
  // 1. find index entry from thread cache
  } else if (OB_FAIL(get_index_entry_from_thread_cache(param, tmp_entry))) {
    LOG_WDIAG("fail to get index entry in thread cache", K(param), K(ret));
  } else if (NULL != tmp_entry) { // thread cache hit
    // ObProxyMutex *mutex_ = param.cont_->mutex_;
    // TODO : add stats
    // PROCESSOR_INCREMENT_DYN_STAT(GET_INDEX_ENTRY_FROM_THREAD_CACHE_HIT);
    // ROUTE_PROMETHEUS_STAT(param.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, INDEX_ENTRY, true, true);
    tmp_entry->renew_last_access_time();
    // hand over ref
    param.result_.target_entry_ = tmp_entry;
    tmp_entry = NULL;
    param.result_.is_from_remote_ = false;
  } else {
    // 2. find index entry from remote or global cache
    cont = op_alloc(ObIndexEntryCont);
    if (OB_ISNULL(cont)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc ObIndexEntryCont", K(ret));
    } else if (OB_FAIL(cont->init(param))) {
      LOG_WDIAG("fail to init index entry cont", K(ret));
    } else {
      action = cont->get_action();
      if (OB_ISNULL(self_ethread().schedule_imm(cont, INDEX_ENTRY_LOOKUP_START_EVENT))) {
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

int ObIndexProcessor::get_index_entry_from_thread_cache(
    ObIndexParam &param,
    ObIndexEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  // if (!param.need_fetch_from_remote()) {
    // find entry from thread cache
  ObIndexRefHashMap &index_map = self_ethread().get_index_map();
  ObIndexEntry *tmp_entry = NULL;
  ObIndexEntryKey key(param.name_,
                      param.cluster_version_,
                      param.cluster_id_);

  tmp_entry = index_map.get(key); // get will inc entry's ref
  if (NULL != tmp_entry) {
    bool find_succ = false;
    if (tmp_entry->is_deleted_state()) {
      LOG_DEBUG("this index entry has deleted", KPC(tmp_entry));
    } else if (get_global_index_cache().is_index_entry_expired(*tmp_entry)) {
      // index entry has expired
      LOG_DEBUG("the index entry is expired",
                "expire_time_us", get_global_index_cache().get_cache_expire_time_us(),
                KPC(tmp_entry), K(param));
    } else if (tmp_entry->is_avail_state() || tmp_entry->is_updating_state()) { // avail
      find_succ = true;
    } else if (tmp_entry->is_building_state()) {
      LOG_EDIAG("building state index entry can not in thread cache", KPC(tmp_entry));
    } else if (tmp_entry->is_dirty_state()) {
      // dirty entry need to fetch from remote
    } else {}

    if (!find_succ) {
      tmp_entry->dec_ref();
      tmp_entry = NULL;
    } else {
      LOG_DEBUG("get index entry from thread cache succ", KPC(tmp_entry));
    }
  }
  entry = tmp_entry;

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
