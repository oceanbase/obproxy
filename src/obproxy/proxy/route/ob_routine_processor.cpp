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
#include "proxy/route/ob_routine_processor.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "obutils/ob_task_flow_controller.h"
#include "stat/ob_processor_stats.h"

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

class ObRoutineEntryCont : public ObContinuation
{
public:
  ObRoutineEntryCont();
  virtual ~ObRoutineEntryCont() {}

  int main_handler(int event, void *data);
  int init(ObRoutineParam &param);
  ObAction *get_action() { return &action_; }
  static const char *get_event_name(const int64_t event);
  void kill_this();

private:
  int start_lookup_entry();
  int lookup_entry_in_cache();
  int lookup_entry_remote();
  int handle_client_resp(void *data);
  int handle_lookup_cache_done();
  int handle_checking_lookup_cache_done();
  int notify_caller();

private:
  uint32_t magic_;
  ObRoutineParam param_;

  ObAction *pending_action_;
  ObAction action_;

  ObRoutineEntry *updating_entry_;
  ObRoutineEntry *gcached_entry_; // the entry from global cache

  bool is_add_building_entry_succ_;
  bool kill_self_;

  DISALLOW_COPY_AND_ASSIGN(ObRoutineEntryCont);
};

ObRoutineEntryCont::ObRoutineEntryCont()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE),
    param_(), pending_action_(NULL), action_(),
    updating_entry_(NULL), gcached_entry_(NULL),
    is_add_building_entry_succ_(false), kill_self_(false)
{
  SET_HANDLER(&ObRoutineEntryCont::main_handler);
}

inline int ObRoutineEntryCont::init(ObRoutineParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid routine param", K(param), K(ret));
  } else if (OB_FAIL(param_.deep_copy(param))) {
    LOG_WARN("fail to deep copy param", K(param), K(ret));
  } else {
    action_.set_continuation(param.cont_);
    mutex_ = param.cont_->mutex_;
  }

  return ret;
}

void ObRoutineEntryCont::kill_this()
{
  param_.reset();
  int ret = OB_SUCCESS;
  if (NULL != pending_action_) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WARN("fail to cancel pending action", K_(pending_action), K(ret));
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

const char *ObRoutineEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case ROUTINE_ENTRY_LOOKUP_START_EVENT: {
      name = "ROUTINE_ENTRY_LOOKUP_START_EVENT";
      break;
    }
    case ROUTINE_ENTRY_LOOKUP_CACHE_EVENT: {
      name = "ROUTINE_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    }
    case ROUTINE_ENTRY_LOOKUP_REMOTE_EVENT: {
      name = "ROUTINE_ENTRY_LOOKUP_REMOTE_EVENT";
      break;
    }
    case ROUTINE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
      name = "ROUTINE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT";
      break;
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      name = "CLIENT_TRANSPORT_MYSQL_RESP_EVENT";
      break;
    }
    case ROUTINE_ENTRY_LOOKUP_CACHE_DONE: {
      name = "ROUTINE_ENTRY_LOOKUP_CACHE_DONE";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

int ObRoutineEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObRoutineEntryCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_CONT_MAGIC_ALIVE != magic_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this routine entry cont is dead", K_(magic), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case ROUTINE_ENTRY_LOOKUP_START_EVENT: {
        if (OB_FAIL(start_lookup_entry())) {
          LOG_WARN("fail to start lookup entry", K(ret));
        }
        break;
      }
      case ROUTINE_ENTRY_LOOKUP_CACHE_EVENT: {
        if (OB_FAIL(lookup_entry_in_cache())) {
          LOG_WARN("fail to lookup enty in cache", K(ret));
        }
        break;
      }
      case ROUTINE_ENTRY_LOOKUP_REMOTE_EVENT: {
        if (OB_FAIL(lookup_entry_remote())) {
          LOG_WARN("fail to lookup enty remote", K(ret));
        }
        break;
      }
      __attribute__ ((fallthrough));
      case ROUTINE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
        // fail to schedule, data must be NULL
        data = NULL;
        // fall through
      }
      __attribute__ ((fallthrough));
      case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
        if (OB_FAIL(handle_client_resp(data))) {
          LOG_WARN("fail to handle client resp", K(ret));
        } else if (OB_FAIL(notify_caller())) {
          LOG_WARN("fail to notify caller result", K(ret));
        }
        break;
      }
      case ROUTINE_ENTRY_LOOKUP_CACHE_DONE: {
        if (OB_FAIL(handle_lookup_cache_done())) {
          LOG_WARN("fail to handle lookup cache done", K(ret));
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknow event", K(event), K(data), K(ret));
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
      LOG_WARN("fail to notify caller result", K(ret));
    }
  }

  if (kill_self_) {
    kill_this();
    he_ret = EVENT_DONE;
  }
  return he_ret;
}

int ObRoutineEntryCont::start_lookup_entry()
{
  int ret = OB_SUCCESS;

  if (param_.need_fetch_from_remote()) {
    if (OB_FAIL(lookup_entry_remote())) {
      LOG_WARN("fail to lookup enty remote", K(ret));
    }
  } else {
    if (OB_FAIL(lookup_entry_in_cache())) {
      LOG_WARN("fail to lookup enty in cache", K(ret));
    }
  }
  return ret;
}

int ObRoutineEntryCont::handle_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  bool is_add_succ = false;
  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    ObRoutineEntry *entry = NULL;
    if (resp->is_resultset_resp()) {
      if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WARN("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rs_fetcher and entry can not be NULL", K(rs_fetcher), K(entry), K(ret));
      } else if (OB_FAIL(ObRouteUtils::fetch_one_routine_entry_info(
              *rs_fetcher, param_.name_, param_.cr_version_, param_.cr_id_, entry))) {
        LOG_WARN("fail to fetch one routine entry info", K(ret));
      } else if (NULL == entry) {
        PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_REMOTE_FAIL);
        LOG_INFO("no valid routine entry, empty resultset", "name", param_.name_);
      } else if (entry->is_valid()) {
        entry->inc_ref(); // Attention!! before add to table cache, must inc_ref
        if (OB_FAIL(get_global_routine_cache().add_routine_entry(*entry, false))) {
          LOG_WARN("fail to add table entry", KPC(entry), K(ret));
          entry->dec_ref(); // paired the ref count above
        } else {
          LOG_INFO("get routine entry from remote succ", KPC(entry));
          PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_REMOTE_SUCC);
          is_add_succ = true;
          param_.result_.is_from_remote_ = true;
          // hand over ref
          param_.result_.target_entry_ = entry;
          if (NULL != updating_entry_ ) {
            if (updating_entry_->is_the_same_entry(*entry)) {
              // current parittion is the same with old one, avoid refequently updating
              entry->renew_last_update_time();
              LOG_INFO("new routine entry is the same with old one, will renew last_update_time "
                       "and avoid refequently updating", KPC_(updating_entry), KPC(entry));
            }
          }
          entry = NULL;
        }
      } else {
        PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_REMOTE_FAIL);
        LOG_INFO("invalid routine entry", K(param_.name_), KPC(entry));
        // free entry
        entry->dec_ref();
        entry = NULL;
      }
    } else {
      PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_REMOTE_FAIL);
      const int64_t error_code = resp->get_err_code();
      LOG_WARN("fail to get routine entry from remote", K(param_.name_),
                K(error_code));
    }

    if (OB_FAIL(ret) && (NULL != entry)) {
      entry->dec_ref();
      entry = NULL;
    }
    op_free(resp); // free the resp come from ObMysqlProxy
    resp = NULL;
  } else {
    // no resp, maybe client_vc disconnect, do not return error
    PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_REMOTE_FAIL);
    LOG_INFO("has no resp, maybe client_vc disconnect");
  }

  // if fail to update dirty routine entry, must set entry state from UPDATING back to DIRTY,
  // or it will never be upated again
  if (NULL != updating_entry_) {
    if (!is_add_succ) {
      if (updating_entry_->is_updating_state()) {
        updating_entry_->renew_last_update_time(); // avoid refequently updating
        // double check
        if (updating_entry_->cas_compare_and_swap_state(ObTableEntry::UPDATING, ObTableEntry::DIRTY)) {
          LOG_INFO("fail to update dirty table entry, set state back to dirty", KPC_(updating_entry));
        }
      }
    }
    updating_entry_->dec_ref();
    updating_entry_ = NULL;
  }

  // if we has add a building state entry to routine cache, but
  // fail to fetch from remote, we should remove it,
  // or will never fetch this part entry again
  if (is_add_building_entry_succ_ && !is_add_succ) {
    ObRoutineEntryKey key(param_.name_, param_.cr_version_, param_.cr_id_);
    LOG_INFO("fail to add this routine entry to cache, "
             "we should remove it from cache", K_(is_add_building_entry_succ),
             K(is_add_succ), K(key));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = get_global_routine_cache().remove_routine_entry(key))) {
      LOG_WARN("fail to remove part entry", K(key), K(ret), K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

int ObRoutineEntryCont::handle_lookup_cache_done()
{
  int ret = OB_SUCCESS;
  bool need_notify_caller = true;
  if (OB_ISNULL(gcached_entry_)) { // not found fetch from remote
    // if not found in routine cache, it will add a building state entry
    is_add_building_entry_succ_ = true;
    need_notify_caller = false;
  } else if (gcached_entry_->is_building_state()) {
    LOG_INFO("there is an building state routine entry, do not build twice,"
             " just notify out", KPC_(gcached_entry));
    int64_t diff_us = hrtime_to_usec(get_hrtime()) - gcached_entry_->get_create_time_us();
    // just for defense
    if (diff_us > (6 * 60 * 1000 * 1000)) { // 6min
      LOG_ERROR("building state entry has cost so mutch time, will fetch from"
                " remote again", K(diff_us), K_(param));
      need_notify_caller = false;
    }
    gcached_entry_->dec_ref();
    gcached_entry_ = NULL;
  } else if (gcached_entry_->is_deleted_state()) {
    LOG_INFO("this routine entry has been deleted", KPC_(gcached_entry));
    // just inform out, treat as no found routine location
    gcached_entry_->dec_ref();
    gcached_entry_ = NULL;
  } else if (gcached_entry_->is_need_update()) {
    // double check
    if (gcached_entry_->cas_compare_and_swap_state(ObRouteEntry::DIRTY, ObRouteEntry::UPDATING)) {
      if (get_pl_task_flow_controller().can_deliver_task()) {
        PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_GLOBAL_CACHE_DIRTY);
        LOG_INFO("this entry is dirty and need to update", KPC_(gcached_entry));
        need_notify_caller = false;
        updating_entry_ = gcached_entry_; // remember the entry, handle later
        gcached_entry_ = NULL;
      } else {
        LOG_INFO("pl update can not deliver as rate limited, set back to dirty",
                 "flow controller info", get_pl_task_flow_controller());
        gcached_entry_->set_dirty_state();
      }
    } else {
      // just use this routine entry
    }
  } else if (gcached_entry_->is_updating_state()) {
    // someone is updating this routine entry, just use it
  } else {
    //do nothing
  }

  if (OB_SUCC(ret)) {
    if (need_notify_caller) {
      PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_GLOBAL_CACHE_HIT);
      param_.result_.target_entry_ = gcached_entry_;
      gcached_entry_ = NULL;
      if (OB_FAIL(notify_caller())) { // notify_caller
        LOG_WARN("fail to notify caller", K(ret));
      }
    } else {
      if (NULL != gcached_entry_) {
        gcached_entry_->dec_ref();
        gcached_entry_ = NULL;
      }

      if (OB_FAIL(lookup_entry_remote())) {
        LOG_WARN("fail to lookup enty remote", K(ret));
      }
    }
  }

  return ret;
}

int ObRoutineEntryCont::lookup_entry_remote()
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = param_.mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  if (OB_FAIL(ObRouteUtils::get_routine_entry_sql(sql, OB_SHORT_SQL_LENGTH, param_.name_))) {
    LOG_WARN("fail to get table entry sql", K(sql), K(ret));
  } else {
    const ObMysqlRequestParam request_param(sql, param_.current_idc_name_);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
      LOG_WARN("fail to nonblock read", K(sql), K_(param), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    // just treat as execute failed
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, ROUTINE_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule imm", K(ret));
    }
  }
  return ret;
}

int ObRoutineEntryCont::lookup_entry_in_cache()
{
  int ret = OB_SUCCESS;
  ObRoutineEntryKey key(param_.name_, param_.cr_version_, param_.cr_id_);
  ObAction *action = NULL;
  bool is_add_building_entry = true;
  if (OB_FAIL(get_global_routine_cache().get_routine_entry(this,
                                                           key,
                                                           &gcached_entry_,
                                                           is_add_building_entry,
                                                           action))) {
    LOG_WARN("fail to get routine loaction entry", K_(param), K(ret));
  } else {
    if (NULL != action) {
      pending_action_ = action;
    } else if (OB_FAIL(handle_lookup_cache_done())) {
      LOG_WARN("fail to handle lookup cache done", K(ret));
    }
  }
  return ret;
}

int ObRoutineEntryCont::notify_caller()
{
  int ret = OB_SUCCESS;
  ObRoutineEntry *&entry = param_.result_.target_entry_;
  if (NULL != entry) {
    entry->renew_last_access_time();
  }

  // update thread cache routine entry
  if (NULL != entry && entry->is_avail_state()) {
    ObRoutineRefHashMap &routine_map = self_ethread().get_routine_map();
    if (OB_FAIL(routine_map.set(entry))) {
      LOG_WARN("fail to set routine map", KPC(entry), K(ret));
      ret = OB_SUCCESS; // ignore ret
    } else {
      LOG_DEBUG("succ to update into thread cache", KPC(entry), K(ret));
    }
  }

  if (!action_.cancelled_) {
    action_.continuation_->handle_event(ROUTINE_ENTRY_LOOKUP_CACHE_DONE, &param_.result_);
    param_.result_.reset();
  } else {
    // when cancelled, do no forget free the table entry
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
    LOG_INFO("ObRoutineEntryCont has been cancelled", K_(param));
  }

  if (OB_SUCC(ret)) {
    kill_self_ = true;
  }
  return ret;
}

int64_t ObRoutineParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(name),
       K_(cr_version),
       K_(cr_id),
       K_(force_renew),
       KP_(mysql_proxy),
       K_(current_idc_name));
  J_OBJ_END();
  return pos;
}

inline int ObRoutineParam::deep_copy(ObRoutineParam &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    cont_ = other.cont_;
    force_renew_ = other.force_renew_;
    cr_version_ = other.cr_version_;
    cr_id_ = other.cr_id_;
    // no need assign result_
    mysql_proxy_ = other.mysql_proxy_;
    if (!other.current_idc_name_.empty()) {
      MEMCPY(current_idc_name_buf_, other.current_idc_name_.ptr(), other.current_idc_name_.length());
      current_idc_name_.assign_ptr(current_idc_name_buf_, other.current_idc_name_.length());
    } else {
      current_idc_name_.reset();
    }

    if (OB_NOT_NULL(name_buf_) && name_buf_len_ > 0) {
      op_fixed_mem_free(name_buf_, name_buf_len_);
      name_buf_ = NULL;
      name_buf_len_ = 0;
    }
    name_buf_len_ = other.name_.get_total_str_len();
    name_buf_ = static_cast<char *>(op_fixed_mem_alloc(name_buf_len_));
    if (OB_ISNULL(name_buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem", K_(name_buf_len), K(ret));
    } else if (OB_FAIL(name_.deep_copy(other.name_, name_buf_, name_buf_len_))) {
      LOG_WARN("fail to deep copy table entry name", K(ret));
    }

    if (OB_FAIL(ret) && (NULL != name_buf_)) {
      op_fixed_mem_free(name_buf_, name_buf_len_);
      name_buf_ = NULL;
      name_buf_len_ = 0;
      name_.reset();
    }
  }
  return ret;
}

int ObRoutineProcessor::get_routine_entry(ObRoutineParam &param, ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObRoutineEntry *tmp_entry = NULL;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(param), K(ret));
  // 1. find routine entry from thread cache
  } else if (OB_FAIL(get_routine_entry_from_thread_cache(param, tmp_entry))) {
    LOG_WARN("fail to get routine entry in thread cache", K(param), K(ret));
  } else if (NULL != tmp_entry) { // thread cache hit
    ObProxyMutex *mutex_ = param.cont_->mutex_;
    PROCESSOR_INCREMENT_DYN_STAT(GET_ROUTINE_ENTRY_FROM_THREAD_CACHE_HIT);
    tmp_entry->renew_last_access_time();
    // hand over ref
    param.result_.target_entry_ = tmp_entry;
    tmp_entry = NULL;
    param.result_.is_from_remote_ = false;
  } else {
    // 2. find routine entry from remote or global cache
    ObRoutineEntryCont *cont = op_alloc(ObRoutineEntryCont);
    if (OB_ISNULL(cont)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc ObRoutineEntryCont", K(ret));
    } else if (OB_FAIL(cont->init(param))) {
      LOG_WARN("fail to init routine entry cont", K(ret));
    } else {
      action = cont->get_action();
      if (OB_ISNULL(self_ethread().schedule_imm(cont, ROUTINE_ENTRY_LOOKUP_START_EVENT))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to schedule imm", K(ret));
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

int ObRoutineProcessor::get_routine_entry_from_thread_cache(
    ObRoutineParam &param,
    ObRoutineEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(param), K(ret));
  } else if (!param.need_fetch_from_remote()) {
    // find entry from thread cache
    ObRoutineRefHashMap &routine_map = self_ethread().get_routine_map();
    ObRoutineEntry *tmp_entry = NULL;
    ObRoutineEntryKey key(param.name_, param.cr_version_, param.cr_id_);

    tmp_entry = routine_map.get(key); // get will inc entry's ref
    LOG_DEBUG("try get routine from thread cache", K(key), KPC(tmp_entry));

    if (NULL != tmp_entry) {
      bool find_succ = false;
      if (tmp_entry->is_deleted_state()) {
        LOG_DEBUG("this routine entry has deleted", KPC(tmp_entry));
      } else if (get_global_routine_cache().is_routine_entry_expired(*tmp_entry)) {
        // routine entry has expired
        LOG_DEBUG("the routine entry is expired",
                  "expire_time_us", get_global_routine_cache().get_cache_expire_time_us(),
                  KPC(tmp_entry), K(param));
      } else if (tmp_entry->is_avail_state() || tmp_entry->is_updating_state()) { // avail
        find_succ = true;
      } else if (tmp_entry->is_building_state()) {
        LOG_ERROR("building state routine entry can not in thread cache", KPC(tmp_entry));
      } else if (tmp_entry->is_dirty_state()) {
        // dirty entry need to fetch from remote
      } else {
        //do nothing
      }

      if (!find_succ) {
        LOG_DEBUG("get routine entry from thread cache failed", KPC(tmp_entry));
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get routine entry from thread cache succ", KPC(tmp_entry));
      }
    }
    entry = tmp_entry;
  } else {
    LOG_DEBUG("need fetch from remote", K(param));
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
