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
#include "proxy/route/ob_partition_processor.h"
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

class ObPartitionEntryCont : public ObContinuation
{
public:
  ObPartitionEntryCont();
  virtual ~ObPartitionEntryCont() {}

  int main_handler(int event, void *data);
  int init(ObPartitionParam &param);
  ObAction *get_action() { return &action_; }
  static const char *get_event_name(const int64_t event);
  void kill_this();
  void set_need_notify(const bool need_notify) { need_notify_ = need_notify; }

private:
  int start_lookup_table_entry();
  int lookup_entry_in_cache();
  int lookup_entry_remote();
  int lookup_batch_entry_remote(bool first_fetch);
  int handle_cleanup_fetching_ids(bool force);
  int handle_client_resp(void *data);
  int handle_batch_client_resp(void *data);
  int handle_lookup_cache_done();
  int handle_checking_lookup_cache_done();
  int notify_caller();

private:
  uint32_t magic_;
  ObPartitionParam param_;

  ObAction *pending_action_;
  ObAction action_;

  ObPartitionEntry *updating_entry_;
  ObPartitionEntry *gcached_entry_; // the entry from global cache

  bool is_add_building_entry_succ_;
  bool is_batch_fetching_;
  bool is_fetch_fetch_;
  bool kill_self_;
  bool need_notify_;
  uint32_t left_fetch_times_;
  ObSEArray<uint64_t, 1> partition_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObPartitionEntryCont);
};

//TODO will be used in ObBatchPartitionEntryCont
class ObBatchPartitionEntryCont : public ObContinuation
{
public:
  ObBatchPartitionEntryCont();
  virtual ~ObBatchPartitionEntryCont() {}

  int main_handler(int event, void *data);
  int init(ObBatchPartitionParam &param);
  ObAction *get_action() { return &action_; }
  static const char *get_event_name(const int64_t event);
  void kill_this();
  void set_need_notify(const bool need_notify) { need_notify_ = need_notify; }

private:
  int lookup_batch_entry_remote();
  int handle_batch_client_resp(void *data);
  // int notify_caller();

private:
  uint32_t magic_;
  ObBatchPartitionParam param_;

  ObAction *pending_action_;
  ObAction action_;

  // ObPartitionEntry *updating_entry_;
  // ObPartitionEntry *gcached_entry_; // the entry from global cache

  bool is_add_building_entry_succ_;
  bool kill_self_;
  bool need_notify_;
public:
  typedef hash::ObHashMap<uint64_t, ObPartitionEntry *> PARTITION_ENTRY_MAP;
  PARTITION_ENTRY_MAP updating_entry_map_;
  event::ObEThread *create_thread_;
  event::ObAction *batch_schedule_action_;
  event::ObAction *batch_schedule_time_action_;

  DISALLOW_COPY_AND_ASSIGN(ObBatchPartitionEntryCont);
};

ObPartitionEntryCont::ObPartitionEntryCont()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE),
    param_(), pending_action_(NULL), action_(),
    updating_entry_(NULL), gcached_entry_(NULL),
    is_add_building_entry_succ_(false), is_batch_fetching_(false), is_fetch_fetch_(false),
    kill_self_(false), need_notify_(true), left_fetch_times_(0), partition_ids_()
{
  SET_HANDLER(&ObPartitionEntryCont::main_handler);
}

inline int ObPartitionEntryCont::init(ObPartitionParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid partition param", K(param), K(ret));
  } else {
    param_.deep_copy(param);
    action_.set_continuation(param.cont_);
    mutex_ = param.cont_->mutex_;
  }

  return ret;
}

void ObPartitionEntryCont::kill_this()
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

const char *ObPartitionEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case PARTITION_ENTRY_LOOKUP_START_EVENT: {
      name = "PARTITION_ENTRY_LOOKUP_START_EVENT";
      break;
    }
    case PARTITION_ENTRY_LOOKUP_CACHE_EVENT: {
      name = "PARTITION_ENTRY_LOOKUP_CACHE_EVENT";
      break;
    }
    case PARTITION_ENTRY_LOOKUP_REMOTE_EVENT: {
      name = "PARTITION_ENTRY_LOOKUP_REMOTE_EVENT";
      break;
    }
    case PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
      name = "PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT";
      break;
    }
    case PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT: {
      name = "PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT";
      break;
    }
    case PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT: {
      name = "PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT";
      break;
    }
    case PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_PERIOD: {
      name = "PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_PERIOD";
      break;
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      name = "CLIENT_TRANSPORT_MYSQL_RESP_EVENT";
      break;
    }
    case PARTITION_ENTRY_LOOKUP_CACHE_DONE: {
      name = "PARTITION_ENTRY_LOOKUP_CACHE_DONE";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

int ObPartitionEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObPartitionEntryCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_CONT_MAGIC_ALIVE != magic_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this partition entry cont is dead", K_(magic), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case PARTITION_ENTRY_LOOKUP_START_EVENT: {
        if (OB_FAIL(start_lookup_table_entry())) {
          LOG_WDIAG("fail to start lookup table entry", K(ret));
        }
        break;
      }
      case PARTITION_ENTRY_LOOKUP_CACHE_EVENT: {
        if (OB_FAIL(lookup_entry_in_cache())) {
          LOG_WDIAG("fail to lookup enty in cache", K(ret));
        }
        break;
      }
      case PARTITION_ENTRY_LOOKUP_REMOTE_EVENT: {
        if (OB_FAIL(lookup_entry_remote())) {
          LOG_WDIAG("fail to lookup enty remote", K(ret));
        }
        break;
      }
      case PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT: {
        if (OB_FAIL(lookup_batch_entry_remote(false))) {
          LOG_WDIAG("fail to lookup batch enty remote", K(ret));
        }
        break;
      }
      case PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT: {
        // fail to schedule, data must be NULL
        data = NULL;
        // fall through
        if (OB_FAIL(handle_cleanup_fetching_ids(true))) {
          LOG_WDIAG("fail to cleanup fetching ids need to retry", K(ret)); 
        }
        if (OB_NEED_RETRY == ret && OB_NOT_NULL(pending_action_)) {
          ret = OB_SUCCESS;
        } else if (OB_FAIL(notify_caller())) {
          LOG_WDIAG("fail to notify caller result", K(ret));
        }
        break;
      }
      case PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT: {
        // fail to schedule, data must be NULL
        data = NULL;
        // fall through
      }
      __attribute__ ((fallthrough));
      case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
        if (!is_batch_fetching_) {
          if (OB_FAIL(handle_client_resp(data))) {
            LOG_WDIAG("fail to handle client resp", K(ret));
          } else if (OB_FAIL(notify_caller())) {
            LOG_WDIAG("fail to notify caller result", K(ret));
          }
        } else {
          if (OB_FAIL(handle_batch_client_resp(data))) {
            LOG_WDIAG("fail to handle client resp", K(ret));
          } else if (OB_FAIL(handle_cleanup_fetching_ids(false))) {
            LOG_WDIAG("fail to cleanup fetching ids need to retry", K(ret));
          } else if (--left_fetch_times_ <= 0 && OB_FAIL(notify_caller())){
            LOG_WDIAG("fail to notify caller result", K(ret));
          } else if (param_.get_table_entry()->get_batch_fetch_size() > 0) {
            param_.partition_id_ = OB_INVALID_INDEX; //not to fetch it any more
            if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("fail to schedule imm", K(ret));
            }
          }
          if (OB_NEED_RETRY == ret && OB_NOT_NULL(pending_action_)) {
            ret = OB_SUCCESS;
          }
        }
        break;
      }
      case PARTITION_ENTRY_LOOKUP_CACHE_DONE: {
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

int ObPartitionEntryCont::start_lookup_table_entry()
{
  int ret = OB_SUCCESS;

  if (param_.need_fetch_from_remote()) {
    if (OB_FAIL(lookup_entry_remote())) {
      LOG_WDIAG("fail to lookup enty remote", K(ret));
    }
  } else {
    if (OB_FAIL(lookup_entry_in_cache())) {
      LOG_WDIAG("fail to lookup enty in cache", K(ret));
    }
  }
  return ret;
}

int ObPartitionEntryCont::handle_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  bool is_add_succ = false;
  const ObTableEntryName &table_name = param_.get_table_entry()->get_names();
  const uint64_t partition_id = param_.partition_id_;
  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    ObPartitionEntry *entry = NULL;
    if (resp->is_resultset_resp()) {
      if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WDIAG("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("rs_fetcher and entry can not be NULL", K(rs_fetcher), K(entry), K(ret));
      } else if (OB_FAIL(ObRouteUtils::fetch_one_partition_entry_info(
              *rs_fetcher, *param_.get_table_entry(), entry, param_.cluster_version_))) {
        LOG_WDIAG("fail to fetch one partition entry info", K(ret));
      } else if (NULL == entry) {
        PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
        ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
        LOG_INFO("no valid partition entry, empty resultset", K(table_name), K(partition_id));
      } else if (entry->is_valid()) {
        entry->inc_ref(); // Attention!! before add to table cache, must inc_ref
        if (!entry->get_pl().exist_leader()) {
          // current the parittion has no leader, avoid refequently updating
          entry->renew_last_update_time();
        }
        if (OB_FAIL(get_global_partition_cache().add_partition_entry(*entry, false))) {
          LOG_WDIAG("fail to add table entry", KPC(entry), K(ret));
          entry->dec_ref(); // paired the ref count above
        } else {
          LOG_INFO("get partition entry from remote succ", KPC(entry));
          PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_SUCC);
          ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, true);
          is_add_succ = true;
          param_.result_.is_from_remote_ = true;
          // hand over ref
          param_.result_.target_entry_ = entry;
          entry->set_tenant_version(param_.tenant_version_);
          if (NULL != updating_entry_ ) {
            if (updating_entry_->is_the_same_entry(*entry)) {
              // current parittion is the same with old one, avoid refequently updating
              entry->renew_last_update_time();
              LOG_INFO("new partition entry is the same with old one, will renew last_update_time "
                       "and avoid refequently updating", KPC_(updating_entry), KPC(entry));
            }
            ObProxyPartitionLocation &this_pl = const_cast<ObProxyPartitionLocation &>(entry->get_pl());
            ObProxyPartitionLocation &new_pl = const_cast<ObProxyPartitionLocation &>(updating_entry_->get_pl());
            const bool is_server_changed = new_pl.check_and_update_server_changed(this_pl);
            if (is_server_changed) {
              LOG_INFO("server is changed, ", "old_entry", PC(updating_entry_),
                                              "new_entry", PC(entry));
            }
          }
          entry = NULL;
        }
      } else {
        PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
        ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
        LOG_INFO("invalid partition entry", K(table_name), K(partition_id), KPC(entry));
        // free entry
        entry->dec_ref();
        entry = NULL;
      }
    } else {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
      ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
      const int64_t error_code = resp->get_err_code();
      LOG_WDIAG("fail to get partition entry from remote", K(table_name),
               K(partition_id), K(error_code));
    }

    if (OB_FAIL(ret) && (NULL != entry)) {
      entry->dec_ref();
      entry = NULL;
    }
    op_free(resp); // free the resp come from ObMysqlProxy
    resp = NULL;
  } else {
    // no resp, maybe client_vc disconnect, do not return error
    PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
    LOG_INFO("has no resp, maybe client_vc disconnect");
  }

  // if fail to update dirty partition entry, must set entry state from UPDATING back to DIRTY,
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

  // if we has add a building state entry to part cache, but
  // fail to fetch from remote, we should remove it,
  // or will never fetch this part entry again
  if (is_add_building_entry_succ_ && !is_add_succ) {
    ObPartitionEntryKey key(param_.get_table_entry()->get_cr_version(),
                            param_.get_table_entry()->get_cr_id(),
                            param_.get_table_entry()->get_table_id(),
                            param_.partition_id_);

    LOG_INFO("fail to add this part entry to part cache, "
             "we should remove it from part cache", K_(is_add_building_entry_succ),
             K(is_add_succ), K(key));
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = get_global_partition_cache().remove_partition_entry(key))) {
      LOG_WDIAG("fail to remove part entry", K(key), K(ret), K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }

  return ret;
}

int ObPartitionEntryCont::handle_lookup_cache_done()
{
  int ret = OB_SUCCESS;
  bool need_notify_caller = true;
  if (OB_ISNULL(gcached_entry_)) { // not found fetch from remote
    // if not found in partition cache, it will add a building state entry
    is_add_building_entry_succ_ = true;
    need_notify_caller = false;
  } else if (gcached_entry_->is_building_state()) {
    LOG_INFO("there is an building state partition entry, do not build twice,"
             " just notify out", KPC_(gcached_entry));
    int64_t diff_us = hrtime_to_usec(get_hrtime()) - gcached_entry_->get_create_time_us();
    // just for defense
    if (diff_us > (6 * 60 * 1000 * 1000)) { // 6min
      LOG_EDIAG("building state entry has cost so mutch time, will fetch from"
                " remote again", K(diff_us), K_(param));
      need_notify_caller = false;
    }
    gcached_entry_->dec_ref();
    gcached_entry_ = NULL;
  } else if (gcached_entry_->is_deleted_state()) {
    LOG_INFO("this partition entry has been deleted", KPC_(gcached_entry));
    // just inform out, treat as no found partition location
    gcached_entry_->dec_ref();
    gcached_entry_ = NULL;
  } else if (gcached_entry_->is_need_update()) {
    // double check
    if (gcached_entry_->cas_compare_and_swap_state(ObRouteEntry::DIRTY, ObRouteEntry::UPDATING)) {
      if (get_pl_task_flow_controller().can_deliver_task()) {
        PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_GLOBAL_CACHE_DIRTY);
        LOG_INFO("this entry is dirty and need to update", KPC_(gcached_entry));
        need_notify_caller = false;
        updating_entry_ = gcached_entry_; // remember the entry, handle later
        gcached_entry_ = NULL;
      } else {
        LOG_INFO("pl update can not deliver as rate limited, set back to dirty",
                 "flow controller info", get_pl_task_flow_controller());
        gcached_entry_->set_dirty_state();
      }

      if (get_global_proxy_config().enable_async_pull_location_cache && !need_notify_caller) {
        if (OB_ISNULL(param_.result_.target_old_entry_)) {
          if (updating_entry_ != NULL) {
            updating_entry_->inc_ref();
            param_.result_.target_old_entry_ = updating_entry_;
          }
        }
      }
    } else {
      // just use this partition entry
    }
  } else if (gcached_entry_->is_updating_state()) {
    // someone is updating this partition entry, just use it
  } else {}

  if (OB_SUCC(ret)) {
    if (need_notify_caller) {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_GLOBAL_CACHE_HIT);
      ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, true, true);
      param_.result_.target_entry_ = gcached_entry_;
      gcached_entry_ = NULL;
      if (OB_FAIL(notify_caller())) { // notify_caller
        LOG_WDIAG("fail to notify caller", K(ret));
      }
    } else {
      if (OB_SUCC(ret) && NULL != param_.result_.target_old_entry_) {
        if (NULL == param_.result_.target_entry_) {
          param_.result_.target_entry_ = param_.result_.target_old_entry_;
          param_.result_.target_old_entry_ = NULL;
          need_notify_ = false;
          if (!action_.cancelled_) {
            action_.continuation_->handle_event(PARTITION_ENTRY_LOOKUP_CACHE_DONE, &param_.result_);
          }
        } else {
          param_.result_.target_old_entry_->dec_ref();
          param_.result_.target_old_entry_ = NULL;
        }
      }
      if (NULL != gcached_entry_) {
        gcached_entry_->dec_ref();
        gcached_entry_ = NULL;
      }

      if (OB_SUCC(ret)) {
        //TODO isolate ObBatchPartitionEntryCont function will be used in NEXT
        /*
        int batch_size = 0;
        ObBatchPartitionEntryCont *cont = NULL;
        bool could_async_batch_fetch = !is_add_building_entry_succ_ // not the first building state 
                                          && get_global_proxy_config().enable_async_pull_location_cache
                                          && get_global_proxy_config().rpc_enable_async_pull_batch_tablets;
        if (could_async_batch_fetch 
              && OB_ISNULL(cont = (oceanbase::obproxy::proxy::ObBatchPartitionEntryCont *)param_.get_table_entry()->get_batch_fetch_cont())) {
          event::ObProxyMutex *batch_mutex = param_.get_table_entry()->get_batch_fetch_mutex();
          MUTEX_LOCK(lock, batch_mutex, this_ethread()); //protect to forbiden parallel threads handle
          if (OB_NOT_NULL(cont = (oceanbase::obproxy::proxy::ObBatchPartitionEntryCont *)param_.get_table_entry()->get_batch_fetch_cont())) { //check it again when locked
            LOG_DEBUG("has init cont in other thread", K(cont));
          } else if (OB_ISNULL(cont = op_alloc(ObBatchPartitionEntryCont))) {
            //not to change ret status for batch fetch, just use single async pull
            LOG_WDIAG("invalid to alloc ObBatchPartitionEntryCont object to fetch", K(ret), K(cont));
          } else {
            ObBatchPartitionParam batch_param;
            batch_param.cluster_version_ = param_.cluster_version_;
            batch_param.tenant_version_ = param_.tenant_version_;
            batch_param.mysql_proxy_ = param_.mysql_proxy_;
            if (!param_.current_idc_name_.empty()) {
              MEMCPY(batch_param.current_idc_name_buf_, param_.current_idc_name_.ptr(), param_.current_idc_name_.length());
              batch_param.current_idc_name_.assign_ptr(batch_param.current_idc_name_buf_, param_.current_idc_name_.length());
            }
            batch_param.set_table_entry(param_.get_table_entry());
            cont->init(batch_param);
            param_.get_table_entry()->set_batch_fetch_cont(cont);
          }
        }

        if (could_async_batch_fetch 
              && OB_NOT_NULL(cont)
              && (OB_SUCC(param_.get_table_entry()->put_batch_fetch_tablet_id(param_.partition_id_, batch_size)) && batch_size> 0)) {
          //has put batch size
          // event::ObProxyMutex *batch_mutex = param_.get_table_entry()->get_batch_fetch_mutex();
          // MUTEX_LOCK(lock, batch_mutex, this_ethread()); //protect to schedule just one all time
          //just one cont's batch_size if more than max_size, and is 1
          if (OB_NOT_NULL(updating_entry_)) {
            event::ObProxyMutex *batch_mutex = param_.get_table_entry()->get_batch_fetch_mutex();
            MUTEX_LOCK(lock, batch_mutex, this_ethread()); //protect to schedule just one all time
            cont->updating_entry_map_.set_refactored(param_.partition_id_, updating_entry_);
          }
          // not to need lock for schedule, for only one thread could get batch size is 1 or full batch_size
          if (batch_size >= get_global_proxy_config().rpc_async_pull_batch_max_size &&
                OB_ISNULL(cont->batch_schedule_action_ = cont->create_thread_->schedule_imm(cont, PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT))) {// lookup_batch_entry_remote()
            LOG_WDIAG("fail to lookup batch enty remote", K(ret), K(batch_size));
          } else if (1 == batch_size) {  //the firt partition id has put batch array
            uint64_t interval_ns = HRTIME_USECONDS(get_global_proxy_config().rpc_async_pull_batch_wait_interval);
            LOG_DEBUG("ObBatchPartitionEntryCont interval", K(interval_ns));
            event::ObProxyMutex *batch_mutex = param_.get_table_entry()->get_batch_fetch_mutex();
            MUTEX_LOCK(lock, batch_mutex, this_ethread()); //protect to schedule just one all time
            cont->batch_schedule_time_action_ = cont->create_thread_->schedule_in(cont, interval_ns, PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT);
          } else if (batch_size > 0 && OB_ISNULL(cont->batch_schedule_action_)) {
            event::ObProxyMutex *batch_mutex = param_.get_table_entry()->get_batch_fetch_mutex();
            MUTEX_LOCK(lock, batch_mutex, this_ethread()); //protect to schedule just one all time
            if (OB_ISNULL(cont->batch_schedule_time_action_)) {
              uint64_t interval_ns = HRTIME_USECONDS(get_global_proxy_config().rpc_async_pull_batch_wait_interval);
              cont->batch_schedule_time_action_ = cont->create_thread_->schedule_in(cont, interval_ns, PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT);
            }
          }
        } else if (OB_FAIL(lookup_entry_remote())) {
          LOG_WDIAG("fail to lookup enty remote", K(ret));
        }
        */
        bool could_async_batch_fetch = !is_add_building_entry_succ_ // not the first building state 
                                         && !need_notify_ // async pull_location_cache
                                         && get_global_proxy_config().enable_async_pull_location_cache
                                         && get_global_proxy_config().rpc_enable_async_pull_batch_tablets;
        if (could_async_batch_fetch && OB_SUCC(lookup_batch_entry_remote(true))) {
          LOG_DEBUG("will lookup entry remote by batch fetch", K(ret));
        } else if (OB_FAIL(lookup_entry_remote())) { //ignore ret in lookup_batch_entry_remote()
          LOG_WDIAG("fail to lookup enty remote", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObPartitionEntryCont::lookup_entry_remote()
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = param_.mysql_proxy_;
  char sql[OB_SHORT_SQL_LENGTH];
  sql[0] = '\0';
  if (OB_FAIL(ObRouteUtils::get_partition_entry_sql(sql, OB_SHORT_SQL_LENGTH,
                 param_.get_table_entry()->get_names(), param_.partition_id_,
                 param_.is_need_force_flush_, param_.cluster_version_))) {
    LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
  } else {
    const ObMysqlRequestParam request_param(sql, param_.current_idc_name_);
    if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
      LOG_WDIAG("fail to nonblock read", K(sql), K_(param), K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    // just treat as execute failed
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_REMOTE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule imm", K(ret));
    }
  }
  return ret;
}

int ObPartitionEntryCont::lookup_entry_in_cache()
{
  int ret = OB_SUCCESS;
  ObPartitionEntryKey key(param_.get_table_entry()->get_cr_version(),
                          param_.get_table_entry()->get_cr_id(),
                          param_.get_table_entry()->get_table_id(),
                          param_.partition_id_);
  ObAction *action = NULL;
  bool is_add_building_entry = true;
  ret = get_global_partition_cache().get_partition_entry(this, key,
      &gcached_entry_, is_add_building_entry, action);
  if (OB_SUCC(ret)) {
    if (NULL != action) {
      pending_action_ = action;
    } else if (OB_FAIL(handle_lookup_cache_done())) {
      LOG_WDIAG("fail to handle lookup cache done", K(ret));
    }
  } else {
    LOG_WDIAG("fail to get partition loaction entry", K_(param), K(ret));
  }

  return ret;
}

int ObPartitionEntryCont::lookup_batch_entry_remote(bool first_fetch)
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = param_.mysql_proxy_;
  char sql[OB_MEDIUM_SQL_LENGTH];
  sql[0] = '\0';
  // ObSEArray<uint64_t, 10> partition_ids;
  int max_count = obutils::get_global_proxy_config().rpc_async_pull_batch_max_size;
  partition_ids_.reserve((int64_t) max_count);
  if (OB_NOT_NULL(pending_action_)) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel batch entry fetch remote", K(ret));
    } else {
      pending_action_ = NULL;
    }
  }

  if (first_fetch) {
     left_fetch_times_ = obutils::get_global_proxy_config().rpc_async_pull_batch_max_times;
     left_fetch_times_ = left_fetch_times_ == 0 ? (1 + param_.get_table_entry()->get_part_num() / obutils::get_global_proxy_config().rpc_async_pull_batch_max_size) : left_fetch_times_;
  }
  if (OB_SUCC(ret)) {
    if (left_fetch_times_ == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_INFO("not have chance left for batch fetch, fetch cont will be remove", K(ret));
    } else if (OB_FAIL(param_.get_table_entry()->get_batch_fetch_tablet_ids(partition_ids_, max_count, param_.partition_id_))) {
      LOG_WDIAG("fail to get batch tablet ids in table entry", K(ret), "table_entry", param_.get_table_entry());
    } else if ((partition_ids_.count() == 0) || (partition_ids_.count() == 1 && first_fetch)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_DEBUG("not need to batch fetch from remote", "count", partition_ids_.count(), K(first_fetch));
    } else if (OB_FAIL(ObRouteUtils::get_batch_partition_entry_sql(sql, OB_MEDIUM_SQL_LENGTH,
                   param_.get_table_entry()->get_names(), partition_ids_,
                   param_.is_need_force_flush_, param_.cluster_version_))) {
      LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
    } else {
      const ObMysqlRequestParam request_param(sql, param_.current_idc_name_);
      if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
        LOG_WDIAG("fail to nonblock read", K(sql), K_(param), K(ret));
      } else {
        is_batch_fetching_ = true;
      }
    }
  }
  // not to ignore ERROR ret for batch fetch
  if (OB_FAIL(ret) && !first_fetch) {
    ret = OB_SUCCESS;
    // just treat as execute failed
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule imm", K(ret));
    }
  } else if (OB_FAIL(ret)) {
    LOG_WDIAG("fail to init task for lookup batch entry remote, will retry by lookup_entry_remote", K(ret));
    handle_cleanup_fetching_ids(true);
  }
  return ret;
}

int ObPartitionEntryCont::handle_cleanup_fetching_ids(bool force)
{
  int ret = OB_SUCCESS;
  if (partition_ids_.count() > 0 && OB_FAIL(param_.get_table_entry()->remove_pending_batch_fetch_tablet_ids(partition_ids_, force))) {
    if (OB_NEED_RETRY == ret && !force) {
      if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this, HRTIME_MSECONDS(1), PARTITION_ENTRY_LOOKUP_BATCH_RETRY_CLEAN))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule imm", K(ret));
      }
    }
  }

  return ret;
}

int ObPartitionEntryCont::handle_batch_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  // bool is_add_succ = false;
  const ObTableEntryName &table_name = param_.get_table_entry()->get_names();
  const uint64_t partition_id = OB_INVALID_PARTITION; //tmp to print
  // ObPartitionEntry *updating_entry = NULL;

  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    ObPartitionEntry *entry = NULL;
    common::ObSEArray<ObPartitionEntry *, 10> entry_array;
    if (resp->is_resultset_resp()) {
      if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WDIAG("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("rs_fetcher and entry can not be NULL", K(rs_fetcher), K(entry), K(ret));
      } else if (OB_FAIL(ObRouteUtils::fetch_more_partition_entrys_info(
              *rs_fetcher, *param_.get_table_entry(), entry_array, param_.cluster_version_))) {
        LOG_WDIAG("fail to fetch one partition entry info", K(ret));
      // } else if (NULL == entry) {
      //   PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
      //   ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
      //   LOG_INFO("no valid partition entry, empty resultset", K(table_name), K(partition_id));
      } else if (entry_array.count() > 0) {
        LOG_DEBUG("ivalid partition entry, to fetch resultset", K(table_name), K(entry_array));
        for (int i = 0; i < entry_array.count(); i++) {
          entry = entry_array.at(i);
          if (OB_NOT_NULL(entry) && entry->is_valid()) {
            entry->inc_ref(); // Attention!! before add to table cache, must inc_ref
            if (!entry->get_pl().exist_leader()) {
              // current the parittion has no leader, avoid refequently updating
              entry->renew_last_update_time();
            }
            if (OB_FAIL(get_global_partition_cache().add_partition_entry(*entry, false))) {
              LOG_WDIAG("fail to add table entry", KPC(entry), K(ret));
              entry->dec_ref(); // paired the ref count above
            } else {
              LOG_INFO("get partition entry from remote succ", KPC(entry));
              PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_SUCC);
              ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, true);
              // is_add_succ = true;
              entry->set_tenant_version(param_.tenant_version_);

              // updating_entry = NULL;
              if (OB_NOT_NULL(updating_entry_)) {
                if (updating_entry_->is_the_same_entry(*entry)) {
                  // current parittion is the same with old one, avoid refequently updating
                  entry->renew_last_update_time();
                  LOG_INFO("new partition entry is the same with old one, will renew last_update_time "
                           "and avoid refequently updating", KPC_(updating_entry), KPC(entry));
                }
                ObProxyPartitionLocation &this_pl = const_cast<ObProxyPartitionLocation &>(entry->get_pl());
                ObProxyPartitionLocation &new_pl = const_cast<ObProxyPartitionLocation &>(updating_entry_->get_pl());
                const bool is_server_changed = new_pl.check_and_update_server_changed(this_pl);
                if (is_server_changed) {
                  LOG_INFO("server is changed, ", "old_entry", PC(updating_entry_),
                                                  "new_entry", PC(entry));
                }
                // updating_entry_map_.erase_refactored(entry->get_partition_id());
                updating_entry_->dec_ref();
                updating_entry_ = NULL;
              }
              entry = NULL;
            }
          } else {
            PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
            ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
            LOG_INFO("invalid partition entry", K(table_name), K(partition_id), KPC(entry));
            // free entry
            if (OB_NOT_NULL(entry)) {
              entry->dec_ref();
              entry = NULL;
            }
          }
        }
      }
    } else {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
      ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
      const int64_t error_code = resp->get_err_code();
      LOG_WDIAG("fail to get batch partition entry from remote", K(table_name),
               K(partition_id), K(error_code));
    }

    // if (OB_FAIL(ret) && (NULL != entry)) {
    //   entry->dec_ref();
    //   entry = NULL;
    // }
    op_free(resp); // free the resp come from ObMysqlProxy
    resp = NULL;
  } else {
    // no resp, maybe client_vc disconnect, do not return error
    PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
    LOG_INFO("has no resp, maybe client_vc disconnect");
  }

  // // if we has add a building state entry to part cache, but
  // // fail to fetch from remote, we should remove it,
  // // or will never fetch this part entry again
  // if (is_add_building_entry_succ_ && !is_add_succ) {
  //   ObPartitionEntryKey key(param_.get_table_entry()->get_cr_version(),
  //                           param_.get_table_entry()->get_cr_id(),
  //                           param_.get_table_entry()->get_table_id(),
  //                           param_.partition_id_);

  //   LOG_INFO("fail to add this part entry to part cache, "
  //            "we should remove it from part cache", K_(is_add_building_entry_succ),
  //            K(is_add_succ), K(key));
  //   int tmp_ret = OB_SUCCESS;
  //   if (OB_SUCCESS != (tmp_ret = get_global_partition_cache().remove_partition_entry(key))) {
  //     LOG_WDIAG("fail to remove part entry", K(key), K(ret), K(tmp_ret));
  //     if (OB_SUCC(ret)) {
  //       ret = tmp_ret;
  //     }
  //   }
  // }

  return ret;
}

int ObPartitionEntryCont::notify_caller()
{
  int ret = OB_SUCCESS;
  ObPartitionEntry *&entry = param_.result_.target_entry_;
  if (NULL != entry) {
    entry->renew_last_access_time();
  }

  if (is_batch_fetching_) {
    handle_cleanup_fetching_ids(true);
  }
  // update thread cache partition entry
  if (NULL != entry && entry->is_avail_state()) {
    ObPartitionRefHashMap &part_map = self_ethread().get_partition_map();
    if (OB_FAIL(part_map.set(entry))) {
      LOG_WDIAG("fail to set partition map", KPC(entry), K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }

  if (action_.cancelled_) {
    // when cancelled, do no forget free the table entry
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
    LOG_INFO("ObPartitionEntryCont has been cancelled", K_(param));
  } else if (need_notify_) {
    action_.continuation_->handle_event(PARTITION_ENTRY_LOOKUP_CACHE_DONE, &param_.result_);
    param_.result_.reset();
  } else {
    // enable async pull partition entry, need dec_ref
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

int64_t ObPartitionParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(partition_id),
       K_(table_entry),
       K_(force_renew),
       K_(is_need_force_flush),
       KP_(mysql_proxy),
       K_(current_idc_name));
  J_OBJ_END();
  return pos;
}

inline void ObPartitionParam::deep_copy(ObPartitionParam &other)
{
  if (this != &other) {
    cont_ = other.cont_;
    partition_id_ = other.partition_id_;
    force_renew_ = other.force_renew_;
    is_need_force_flush_ = other.is_need_force_flush_;
    // no need assign result_
    mysql_proxy_ = other.mysql_proxy_;
    tenant_version_ = other.tenant_version_;
    cluster_version_ = other.cluster_version_;
    set_table_entry(other.get_table_entry());
    if (!other.current_idc_name_.empty()) {
      MEMCPY(current_idc_name_buf_, other.current_idc_name_.ptr(), other.current_idc_name_.length());
      current_idc_name_.assign_ptr(current_idc_name_buf_, other.current_idc_name_.length());
    } else {
      current_idc_name_.reset();
    }

  }
}

int ObPartitionProcessor::get_partition_entry(ObPartitionParam &param, ObAction *&action)
{
  int ret = OB_SUCCESS;
  ObPartitionEntry *tmp_entry = NULL;
  ObPartitionEntryCont *cont = NULL;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(param), K(ret));
  // 1. find partition entry from thread cache
  } else if (OB_FAIL(get_partition_entry_from_thread_cache(param, tmp_entry))) {
    LOG_WDIAG("fail to get partition entry in thread cache", K(param), K(ret));
  } else if (NULL != tmp_entry) { // thread cache hit
    ObProxyMutex *mutex_ = param.cont_->mutex_;
    PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_THREAD_CACHE_HIT);
    ROUTE_PROMETHEUS_STAT(param.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, true, true);
    tmp_entry->renew_last_access_time();
    // hand over ref
    param.result_.target_entry_ = tmp_entry;
    tmp_entry = NULL;
    param.result_.is_from_remote_ = false;
  } else {
    // 2. find partition entry from remote or global cache
    cont = op_alloc(ObPartitionEntryCont);
    if (OB_ISNULL(cont)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to alloc ObPartitionEntryCont", K(ret));
    } else if (OB_FAIL(cont->init(param))) {
      LOG_WDIAG("fail to init partition entry cont", K(ret));
    } else {
      action = cont->get_action();
      if (OB_ISNULL(self_ethread().schedule_imm(cont, PARTITION_ENTRY_LOOKUP_START_EVENT))) {
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

int ObPartitionProcessor::get_partition_entry_from_thread_cache(
    ObPartitionParam &param,
    ObPartitionEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  if (!param.need_fetch_from_remote()) {
    // find entry from thread cache
    ObPartitionRefHashMap &partition_map = self_ethread().get_partition_map();
    ObPartitionEntry *tmp_entry = NULL;
    ObPartitionEntryKey key(param.get_table_entry()->get_cr_version(),
                            param.get_table_entry()->get_cr_id(),
                            param.get_table_entry()->get_table_id(),
                            param.partition_id_);

    tmp_entry = partition_map.get(key); // get will inc entry's ref
    if (NULL != tmp_entry) {
      bool find_succ = false;
      if (tmp_entry->is_deleted_state()) {
        LOG_DEBUG("this partition entry has deleted", KPC(tmp_entry));
      } else if (get_global_partition_cache().is_partition_entry_expired(*tmp_entry)) {
        // partition entry has expired
        LOG_DEBUG("the partition entry is expired",
                  "expire_time_us", get_global_partition_cache().get_cache_expire_time_us(),
                  KPC(tmp_entry), K(param));
      } else if (tmp_entry->is_avail_state() || tmp_entry->is_updating_state()) { // avail
        find_succ = true;
      } else if (tmp_entry->is_building_state()) {
        LOG_EDIAG("building state partition entry can not in thread cache", KPC(tmp_entry));
      } else if (tmp_entry->is_dirty_state()) {
        // dirty entry need to fetch from remote
      } else {}

      if (!find_succ) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get partition entry from thread cache succ", KPC(tmp_entry));
      }
    }
    entry = tmp_entry;
  }

  return ret;
}

//TODO will be used in ObBatchPartitionEntryCont
int ObPartitionProcessor::put_and_check_schedule_async_batch_fetch(ObTableEntry *entry, uint64_t partition_id)
{
  int ret = OB_SUCCESS;
  bool is_batch_async_fetch = obutils::get_global_proxy_config().enable_async_pull_location_cache  
      && obutils::get_global_proxy_config().rpc_enable_async_pull_batch_tablets;
  ObBatchPartitionEntryCont *cont = NULL;
  event::ObProxyMutex *batch_mutex = NULL;//param_.get_table_entry()->get_batch_fetch_mutex();
  if (OB_NOT_NULL(entry) 
          && is_batch_async_fetch 
          // && OB_NOT_NULL(cont = (ObBatchPartitionEntryCont*)entry->get_batch_fetch_cont())
          // && OB_NOT_NULL(cont->create_thread_)
          && OB_NOT_NULL(batch_mutex = entry->get_batch_fetch_mutex())) {
    MUTEX_TRY_LOCK(lock, batch_mutex, this_ethread());
    if (lock.is_locked() && OB_ISNULL(cont->batch_schedule_action_)) { //do it only locked and not have any other async batch task handled
      // int count = entry->get_batch_fetch_size();
      int count = 0;
      if (OB_SUCC(entry->put_batch_fetch_tablet_id(partition_id, count)) && count == 0) { //has put succ
        //not do any thing for not put it to batch
      } else if (count == obutils::get_global_proxy_config().rpc_async_pull_batch_max_size
        && OB_ISNULL(cont->batch_schedule_action_ = cont->create_thread_->schedule_imm(cont, PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT))) {
          LOG_WDIAG("fail to schedule lookup batch enty remote", K(ret));
          entry->reset_batch_tablet_ids(); //clean all
      } else if (OB_ISNULL(cont->batch_schedule_time_action_)) { //time action is NULL when count > 0
        uint64_t interval_ns = HRTIME_USECONDS(get_global_proxy_config().rpc_async_pull_batch_wait_interval);
        if (OB_ISNULL((cont->batch_schedule_time_action_ = cont->create_thread_->schedule_in(cont, interval_ns, PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT)))) {
          LOG_WDIAG("fail to schedule lookup batch enty remote interval", K(ret), K(interval_ns));
        }
      }
      if (OB_LIKELY(count > 0)) {
        if (OB_ISNULL(cont->batch_schedule_time_action_)) {}
      }
    }
  }
  return ret;
}

int64_t ObBatchPartitionParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP_(cont),
       K_(table_entry),
       K_(force_renew),
       K_(is_need_force_flush),
       KP_(mysql_proxy),
       K_(current_idc_name));
  J_OBJ_END();
  return pos;
}

inline void ObBatchPartitionParam::deep_copy(ObBatchPartitionParam &other)
{
  if (this != &other) {
    cont_ = other.cont_;
    force_renew_ = other.force_renew_;
    is_need_force_flush_ = other.is_need_force_flush_;
    // no need assign result_
    mysql_proxy_ = other.mysql_proxy_;
    tenant_version_ = other.tenant_version_;
    cluster_version_ = other.cluster_version_;
    set_table_entry(other.get_table_entry());
    if (!other.current_idc_name_.empty()) {
      MEMCPY(current_idc_name_buf_, other.current_idc_name_.ptr(), other.current_idc_name_.length());
      current_idc_name_.assign_ptr(current_idc_name_buf_, other.current_idc_name_.length());
    } else {
      current_idc_name_.reset();
    }

  }
}

ObBatchPartitionEntryCont::ObBatchPartitionEntryCont()
  : ObContinuation(), magic_(OB_CONT_MAGIC_ALIVE),
    param_(), pending_action_(NULL), action_(),
    // updating_entry_(NULL), gcached_entry_(NULL),
    is_add_building_entry_succ_(false), kill_self_(false),
    need_notify_(true), updating_entry_map_(), create_thread_(NULL),
    batch_schedule_action_(NULL), batch_schedule_time_action_(NULL)
{
  SET_HANDLER(&ObBatchPartitionEntryCont::main_handler);
}

inline int ObBatchPartitionEntryCont::init(ObBatchPartitionParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid partition param", K(param), K(ret));
  } else {
    create_thread_ = this_ethread();
    updating_entry_map_.create(10, ObModIds::OB_RPC);
    param_.deep_copy(param);
    if (OB_NOT_NULL(param.cont_)) {
      action_.set_continuation(param.cont_);
      mutex_ = param.cont_->mutex_;
    }
  }

  return ret;
}

void ObBatchPartitionEntryCont::kill_this()
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

  if (NULL != batch_schedule_action_) {
    if (OB_FAIL(batch_schedule_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending schedule action", K_(batch_schedule_action), K(ret));
    } else {
      batch_schedule_action_ = NULL;
    }
  }

  if (NULL != batch_schedule_time_action_) {
    if (OB_FAIL(batch_schedule_time_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending schedule action", K_(batch_schedule_time_action), K(ret));
    } else {
      batch_schedule_time_action_ = NULL;
    }

  }

  updating_entry_map_.destroy(); //todo need dec_ref of object

  // if (NULL != updating_entry_) {
  //   updating_entry_->dec_ref();
  //   updating_entry_ = NULL;
  // }

  // if (NULL != gcached_entry_) {
  //   gcached_entry_->dec_ref();
  //   gcached_entry_ = NULL;
  // }

  action_.set_continuation(NULL);
  magic_ = OB_CONT_MAGIC_DEAD;
  mutex_.release();

  op_free(this);
}

const char *ObBatchPartitionEntryCont::get_event_name(const int64_t event)
{
  const char *name = NULL;
  switch (event) {
    case PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT: {
      name = "PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT";
      break;
    }
    case PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT: {
      name = "PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT";
      break;
    }
    case PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_PERIOD: {
      name = "PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_PERIOD";
      break;
    }
    case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
      name = "CLIENT_TRANSPORT_MYSQL_RESP_EVENT";
      break;
    }
    default: {
      name = "unknown event name";
      break;
    }
  }
  return name;
}

int ObBatchPartitionEntryCont::main_handler(int event, void *data)
{
  int he_ret = EVENT_CONT;
  int ret = OB_SUCCESS;
  LOG_DEBUG("ObBatchPartitionEntryCont::main_handler, received event",
            "event", get_event_name(event), K(data));
  if (OB_CONT_MAGIC_ALIVE != magic_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this batch partition entry cont is dead", K_(magic), K(ret));
  } else if (this_ethread() != mutex_->thread_holding_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("this_ethread must be equal with thread_holding", "this_ethread",
              this_ethread(), "thread_holding", mutex_->thread_holding_, K(ret));
  } else {
    pending_action_ = NULL;
    switch (event) {
      case PARTITION_ENTRY_LOOKUP_BATCH_REMOTE_EVENT: {
        if (OB_FAIL(lookup_batch_entry_remote())) {
          LOG_WDIAG("fail to lookup batch enty remote", K(ret));
        }
        break;
      }
      case PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT: {
        // fail to schedule, data must be NULL
        data = NULL;
        // fall through
      }
      __attribute__ ((fallthrough));
      case CLIENT_TRANSPORT_MYSQL_RESP_EVENT: {
        batch_schedule_time_action_ = NULL;
        if (OB_FAIL(handle_batch_client_resp(data))) {
          LOG_WDIAG("fail to handle client resp", K(ret));
        // } else if (OB_FAIL(notify_caller())) {
        //   LOG_WDIAG("fail to notify caller result", K(ret));
        }
        // kill_self_ = true; //to release
        break;
      }
      case PARTITION_ENTRY_LOOKUP_BATCH_TO_RELEASE: {
        if (create_thread_ == this_ethread()) {
          kill_self_ = true; //to kill myself
        } else {
          create_thread_->schedule_imm(this, PARTITION_ENTRY_LOOKUP_BATCH_TO_RELEASE);
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
    // if (NULL != (param_.result_.target_entry_)) {
    //   param_.result_.target_entry_->dec_ref();
    //   param_.result_.target_entry_ = NULL;
    // }
    // param_.result_.is_from_remote_ = false;
    // if (OB_FAIL(notify_caller())) {
    //   LOG_WDIAG("fail to notify caller result", K(ret));
    // }
  }

  //TODO ethread_schedule
  if (kill_self_) {
    kill_this();
    he_ret = EVENT_DONE;
  }
  return he_ret;
}

int ObBatchPartitionEntryCont::lookup_batch_entry_remote()
{
  int ret = OB_SUCCESS;
  PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE);
  ObMysqlProxy *mysql_proxy = param_.mysql_proxy_;
  char sql[OB_MEDIUM_SQL_LENGTH];
  sql[0] = '\0';
  ObSEArray<uint64_t, 10> partition_ids;
  if (OB_NOT_NULL(pending_action_)) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel batch entry fetch remote", K(ret));
    } else {
      pending_action_ = NULL;
    }
  }
  if (OB_NOT_NULL(batch_schedule_time_action_)) {
    if (OB_FAIL(batch_schedule_time_action_->cancel())) {
      LOG_WDIAG("fail to cancel batch entry fetch remote", K(ret));
    } else {
      batch_schedule_time_action_ = NULL;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(param_.get_table_entry()->get_batch_fetch_tablet_ids(partition_ids))) {
      LOG_WDIAG("fail to get batch tablet ids in table entry", K(ret), "table_entry", param_.get_table_entry());
    } else if (OB_FAIL(ObRouteUtils::get_batch_partition_entry_sql(sql, OB_MEDIUM_SQL_LENGTH,
                   param_.get_table_entry()->get_names(), partition_ids,
                   param_.is_need_force_flush_, param_.cluster_version_))) {
      LOG_WDIAG("fail to get table entry sql", K(sql), K(ret));
    } else if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to get mysql_proxy for batch fetch", K(mysql_proxy), K(ret));
    } else {
      const ObMysqlRequestParam request_param(sql, param_.current_idc_name_);
      if (OB_FAIL(mysql_proxy->async_read(this, request_param, pending_action_))) {
        LOG_WDIAG("fail to nonblock read", K(sql), K_(param), K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    // just treat as execute failed
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_imm(this, PARTITION_ENTRY_FAIL_SCHEDULE_LOOKUP_BATCH_REMOTE_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule imm", K(ret));
    }
  }
  return ret;
}

int ObBatchPartitionEntryCont::handle_batch_client_resp(void *data)
{
  int ret = OB_SUCCESS;
  bool is_add_succ = false;
  const ObTableEntryName &table_name = param_.get_table_entry()->get_names();
  const uint64_t partition_id = OB_INVALID_PARTITION; //tmp to print
  ObPartitionEntry *updating_entry = NULL;

  if (NULL != data) {
    ObClientMysqlResp *resp = reinterpret_cast<ObClientMysqlResp *>(data);
    ObResultSetFetcher *rs_fetcher = NULL;
    ObPartitionEntry *entry = NULL;
    common::ObSEArray<ObPartitionEntry *, 10> entry_array;
    if (resp->is_resultset_resp()) {
      if (OB_FAIL(resp->get_resultset_fetcher(rs_fetcher))) {
        LOG_WDIAG("fail to get resultset fetcher", K(ret));
      } else if (OB_ISNULL(rs_fetcher)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("rs_fetcher and entry can not be NULL", K(rs_fetcher), K(entry), K(ret));
      } else if (OB_FAIL(ObRouteUtils::fetch_more_partition_entrys_info(
              *rs_fetcher, *param_.get_table_entry(), entry_array, param_.cluster_version_))) {
        LOG_WDIAG("fail to fetch one partition entry info", K(ret));
      // } else if (NULL == entry) {
      //   PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
      //   ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
      //   LOG_INFO("no valid partition entry, empty resultset", K(table_name), K(partition_id));
      } else if (entry_array.count() > 0) {
        LOG_INFO("valid partition entry, to fetch resultset", K(table_name), K(entry_array));
        for (int i = 0; i < entry_array.count(); i++) {
          entry = entry_array.at(i);
          if (OB_NOT_NULL(entry) && entry->is_valid()) {
            entry->inc_ref(); // Attention!! before add to table cache, must inc_ref
            if (!entry->get_pl().exist_leader()) {
              // current the parittion has no leader, avoid refequently updating
              entry->renew_last_update_time();
            }
            if (OB_FAIL(get_global_partition_cache().add_partition_entry(*entry, false))) {
              LOG_WDIAG("fail to add table entry", KPC(entry), K(ret));
              entry->dec_ref(); // paired the ref count above
            } else {
              LOG_INFO("get partition entry from remote succ", KPC(entry));
              PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_SUCC);
              ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, true);
              is_add_succ = true;
              entry->set_tenant_version(param_.tenant_version_);

              updating_entry = NULL;
              if (OB_SUCC(updating_entry_map_.get_refactored(entry->get_partition_id(), updating_entry)) && OB_NOT_NULL(updating_entry)) {
                if (updating_entry->is_the_same_entry(*entry)) {
                  // current parittion is the same with old one, avoid refequently updating
                  entry->renew_last_update_time();
                  LOG_INFO("new partition entry is the same with old one, will renew last_update_time "
                           "and avoid refequently updating", KPC(updating_entry), KPC(entry));
                }
                ObProxyPartitionLocation &this_pl = const_cast<ObProxyPartitionLocation &>(entry->get_pl());
                ObProxyPartitionLocation &new_pl = const_cast<ObProxyPartitionLocation &>(updating_entry->get_pl());
                const bool is_server_changed = new_pl.check_and_update_server_changed(this_pl);
                if (is_server_changed) {
                  LOG_INFO("server is changed, ", "old_entry", PC(updating_entry),
                                                  "new_entry", PC(entry));
                }
                updating_entry_map_.erase_refactored(entry->get_partition_id());
              }
              entry = NULL;
            }
          } else {
            PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
            ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
            LOG_INFO("invalid partition entry", K(table_name), K(partition_id), KPC(entry));
            // free entry
            // entry->dec_ref();
            // entry = NULL;
          }
        }
      }
    } else {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
      ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
      const int64_t error_code = resp->get_err_code();
      LOG_WDIAG("fail to get batch partition entry from remote", K(table_name),
               K(partition_id), K(error_code));
    }

    if (OB_FAIL(ret) && (NULL != entry)) {
      entry->dec_ref();
      entry = NULL;
    }
    op_free(resp); // free the resp come from ObMysqlProxy
    resp = NULL;
  } else {
    // no resp, maybe client_vc disconnect, do not return error
    PROCESSOR_INCREMENT_DYN_STAT(GET_PARTITION_ENTRY_FROM_REMOTE_FAIL);
    ROUTE_PROMETHEUS_STAT(param_.get_table_entry()->get_names(), PROMETHEUS_ENTRY_LOOKUP_COUNT, PARTITION_ENTRY, false, false);
    LOG_INFO("has no resp, maybe client_vc disconnect");
  }

  // if fail to update dirty partition entry, must set entry state from UPDATING back to DIRTY,
  // or it will never be upated again
  PARTITION_ENTRY_MAP::iterator iter = updating_entry_map_.begin();
  for (; iter != updating_entry_map_.end(); iter++) {
    if (OB_NOT_NULL(updating_entry = iter->second)) {
      if (!is_add_succ) {
        if (updating_entry->is_updating_state()) {
          updating_entry->renew_last_update_time(); // avoid refequently updating
          // double check
          if (updating_entry->cas_compare_and_swap_state(ObTableEntry::UPDATING, ObTableEntry::DIRTY)) {
            LOG_INFO("fail to update dirty table entry, set state back to dirty", KPC(updating_entry));
          }
        }
      }
      updating_entry->dec_ref();
      updating_entry = NULL;
    }
  }

  // // if we has add a building state entry to part cache, but
  // // fail to fetch from remote, we should remove it,
  // // or will never fetch this part entry again
  // if (is_add_building_entry_succ_ && !is_add_succ) {
  //   ObPartitionEntryKey key(param_.get_table_entry()->get_cr_version(),
  //                           param_.get_table_entry()->get_cr_id(),
  //                           param_.get_table_entry()->get_table_id(),
  //                           param_.partition_id_);

  //   LOG_INFO("fail to add this part entry to part cache, "
  //            "we should remove it from part cache", K_(is_add_building_entry_succ),
  //            K(is_add_succ), K(key));
  //   int tmp_ret = OB_SUCCESS;
  //   if (OB_SUCCESS != (tmp_ret = get_global_partition_cache().remove_partition_entry(key))) {
  //     LOG_WDIAG("fail to remove part entry", K(key), K(ret), K(tmp_ret));
  //     if (OB_SUCC(ret)) {
  //       ret = tmp_ret;
  //     }
  //   }
  // }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
