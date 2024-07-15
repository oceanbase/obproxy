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

#include "obutils/ob_proxy_config.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_cache.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_cache.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx_cache.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_cache_cleaner.h"
#include "iocore/eventsystem/ob_event_processor.h"
#include "iocore/net/ob_net_def.h"
#include "utils/ob_proxy_utils.h"
#include "utils/ob_ref_hash_map.h"
#include "stat/ob_processor_stats.h"
#include "lib/lock/ob_drw_lock.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::obproxy;
using namespace oceanbase::obproxy::net;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObRpcCacheCleaner::ObRpcCacheCleaner()
    : ObContinuation(), is_inited_(false), triggered_(false), cleaner_reschedule_interval_us_(0),
      total_cleaner_count_(0), this_cleaner_idx_(), next_action_(IDLE_CLEAN_ACTION),
      ethread_(NULL), table_query_async_cache_(NULL), table_query_async_cache_range_(),
      tablegroup_cache_deleted_cr_version_(), tablegroup_cache_last_expire_time_us_(0), pending_action_(NULL)
{
  SET_HANDLER(&ObRpcCacheCleaner::main_handler);
}

int ObRpcCacheCleaner::init(ObTableQueryAsyncCache &table_query_async_cache,
                            ObTableGroupCache &tablegroup_cache,
                            ObRpcReqCtxCache &rpc_ctx_cache,
                            const ObCountRange &range, const int64_t total_count,
                            const int64_t idx, const int64_t clean_interval_us)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (!range.is_valid() || total_count <= 0 || idx >= total_count || clean_interval_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(range), K(total_count), K(idx), K(clean_interval_us), K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex(CACHE_CLEANER_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to allocate mutex", K(ret));
  } else {
    is_inited_ = true;
    mutex_ = mutex;
    table_query_async_cache_ = &table_query_async_cache;
    tablegroup_cache_ = &tablegroup_cache;
    rpc_ctx_cache_ = &rpc_ctx_cache;
    table_query_async_cache_range_ = range;
    tablegroup_cache_range_ = range;
    rpc_ctx_cache_range_ = range;
    tc_part_clean_count_ = 0;
    pending_action_ = NULL;
    total_cleaner_count_ = total_count;
    this_cleaner_idx_ = idx;
    cleaner_reschedule_interval_us_ = clean_interval_us;
  }
  return ret;
}

int ObRpcCacheCleaner::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_CONT;
  UNUSED(event);
  UNUSED(data);

  LOG_DEBUG("[ObRpcCacheCleaner::main_handler]", K(event), K(data), K_(triggered), K(*this));

  switch (event) {
    case EVENT_IMMEDIATE: {
      if (OB_FAIL(cancel_pending_action())) {
        LOG_WDIAG("fail to cancel pending_action", K(ret));
      } else {
        LOG_INFO("cleaner clean interval has changed", "current interval",
                 cleaner_reschedule_interval_us_);
        if (OB_FAIL(schedule_in(cleaner_reschedule_interval_us_))) {
          LOG_EDIAG("fail to schedule cleaner", K(ret));
        }
      }
      break;
    }
    case CLEANER_TRIGGER_EVENT: {
      if (OB_FAIL(cancel_pending_action())) {
        LOG_WDIAG("fail to cancel pending_action", K(ret));
      } else if (OB_FAIL(cleanup())) {
        LOG_WDIAG("fail to cleanup", K(ret));
      }
      break;
    }
    case EVENT_INTERVAL: {
      pending_action_ = NULL;
      if (OB_FAIL(cleanup())) {
        LOG_WDIAG("fail to cleanup", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("unexpected event", K(event), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_EDIAG("ObRpcCacheCleaner will exit, something error",
              "this_thread", this_ethread(), K(ret));
    event_ret = EVENT_DONE;
    self_ethread().cache_cleaner_ = NULL;
    delete this;
  }

  return event_ret;
}

int ObRpcCacheCleaner::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_clean_job())) {
    LOG_WDIAG("fail to do clean job", K(ret));
  } else {
    triggered_ = false;
  }

  // some error unexpected, just clear some state and retry
  if (OB_FAIL(ret)) {
    LOG_EDIAG("some internal error, just clear state and retry", K(ret));
    next_action_ = CLEAN_TABLE_QUERY_ASYNC_CACHE_ACTION;
    table_query_async_cache_range_.again();
    tc_part_clean_count_ = 0;
    if (NULL != pending_action_) {
      pending_action_->cancel();
      pending_action_ = NULL;
    }

    ret = OB_SUCCESS;
    // schedule next clean action
    if (OB_FAIL(schedule_in(cleaner_reschedule_interval_us_))) {
      LOG_EDIAG("fail to schedule next clean action, cache cleaner will stop working", K(ret));
    }
  }

  return ret;
}

int ObRpcCacheCleaner::do_clean_job()
{
  LOG_DEBUG("[ObRpcCacheCleaner::do_clean_job]", K(*this));
  int ret = OB_SUCCESS;

  bool stop = false;
  while (!stop) {
    LOG_DEBUG("ObRpcCacheCleaner will do ", "action", get_cleaner_action_name(next_action_));
    switch (next_action_) {
      case IDLE_CLEAN_ACTION: {
        // begin to work
        next_action_ = CLEAN_TABLE_QUERY_ASYNC_CACHE_ACTION;
        break;
      }

      case CLEAN_TABLE_QUERY_ASYNC_CACHE_ACTION: {
        if (OB_FAIL(clean_table_query_async_cache())) {
          LOG_WDIAG("fail to clean table query async cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_THREAD_CACHE_TABLE_QUERY_ASYNC_ENTRY_ACTION;
        break;
      }

      case CLEAN_THREAD_CACHE_TABLE_QUERY_ASYNC_ENTRY_ACTION: {
        ObTableQueryAsyncRefHashMap &table_query_async_map = self_ethread().get_table_query_async_map();
        if (OB_FAIL(table_query_async_map.clean_hash_map())) {
          LOG_WDIAG("fail to clean table query async map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_RPC_CTX_CACHE_ACTION;
        break;
      }

      case CLEAN_RPC_CTX_CACHE_ACTION: {
        if (OB_FAIL(clean_rpc_ctx_cache())) {
          LOG_WDIAG("fail to clean rpc ctx cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_THREAD_CACHE_RPC_CTX_ACTION;
        break;
      }

      case CLEAN_THREAD_CACHE_RPC_CTX_ACTION: {
        ObRpcReqCtxRefHashMap &rpc_ctx_map = self_ethread().get_rpc_req_ctx_map();
        if (OB_FAIL(rpc_ctx_map.clean_hash_map())) {
          LOG_WDIAG("fail to clean rpc ctx map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = EXPIRE_TABLEGROUP_ENTRY_ACTION;
        break;
      }

      case EXPIRE_TABLEGROUP_ENTRY_ACTION: {
        if (OB_FAIL(do_expire_tablegroup_entry())) {
          LOG_WDIAG("fail to do tablegroup entry", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_TABLEGROUP_CACHE_ACTION;
        break;
      }

     case CLEAN_TABLEGROUP_CACHE_ACTION: {
        if (OB_FAIL(clean_tablegroup_cache())) {
          LOG_WDIAG("fail to clean tablegroup cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_THREAD_CACHE_TABLEGROUP_ENTRY_ACTION;
        break;
      }

      case CLEAN_THREAD_CACHE_TABLEGROUP_ENTRY_ACTION: {
        ObTableGroupRefHashMap &tablegroup_map = self_ethread().get_tablegroup_map();
        if (OB_FAIL(tablegroup_map.clean_hash_map())) {
          LOG_WDIAG("fail to clean tablegroup hash map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = IDLE_CLEAN_ACTION;
        stop = true;
        break;
      }
      default: {
        LOG_EDIAG("never reach here", "action", get_cleaner_action_name(next_action_));
        stop = true;
        next_action_ = IDLE_CLEAN_ACTION; // from the beginning
      }
    }
  }

  if (OB_FAIL(schedule_in(cleaner_reschedule_interval_us_))) {
    LOG_EDIAG("fail to schedule in cache cleaner", K_(cleaner_reschedule_interval_us), K(ret));
  }

  return ret;
}

bool ObRpcCacheCleaner::is_tablegroup_cache_expire_time_changed()
{
  return (tablegroup_cache_last_expire_time_us_ != tablegroup_cache_->get_cache_expire_time_us());
}

int ObRpcCacheCleaner::do_expire_tablegroup_entry()
{
  int ret = OB_SUCCESS;

  if (!tablegroup_cache_deleted_cr_version_.empty() || is_tablegroup_cache_expire_time_changed()) {
    ObProxyMutex *bucket_mutex = NULL;
    bool all_locked = true;
    if (tablegroup_cache_range_.count() > 0) {
      // every bucket
      for (int64_t i = tablegroup_cache_range_.start_idx_; i <= tablegroup_cache_range_.end_idx_; ++i) {
        bucket_mutex = tablegroup_cache_->lock_for_key(i);
        MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
        if (lock.is_locked()) {
          tablegroup_cache_->gc(i);
        } else {
          all_locked = false;
        }
      }
    }

    // if all buckets are locked at once, we will reset partition_cache_deleted_cr_version_
    if (all_locked) {
      tablegroup_cache_deleted_cr_version_.reset();
      tablegroup_cache_last_expire_time_us_ = tablegroup_cache_->get_cache_expire_time_us();
    }
  }

  return ret;
}

int ObRpcCacheCleaner::clean_tablegroup_cache()
{
  int ret = OB_SUCCESS;
  ObCountRange &range = tablegroup_cache_range_;
  int64_t mt_part_count_for_clean = range.count();
  if (mt_part_count_for_clean > 0) {
    ObProxyConfig &config = get_global_proxy_config();
    int64_t mt_part_num = tablegroup_cache_->get_sub_part_count();
    int64_t mem_limited = (((config.routing_cache_mem_limited) / 1) / mt_part_num);
    int64_t mem_limited_threshold = ((mem_limited * 3) / 4);
    int64_t max_sub_bucket_count = mem_limited_threshold / AVG_TABLEGROUP_ENTRY_SIZE;

    if (max_sub_bucket_count > 0) {
      // get max count for every sub bucket
      for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
        int64_t entry_count = tablegroup_cache_->get_part_cur_size(i);
        int64_t clean_count = entry_count - max_sub_bucket_count;
        LOG_DEBUG("after calc", "sub bucket idx", i, K(clean_count), K(entry_count),
                  K(max_sub_bucket_count));
        if (clean_count > 0)  {
          LOG_INFO("will clean tablegroup cache", "sub bucket idx", i, K(clean_count),
                   K(max_sub_bucket_count), K(entry_count), K(mem_limited));
          if (OB_FAIL(clean_one_sub_bucket_tablegroup_cache(i, clean_count))) {
            LOG_WDIAG("fail to clean sub bucket tablegroup cache", "sub bucket idx", i,
                     K(clean_count), K(max_sub_bucket_count), K(mem_limited));
            ret = OB_SUCCESS; // ignore, and coutine
          }
        }
      }
    }
  }
  return ret;
}

bool ObRpcCacheCleaner::is_tablegroup_entry_expired(ObTableGroupEntry &entry)
{
  bool expired = false;
  if (tablegroup_cache_->is_tablegroup_entry_expired(entry)) {
    expired = true;
  }
  if (!expired) {
    // must acquire ObCacheCleaner' mustex, or deleted_cr_verison_ maybe multi read and write
    MUTEX_TRY_LOCK(lock, this->mutex_, this_ethread());
    if (lock.is_locked()) {
      for (int64_t i = 0; (i < tablegroup_cache_deleted_cr_version_.count()) && !expired; ++i) {
        if (entry.get_cr_version() == tablegroup_cache_deleted_cr_version_.at(i)) {
          expired = true;
        }
      }
    }
  }
  return expired;
}

struct ObTableGroupEntryElem
{
  ObTableGroupEntryElem() : entry_(NULL) {}
  ~ObTableGroupEntryElem() {}

  ObTableGroupEntry *entry_;
};

struct ObTableGroupEntryCmp
{
  bool operator() (const ObTableGroupEntryElem& lhs, const ObTableGroupEntryElem& rhs) const
  {
    bool bret = false;
    if ((NULL != lhs.entry_) && (NULL != rhs.entry_)) {
      bret = (lhs.entry_->get_last_access_time_us() <= rhs.entry_->get_last_access_time_us());
    } else if (NULL != lhs.entry_) {
      bret = (lhs.entry_->get_last_access_time_us() <= 0);
    } else if (NULL != rhs.entry_) {
      bret = (0 <= rhs.entry_->get_last_access_time_us());
    } else {
      bret = true;
    }
    return bret;
  }
};

int ObRpcCacheCleaner::clean_one_sub_bucket_tablegroup_cache(const int64_t bucket_idx, const int64_t clean_count)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = tablegroup_cache_->get_sub_part_count();
  if ((bucket_idx < 0) || (bucket_idx >= bucket_num) || (clean_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_idx), K(clean_count), K(bucket_num), K(ret));
  } else {
    ObTableGroupEntryCmp cmp;
    ObTableGroupEntry *entry = NULL;
    TableGroupIter it;
    ObProxyMutex *bucket_mutex = tablegroup_cache_->lock_for_key(bucket_idx);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      int64_t tmp_clean_count = std::max(clean_count, 2L);
      int64_t buf_len = sizeof(ObTableGroupEntryElem) * tmp_clean_count;
      char *buf = static_cast<char *>(op_fixed_mem_alloc(buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc memory", K(buf_len), K(ret));
      } else if (OB_FAIL(tablegroup_cache_->run_todo_list(bucket_idx))) {
        LOG_WDIAG("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        // 1.make a smallest heap
        ObTableGroupEntryElem *eles = new (buf) ObTableGroupEntryElem[tmp_clean_count];
        ObTableGroupEntryElem tmp_ele;
        int64_t i = 0;
        entry = tablegroup_cache_->first_entry(bucket_idx, it);
        while (NULL != entry) {
          if (i < tmp_clean_count) {
            eles[i].entry_ = entry;
            ++i;
            if (i == tmp_clean_count) {
              std::make_heap(eles, eles + tmp_clean_count, cmp);
            }
          } else {
            std::pop_heap(eles, eles + tmp_clean_count, cmp);
            tmp_ele.entry_ = entry;
            if (cmp(tmp_ele, eles[tmp_clean_count - 1])) {
              eles[tmp_clean_count - 1].entry_ = entry;
              std::push_heap(eles, eles + tmp_clean_count, cmp);
            } else {
              std::make_heap(eles, eles + tmp_clean_count, cmp);
            }
          }

          entry = tablegroup_cache_->next_entry(bucket_idx, it);
        }

        // 2. calc wash count again for defense
        int64_t part_entry_count = tablegroup_cache_->get_part_cur_size(bucket_idx);
        int64_t orig_clean_count = tmp_clean_count;
        if (part_entry_count <= PART_TABLEGROUP_ENTRY_MIN_COUNT) {
          tmp_clean_count = 0; // don't clean
        } else if ((part_entry_count - tmp_clean_count) <= PART_TABLEGROUP_ENTRY_MIN_COUNT) {
          int64_t dcount = part_entry_count - PART_TABLEGROUP_ENTRY_MIN_COUNT;
          tmp_clean_count = ((dcount >= 0) ? dcount : 0);
        }
        LOG_INFO("begin to wash tablegroup entry", "wash_count",
                 tmp_clean_count, K(part_entry_count), K(orig_clean_count), K(bucket_idx),
                 LITERAL_K(PART_TABLEGROUP_ENTRY_MIN_COUNT));

        // 3. remove the LRU entry
        ObTableGroupEntryKey key;
        for (int64_t i = 0; (i < tmp_clean_count) && OB_SUCC(ret); ++i) {
          entry = eles[i].entry_;
          // do not remove building state tablegroup entry
          if (NULL != entry && !entry->is_building_state()) {
            // PROCESSOR_INCREMENT_DYN_STAT(KICK_OUT_PARTITION_ENTRY_FROM_GLOBAL_CACHE);
            LOG_INFO("this tablegroup entry will be washed", KPC(entry));
            key.reset();
            entry->get_key(key);
            if (OB_FAIL(tablegroup_cache_->remove_tablegroup_entry(key))) {
              LOG_WDIAG("fail to remove tablegroup entry", KPC(entry), K(ret));
            }
          }
        }
      }

      // 3. free the mem
      if ((NULL != buf) && (buf_len > 0)) {
        op_fixed_mem_free(buf, buf_len);
        buf = NULL;
        buf_len = 0;
      }
    } else { // fail to try lock
      LOG_INFO("fail to try lock, wait next round", K(bucket_idx), K(clean_count));
    }
  }

  return ret;
}

int ObRpcCacheCleaner::clean_table_query_async_cache() {
  int ret = OB_SUCCESS;
  ObCountRange &range = table_query_async_cache_range_;
  int64_t mt_part_count_for_clean = range.count();
  if (mt_part_count_for_clean > 0) {
    for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
      LOG_INFO("will clean table query async cache", "sub bucket idx", i);
      if (OB_FAIL(clean_one_sub_bucket_table_query_async_cache(i))) {
        LOG_WDIAG("fail to clean sub bucket table query async cache", "sub bucket idx", i);
        ret = OB_SUCCESS; // ignore, and coutine
      }
    }
  }
  return ret;
}

int ObRpcCacheCleaner::clean_one_sub_bucket_table_query_async_cache(const int64_t bucket_idx)
                                                         
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = table_query_async_cache_->get_sub_part_count();
  if ((bucket_idx < 0) || (bucket_idx >= bucket_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_idx), K(bucket_num), K(ret));
  } else {
    ObTableQueryAsyncEntry *entry = NULL;
    TableQueryAsyncIter it;
    ObProxyMutex *bucket_mutex = table_query_async_cache_->lock_for_key(bucket_idx);
    ObSEArray<uint64_t, 2> need_delete_key;
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(table_query_async_cache_->run_todo_list(bucket_idx))) {
        LOG_WDIAG("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        entry = table_query_async_cache_->first_entry(bucket_idx, it);
        while (NULL != entry) {
          if (!entry->is_deleted_state() && (ObTimeUtility::current_time() > entry->get_timeout_ts())) {
            entry->set_deleted_state();
          }
          if (entry->is_deleted_state()) {
            LOG_INFO("this table query async entry will be washed", KPC(entry));
            need_delete_key.push_back(entry->get_client_query_session_id());
          }
          entry = table_query_async_cache_->next_entry(bucket_idx, it);
        }
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < need_delete_key.count(); ++i) {
            uint64_t key = need_delete_key.at(i);
            if (OB_FAIL(table_query_async_cache_->remove_table_query_async_entry(key))) {
              LOG_WDIAG("fail to remove table query async entry", KPC(entry), K(ret));
            }
          }
        }
      }
    } else { // fail to try lock
      LOG_INFO("fail to try lock, wait next round", K(bucket_idx));
    }
  }

  return ret;
}

int ObRpcCacheCleaner::clean_rpc_ctx_cache() {
  int ret = OB_SUCCESS;
  ObCountRange &range = rpc_ctx_cache_range_;
  int64_t mt_part_count_for_clean = range.count();
  if (mt_part_count_for_clean > 0) {
    for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
      LOG_INFO("will clean rpc ctx cache", "sub bucket idx", i);
      if (OB_FAIL(clean_one_sub_bucket_rpc_ctx_cache(i))) {
        LOG_WDIAG("fail to clean sub bucket rpc ctx cache", "sub bucket idx", i);
        ret = OB_SUCCESS; // ignore, and coutine
      }
    }
  }
  return ret;
}

int ObRpcCacheCleaner::clean_one_sub_bucket_rpc_ctx_cache(const int64_t bucket_idx)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = rpc_ctx_cache_->get_sub_part_count();
  if ((bucket_idx < 0) || (bucket_idx >= bucket_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_idx), K(bucket_num), K(ret));
  } else {
    ObRpcReqCtx *rpc_ctx = NULL;
    RpcReqCtxIter it;
    ObProxyMutex *bucket_mutex = rpc_ctx_cache_->lock_for_key(bucket_idx);
    ObSEArray<obkv::ObTableApiCredential , 2> need_delete_key;
    int64_t expired_time_us = get_global_proxy_config().rpc_ctx_expire_time;  // us
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(rpc_ctx_cache_->run_todo_list(bucket_idx))) {
        LOG_WDIAG("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        rpc_ctx = rpc_ctx_cache_->first_entry(bucket_idx, it);
        while (NULL != rpc_ctx) {
          if (!rpc_ctx->is_deleting() && (rpc_ctx->is_expired(HRTIME_USECONDS(expired_time_us)))) {
            rpc_ctx->set_deleting_state();
          }
          if (rpc_ctx->is_deleting()) {
            LOG_INFO("this rpc req ctx will be washed", KPC(rpc_ctx));
            need_delete_key.push_back(rpc_ctx->get_credential());
          }
          rpc_ctx = rpc_ctx_cache_->next_entry(bucket_idx, it);
        }
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < need_delete_key.count(); ++i) {
            if (OB_FAIL(rpc_ctx_cache_->remove_rpc_req_ctx(need_delete_key.at(i)))) {
              LOG_WDIAG("fail to remove rpc ctx", K(ret));
            }
          }
        }
      }
    } else { // fail to try lock
      LOG_INFO("fail to try lock, wait next round", K(bucket_idx));
    }
  }

  return ret;
}


int ObRpcCacheCleaner::schedule_in(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (timeout_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(timeout_us), K(ret));
  } else {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(timeout_us);
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this, HRTIME_USECONDS(interval_us)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule in", K(interval_us), K(ret));
    }
  }

  return ret;
}

const char *ObRpcCacheCleaner::get_cleaner_action_name(const ObRpcCleanAction action) const
{
  const char *name = NULL;
  switch (action) {
    case IDLE_CLEAN_ACTION:
      name = "IDLE_CLEAN_ACTION";
      break;
    case CLEAN_TABLE_QUERY_ASYNC_CACHE_ACTION:
      name = "CLEAN_TABLE_QUERY_ASYNC_CACHE_ACTION";
      break;
    case CLEAN_THREAD_CACHE_TABLE_QUERY_ASYNC_ENTRY_ACTION:
      name = "CLEAN_THREAD_CACHE_TABLE_QUERY_ASYNC_ENTRY_ACTION";
      break;

    default:
      name = "CLIENT_ACTION_UNKNOWN";
      break;
  }
  return name;
}

int64_t ObRpcCacheCleaner::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited),
       "next action", get_cleaner_action_name(next_action_),
       K_(table_query_async_cache_range),
       KP_(pending_action));
  J_OBJ_END();
  return pos;
}

int ObRpcCacheCleaner::schedule_cache_cleaner()
{
  int ret = OB_SUCCESS;

  ObEventThreadType etype = ET_NET;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];

  const int64_t mt_part_num = MT_HASHTABLE_PARTITIONS;
  if (net_thread_count > 0 && mt_part_num > 0) {
    for (int64_t i = 0; i < net_thread_count; ++i) {
      // calc range
      if (OB_FAIL(schedule_one_cache_cleaner(i))) {
        LOG_WDIAG("fail to init cleaner", K(i), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("thread_num or mt_part_num can not be NULL", K(mt_part_num), K(net_thread_count), K(ret));
  }

  return ret;
}

int ObRpcCacheCleaner::schedule_one_cache_cleaner(int64_t index)
{
  int ret = OB_SUCCESS;

  ObEventThreadType etype = ET_NET;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];
  ObEThread **netthreads = g_event_processor.event_thread_[etype];
  ObEThread *target_ethread = NULL;
  int64_t clean_interval = get_global_proxy_config().cache_cleaner_clean_interval;

  ObCountRange range;
  ObRpcCacheCleaner *cleaner = NULL;
  ObTableQueryAsyncCache &table_query_async_cache = get_global_table_query_async_cache();
  ObTableGroupCache &tablegroup_cache = get_global_tablegroup_cache();
  ObRpcReqCtxCache &rpc_ctx_cache = get_global_rpc_req_ctx_cache();

  const int64_t mt_part_num = MT_HASHTABLE_PARTITIONS;
  if (net_thread_count > 0 && mt_part_num > 0) {
    int64_t part_num_per_thread = mt_part_num / net_thread_count;
    int64_t remain_num = mt_part_num % net_thread_count;
    //for (int64_t i = 0; i < net_thread_count; ++i) {
      // calc range
      range.reset();

      if (part_num_per_thread > 0) { // thread_num <= mt_part_num
        if (index < remain_num) {
          range.start_idx_ = (part_num_per_thread + 1) * index;
          range.end_idx_ = ((part_num_per_thread + 1) * (index + 1) - 1);
        } else {
          int64_t base = (part_num_per_thread + 1) * remain_num;
          range.start_idx_ = base + (part_num_per_thread) * (index - remain_num);
          range.end_idx_ = base + ((part_num_per_thread) * (index - remain_num + 1) - 1);
        }
      } else if (0 == part_num_per_thread) { // thread_num > mt_part_num
        if (index < mt_part_num) {
          range.start_idx_ = index;
          range.end_idx_ = index;
        }
      }

      range.again();
      if (OB_ISNULL(cleaner = new (std::nothrow) ObRpcCacheCleaner())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc cleaner", K(ret));
      } else if (OB_FAIL(cleaner->init(table_query_async_cache, tablegroup_cache, rpc_ctx_cache, range, net_thread_count, index, clean_interval))) {
        LOG_WDIAG("fail to init cleaner", K(range), K(ret));
      } else if (OB_ISNULL(target_ethread = netthreads[index])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("ethread can not be NULL", K(target_ethread), K(ret));
      } else if (OB_FAIL(cleaner->start_clean_cache(*target_ethread))) {
        LOG_WDIAG("fail to start clean cache", K(ret));
      } else {
        target_ethread->rpc_cache_cleaner_ = cleaner;
        LOG_INFO("succ schedule rpc cache cleaners", K(target_ethread), K(range), K(index),
                 K(part_num_per_thread), K(remain_num), K(net_thread_count), K(ret));
      }
    //}
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_EDIAG("thread_num or mt_part_num can not be NULL", K(mt_part_num), K(net_thread_count), K(ret));
  }

  return ret;
}

int ObRpcCacheCleaner::cancel_pending_action()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != pending_action_)) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WDIAG("fail to cancel pending_action", K(ret));
    } else {
      pending_action_ = NULL;
    }
  }
  return ret;
}

int ObRpcCacheCleaner::set_clean_interval(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid clean interval", K(interval_us), K(ret));
  } else {
    cleaner_reschedule_interval_us_ = interval_us;
  }
  return ret;
}

int ObRpcCacheCleaner::update_clean_interval()
{
  int ret = OB_SUCCESS;
  int64_t interval_us = get_global_proxy_config().cache_cleaner_clean_interval;
  int64_t thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  ObEThread **threads = g_event_processor.event_thread_[ET_CALL];
  ObRpcCacheCleaner *cleaner = NULL;
  ObEThread *ethread = NULL;
  for (int64_t i = 0; (i < thread_count) && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(ethread = threads[i]) || OB_ISNULL(cleaner = threads[i]->rpc_cache_cleaner_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("ethread and cache cleaner can not be NULL", K(i), K(thread_count), K(ethread), K(cleaner), K(ret));
    } else if (OB_FAIL(cleaner->set_clean_interval(interval_us))) {
      LOG_WDIAG("fail to set clean interval", K(interval_us), K(ret));
    } else if (OB_ISNULL(ethread->schedule_imm(cleaner))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to schedule imm", K(ret));
    }
  }
  return ret;
}

int ObRpcCacheCleaner::trigger()
{
  int ret = OB_SUCCESS;
  if (!triggered_) {
    if (OB_ISNULL(ethread_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("ethread can not be NULL", K_(ethread), K(ret));
    } else if (OB_ISNULL(ethread_->schedule_imm(this, CLEANER_TRIGGER_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_EDIAG("fail to schedule imm", K(ret));
    } else {
      triggered_ = true;
    }
  }
  return ret;
}

int ObRpcCacheCleaner::start_clean_cache(ObEThread &ethread)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(cleaner_reschedule_interval_us_ <= 0 || NULL != pending_action_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WDIAG("unexpect state", K_(cleaner_reschedule_interval_us), K_(pending_action), K(ret));
  } else {
    ethread_ = &ethread;
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(cleaner_reschedule_interval_us_);
    if (OB_ISNULL(pending_action_ = ethread.schedule_in(this, HRTIME_USECONDS(interval_us)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("fail to schedule in", K(interval_us), K(ret));
    }
  }
  return ret;
}
} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
