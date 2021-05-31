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
#include "obutils/ob_congestion_manager.h"
#include "obutils/ob_resource_pool_processor.h"
#include "proxy/route/ob_cache_cleaner.h"
#include "proxy/route/ob_table_cache.h"
#include "proxy/route/ob_partition_cache.h"
#include "proxy/route/ob_routine_cache.h"
#include "proxy/route/ob_sql_table_cache.h"
#include "proxy/mysql/ob_mysql_client_session.h"
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

int64_t ObCountRange::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(start_idx),
       K_(end_idx),
       K_(cur_idx));
  J_OBJ_END();
  return pos;
}

ObCacheCleaner::ObCacheCleaner()
    : ObContinuation(), is_inited_(false), triggered_(false), cleaner_reschedule_interval_us_(0),
      total_cleaner_count_(0), this_cleaner_idx_(), next_action_(IDLE_CLEAN_ACTION),
      ethread_(NULL), table_cache_(NULL), partition_cache_(NULL), routine_cache_(NULL), sql_table_cache_(NULL),
      table_cache_range_(), partition_cache_range_(), routine_cache_range_(), sql_table_cache_range_(),
      tc_part_clean_count_(0), table_cache_deleted_cr_version_(),
      partition_cache_deleted_cr_version_(), routine_cache_deleted_cr_version_(), sql_table_cache_deleted_cr_version_(),
      table_cache_last_expire_time_us_(0), partition_cache_last_expire_time_us_(0),
      routine_cache_last_expire_time_us_(0), sql_table_cache_last_expire_time_us_(0), pending_action_(NULL)
{
  SET_HANDLER(&ObCacheCleaner::main_handler);
}

int ObCacheCleaner::init(ObTableCache &table_cache, ObPartitionCache &partition_cache,
                         ObRoutineCache &routine_cache, ObSqlTableCache &sql_table_cache,
                         const ObCountRange &range, const int64_t total_count,
                         const int64_t idx, const int64_t clean_interval_us)
{
  int ret = OB_SUCCESS;
  ObProxyMutex *mutex = NULL;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K_(is_inited), K(ret));
  } else if (!range.is_valid() || total_count <= 0 || idx >= total_count || clean_interval_us <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(range), K(total_count), K(idx), K(clean_interval_us), K(ret));
  } else if (OB_FAIL(deleting_cr_list_.init("cr deleting list",
          reinterpret_cast<int64_t>(&(reinterpret_cast<ObResourceDeleteActor *>(0))->link_)))) {
    LOG_WARN("fail to init deleting cr list", K(ret));
  } else if (OB_ISNULL(mutex = new_proxy_mutex(CACHE_CLEANER_LOCK))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate mutex", K(ret));
  } else {
    is_inited_ = true;
    mutex_ = mutex;
    next_action_ = CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION;
    table_cache_ = &table_cache;
    partition_cache_ = &partition_cache;
    routine_cache_ = &routine_cache;
    sql_table_cache_ = &sql_table_cache;
    // table cache and partition cache have the same sub partitions
    table_cache_range_ = range;
    partition_cache_range_ = range;
    routine_cache_range_ = range;
    sql_table_cache_range_ = range;
    tc_part_clean_count_ = 0;
    pending_action_ = NULL;
    total_cleaner_count_ = total_count;
    this_cleaner_idx_ = idx;
    table_cache_last_expire_time_us_ = table_cache.get_cache_expire_time_us();
    partition_cache_last_expire_time_us_ = partition_cache.get_cache_expire_time_us();
    routine_cache_last_expire_time_us_ = routine_cache.get_cache_expire_time_us();
    sql_table_cache_last_expire_time_us_ = sql_table_cache.get_cache_expire_time_us();
    cleaner_reschedule_interval_us_ = clean_interval_us;
  }
  return ret;
}

int ObCacheCleaner::main_handler(int event, void *data)
{
  int ret = OB_SUCCESS;
  int event_ret = EVENT_CONT;
  UNUSED(event);
  UNUSED(data);

  LOG_DEBUG("[ObCacheCleaner::main_handler]", K(event), K(data), K_(triggered), K(*this));

  switch (event) {
    case EVENT_IMMEDIATE: {
      if (OB_FAIL(cancel_pending_action())) {
        LOG_WARN("fail to cancel pending_action", K(ret));
      } else {
        LOG_INFO("cleaner clean interval has changed", "current interval",
                 cleaner_reschedule_interval_us_);
        if (OB_FAIL(schedule_in(cleaner_reschedule_interval_us_))) {
          LOG_ERROR("fail to schedule cleaner", K(ret));
        }
      }
      break;
    }
    case CLEANER_TRIGGER_EVENT: {
      if (OB_FAIL(cancel_pending_action())) {
        LOG_WARN("fail to cancel pending_action", K(ret));
      } else if (OB_FAIL(cleanup())) {
        LOG_WARN("fail to cleanup", K(ret));
      }
      break;
    }
    case EVENT_INTERVAL: {
      pending_action_ = NULL;
      if (OB_FAIL(cleanup())) {
        LOG_WARN("fail to cleanup", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected event", K(event), K(ret));
      break;
    }
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("ObCacheCleaner will exit, something error",
              "this_thread", this_ethread(), K(ret));
    event_ret = EVENT_DONE;
    self_ethread().cache_cleaner_ = NULL;
    delete this;
  }

  return event_ret;
}

int ObCacheCleaner::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_clean_job())) {
    LOG_WARN("fail to do clean job", K(ret));
  } else {
    triggered_ = false;
  }

  // some error unexpected, just clear some state and retry
  if (OB_FAIL(ret)) {
    LOG_ERROR("some internal error, just clear state and retry", K(ret));
    next_action_ = CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION;
    table_cache_range_.again();
    tc_part_clean_count_ = 0;
    if (NULL != pending_action_) {
      pending_action_->cancel();
      pending_action_ = NULL;
    }

    ret = OB_SUCCESS;
    // schedule next clean action
    if (OB_FAIL(schedule_in(cleaner_reschedule_interval_us_))) {
      LOG_ERROR("fail to schedule next clean action, cache cleaner will stop working", K(ret));
    }
  }

  return ret;
}

int ObCacheCleaner::do_clean_job()
{
  LOG_DEBUG("[ObCacheCleaner::do_clean_job]", K(*this));
  int ret = OB_SUCCESS;

  bool stop = false;
  while (!stop) {
    LOG_DEBUG("ObCacheCleaner will do ", "action", get_cleaner_action_name(next_action_));
    switch (next_action_) {
      case IDLE_CLEAN_ACTION: {
        // begin to work
        next_action_ =  CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION;
        break;
      }

      case CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION: {
        ObCongestionRefHashMap &cgt_map = self_ethread().get_cgt_map();
        if (OB_FAIL(cgt_map.clean_hash_map())) {
          LOG_WARN("fail to clean cgt hash map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_CLUSTER_RESOURCE_ACTION;
        break;
      }

      case CLEAN_CLUSTER_RESOURCE_ACTION: {
        clean_cluster_resource();
        next_action_ = EXPIRE_TABLE_ENTRY_ACTION;
        break;
      }

      case EXPIRE_TABLE_ENTRY_ACTION: {
        if (OB_FAIL(do_expire_table_entry())) {
          LOG_WARN("fail to do expire table entry", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_TABLE_CACHE_ACTION;
        break;
      }

      case CLEAN_TABLE_CACHE_ACTION: {
        bool need_try_lock = false;
        if (OB_FAIL(clean_table_cache(need_try_lock))) {
          LOG_WARN("fail to clean table cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        if (need_try_lock) {
          // reschedule try lock, next_action_ remain unchanged
          stop = true;
        } else {
          next_action_ = CLEAN_THREAD_CACHE_TABLE_ENTRY_ACTION;
        }
        break;
      }

      case CLEAN_THREAD_CACHE_TABLE_ENTRY_ACTION: {
        ObTableRefHashMap &table_map = self_ethread().get_table_map();
        if (OB_FAIL(table_map.clean_hash_map())) {
          LOG_WARN("fail to clean table hash map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = EXPIRE_PARTITION_ENTRY_ACTION;
        break;
      }

      case EXPIRE_PARTITION_ENTRY_ACTION: {
        if (OB_FAIL(do_expire_partition_entry())) {
          LOG_WARN("fail to do partition entry", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_PARTITION_CACHE_ACTION;
        break;
      }

     case CLEAN_PARTITION_CACHE_ACTION: {
        if (OB_FAIL(clean_partition_cache())) {
          LOG_WARN("fail to clean partition cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_THREAD_CACHE_PARTITION_ENTRY_ACTION;
        break;
      }

      case CLEAN_THREAD_CACHE_PARTITION_ENTRY_ACTION: {
        ObPartitionRefHashMap &partition_map = self_ethread().get_partition_map();
        if (OB_FAIL(partition_map.clean_hash_map())) {
          LOG_WARN("fail to clean partition hash map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = EXPIRE_ROUTINE_ENTRY_ACTION;
        break;
      }

      case EXPIRE_ROUTINE_ENTRY_ACTION: {
        if (OB_FAIL(do_expire_routine_entry())) {
          LOG_WARN("fail to do routine entry", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_ROUTINE_CACHE_ACTION;
        break;
      }
      case CLEAN_ROUTINE_CACHE_ACTION: {
        if (OB_FAIL(clean_routine_cache())) {
          LOG_WARN("fail to clean routine cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_THREAD_CACHE_ROUTINE_ENTRY_ACTION;
        break;
      }
      case CLEAN_THREAD_CACHE_ROUTINE_ENTRY_ACTION: {
        ObRoutineRefHashMap &routine_map = self_ethread().get_routine_map();
        if (OB_FAIL(routine_map.clean_hash_map())) {
          LOG_WARN("fail to clean routine hash map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = EXPIRE_SQL_TABLE_ENTRY_ACTION; // from the beginning
        stop = true;
        break;
      }
      case EXPIRE_SQL_TABLE_ENTRY_ACTION: {
        if (OB_FAIL(do_expire_sql_table_entry())) {
          LOG_WARN("fail to do sql table entry", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_SQL_TABLE_CACHE_ACTION;
        break;
      }
      case CLEAN_SQL_TABLE_CACHE_ACTION: {
        if (OB_FAIL(clean_sql_table_cache())) {
          LOG_WARN("fail to clean sql_table cache", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = CLEAN_THREAD_CACHE_SQL_TABLE_ENTRY_ACTION;
        break;
      }
      case CLEAN_THREAD_CACHE_SQL_TABLE_ENTRY_ACTION: {
        ObSqlTableRefHashMap &sql_table_map = self_ethread().get_sql_table_map();
        if (OB_FAIL(sql_table_map.clean_hash_map())) {
          LOG_WARN("fail to clean sql table hash map", K(ret));
          ret = OB_SUCCESS; // continue
        }
        next_action_ = IDLE_CLEAN_ACTION; // from the beginning
        stop = true;
        break;
      }
      default: {
        LOG_ERROR("never reach here", "action", get_cleaner_action_name(next_action_));
        stop = true;
        next_action_ = IDLE_CLEAN_ACTION; // from the beginning
      }
    }
  }

  if (OB_FAIL(schedule_in(cleaner_reschedule_interval_us_))) {
    LOG_ERROR("fail to schedule in cache cleaner", K_(cleaner_reschedule_interval_us), K(ret));
  }

  return ret;
}

void ObCacheCleaner::clean_cluster_resource()
{
  int ret = OB_SUCCESS;
  // only the first clean thread do the exipre work
  if (need_expire_cluster_resource()) {
    // expire cluster resource
    if (OB_FAIL(do_expire_cluster_resource())) {
      LOG_WARN("fail to expire cluster resource", K(ret));
      ret = OB_SUCCESS;
    }
  }

  // delete cluster resource
  if (OB_FAIL(do_delete_cluster_resource())) {
    LOG_WARN("fail to delete cluster resource", K(ret));
    ret = OB_SUCCESS;
  }
}

int64_t ObCacheCleaner::calc_table_entry_clean_count()
{
  int64_t clean_count = 0; // the count of entry every mt partition should clean;
  int64_t mt_part_count_for_clean = table_cache_range_.count();
  if (mt_part_count_for_clean > 0) {
    ObProxyConfig &config = get_global_proxy_config();
    int64_t mt_part_num = table_cache_->get_sub_part_count();
    int64_t mem_limited = ((((config.routing_cache_mem_limited) / 1) / mt_part_num) * mt_part_count_for_clean);
    int64_t mem_limited_threshold = ((mem_limited * 3) / 4);

    int64_t total_entry_count = 0;
    for (int64_t i = table_cache_range_.start_idx_; i <= table_cache_range_.end_idx_; ++i) {
      total_entry_count += table_cache_->get_part_cur_size(i);
    }
    int64_t mem_used = (total_entry_count * AVG_TABLE_ENTRY_SIZE);

    if (mem_used > mem_limited_threshold) {
      clean_count = (((mem_used - mem_limited_threshold) / AVG_TABLE_ENTRY_SIZE) / mt_part_count_for_clean);
    }

    if (clean_count > 0) {
      LOG_INFO("after calc_table_entry_clean_count", K(clean_count), K(mem_used),
               K(mem_limited_threshold), K(mem_limited), K(total_entry_count),
               K(mt_part_count_for_clean), K(mt_part_num));
    } else {
      LOG_DEBUG("after calc_table_entry_clean_count", K(clean_count), K(mem_used),
                K(mem_limited_threshold), K(mem_limited), K(total_entry_count),
                K(mt_part_count_for_clean), K(mt_part_num));
    }
  }

  return clean_count;
}

int ObCacheCleaner::clean_table_cache(bool &need_try_lock)
{
  int ret = OB_SUCCESS;
  ObCountRange &range = table_cache_range_;
  int64_t mt_part_count_for_clean = range.count();

  if (mt_part_count_for_clean > 0) {
    if (tc_part_clean_count_ <= 0) {
      tc_part_clean_count_ = calc_table_entry_clean_count();
    }

    if ((tc_part_clean_count_ > 0) && range.has_remain()) {
      for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
        if (OB_FAIL(clean_one_part_table_cache(i))) {
          if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT != ret) {
            LOG_WARN("fail to clean one part table cache", K(ret));
          } else {
            LOG_INFO("fail to trylock parittion mutex", "partition_id", i, K(ret));
          }
        } else {
          range.cur_idx_++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (!range.has_remain() // every partition has clean entry complete
          || tc_part_clean_count_ <= 0 // after calc, no need to clean table entry
          ) {
        LOG_DEBUG("clean table cache done", K(range), K_(tc_part_clean_count));
        tc_part_clean_count_ = 0;
        range.again();
      }
    }

    if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
      ret = OB_SUCCESS;
      need_try_lock = true;
    }
  }
  return ret;
}

struct ObTableEntryElem
{
  ObTableEntryElem() : entry_(NULL) {}
  ~ObTableEntryElem() {}

  ObTableEntry *entry_;
};

struct ObTableEntryCmp
{
  bool operator() (const ObTableEntryElem& lhs, const ObTableEntryElem& rhs) const
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


int ObCacheCleaner::clean_one_part_table_cache(const int64_t part_idx)
{
  int ret = OB_SUCCESS;
  int64_t mt_part_num = table_cache_->get_sub_part_count();
  if (part_idx < 0 || part_idx >= mt_part_num) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part idx", K(part_idx), K(mt_part_num), K(ret));
  } else {
    ObTableEntryCmp cmp;
    ObTableEntry *entry = NULL;
    TableIter it;
    ObProxyMutex *bucket_mutex = table_cache_->lock_for_key(part_idx);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      int64_t tmp_clean_count = std::max(tc_part_clean_count_, 2L);
      int64_t buf_len = sizeof(ObTableEntryElem) * tmp_clean_count;
      char *buf = static_cast<char *>(op_fixed_mem_alloc(buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(buf_len), K(ret));
      } else if (OB_FAIL(table_cache_->run_todo_list(part_idx))) {
        LOG_WARN("fail to run todo list", K(part_idx), K(ret));
      } else {
        // 1.make a smallest heap
        ObTableEntryElem *eles = new (buf) ObTableEntryElem[tmp_clean_count];
        ObTableEntryElem tmp_ele;
        int64_t i = 0;
        entry = table_cache_->first_entry(part_idx, it);
        while (NULL != entry) {
          if (entry->is_non_partition_table()) { // only non-partition table affect
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
          }

          entry = table_cache_->next_entry(part_idx, it);
        }

        // 2. calc wash count again for defense
        int64_t part_entry_count = table_cache_->get_part_cur_size(part_idx);
        int64_t orig_clean_count = tmp_clean_count;
        if (part_entry_count <= PART_TABLE_ENTRY_MIN_COUNT) {
          tmp_clean_count = 0;
        } else if ((part_entry_count - tmp_clean_count) <= PART_TABLE_ENTRY_MIN_COUNT) {
          int64_t dcount = part_entry_count - PART_TABLE_ENTRY_MIN_COUNT;
          tmp_clean_count = ((dcount >= 0) ? dcount : 0);
        }
        LOG_INFO("begin to wash table entry partition", "wash_count",
                 tmp_clean_count, K(part_entry_count), K(orig_clean_count), K(part_idx),
                 LITERAL_K(PART_TABLE_ENTRY_MIN_COUNT));

        // 3. remove the LRU entry
        ObTableEntryKey key;
        for (int64_t i = 0; i < tmp_clean_count; ++i) {
          entry = eles[i].entry_;
          PROCESSOR_INCREMENT_DYN_STAT(KICK_OUT_TABLE_ENTRY_FROM_GLOBAL_CACHE);
          // do not revome tenant's dummy entry, and building state table entry
          if (NULL != entry && !entry->is_dummy_entry() && !entry->is_building_state()) {
            LOG_INFO("this table entry will be washed", KPC(entry));
            key.reset();
            entry->get_key(key);
            if (OB_FAIL(table_cache_->remove_table_entry(key))) {
              LOG_WARN("fail to remote table entry", KPC(entry));
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
      ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    }
  }

  return ret;
}

int ObCacheCleaner::clean_partition_cache() {
  int ret = OB_SUCCESS;
  ObCountRange &range = partition_cache_range_;
  int64_t mt_part_count_for_clean = range.count();
  if (mt_part_count_for_clean > 0) {
    ObProxyConfig &config = get_global_proxy_config();
    int64_t mt_part_num = partition_cache_->get_sub_part_count();
    int64_t mem_limited = (((config.routing_cache_mem_limited) / 1) / mt_part_num);
    int64_t mem_limited_threshold = ((mem_limited * 3) / 4);
    int64_t max_sub_bucket_count = mem_limited_threshold / AVG_PARTITION_ENTRY_SIZE;

    if (max_sub_bucket_count > 0) {
      // get max count for every sub bucket
      for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
        int64_t entry_count = partition_cache_->get_part_cur_size(i);
        int64_t clean_count = entry_count - max_sub_bucket_count;
        LOG_DEBUG("after calc", "sub bucket idx", i, K(clean_count), K(entry_count),
                  K(max_sub_bucket_count));
        if (clean_count > 0)  {
          LOG_INFO("will clean partition cache", "sub bucket idx", i, K(clean_count),
                   K(max_sub_bucket_count), K(entry_count), K(mem_limited));
          if (OB_FAIL(clean_one_sub_bucket_partition_cache(i, clean_count))) {
            LOG_WARN("fail to clean sub bucket partition cache", "sub bucket idx", i,
                     K(clean_count), K(max_sub_bucket_count), K(mem_limited));
            ret = OB_SUCCESS; // ignore, and coutine
          }
        }
      }
    }
  }
  return ret;
}

struct ObPartitionEntryElem
{
  ObPartitionEntryElem() : entry_(NULL) {}
  ~ObPartitionEntryElem() {}

  ObPartitionEntry *entry_;
};

struct ObPartitionEntryCmp
{
  bool operator() (const ObPartitionEntryElem& lhs, const ObPartitionEntryElem& rhs) const
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

int ObCacheCleaner::clean_one_sub_bucket_partition_cache(const int64_t bucket_idx,
                                                         const int64_t clean_count)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = partition_cache_->get_sub_part_count();
  if ((bucket_idx < 0) || (bucket_idx >= bucket_num) || (clean_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(bucket_idx), K(clean_count), K(bucket_num), K(ret));
  } else {
    ObPartitionEntryCmp cmp;
    ObPartitionEntry *entry = NULL;
    PartitionIter it;
    ObProxyMutex *bucket_mutex = partition_cache_->lock_for_key(bucket_idx);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      int64_t tmp_clean_count = std::max(clean_count, 2L);
      int64_t buf_len = sizeof(ObPartitionEntryElem) * tmp_clean_count;
      char *buf = static_cast<char *>(op_fixed_mem_alloc(buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(buf_len), K(ret));
      } else if (OB_FAIL(partition_cache_->run_todo_list(bucket_idx))) {
        LOG_WARN("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        // 1.make a smallest heap
        ObPartitionEntryElem *eles = new (buf) ObPartitionEntryElem[tmp_clean_count];
        ObPartitionEntryElem tmp_ele;
        int64_t i = 0;
        entry = partition_cache_->first_entry(bucket_idx, it);
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

          entry = partition_cache_->next_entry(bucket_idx, it);
        }

        // 2. calc wash count again for defense
        int64_t part_entry_count = partition_cache_->get_part_cur_size(bucket_idx);
        int64_t orig_clean_count = tmp_clean_count;
        if (part_entry_count <= PART_PARTITION_ENTRY_MIN_COUNT) {
          tmp_clean_count = 0; // don't clean
        } else if ((part_entry_count - tmp_clean_count) <= PART_PARTITION_ENTRY_MIN_COUNT) {
          int64_t dcount = part_entry_count - PART_PARTITION_ENTRY_MIN_COUNT;
          tmp_clean_count = ((dcount >= 0) ? dcount : 0);
        }
        LOG_INFO("begin to wash partition entry", "wash_count",
                 tmp_clean_count, K(part_entry_count), K(orig_clean_count), K(bucket_idx),
                 LITERAL_K(PART_PARTITION_ENTRY_MIN_COUNT));

        // 3. remove the LRU entry
        ObPartitionEntryKey key;
        for (int64_t i = 0; (i < tmp_clean_count) && OB_SUCC(ret); ++i) {
          entry = eles[i].entry_;
          // do not remove building state partition entry
          if (NULL != entry && !entry->is_building_state()) {
            PROCESSOR_INCREMENT_DYN_STAT(KICK_OUT_PARTITION_ENTRY_FROM_GLOBAL_CACHE);
            LOG_INFO("this partition entry will be washed", KPC(entry));
            key.reset();
            key = entry->get_key();
            if (OB_FAIL(partition_cache_->remove_partition_entry(key))) {
              LOG_WARN("fail to remove partition entry", KPC(entry), K(ret));
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


int ObCacheCleaner::schedule_in(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (timeout_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(timeout_us), K(ret));
  } else {
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(timeout_us);
    if (OB_ISNULL(pending_action_ = self_ethread().schedule_in(this, HRTIME_USECONDS(interval_us)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule in", K(interval_us), K(ret));
    }
  }

  return ret;
}

const char *ObCacheCleaner::get_cleaner_action_name(const ObCleanAction action) const
{
  const char *name = NULL;
  switch (action) {
    case CLEAN_TABLE_CACHE_ACTION:
      name = "CLEAN_TABLE_CACHE_ACTION";
      break;
    case CLEAN_PARTITION_CACHE_ACTION:
      name = "CLEAN_PARTITION_CACHE_ACTION";
      break;
    case EXPIRE_PARTITION_ENTRY_ACTION:
      name = "EXPIRE_PARTITION_ENTRY_ACTION";
      break;
    case CLEAN_THREAD_CACHE_PARTITION_ENTRY_ACTION:
      name = "CLEAN_THREAD_CACHE_PARTITION_ENTRY_ACTION";
      break;
    case CLEAN_THREAD_CACHE_TABLE_ENTRY_ACTION:
      name = "CLEAN_THREAD_CACHE_TABLE_ENTRY_ACTION";
      break;
    case CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION:
      name = "CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION";
      break;
    case CLEAN_CLUSTER_RESOURCE_ACTION:
      name = "CLEAN_CLUSTER_RESOURCE_ACTION";
      break;
    case EXPIRE_TABLE_ENTRY_ACTION:
      name = "EXPIRE_TABLE_ENTRY_ACTION";
      break;
    case IDLE_CLEAN_ACTION:
      name = "IDLE_CLEAN_ACTION";
      break;
    case CLEAN_ROUTINE_CACHE_ACTION:
      name = "CLEAN_ROUTINE_CACHE_ACTION";
      break;
    case EXPIRE_ROUTINE_ENTRY_ACTION:
      name = "EXPIRE_ROUTINE_ENTRY_ACTION";
      break;
    case CLEAN_THREAD_CACHE_ROUTINE_ENTRY_ACTION:
      name = "CLEAN_THREAD_CACHE_ROUTINE_ENTRY_ACTION";
      break;

    default:
      name = "CLIENT_ACTION_UNKNOWN";
      break;
  }
  return name;
}

int64_t ObCacheCleaner::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited),
       "next action", get_cleaner_action_name(next_action_),
       K_(table_cache_range),
       K_(partition_cache_range),
       K_(routine_cache_range),
       K_(tc_part_clean_count),
       K_(table_cache_last_expire_time_us),
       K_(partition_cache_last_expire_time_us),
       K_(routine_cache_last_expire_time_us),
       K_(sql_table_cache_last_expire_time_us),
       KP_(pending_action));
  J_OBJ_END();
  return pos;
}

int ObCacheCleaner::schedule_cache_cleaner()
{
  int ret = OB_SUCCESS;

  ObEventThreadType etype = ET_NET;
  int64_t net_thread_count = g_event_processor.thread_count_for_type_[etype];
  ObEThread **netthreads = g_event_processor.event_thread_[etype];
  ObEThread *target_ethread = NULL;
  int64_t clean_interval = get_global_proxy_config().cache_cleaner_clean_interval;

  ObCountRange range;
  ObCacheCleaner *cleaner = NULL;
  ObTableCache &table_cache = get_global_table_cache();
  ObPartitionCache &partition_cache = get_global_partition_cache();
  ObRoutineCache &routine_cache = get_global_routine_cache();
  ObSqlTableCache &sql_table_cache = get_global_sql_table_cache();

  const int64_t mt_part_num = MT_HASHTABLE_PARTITIONS;
  if (net_thread_count > 0 && mt_part_num > 0) {
    int64_t part_num_per_thread = mt_part_num / net_thread_count;
    int64_t remain_num = mt_part_num % net_thread_count;
    for (int64_t i = 0; i < net_thread_count; ++i) {
      // calc range
      range.reset();

      if (part_num_per_thread > 0) { // thread_num <= mt_part_num
        if (i < remain_num) {
          range.start_idx_ = (part_num_per_thread + 1) * i;
          range.end_idx_ = ((part_num_per_thread + 1) * (i + 1) - 1);
        } else {
          int64_t base = (part_num_per_thread + 1) * remain_num;
          range.start_idx_ = base + (part_num_per_thread) * (i - remain_num);
          range.end_idx_ = base + ((part_num_per_thread) * (i - remain_num + 1) - 1);
        }
      } else if (0 == part_num_per_thread) { // thread_num > mt_part_num
        if (i < mt_part_num) {
          range.start_idx_ = i;
          range.end_idx_ = i;
        }
      }

      range.again();
      if (OB_ISNULL(cleaner = new (std::nothrow) ObCacheCleaner())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc cleaner", K(ret));
      } else if (OB_FAIL(cleaner->init(table_cache, partition_cache, routine_cache, sql_table_cache, range,
              net_thread_count, i, clean_interval))) {
        LOG_WARN("fail to init cleaner", K(range), K(ret));
      } else if (OB_ISNULL(target_ethread = netthreads[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ethread can not be NULL", K(target_ethread), K(ret));
      } else if (OB_FAIL(cleaner->start_clean_cache(*target_ethread))) {
        LOG_WARN("fail to start clean cache", K(ret));
      } else {
        target_ethread->cache_cleaner_ = cleaner;
        LOG_INFO("succ schedule cache cleaners", K(target_ethread), K(range), K(i),
                 K(part_num_per_thread), K(remain_num), K(net_thread_count), K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("thread_num or mt_part_num can not be NULL", K(mt_part_num), K(net_thread_count), K(ret));
  }

  return ret;
}

struct ObClientSessionCloseHandler
{
  ObClientSessionCloseHandler() : cs_(NULL), force_close_(false) {}
  ~ObClientSessionCloseHandler() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;

  ObMysqlClientSession *cs_;
  bool force_close_;
};

int64_t ObClientSessionCloseHandler::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(cs),
       K_(force_close));
  J_OBJ_END();
  return pos;
}

int ObCacheCleaner::do_delete_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObResourceDeleteActor *cr_actors = reinterpret_cast<ObResourceDeleteActor *>(deleting_cr_list_.popall());
  ObClusterResource *cr = NULL;
  ObResourceDeleteActor *cr_actor = NULL;
  int64_t hold_cr_count = 0;
  ObSEArray<ObClientSessionCloseHandler, 64> cs_handlers;
  ObMysqlClientSessionMap::IDHashMap::iterator last;
  ObMysqlClientSessionMap *map = NULL;
  ObClientSessionCloseHandler cs_handler;

  while (NULL != cr_actors) {
    hold_cr_count = 0;
    cr_actor = cr_actors;
    cr_actors = cr_actors->link_.next_;
    cr_actor->link_.next_ = NULL;
    cr = cr_actor->cr_;

    map = &get_client_session_map(self_ethread());
    last = map->id_map_.end();
    for (ObMysqlClientSessionMap::IDHashMap::iterator cr_iter = map->id_map_.begin();
            (cr_iter != last) && OB_SUCC(ret); ++cr_iter) {
      // here we just read cs, no need try lock it
      if (cr == cr_iter->cluster_resource_) {
        ++hold_cr_count;
        cs_handler.cs_ = &(*cr_iter);
        cs_handler.force_close_ = (cr_actor->retry_count_ >= MAX_COLSE_CLIENT_SESSION_RETYR_TIME);
        if (cr->is_deleting() && OB_DEFAULT_CLUSTER_ID == cr->get_cluster_id()) {
          cs_handler.force_close_ = true;
        }
        if (OB_FAIL(cs_handlers.push_back(cs_handler))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }

    if (hold_cr_count > 0) {
      LOG_DEBUG("hold cluster resource count", K(hold_cr_count),
                "cluster_name", cr->get_cluster_name(),
                "cluster_id", cr->get_cluster_id());
      ++cr_actor->retry_count_;
      deleting_cr_list_.push(cr_actor); // push to list again
    } else {
      if (OB_FAIL(table_cache_deleted_cr_version_.push_back(cr->version_))) {
        LOG_WARN("fail to push back table cache cr version", K(cr), KPC(cr), K(ret));
        ret = OB_SUCCESS; // continue
      }
      if (OB_FAIL(partition_cache_deleted_cr_version_.push_back(cr->version_))) {
        LOG_WARN("fail to push back partition cache cr version", K(cr), KPC(cr), K(ret));
        ret = OB_SUCCESS; // continue
      }
      if (OB_FAIL(routine_cache_deleted_cr_version_.push_back(cr->version_))) {
        LOG_WARN("fail to push back routine cache cr version", K(cr), KPC(cr), K(ret));
        ret = OB_SUCCESS; // continue
      }
      if (OB_FAIL(sql_table_cache_deleted_cr_version_.push_back(cr->version_))) {
        LOG_WARN("fail to push back sql table cache cr version", K(cr), KPC(cr), K(ret));
        ret = OB_SUCCESS; // continue
      }
      // this thread has clean complete
      if (cr->inc_and_test_deleting_complete()) {
        // all work thread has clean complete
        LOG_INFO("this cluster resource has been clean complete, and will destroy",
                 "cluster_name", cr->get_cluster_name(), K(cr));
        cr->destroy();
      }
      LOG_INFO("this thread has clean cluster resource complete",
               "thread_id", gettid(), KPC(cr));
      cr_actor->free(); // will dec cr ref
      cr_actor = NULL;
      cr = NULL;
    }
  }

  int64_t count = cs_handlers.count();
  ObMysqlClientSession *cs = NULL;
  bool force_close = false;
  for (int64_t i = 0; i < count; ++i) {
    cs = cs_handlers.at(i).cs_;
    force_close = cs_handlers.at(i).force_close_;
    MUTEX_TRY_LOCK(lock, cs->mutex_, mutex_->thread_holding_);
    if (lock.is_locked()) {
      if (!cs->is_in_trans() || force_close) {
        LOG_INFO("because of deleting cluster resource, this client session"
                 " will timeout soon", K(force_close), KPC(cs), K(cs));
        cs->set_inactivity_timeout(1); // 1ns, timeout imm
      } else {
        LOG_DEBUG("this client session is in trans, can not close", K(cs), KPC(cs));
      }
    } else {
      LOG_DEBUG("fail to lock this client session", K(cs), KPC(cs));
      // lock failed, do nothing and wait next round
    }
  }

  return ret;
}

int ObCacheCleaner::push_deleting_cr(ObResourceDeleteActor *actor)
{
  int ret = OB_SUCCESS;
  if (NULL != actor && actor->is_valid()) {
    deleting_cr_list_.push(actor);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cr actor", K(actor), K(ret));
  }
  return ret;
}

int ObCacheCleaner::do_expire_cluster_resource()
{
  int ret = OB_SUCCESS;
  ObResourcePoolProcessor &rpp = get_global_resource_pool_processor();
  if (OB_FAIL(rpp.expire_cluster_resource())) {
    LOG_WARN("fail to expire cluster reousrce", K(ret));
  }
  return ret;
}

int ObCacheCleaner::do_expire_table_entry()
{
  int ret = OB_SUCCESS;
  if (!table_cache_deleted_cr_version_.empty() || is_table_cache_expire_time_changed()) {
    ObProxyMutex *bucket_mutex = NULL;
    bool all_locked = true;
    if (table_cache_range_.count() > 0) {
      // every bucket
      for (int64_t i = table_cache_range_.start_idx_; i <= table_cache_range_.end_idx_; ++i) {
        bucket_mutex = table_cache_->lock_for_key(i);
        MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
        if (lock.is_locked()) {
          table_cache_->gc(i);
        } else {
          all_locked = false;
        }
      }
    }

    // if all buckets are locked at once, we will reset table_cache_deleted_cr_version_
    if (all_locked) {
      table_cache_deleted_cr_version_.reset();
      table_cache_last_expire_time_us_ = table_cache_->get_cache_expire_time_us();
    }
  }

  return ret;
}

int ObCacheCleaner::do_expire_partition_entry()
{
  int ret = OB_SUCCESS;
  if (!partition_cache_deleted_cr_version_.empty() || is_partition_cache_expire_time_changed()) {
    ObProxyMutex *bucket_mutex = NULL;
    bool all_locked = true;
    if (partition_cache_range_.count() > 0) {
      // every bucket
      for (int64_t i = partition_cache_range_.start_idx_; i <= partition_cache_range_.end_idx_; ++i) {
        bucket_mutex = partition_cache_->lock_for_key(i);
        MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
        if (lock.is_locked()) {
          partition_cache_->gc(i);
        } else {
          all_locked = false;
        }
      }
    }

    // if all buckets are locked at once, we will reset partition_cache_deleted_cr_version_
    if (all_locked) {
      partition_cache_deleted_cr_version_.reset();
      partition_cache_last_expire_time_us_ = partition_cache_->get_cache_expire_time_us();
    }
  }

  return ret;
}

bool ObCacheCleaner::is_table_entry_expired(ObTableEntry &entry)
{
  bool expired = false;
  if (table_cache_->is_table_entry_expired(entry)) {
    expired = true;
  }
  if (!expired) {
    // must acquire CacheCleaner' mustex, or deleted_cr_verison_ maybe multi read and write
    MUTEX_TRY_LOCK(lock, this->mutex_, this_ethread());
    if (lock.is_locked()) {
      for (int64_t i = 0; (i < table_cache_deleted_cr_version_.count()) && !expired; ++i) {
        if (entry.get_cr_version() == table_cache_deleted_cr_version_.at(i)) {
          expired = true;
        }
      }
    }
  }
  return expired;
}

bool ObCacheCleaner::is_partition_entry_expired(ObPartitionEntry &entry)
{
  bool expired = false;
  if (partition_cache_->is_partition_entry_expired(entry)) {
    expired = true;
  }
  if (!expired) {
    // must acquire ObCacheCleaner' mustex, or deleted_cr_verison_ maybe multi read and write
    MUTEX_TRY_LOCK(lock, this->mutex_, this_ethread());
    if (lock.is_locked()) {
      for (int64_t i = 0; (i < partition_cache_deleted_cr_version_.count()) && !expired; ++i) {
        if (entry.get_cr_version() == partition_cache_deleted_cr_version_.at(i)) {
          expired = true;
        }
      }
    }
  }
  return expired;
}

bool ObCacheCleaner::is_routine_entry_expired(ObRoutineEntry &entry)
{
  bool expired = false;
  if (routine_cache_->is_routine_entry_expired(entry)) {
    expired = true;
  }
  if (!expired) {
    // must acquire ObCacheCleaner' mustex, or deleted_cr_verison_ maybe multi read and write
    MUTEX_TRY_LOCK(lock, this->mutex_, this_ethread());
    if (lock.is_locked()) {
      for (int64_t i = 0; (i < routine_cache_deleted_cr_version_.count()) && !expired; ++i) {
        if (entry.get_cr_version() == routine_cache_deleted_cr_version_.at(i)) {
          expired = true;
        }
      }
    }
  }
  return expired;
}

bool ObCacheCleaner::is_sql_table_entry_expired(ObSqlTableEntry &entry)
{
  bool expired = false;
  if (sql_table_cache_->is_sql_table_entry_expired(entry)) {
    expired = true;
  } else {
    // must acquire ObCacheCleaner' mustex, or deleted_cr_verison_ maybe multi read and write
    MUTEX_TRY_LOCK(lock, this->mutex_, this_ethread());
    if (lock.is_locked()) {
      for (int64_t i = 0; (i < sql_table_cache_deleted_cr_version_.count()) && !expired; ++i) {
        if (entry.get_cr_version() == sql_table_cache_deleted_cr_version_.at(i)) {
          expired = true;
        }
      }
    }
  }
  return expired;
}

bool ObCacheCleaner::is_table_cache_expire_time_changed()
{
  return (table_cache_last_expire_time_us_ != table_cache_->get_cache_expire_time_us());
}

bool ObCacheCleaner::is_partition_cache_expire_time_changed()
{
  return (partition_cache_last_expire_time_us_ != partition_cache_->get_cache_expire_time_us());
}

bool ObCacheCleaner::is_routine_cache_expire_time_changed()
{
  return (routine_cache_last_expire_time_us_ != routine_cache_->get_cache_expire_time_us());
}

bool ObCacheCleaner::is_sql_table_cache_expire_time_changed()
{
  return (sql_table_cache_last_expire_time_us_ != sql_table_cache_->get_cache_expire_time_us());
}

int ObCacheCleaner::cancel_pending_action()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(NULL != pending_action_)) {
    if (OB_FAIL(pending_action_->cancel())) {
      LOG_WARN("fail to cancel pending_action", K(ret));
    } else {
      pending_action_ = NULL;
    }
  }
  return ret;
}

int ObCacheCleaner::set_clean_interval(const int64_t interval_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid clean interval", K(interval_us), K(ret));
  } else {
    cleaner_reschedule_interval_us_ = interval_us;
  }
  return ret;
}

int ObCacheCleaner::update_clean_interval()
{
  int ret = OB_SUCCESS;
  int64_t interval_us = get_global_proxy_config().cache_cleaner_clean_interval;
  int64_t thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  ObEThread **threads = g_event_processor.event_thread_[ET_CALL];
  ObCacheCleaner *cleaner = NULL;
  ObEThread *ethread = NULL;
  for (int64_t i = 0; (i < thread_count) && OB_SUCC(ret); ++i) {
    if (OB_ISNULL(ethread = threads[i]) || OB_ISNULL(cleaner = threads[i]->cache_cleaner_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ethread and cache cleaner can not be NULL", K(ethread), K(cleaner), K(ret));
    } else if (OB_FAIL(cleaner->set_clean_interval(interval_us))) {
      LOG_WARN("fail to set clean interval", K(interval_us), K(ret));
    } else if (OB_ISNULL(ethread->schedule_imm(cleaner))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to schedule imm", K(ret));
    }
  }
  return ret;
}

int ObCacheCleaner::trigger()
{
  int ret = OB_SUCCESS;
  if (!triggered_) {
    if (OB_ISNULL(ethread_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ethread can not be NULL", K_(ethread), K(ret));
    } else if (OB_ISNULL(ethread_->schedule_imm(this, CLEANER_TRIGGER_EVENT))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fail to schedule imm", K(ret));
    } else {
      triggered_ = true;
    }
  }
  return ret;
}

int ObCacheCleaner::start_clean_cache(ObEThread &ethread)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(cleaner_reschedule_interval_us_ <= 0 || NULL != pending_action_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("unexpect state", K_(cleaner_reschedule_interval_us), K_(pending_action), K(ret));
  } else {
    ethread_ = &ethread;
    int64_t interval_us = ObRandomNumUtils::get_random_half_to_full(cleaner_reschedule_interval_us_);
    if (OB_ISNULL(pending_action_ = ethread.schedule_in(this, HRTIME_USECONDS(interval_us)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to schedule in", K(interval_us), K(ret));
    }
  }
  return ret;
}

int ObCacheCleaner::do_expire_routine_entry()
{
  int ret = OB_SUCCESS;
  if (!routine_cache_deleted_cr_version_.empty() || is_routine_cache_expire_time_changed()) {
    ObProxyMutex *bucket_mutex = NULL;
    bool all_locked = true;
    if (routine_cache_range_.count() > 0) {
      // every bucket
      for (int64_t i = routine_cache_range_.start_idx_; i <= routine_cache_range_.end_idx_; ++i) {
        bucket_mutex = routine_cache_->lock_for_key(i);
        MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
        if (lock.is_locked()) {
          routine_cache_->gc(i);
        } else {
          all_locked = false;
        }
      }
    }

    // if all buckets are locked at once, we will reset routine_cache_deleted_cr_version_
    if (all_locked) {
      routine_cache_deleted_cr_version_.reset();
      routine_cache_last_expire_time_us_ = routine_cache_->get_cache_expire_time_us();
    }
  }

  return ret;
}

int ObCacheCleaner::clean_routine_cache() {
  int ret = OB_SUCCESS;
  ObCountRange &range = routine_cache_range_;
  int64_t mt_part_count_for_clean = range.count();
  if (mt_part_count_for_clean > 0) {
    ObProxyConfig &config = get_global_proxy_config();
    int64_t mt_part_num = routine_cache_->get_sub_part_count();
    int64_t mem_limited = (((config.routing_cache_mem_limited) / 1) / mt_part_num);
    int64_t mem_limited_threshold = ((mem_limited * 3) / 4);
    int64_t max_sub_bucket_count = mem_limited_threshold / AVG_ROUTINE_ENTRY_SIZE;

    if (max_sub_bucket_count > 0) {
      // get max count for every sub bucket
      for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
        int64_t entry_count = routine_cache_->get_part_cur_size(i);
        int64_t clean_count = entry_count - max_sub_bucket_count;
        LOG_DEBUG("after calc", "sub bucket idx", i, K(clean_count), K(entry_count),
                  K(max_sub_bucket_count));
        if (clean_count > 0)  {
          LOG_INFO("will clean routine cache", "sub bucket idx", i, K(clean_count),
                   K(max_sub_bucket_count), K(entry_count), K(mem_limited));
          if (OB_FAIL(clean_one_sub_bucket_routine_cache(i, clean_count))) {
            LOG_WARN("fail to clean sub bucket routine cache", "sub bucket idx", i,
                     K(clean_count), K(max_sub_bucket_count), K(mem_limited));
            ret = OB_SUCCESS; // ignore, and coutine
          }
        }
      }
    }
  }
  return ret;
}


struct ObRoutineEntryElem
{
  ObRoutineEntryElem() : entry_(NULL) {}
  ~ObRoutineEntryElem() {}

  ObRoutineEntry *entry_;
};

struct ObRoutineEntryCmp
{
  bool operator() (const ObRoutineEntryElem& lhs, const ObRoutineEntryElem& rhs) const
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

int ObCacheCleaner::clean_one_sub_bucket_routine_cache(const int64_t bucket_idx,
                                                       const int64_t clean_count)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = routine_cache_->get_sub_part_count();
  if ((bucket_idx < 0) || (bucket_idx >= bucket_num) || (clean_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(bucket_idx), K(clean_count), K(bucket_num), K(ret));
  } else {
    ObRoutineEntryCmp cmp;
    ObRoutineEntry *entry = NULL;
    RoutineIter it;
    ObProxyMutex *bucket_mutex = routine_cache_->lock_for_key(bucket_idx);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      int64_t tmp_clean_count = std::max(clean_count, 2L);
      int64_t buf_len = sizeof(ObRoutineEntryElem) * tmp_clean_count;
      char *buf = static_cast<char *>(op_fixed_mem_alloc(buf_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(buf_len), K(ret));
      } else if (OB_FAIL(routine_cache_->run_todo_list(bucket_idx))) {
        LOG_WARN("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        // 1.make a smallest heap
        ObRoutineEntryElem *eles = new (buf) ObRoutineEntryElem[tmp_clean_count];
        ObRoutineEntryElem tmp_ele;
        int64_t i = 0;
        entry = routine_cache_->first_entry(bucket_idx, it);
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

          entry = routine_cache_->next_entry(bucket_idx, it);
        }

        // 2. calc wash count again for defense
        int64_t part_entry_count = routine_cache_->get_part_cur_size(bucket_idx);
        int64_t orig_clean_count = tmp_clean_count;
        if (part_entry_count <= PART_ROUTINE_ENTRY_MIN_COUNT) {
          tmp_clean_count = 0; // don't clean
        } else if ((part_entry_count - tmp_clean_count) <= PART_ROUTINE_ENTRY_MIN_COUNT) {
          int64_t dcount = part_entry_count - PART_ROUTINE_ENTRY_MIN_COUNT;
          tmp_clean_count = ((dcount >= 0) ? dcount : 0);
        }
        LOG_INFO("begin to wash routine entry", "wash_count",
                 tmp_clean_count, K(part_entry_count), K(orig_clean_count), K(bucket_idx),
                 LITERAL_K(PART_ROUTINE_ENTRY_MIN_COUNT));

        // 3. remove the LRU entry
        ObRoutineEntryKey key;
        for (int64_t i = 0; (i < tmp_clean_count) && OB_SUCC(ret); ++i) {
          entry = eles[i].entry_;
          // do not remove building state routine entry
          if (NULL != entry && !entry->is_building_state()) {
            PROCESSOR_INCREMENT_DYN_STAT(KICK_OUT_ROUTINE_ENTRY_FROM_GLOBAL_CACHE);
            LOG_INFO("this routine entry will be washed", KPC(entry));
            key.reset();
            entry->get_key(key);
            if (OB_FAIL(routine_cache_->remove_routine_entry(key))) {
              LOG_WARN("fail to remove routine entry", KPC(entry), K(ret));
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

int ObCacheCleaner::do_expire_sql_table_entry()
{
  int ret = OB_SUCCESS;
  if (!sql_table_cache_deleted_cr_version_.empty() || is_sql_table_cache_expire_time_changed()) {
    if (sql_table_cache_range_.count() > 0) {
      // every bucket
      for (int64_t i = sql_table_cache_range_.start_idx_; i <= sql_table_cache_range_.end_idx_; ++i) {
        DRWLock &rw_lock = sql_table_cache_->rw_lock_for_key(i);
        DRWLock::WRLockGuard lock(rw_lock);
        sql_table_cache_->gc(i);
      }
    }

    sql_table_cache_deleted_cr_version_.reset();
    sql_table_cache_last_expire_time_us_ = sql_table_cache_->get_cache_expire_time_us();
  }

  return ret;
}

int ObCacheCleaner::clean_sql_table_cache() {
  int ret = OB_SUCCESS;
  ObCountRange &range = sql_table_cache_range_;
  int64_t mt_part_count_for_clean = range.count();
  if (mt_part_count_for_clean > 0) {
    ObProxyConfig &config = get_global_proxy_config();
    int64_t mt_part_num = sql_table_cache_->get_sub_part_count();
    int64_t mem_limited = (((config.sql_table_cache_mem_limited) / 1) / mt_part_num);
    int64_t mem_limited_threshold = ((mem_limited * 3) / 4);
    int64_t max_sub_bucket_count = mem_limited_threshold / AVG_SQL_TABLE_ENTRY_SIZE;

    if (max_sub_bucket_count > 0) {
      // get max count for every sub bucket
      for (int64_t i = range.cur_idx_; (i <= range.end_idx_) && OB_SUCC(ret); ++i) {
        int64_t entry_count = sql_table_cache_->get_part_cur_size(i);
        int64_t clean_count = entry_count - max_sub_bucket_count;
        LOG_DEBUG("after calc", "sub bucket idx", i, K(clean_count), K(entry_count),
                  K(max_sub_bucket_count));
        if (clean_count > 0)  {
          LOG_INFO("will clean sql table cache", "sub bucket idx", i, K(clean_count),
                   K(max_sub_bucket_count), K(entry_count), K(mem_limited));
          if (OB_FAIL(clean_one_sub_bucket_sql_table_cache(i, clean_count))) {
            LOG_WARN("fail to clean sub bucket sql table cache", "sub bucket idx", i,
                     K(clean_count), K(max_sub_bucket_count), K(mem_limited));
            ret = OB_SUCCESS; // ignore, and coutine
          }
        }
      }
    }
  }
  return ret;
}

struct ObSqlTableEntryElem
{
  ObSqlTableEntryElem() : entry_(NULL) {}
  ~ObSqlTableEntryElem() {}

  ObSqlTableEntry *entry_;
};

struct ObSqlTableEntryCmp
{
  bool operator() (const ObSqlTableEntryElem& lhs, const ObSqlTableEntryElem& rhs) const
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

int ObCacheCleaner::clean_one_sub_bucket_sql_table_cache(const int64_t bucket_idx,
                                                         const int64_t clean_count)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = sql_table_cache_->get_sub_part_count();
  if ((bucket_idx < 0) || (bucket_idx >= bucket_num) || (clean_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(bucket_idx), K(clean_count), K(bucket_num), K(ret));
  } else {
    ObSqlTableEntryCmp cmp;
    ObSqlTableEntry *entry = NULL;
    SqlTableIter it;
    int64_t tmp_clean_count = std::max(clean_count, 2L);
    int64_t buf_len = sizeof(ObSqlTableEntryElem) * tmp_clean_count;
    char *buf = static_cast<char *>(op_fixed_mem_alloc(buf_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(buf_len), K(ret));
    } else {
      // 1.make a smallest heap
      ObSqlTableEntryElem *eles = new (buf) ObSqlTableEntryElem[tmp_clean_count];
      ObSqlTableEntryElem tmp_ele;
      int64_t i = 0;
      DRWLock &rw_lock = sql_table_cache_->rw_lock_for_key(bucket_idx);
      {
        DRWLock::RDLockGuard lock(rw_lock);
        entry = sql_table_cache_->first_entry(bucket_idx, it);
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

          entry = sql_table_cache_->next_entry(bucket_idx, it);
        }
      }

      // 2. calc wash count again for defense
      int64_t part_entry_count = sql_table_cache_->get_part_cur_size(bucket_idx);
      int64_t orig_clean_count = tmp_clean_count;
      if (part_entry_count <= PART_SQL_TABLE_ENTRY_MIN_COUNT) {
        tmp_clean_count = 0; // don't clean
      } else if ((part_entry_count - tmp_clean_count) <= PART_SQL_TABLE_ENTRY_MIN_COUNT) {
        int64_t dcount = part_entry_count - PART_SQL_TABLE_ENTRY_MIN_COUNT;
        tmp_clean_count = ((dcount >= 0) ? dcount : 0);
      }
      LOG_INFO("begin to wash sql table entry", "wash_count",
               tmp_clean_count, K(part_entry_count), K(orig_clean_count), K(bucket_idx),
               LITERAL_K(PART_SQL_TABLE_ENTRY_MIN_COUNT));

      // 3. remove the LRU entry
      ObSqlTableEntryKey key;
      for (int64_t i = 0; (i < tmp_clean_count) && OB_SUCC(ret); ++i) {
        entry = eles[i].entry_;
        // do not remove building state routine entry
        if (NULL != entry) {
          LOG_INFO("this sql table entry will be washed", KPC(entry));
          key.reset();
          key = entry->get_key();
          if (OB_FAIL(sql_table_cache_->remove_sql_table_entry(key))) {
            LOG_WARN("fail to remove sql table entry", KPC(entry), K(ret));
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
  }

  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
