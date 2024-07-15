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
#include "proxy/route/ob_table_processor.h"
#include "lib/profile/ob_trace_id.h"
#include "utils/ob_ref_hash_map.h"
#include "stat/ob_processor_stats.h"
#include "proxy/route/ob_route_utils.h"
#include "proxy/client/ob_mysql_proxy.h"
#include "proxy/client/ob_client_vc.h"
#include "obutils/ob_task_flow_controller.h"
#include "obutils/ob_resource_pool_processor.h"
#include "obutils/ob_config_server_processor.h"
#include "proxy/route/ob_table_entry_cont.h"
#include "proxy/route/ob_table_cache.h"
#include "prometheus/ob_route_prometheus.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
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

int ObTableProcessor::init(ObTableCache *table_cache)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K(ret));
  } else if (OB_ISNULL(table_cache)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(table_cache), K(ret));
  } else {
    table_cache_ = table_cache;
    is_inited_ = true;
  }
  return ret;
}

DEF_TO_STRING(ObTableProcessor)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited), KPC_(table_cache));
  J_OBJ_END();
  return pos;
}

int ObTableProcessor::add_table_entry_with_rslist(ObTableRouteParam &table_param,
                                                  ObTableEntry *&entry,
                                                  const bool is_old_entry_from_rslist)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  ObConfigServerProcessor &cs_processor = get_global_config_server_processor();
  ObTableCache &table_cache = get_global_table_cache();
  const bool is_rslist = false;
  bool entry_from_last_dummy =  false;
  //if old dummy entry from rslist, try use last dummy entry
  if (is_old_entry_from_rslist) {
    ObClusterResource *cr = NULL;
    if (OB_ISNULL(cr = get_global_resource_pool_processor().acquire_avail_cluster_resource(
        table_param.name_.cluster_name_, table_param.cr_id_))) {
      LOG_INFO("fail to acuqire avail cluster resource, ignore" ,
               "cluster_name", table_param.name_.cluster_name_,
               "cluster_id", table_param.cr_id_);
    } else {
      obsys::CRLockGuard guard(cr->dummy_entry_rwlock_);
      if (NULL != cr->dummy_entry_) {
        entry = cr->dummy_entry_;
        entry->inc_ref();
        entry_from_last_dummy = true;
      }
      get_global_resource_pool_processor().release_cluster_resource(cr);
      cr = NULL;
    }
  }

  if (!entry_from_last_dummy) {
    ObProxyJsonConfigInfo *json_info = NULL;
    ObProxySubClusterInfo *sub_cluster_info = NULL;
    if (OB_ISNULL(json_info = cs_processor.acquire())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("json info is null", K(ret));
    } else if (OB_FAIL(json_info->get_sub_cluster_info(table_param.name_.cluster_name_, table_param.cr_id_, sub_cluster_info))) {
      LOG_WDIAG("fail to get cluster info", K(ret));
    } else if (OB_ISNULL(sub_cluster_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("cluster info is null", K(ret));
    } else if (OB_FAIL(ObRouteUtils::build_sys_dummy_entry(table_param.name_.cluster_name_,
              table_param.cr_id_,
              sub_cluster_info->web_rs_list_, is_rslist, entry))) {
      LOG_WDIAG("fail to build sys dummy entry", K(table_param), K(ret));
    } else if (OB_ISNULL(entry) || OB_UNLIKELY(!entry->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WDIAG("entry can not be NULL here", K(entry), K(ret));
    }
    cs_processor.release(json_info);
  }

  if (OB_SUCC(ret)) {
    const bool direct_add = false;
    ObTableEntry *tmp_entry = entry;
    tmp_entry->inc_ref();
    if (OB_FAIL(table_cache.add_table_entry(*tmp_entry, direct_add))) {
      LOG_WDIAG("fail to add sys dummy entry", KPC(tmp_entry), K(ret));
    } else {
      LOG_INFO("succ to add sys dummy entry with rslist", K(table_param), KPC(tmp_entry),
               K(entry_from_last_dummy));
    }
    tmp_entry->dec_ref();
    tmp_entry = NULL;
  }

  if (OB_FAIL(ret) && OB_LIKELY(NULL != entry)) {
    entry->dec_ref();
    entry = NULL;
  }
  return ret;
}

inline int ObTableProcessor::get_table_entry_from_rslist(
    ObTableRouteParam &table_param,
    ObTableEntry *&entry,
    ObTableEntryLookupOp &op,
    const bool is_old_entry_from_rslist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_table_entry_with_rslist(table_param, entry, is_old_entry_from_rslist))) {
    LOG_WDIAG("fail to add table entry with rslist", K(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("entry is null", K(ret));
  } else {
    ObProxyMutex *mutex_ = table_param.cont_->mutex_;
    PROCESSOR_INCREMENT_DYN_STAT(GET_PL_BY_RS_LIST_SUCC);
    ROUTE_PROMETHEUS_STAT(table_param.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, false, true);
    entry->inc_ref();
    op = LOOKUP_GLOBAL_CACHE_HIT_OP;
  }
  return ret;
}

int ObTableProcessor::get_table_entry_from_global_cache(
    ObTableRouteParam &table_param,
    ObTableCache &table_cache,
    ObTableEntryCont *te_cont,
    ObAction *&action,
    ObTableEntry *&entry,
    ObTableEntryLookupOp &op)
{
  int ret = OB_SUCCESS;
  op = LOOKUP_MIN_OP;
  ObTableEntry *target_entry = NULL;

  if (table_param.need_fetch_remote()) { // need fetch from remote direct
    op = LOOKUP_REMOTE_DIRECT_OP;
  } else { // fetch from global cache
    ObTableEntryKey key(table_param.name_, table_param.cr_version_, table_param.cr_id_);
    uint64_t hash = key.hash();

    ObProxyMutex *bucket_mutex = table_cache.lock_for_key(hash);
    MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
    if (lock_bucket.is_locked()) {
      if (OB_FAIL(table_cache.run_todo_list(table_cache.part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        target_entry = table_cache.lookup_entry(hash, key);
        bool is_entry_from_rslist = false;
        if (NULL == target_entry) { // find nothing, should alloc building state table entry and fetch from remote
          if (OB_UNLIKELY(table_param.name_.is_sys_dummy())) {
            is_entry_from_rslist = true;
            LOG_WDIAG("sys tenant' all dummy entry is not in global cache, will add one with rslist",
                     K(table_param));
            if (OB_FAIL(get_table_entry_from_rslist(table_param, target_entry, op, is_entry_from_rslist))) {
              LOG_WDIAG("fail to get table entry from rslist", K(ret));
            }
          } else if (OB_FAIL(add_building_state_table_entry(table_param, table_cache, target_entry))) {
            LOG_WDIAG("fail to add building state table entry", K(table_param), K(ret));
          } else if (OB_ISNULL(target_entry)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("target_entry can not be NULL here", K(target_entry), K(ret));
          } else {
            op = LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP;
          }
        } else {
          target_entry->inc_ref();
          if (table_cache.is_table_entry_expired_in_time_mode(*target_entry) && target_entry->is_avail_state()) {
            target_entry->set_dirty_state();
          }
          if (target_entry->is_deleted_state()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_EDIAG("deleting table entry must not be in global cache", KPC(target_entry), K(ret));
          } else if (table_cache.is_table_entry_expired_in_qa_mode(*target_entry)
             || ((!get_global_proxy_config().enable_async_pull_location_cache || get_global_proxy_config().rpc_enable_direct_expire_route_entry)
                 && table_cache.is_table_entry_expired_in_time_mode(*target_entry))) {
            // cr_version or expire time mismatch
            LOG_DEBUG("the table entry in global cache is expired", "expire_time_us",
                      table_cache.get_cache_expire_time_us(), KPC(target_entry), K(table_param));
            is_entry_from_rslist = target_entry->is_entry_from_rslist();
            target_entry->dec_ref();
            target_entry = NULL;
            // remove this expired entry from table cache
            if (OB_FAIL(table_cache.remove_table_entry(key))) {
              LOG_WDIAG("fail to remove table entry", K(key), K(ret));
            } else if (OB_UNLIKELY(table_param.name_.is_sys_dummy())) {
              // if sys dummy entry, just use rslist
              LOG_WDIAG("sys tenant' all dummy entry is expired or version does not match, "
                       "will add one with rslist", K(is_entry_from_rslist), K(table_param));
              if (OB_FAIL(get_table_entry_from_rslist(table_param, target_entry, op, is_entry_from_rslist))) {
                LOG_WDIAG("fail to get table entry from rslist", K(ret));
              }
            } else if (OB_FAIL(add_building_state_table_entry(table_param, table_cache, target_entry))) {
              LOG_WDIAG("fail to add building state table entry", K(table_param), K(ret));
            } else if (OB_ISNULL(target_entry)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("target_entry can not be NULL here", K(target_entry), K(ret));
            } else {
              op = LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP;
            }
          } else if (target_entry->is_need_update()) {
            if (OB_UNLIKELY(table_param.name_.is_sys_dummy())) {
              is_entry_from_rslist = target_entry->is_entry_from_rslist();
              LOG_WDIAG("sys_dummy_entry can not be updating state, will add one with rslist", K(is_entry_from_rslist), K(table_param));
              target_entry->dec_ref();
              target_entry = NULL;
              if (OB_FAIL(get_table_entry_from_rslist(table_param, target_entry, op, is_entry_from_rslist))) {
                LOG_WDIAG("fail to get table entry from rslist", K(ret));
              }
              // double check
            } else if (target_entry->cas_compare_and_swap_state(ObTableEntry::DIRTY, ObTableEntry::UPDATING)) {
              if (get_pl_task_flow_controller().can_deliver_task()) {
                op = LOOKUP_REMOTE_FOR_UPDATE_OP;
              } else {
                LOG_INFO("pl update can not deliver as rate limited, set back to dirty",
                         "flow controller info", get_pl_task_flow_controller());
                // set back to diry state
                target_entry->set_dirty_state();
                op = LOOKUP_GLOBAL_CACHE_HIT_OP;
              }
            } else {
              op = LOOKUP_GLOBAL_CACHE_HIT_OP;
            }

            if (get_global_proxy_config().enable_async_pull_location_cache) {
              if (OB_ISNULL(table_param.result_.target_old_entry_)
                  && !target_entry->is_dummy_entry()) {
                target_entry->inc_ref();
                table_param.result_.target_old_entry_ = target_entry;
              }
            }
          } else if (target_entry->is_building_state()) {
            if (OB_UNLIKELY(table_param.name_.is_sys_dummy())) {
              is_entry_from_rslist = target_entry->is_entry_from_rslist();
              LOG_WDIAG("sys_dummy_entry can not be building state, will add one with rslist", K(is_entry_from_rslist), K(table_param));
              target_entry->dec_ref();
              target_entry = NULL;
              if (OB_FAIL(get_table_entry_from_rslist(table_param, target_entry, op, is_entry_from_rslist))) {
                LOG_WDIAG("fail to get table entry from rslist", K(ret));
              }
            } else {
              int64_t diff_us = hrtime_to_usec(get_hrtime()) - target_entry->get_create_time_us();
              // just for defense
              if (diff_us > (6 * 60 * 1000 * 1000)) { // 6min
                LOG_EDIAG("building state entry has cost so mutch time",
                          K(diff_us), KPC(target_entry));
              }
              LOG_INFO("building state table entry, fetch from remote or push into pending_queue",
                       KPC(target_entry));

              // someone is fetch this entry from remote, just push into pending_queue
              if (NULL == te_cont) {
                if (OB_ISNULL(te_cont = op_alloc(ObTableEntryCont))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WDIAG("fail to alloc ObTableEntryCont", K(ret));
                } else if (OB_FAIL(te_cont->init(table_cache, table_param, target_entry))) {
                  LOG_WDIAG("fail to init table entry cont", K(ret));
                } else {
                  action = &te_cont->get_action();
                }
              }
              // Attention!! must be out of the else{} above
              if (OB_SUCC(ret)) {
                // push into pending queue
                target_entry->pending_queue_.push(te_cont);
                op = LOOKUP_PUSH_INTO_PENDING_LIST_OP;
              }
            }
          } else {
            // updating, avail state treat as succ
            op = LOOKUP_GLOBAL_CACHE_HIT_OP;
          }
        }
      }
    } else {
      op = RETRY_LOOKUP_GLOBAL_CACHE_OP;
    }
  }

  if (OB_SUCC(ret)) {
    entry = target_entry;
  } else {
    if (NULL != target_entry) {
      target_entry->dec_ref();
      target_entry = NULL;
    }
  }
  LOG_DEBUG("after get table entry in global cache", K(table_param), KPC(entry), K(op), K(ret));

  return ret;
}

int ObTableProcessor::get_table_entry_from_thread_cache(
    ObTableRouteParam &table_param,
    ObTableCache &table_cache,
    ObTableEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  if (OB_UNLIKELY(!table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(table_param), K(ret));
  } else {
    if (OB_LIKELY(!table_param.need_fetch_remote())) {
      // find entry from thread cache
      ObTableRefHashMap &table_map = self_ethread().get_table_map();
      ObTableEntry *tmp_entry = NULL;
      ObTableEntryKey key(table_param.name_, table_param.cr_version_, table_param.cr_id_);
      tmp_entry = table_map.get(key); // get will inc entry's ref
      if (OB_LIKELY(NULL != tmp_entry)) {
        bool find_succ = false;
        if (tmp_entry->is_deleted_state()) {
          LOG_DEBUG("this table entry in thread cache has deleted", KPC(tmp_entry));
        } else if (OB_UNLIKELY(table_cache.is_table_entry_expired(*tmp_entry))) {
          // table entry has expired
          LOG_DEBUG("the table entry in thread cache is expired", "expire_time_us",
                    table_cache.get_cache_expire_time_us(), KPC(tmp_entry), K(table_param));
        } else if (OB_LIKELY(tmp_entry->is_avail_state() || tmp_entry->is_updating_state())) { // avail
          find_succ = true;
        } else if (tmp_entry->is_building_state()) {
          LOG_EDIAG("building state table entry can not in thread cache", KPC(tmp_entry));
        } else if (tmp_entry->is_dirty_state()) {
          // dirty entry need to fetch from remote
        } else {}
        if (!find_succ) {
          tmp_entry->dec_ref();
          tmp_entry = NULL;
        }
      }
      entry = tmp_entry;
    }
  }

  return ret;
}

int ObTableProcessor::handle_lookup_global_cache_done(
    ObTableRouteParam &table_param,
    ObTableCache &table_cache,
    ObTableEntry *entry,
    ObAction *&action,
    const ObTableEntryLookupOp &op)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(table_param), K(ret));
  } else {
    ObProxyMutex *mutex_ = table_param.cont_->mutex_;
    ObTableEntryCont *te_cont = NULL;
    switch (op) {
      case LOOKUP_PUSH_INTO_PENDING_LIST_OP: {
        if (OB_ISNULL(action) || OB_ISNULL(entry)) { // for defense
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("action can not be NULL here", K(action), K(ret));
        } else {
          entry->dec_ref();
          entry = NULL;
        }
        break;
      }
      case LOOKUP_GLOBAL_CACHE_HIT_OP: {
        if (OB_ISNULL(entry)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("table entry must not be NULL here", K(entry), K(ret));
        } else {
          // entry has already inc_ref
          entry->renew_last_access_time();
          table_param.result_.target_entry_ = entry;
          table_param.result_.is_need_force_flush_ = entry->is_need_force_flush();
          entry->set_need_force_flush(false);

          PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_GLOBAL_CACHE_HIT);
          ROUTE_PROMETHEUS_STAT(table_param.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, true, true);
          // update thread cache table entry
          if (entry->is_avail_state() || entry->is_updating_state()) {
            ObTableRefHashMap &table_map = self_ethread().get_table_map();
            if (OB_FAIL(table_map.set(entry))) {
              LOG_WDIAG("fail to set table map", KPC(entry), K(ret));
              ret = OB_SUCCESS; // ignore ret
            }
          }
          // hand the ref count to target_entry
          entry = NULL;
        }
        break;
      }
      case RETRY_LOOKUP_GLOBAL_CACHE_OP:
      case LOOKUP_REMOTE_DIRECT_OP:
      case LOOKUP_REMOTE_FOR_UPDATE_OP:
      case LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP: {
        if (OB_ISNULL(te_cont = op_alloc(ObTableEntryCont))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc ObTableEntryCont", K(ret));
        } else if (OB_FAIL(te_cont->init(table_cache, table_param, entry))) { // init will inc entry ref
          LOG_WDIAG("fail to init table entry cont", K(ret));
        } else {
          te_cont->set_table_entry_op(op); // do not forget
          int te_event = TABLE_ENTRY_LOOKUP_REMOTE_EVENT;
          if (RETRY_LOOKUP_GLOBAL_CACHE_OP == op) {
            te_event = TABLE_ENTRY_LOOKUP_CACHE_EVENT;
            if (OB_UNLIKELY(NULL != entry)) { // for defense
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("table entry must be NULL when it is RETRY_LOOKUP_GLOBAL_CACHE_OP", KPC(entry), K(ret));
            }
          } else if (LOOKUP_REMOTE_DIRECT_OP == op) {
            PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE);
          } else if (LOOKUP_REMOTE_FOR_UPDATE_OP == op) {
            PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_GLOBAL_CACHE_DIRTY);
          } else if (LOOKUP_REMOTE_WITH_BUILDING_ENTRY_OP == op) {
            PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_REMOTE);
          }
          if (OB_SUCC(ret)) {
            action = &te_cont->get_action();
            if (RETRY_LOOKUP_GLOBAL_CACHE_OP == op) {
              ObEvent *event = self_ethread().schedule_in(te_cont,
                    ObTableEntryCont::SCHEDULE_TABLE_ENTRY_LOOKUP_INTERVAL, te_event);
              if (OB_ISNULL(event)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WDIAG("fail to schedule in", K(event), K(ret));
              }
            } else {
              te_cont->handle_event(te_event, NULL);
            }
          }
          if (OB_FAIL(ret) && (NULL != te_cont)) {
            te_cont->kill_this();
            te_cont = NULL;
          }
          if (NULL != entry) {
            entry->dec_ref();
            entry = NULL;
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("unknown op", K(op), K(ret));
        break;
      }
    }

    if (NULL != te_cont && NULL != table_param.result_.target_old_entry_) {
      te_cont->set_need_notify(false);
      action = NULL;
      LOG_DEBUG("enable async pull table entry set nedd notify false", KPC(table_param.result_.target_old_entry_),
           KPC(table_param.result_.target_entry_));
    }
  }
  return ret;
}

int ObTableProcessor::get_table_entry(ObTableRouteParam &table_param, ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K(ret));
  } else if (OB_UNLIKELY(!table_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(table_param), K(ret));
  } else {
    ObProxyMutex *mutex_ = table_param.cont_->mutex_;
    PROCESSOR_INCREMENT_DYN_STAT(GET_PL_TOTAL);
    if (table_param.name_.is_all_dummy_table()) {
      PROCESSOR_INCREMENT_DYN_STAT(GET_PL_BY_ALL_DUMMY);
      // sys dummy's cr version must be 0
      if (table_param.name_.is_sys_dummy()) {
        table_param.cr_version_ = 0;
      }
    }

    ObTableEntry *tmp_entry = NULL;
    // 1. find table entry from thread cache
    if (OB_FAIL(get_table_entry_from_thread_cache(table_param, *table_cache_, tmp_entry))) {
      LOG_WDIAG("fail to get table entry in thread cache", K(table_param), K(ret));
    } else {
      if (OB_LIKELY(NULL != tmp_entry)) {
        PROCESSOR_INCREMENT_DYN_STAT(GET_PL_FROM_THREAD_CACHE_HIT);
        ROUTE_PROMETHEUS_STAT(table_param.name_, PROMETHEUS_ENTRY_LOOKUP_COUNT, TBALE_ENTRY, true, true);
        tmp_entry->renew_last_access_time();
        table_param.result_.target_entry_ = tmp_entry;
        table_param.result_.is_need_force_flush_ = tmp_entry->is_need_force_flush();
        tmp_entry->set_need_force_flush(false);

        LOG_DEBUG("get table entry from thread cache", KPC(tmp_entry));
      } else {
        ObTableEntryCont *te_cont = NULL;
        ObTableEntryLookupOp op = LOOKUP_MIN_OP;
        // 2. find entry from global cache or remote
        if (OB_FAIL(get_table_entry_from_global_cache( // if get succ, will inc_ref
                        table_param, *table_cache_, te_cont, action, tmp_entry, op))) {
          LOG_WDIAG("fail to get table entry in global cache", K(table_param), K(ret));
        } else if (OB_FAIL(handle_lookup_global_cache_done(
                               table_param, *table_cache_, tmp_entry, action, op))) {
          LOG_WDIAG("fail to handle lookup global cachd done", K(table_param), K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && NULL != table_param.result_.target_old_entry_) {
      if (NULL == table_param.result_.target_entry_) {
        table_param.result_.target_entry_ = table_param.result_.target_old_entry_;
        table_param.result_.target_old_entry_ = NULL;
      } else {
        table_param.result_.target_old_entry_->dec_ref();
        table_param.result_.target_old_entry_ = NULL;
      }
    }
  }

  if (OB_FAIL(ret)) {
    action = NULL;
  }

  return ret;
}

int ObTableProcessor::add_building_state_table_entry(
    const ObTableRouteParam &table_param,
    ObTableCache &table_cache,
    ObTableEntry *&entry)
{
  int ret = OB_SUCCESS;
  entry = NULL;
  ObTableEntry *target_entry = NULL;
  if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(table_param.name_, table_param.cr_version_,
                                                       table_param.cr_id_, target_entry))) {
    LOG_WDIAG("fail to alloc table entry", K(table_param), K(ret));
  } else {
    target_entry->set_building_state();
    bool direct_add = false;
    target_entry->inc_ref(); // before add to table_cache, must inc_ref
    if (OB_FAIL(table_cache.add_table_entry(*target_entry, direct_add))) {
      LOG_WDIAG("fail to add table entry", K(ret));
      target_entry->dec_ref();
    } else {
      entry = target_entry;
      target_entry = NULL;
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != target_entry) {
      target_entry->dec_ref();
      target_entry = NULL;
    }
  }

  return ret;
}

ObTableProcessor &get_global_table_processor()
{
  static ObTableProcessor table_processor;
  return table_processor;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
