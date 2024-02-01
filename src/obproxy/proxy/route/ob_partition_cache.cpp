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

#include "proxy/route/ob_partition_cache.h"
#include "stat/ob_processor_stats.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//---------------------------ObPartitionCacheCont----------------------//
class ObPartitionCacheCont : public event::ObContinuation
{
public:
  explicit ObPartitionCacheCont(ObPartitionCache &partition_cache)
    : ObContinuation(NULL), partition_cache_(partition_cache), ppentry_(NULL),
      hash_(0), is_add_building_entry_(false), key_() {}
  virtual ~ObPartitionCacheCont() {}
  void destroy();
  int get_partition_entry(const int event, ObEvent *e);
  static int get_partition_entry_local(ObPartitionCache &partition_cache,
                                       const ObPartitionEntryKey &key,
                                       const uint64_t hash,
                                       const bool is_add_building_entry,
                                       bool &is_locked,
                                       ObPartitionEntry *&partition);


   static int add_building_part_entry(ObPartitionCache &partition_cache,
                                      const ObPartitionEntryKey &key);
  event::ObAction action_;
  ObPartitionCache &partition_cache_;
  ObPartitionEntry **ppentry_;
  uint64_t hash_;
  bool is_add_building_entry_;
  ObPartitionEntryKey key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionCacheCont);
};

inline void ObPartitionCacheCont::ObPartitionCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

int ObPartitionCacheCont::get_partition_entry(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_partition_entry started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObPartitionEntry *tmp_entry = NULL;
    if (OB_FAIL(get_partition_entry_local(partition_cache_, key_, hash_,
            is_add_building_entry_, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get partition entry", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_partition_entry MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObPartitionCacheParam::SCHEDULE_PARTITION_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObPartitionCacheParam::SCHEDULE_PARTITION_CACHE_CONT_INTERVAL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule in", K(ret));
      }
      he_ret = EVENT_CONT;
    } else {
      if (NULL != *ppentry_) {
        (*ppentry_)->dec_ref();
        (*ppentry_) = NULL;
      }

      *ppentry_ = tmp_entry;
      tmp_entry = NULL;
      // failed or locked
      action_.continuation_->handle_event(PARTITION_ENTRY_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObPartitionCacheCont::get_partition_entry_local(
    ObPartitionCache &partition_cache,
    const ObPartitionEntryKey &key,
    const uint64_t hash,
    const bool is_add_building_entry,
    bool &is_locked,
    ObPartitionEntry *&entry)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  entry = NULL;

  ObProxyMutex *bucket_mutex = partition_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(partition_cache.run_todo_list(partition_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      entry = partition_cache.lookup_entry(hash, key);
      if (NULL != entry) {
        if (partition_cache.is_partition_entry_expired_in_time_mode(*entry)
            && entry->is_avail_state()) {
          entry->set_dirty_state();
        }
        if (partition_cache.is_partition_entry_expired_in_qa_mode(*entry)
            || (!get_global_proxy_config().enable_async_pull_location_cache
                && partition_cache.is_partition_entry_expired_in_time_mode(*entry))) {
          LOG_INFO("the partition entry is expired", "expire_time_us",
                   partition_cache.get_cache_expire_time_us(), KPC(entry));
          entry = NULL;
          // remove the expired partition entry in locked
          if (OB_FAIL(partition_cache.remove_partition_entry(key))) {
            LOG_WDIAG("fail to remove partition entry", K(key), K(ret));
          }
        } else {
          entry->inc_ref();
          LOG_DEBUG("cont::get_partition_entry_local, entry found succ", KPC(entry));
        }
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == entry) {
      if (is_add_building_entry) {
        if (OB_FAIL(add_building_part_entry(partition_cache, key))) {
          LOG_WDIAG("fail to building part entry", K(key), K(ret));
        } else {
          // nothing
        }
      } else {
        LOG_DEBUG("cont::get_partition_entry_local, entry not found", K(key));
      }
    }
    lock_bucket.release();
  }

  return ret;
}

int ObPartitionCacheCont::add_building_part_entry(ObPartitionCache &partition_cache,
                                                  const ObPartitionEntryKey &key)
{
  int ret = OB_SUCCESS;
  ObPartitionEntry *entry = NULL;
  ObProxyReplicaLocation replica_location;
  replica_location.role_ = FOLLOWER;
  const char *ip_str = "88.88.88.88"; // fake ip
  int32_t port = 8; // fake port
  if (OB_FAIL(replica_location.add_addr(ip_str, port))) {
    LOG_WDIAG("fail to add ip or port", K(ip_str), K(port), K(ret));
  } else if (OB_FAIL(ObPartitionEntry::alloc_and_init_partition_entry(key, replica_location, entry))) {
    LOG_WDIAG("fail to alloc and init partition entry", K(key), K(replica_location), K(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part entry is NULL", K(entry), K(ret));
  } else {
    entry->set_building_state();
    if (OB_FAIL(partition_cache.add_partition_entry(*entry, false))) {
      LOG_WDIAG("fail to add part entry", KPC(entry), K(ret));
      entry->dec_ref();
      entry = NULL;
    } else {
      LOG_INFO("add building part entry succ", KPC(entry));
      entry = NULL;
    }
  }

  return ret;
}

//---------------------------ObPartitionCacheParam-------------------------//
const char *ObPartitionCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_PARTITION_OP : {
      name = "INVALID_PARTITION_OP";
      break;
    }
    case ADD_PARTITION_OP : {
      name = "ADD_PARTITION_OP";
      break;
    }
    case REMOVE_PARTITION_OP : {
      name = "REMOVE_PARTITION_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

int64_t ObPartitionCacheParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("op_name", get_op_name(op_),
      K_(hash),
      K_(key));
  J_COMMA();
  if (NULL != entry_) {
    J_KV(K_(*entry));
  }
  J_OBJ_END();
  return pos;
}

//---------------------------ObPartitionCache-------------------------//
int ObPartitionCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(PartitionEntryHashMap::init(sub_bucket_size, PARTITION_ENTRY_MAP_LOCK, gc_partition_entry))) {
    LOG_WDIAG("fail to init hash partition of partition cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("partition_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObPartitionCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObPartitionCache::destroy()
{
  LOG_INFO("ObPartitionCache will desotry");
  if (is_inited_) {
    // TODO, modify later
    ObPartitionCacheParam *param = NULL;
    ObPartitionCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObPartitionCacheParam *>(todo_lists_[i].popall()))) {
        while (NULL != param) {
          cur = param;
          param = param->link_.next_;
          op_free(cur);
        }
      }
    }
    is_inited_ = false;
  }
}

int ObPartitionCache::get_partition_entry(
    event::ObContinuation *cont,
    const ObPartitionEntryKey &key,
    ObPartitionEntry **ppentry,
    const bool is_add_building_entry,
    ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(ppentry) || OB_ISNULL(cont)
             || OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid arugument", K(ppentry), K(key), K(cont), K(ret));
  } else {
    uint64_t hash = key.hash();
    LOG_DEBUG("begin to get partition location entry", K(ppentry), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObPartitionEntry *tmp_entry = NULL;
    if (OB_FAIL(ObPartitionCacheCont::get_partition_entry_local(*this, key, hash,
            is_add_building_entry, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get partition entry", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppentry = tmp_entry;
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get_partition_entry, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObPartitionCacheParam::SCHEDULE_PARTITION_CACHE_CONT_INTERVAL));
        ObPartitionCacheCont *partition_cont = NULL;
        if (OB_ISNULL(partition_cont = op_alloc_args(ObPartitionCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for partition cache continuation", K(ret));
        } else {
          partition_cont->action_.set_continuation(cont);
          partition_cont->mutex_ = cont->mutex_;
          partition_cont->hash_ = hash;
          partition_cont->ppentry_ = ppentry;
          partition_cont->key_ = key;
          partition_cont->is_add_building_entry_ = is_add_building_entry;

          SET_CONTINUATION_HANDLER(partition_cont, &ObPartitionCacheCont::get_partition_entry);
          if (OB_ISNULL(self_ethread().schedule_in(partition_cont,
                  ObPartitionCacheParam::SCHEDULE_PARTITION_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(partition_cont), K(ret));
          } else {
            action = &partition_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != partition_cont)) {
          partition_cont->destroy();
          partition_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppentry = NULL;
    }
  }
  return ret;
}

int ObPartitionCache::add_partition_entry(ObPartitionEntry &entry, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObPartitionEntryKey key = entry.get_key();
    uint64_t hash = key.hash();
    LOG_DEBUG("add partition location", K(part_num(hash)), K(entry), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObPartitionEntry *tmp_entry = insert_entry(hash, key, &entry);
          if (NULL != tmp_entry) {
            tmp_entry->set_deleted_state(); // used to update tc_partition_map
            tmp_entry->dec_ref();
            tmp_entry = NULL;
          }
        }
      } else {
        direct_add = true;
      }
    }

    if (direct_add) {
      // add todo list
      ObPartitionCacheParam *param = op_alloc(ObPartitionCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for partition param", K(param), K(ret));
      } else {
        param->op_ = ObPartitionCacheParam::ADD_PARTITION_OP;
        param->hash_ = hash;
        param->key_ = key;
        entry.inc_ref();
        param->entry_ = &entry;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObPartitionCache::remove_partition_entry(const ObPartitionEntryKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(key), K(ret));
  } else {
    uint64_t hash = key.hash();
    ObPartitionEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        entry = remove_entry(hash, key);
        LOG_INFO("this entry will be removed from partition cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
      }
    } else {
      ObPartitionCacheParam *param = op_alloc(ObPartitionCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObPartitionCacheParam::REMOVE_PARTITION_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->entry_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObPartitionCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObPartitionCacheParam *pre = NULL;
    ObPartitionCacheParam *cur = NULL;
    ObPartitionCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObPartitionCacheParam *>(todo_lists_[buck_id].popall()))) {
      // 1. start the work at the end of the list, so reverse the list
      next = cur->link_.next_;
      while (NULL != next) {
        cur->link_.next_ = pre;
        pre = cur;
        cur = next;
        next = cur->link_.next_;
      };
      cur->link_.next_ = pre;

      // 2. process the param
      ObPartitionCacheParam *param = NULL;
      while ((NULL != cur) && (OB_SUCC(ret))) {
        process(buck_id, cur); // ignore ret, must clear todo_list, or will cause mem leak;
        param = cur;
        cur = cur->link_.next_;
        op_free(param);
        param = NULL;
      }
    }
  }
  return ret;
}

int ObPartitionCache::process(const int64_t buck_id, ObPartitionCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObPartitionCacheParam", K(buck_id), KPC(param));
    ObPartitionEntry *entry = NULL;
    switch (param->op_) {
      case ObPartitionCacheParam::ADD_PARTITION_OP: {
        entry = insert_entry(param->hash_, param->key_, param->entry_);
        if (NULL != entry) {
          entry->set_deleted_state(); // used to update tc_partition_map
          entry->dec_ref(); // free old entry
          entry = NULL;
        }
        if (NULL != param->entry_) {
          // dec_ref, it was inc before push param into todo list
          param->entry_->dec_ref();
          param->entry_ = NULL;
        }
        break;
      }
      case ObPartitionCacheParam::REMOVE_PARTITION_OP: {
        entry = remove_entry(param->hash_, param->key_);
        LOG_INFO("this entry will be removed from partition cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref(); // free old entry
          entry = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObPartitionCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObPartitionCache::gc_partition_entry(ObPartitionEntry *entry)
{
  bool expired = false;
  ObCacheCleaner *cleaner = self_ethread().cache_cleaner_;
  if ((NULL != cleaner) && (NULL != entry)) {
    if (cleaner->is_partition_entry_expired(*entry)) {
      LOG_INFO("this partition entry has expired, will be deleted", KPC(entry));
      expired = true;
      entry->set_deleted_state();
      entry->dec_ref();
     if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
        ObProxyMutex *mutex_ = this_ethread()->mutex_;
        PROCESSOR_INCREMENT_DYN_STAT(GC_PARTITION_ENTRY_FROM_GLOBAL_CACHE);
      }
    }
  }

  return expired;
}

ObPartitionCache &get_global_partition_cache()
{
  static ObPartitionCache partition_cache;
  return partition_cache;
}

int init_partition_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_partition_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init partition_map", K(i), K(ret));
    }
  }
  return ret;
}

int init_partition_map_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  ObEThread **ethreads = NULL;
  if (OB_ISNULL(ethreads = g_event_processor.event_thread_[ET_CALL])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "fail to get ET_NET thread", K(ret));
  } else if (OB_ISNULL(ethreads[index])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "fail to get ET_NET thread", K(ret));
  } else {
    if (OB_ISNULL(ethreads[index]->partition_map_ = new (std::nothrow) ObPartitionRefHashMap(ObModIds::OB_PROXY_PARTITION_ENTRY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObPartitionRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->partition_map_->init())) {
      LOG_WDIAG("fail to init partition_map", K(ret));
    }
  }
  return ret;
}

int ObPartitionRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this partition entry will erase from tc map", KPC((*it)));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase partition entry", K(i), K(ret));
        }

        if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
          ObProxyMutex *mutex_ = this_ethread()->mutex_;
          PROCESSOR_INCREMENT_DYN_STAT(GC_PARTITION_ENTRY_FROM_THREAD_CACHE);
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
