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

#include "proxy/route/ob_index_entry.h"
#include "proxy/route/ob_index_cache.h"
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
//---------------------------ObIndexCacheCont----------------------//
class ObIndexCacheCont : public event::ObContinuation
{
public:
  explicit ObIndexCacheCont(ObIndexCache &index_cache)
    : ObContinuation(NULL), index_cache_(index_cache), ppentry_(NULL),
      hash_(0), is_add_building_entry_(false), key_() {}
  virtual ~ObIndexCacheCont() {}
  void destroy();
  int get_index_entry(const int event, ObEvent *e);
  static int get_index_entry_local(ObIndexCache &index_cache,
                                       const ObIndexEntryKey &key,
                                       const uint64_t hash,
                                       bool &is_locked,
                                       ObIndexEntry *&index);


  static int add_building_index_entry(ObIndexCache &index_cache,
                                      const ObIndexEntryKey &key);
  event::ObAction action_;
  ObIndexCache &index_cache_;
  ObIndexEntry **ppentry_;
  uint64_t hash_;
  bool is_add_building_entry_;
  ObIndexEntryKey key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexCacheCont);
};

inline void ObIndexCacheCont::ObIndexCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

int ObIndexCacheCont::get_index_entry(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_index_entry started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObIndexEntry *tmp_entry = NULL;
    if (OB_FAIL(get_index_entry_local(index_cache_, key_, hash_, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get index entry", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_index_entry MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObIndexCacheParam::SCHEDULE_INDEX_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObIndexCacheParam::SCHEDULE_INDEX_CACHE_CONT_INTERVAL))) {
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
      action_.continuation_->handle_event(INDEX_ENTRY_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObIndexCacheCont::get_index_entry_local(
    ObIndexCache &index_cache,
    const ObIndexEntryKey &key,
    const uint64_t hash,
    bool &is_locked,
    ObIndexEntry *&entry)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  entry = NULL;

  ObProxyMutex *bucket_mutex = index_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(index_cache.run_todo_list(index_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      entry = index_cache.lookup_entry(hash, key);
      if (NULL != entry) {
        if (index_cache.is_index_entry_expired_in_time_mode(*entry)
            && entry->is_avail_state()) {
          entry->set_dirty_state();
        }
        if (index_cache.is_index_entry_expired_in_qa_mode(*entry)
            || (!get_global_proxy_config().enable_async_pull_location_cache
                && index_cache.is_index_entry_expired_in_time_mode(*entry))) {
          LOG_INFO("the index entry is expired", "expire_time_us",
                   index_cache.get_cache_expire_time_us(), KPC(entry));
          entry = NULL;
          // remove the expired index entry in locked
          if (OB_FAIL(index_cache.remove_index_entry(key))) {
            LOG_WDIAG("fail to remove index entry", K(key), K(ret));
          }
        } else {
          entry->inc_ref();
          LOG_DEBUG("cont::get_index_entry_local, entry found succ", KPC(entry));
        }
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == entry) {
      LOG_DEBUG("cont::get_index_entry_local, entry not found", K(key));
    }
    lock_bucket.release();
  }

  return ret;
}

int ObIndexCacheCont::add_building_index_entry(ObIndexCache &index_cache,
                                                  const ObIndexEntryKey &key)
{
  int ret = OB_SUCCESS;
  ObIndexEntry *entry = NULL;

  if (OB_ISNULL(key.name_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid ObIndexEntryKey", K(key), K(ret));
  } else if (OB_FAIL(ObIndexEntry::alloc_and_init_index_entry(*key.name_, key.cr_version_, key.cr_id_, entry))) {
    LOG_WDIAG("fail to alloc and init index entry", K(key), K(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part entry is NULL", K(entry), K(ret));
  } else {
    entry->set_building_state();
    if (OB_FAIL(index_cache.add_index_entry(*entry, false))) {
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

//---------------------------ObIndexCacheParam-------------------------//
const char *ObIndexCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_INDEX_OP : {
      name = "INVALID_INDEX_OP";
      break;
    }
    case ADD_INDEX_OP : {
      name = "ADD_INDEX_OP";
      break;
    }
    case REMOVE_INDEX_OP : {
      name = "REMOVE_INDEX_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

int64_t ObIndexCacheParam::to_string(char *buf, const int64_t buf_len) const
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

//---------------------------ObIndexCache-------------------------//
int ObIndexCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start init ObIndexCache");
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(IndexEntryHashMap::init(sub_bucket_size, INDEX_ENTRY_MAP_LOCK, gc_index_entry))) {
    LOG_WDIAG("fail to init hash index of index cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("index_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObIndexCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObIndexCache::destroy()
{
  LOG_INFO("ObIndexCache will desotry");
  if (is_inited_) {
    // TODO oushen, modify later
    ObIndexCacheParam *param = NULL;
    ObIndexCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObIndexCacheParam *>(todo_lists_[i].popall()))) {
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

int ObIndexCache::get_index_entry(
    event::ObContinuation *cont,
    const ObIndexEntryKey &key,
    ObIndexEntry **ppentry,
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
    LOG_DEBUG("begin to get index location entry", K(ppentry), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObIndexEntry *tmp_entry = NULL;
    if (OB_FAIL(ObIndexCacheCont::get_index_entry_local(*this, key, hash, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get index entry", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppentry = tmp_entry;
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get_index_entry, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObIndexCacheParam::SCHEDULE_INDEX_CACHE_CONT_INTERVAL));
        ObIndexCacheCont *index_cont = NULL;
        if (OB_ISNULL(index_cont = op_alloc_args(ObIndexCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for index cache continuation", K(ret));
        } else {
          index_cont->action_.set_continuation(cont);
          index_cont->mutex_ = cont->mutex_;
          index_cont->hash_ = hash;
          index_cont->ppentry_ = ppentry;
          index_cont->key_ = key;

          SET_CONTINUATION_HANDLER(index_cont, &ObIndexCacheCont::get_index_entry);
          if (OB_ISNULL(self_ethread().schedule_in(index_cont,
                  ObIndexCacheParam::SCHEDULE_INDEX_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(index_cont), K(ret));
          } else {
            action = &index_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != index_cont)) {
          index_cont->destroy();
          index_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppentry = NULL;
    }
  }
  return ret;
}

int ObIndexCache::add_index_entry(ObIndexEntry &entry, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObIndexEntryKey key;
    entry.get_key(key);
    uint64_t hash = key.hash();
    LOG_DEBUG("add index location", K(part_num(hash)), K(entry), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObIndexEntry *tmp_entry = insert_entry(hash, key, &entry);
          if (NULL != tmp_entry) {
            LOG_DEBUG("remove from index entry", KPC(tmp_entry));
            tmp_entry->set_deleted_state(); // used to update tc_index_map
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
      ObIndexCacheParam *param = op_alloc(ObIndexCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for index param", K(param), K(ret));
      } else {
        param->op_ = ObIndexCacheParam::ADD_INDEX_OP;
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

int ObIndexCache::remove_index_entry(const ObIndexEntryKey &key)
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
    ObIndexEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        entry = remove_entry(hash, key);
        LOG_INFO("this entry will be removed from index cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
      }
    } else {
      ObIndexCacheParam *param = op_alloc(ObIndexCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObIndexCacheParam::REMOVE_INDEX_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->entry_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObIndexCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObIndexCacheParam *pre = NULL;
    ObIndexCacheParam *cur = NULL;
    ObIndexCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObIndexCacheParam *>(todo_lists_[buck_id].popall()))) {
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
      ObIndexCacheParam *param = NULL;
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

int ObIndexCache::process(const int64_t buck_id, ObIndexCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObIndexCacheParam", K(buck_id), KPC(param));
    ObIndexEntry *entry = NULL;
    switch (param->op_) {
      case ObIndexCacheParam::ADD_INDEX_OP: {
        entry = insert_entry(param->hash_, param->key_, param->entry_);
        if (NULL != entry) {
          entry->set_deleted_state(); // used to update tc_index_map
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
      case ObIndexCacheParam::REMOVE_INDEX_OP: {
        entry = remove_entry(param->hash_, param->key_);
        LOG_INFO("this entry will be removed from index cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref(); // free old entry
          entry = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObIndexCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObIndexCache::gc_index_entry(ObIndexEntry *entry)
{
  bool expired = false;
  ObCacheCleaner *cleaner = self_ethread().cache_cleaner_;
  if ((NULL != cleaner) && (NULL != entry)) {
    if (cleaner->is_global_index_entry_expired(*entry)) {
      LOG_INFO("this index entry has expired, will be deleted", KPC(entry));
      expired = true;
      entry->set_deleted_state();
      entry->dec_ref();
    //  if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
    //     ObProxyMutex *mutex_ = this_ethread()->mutex_;
    //     PROCESSOR_INCREMENT_DYN_STAT(GC_INDEX_ENTRY_FROM_GLOBAL_CACHE);
    //   }
    }
  }

  return expired;
}

ObIndexCache &get_global_index_cache()
{
  static ObIndexCache index_cache;
  return index_cache;
}

int init_index_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_index_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init index_map", K(i), K(ret));
    }
  }
  return ret;
}

int init_index_map_for_one_thread(int64_t index)
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
    if (OB_ISNULL(ethreads[index]->index_map_ = new (std::nothrow) ObIndexRefHashMap(ObModIds::OB_PROXY_INDEX_ENTRY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObIndexRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->index_map_->init())) {
      LOG_WDIAG("fail to init index_map", K(ret));
    }
  }
  return ret;
}

int ObIndexRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this index entry will erase from tc map", KPC((*it)));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase index entry", K(i), K(ret));
        }

        if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
          // ObProxyMutex *mutex_ = this_ethread()->mutex_;
          // TODO : add states
          // PROCESSOR_INCREMENT_DYN_STAT(GC_PARTITION_ENTRY_FROM_THREAD_CACHE);
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
