/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "proxy/rpc/rpclib/ob_tablet_ls_processor.h"
#include "proxy/rpc/rpclib/ob_tablet_ls_cache.h"
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
//---------------------------ObTabletLsCacheCont----------------------//
class ObTabletLsCacheCont : public event::ObContinuation
{
public:
  explicit ObTabletLsCacheCont(ObTabletLsCache &tablet_ls_cache)
    : ObContinuation(NULL), tablet_ls_cache_(tablet_ls_cache), ppentry_(NULL),
      hash_(0), is_add_building_entry_(false), key_() {}
  virtual ~ObTabletLsCacheCont() {}
  void destroy();
  int get_tablet_ls_entry(const int event, ObEvent *e);
  static int get_tablet_ls_entry_local(ObTabletLsCache &tablet_ls_cache,
                                       const ObTabletLsEntryKey &key,
                                       const uint64_t hash,
                                       bool &is_locked,
                                       ObTabletLsEntry *&tablet_ls);


  static int add_building_tablet_ls_entry(ObTabletLsCache &tablet_ls_cache,
                                      const ObTabletLsEntryKey &key);
  event::ObAction action_;
  ObTabletLsCache &tablet_ls_cache_;
  ObTabletLsEntry **ppentry_;
  uint64_t hash_;
  bool is_add_building_entry_;
  ObTabletLsEntryKey key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletLsCacheCont);
};

inline void ObTabletLsCacheCont::ObTabletLsCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

int ObTabletLsCacheCont::get_tablet_ls_entry(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_tablet_ls_entry started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObTabletLsEntry *tmp_entry = NULL;
    if (OB_FAIL(get_tablet_ls_entry_local(tablet_ls_cache_, key_, hash_, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get tablet_ls entry", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_tablet_ls_entry MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObTabletLsCacheParam::SCHEDULE_TABLET_LS_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObTabletLsCacheParam::SCHEDULE_TABLET_LS_CACHE_CONT_INTERVAL))) {
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
      action_.continuation_->handle_event(TABLET_LS_ENTRY_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObTabletLsCacheCont::get_tablet_ls_entry_local(
    ObTabletLsCache &tablet_ls_cache,
    const ObTabletLsEntryKey &key,
    const uint64_t hash,
    bool &is_locked,
    ObTabletLsEntry *&entry)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  entry = NULL;

  ObProxyMutex *bucket_mutex = tablet_ls_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(tablet_ls_cache.run_todo_list(tablet_ls_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      entry = tablet_ls_cache.lookup_entry(hash, key);
      if (NULL != entry) {
        if (tablet_ls_cache.is_tablet_ls_entry_expired_in_time_mode(*entry)
            && entry->is_avail_state()) {
          entry->set_dirty_state();
        }
        if (tablet_ls_cache.is_tablet_ls_entry_expired_in_qa_mode(*entry)
            || (!get_global_proxy_config().enable_async_pull_location_cache
                && tablet_ls_cache.is_tablet_ls_entry_expired_in_time_mode(*entry))) {
          LOG_INFO("the tablet_ls entry is expired", "expire_time_us",
                   tablet_ls_cache.get_cache_expire_time_us(), KPC(entry));
          entry = NULL;
          // remove the expired tablet_ls entry in locked
          if (OB_FAIL(tablet_ls_cache.remove_tablet_ls_entry(key))) {
            LOG_WDIAG("fail to remove tablet_ls entry", K(key), K(ret));
          }
        } else {
          entry->inc_ref();
          LOG_DEBUG("cont::get_tablet_ls_entry_local, entry found succ", KPC(entry));
        }
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == entry) {
      LOG_DEBUG("cont::get_tablet_ls_entry_local, entry not found", K(key));
    }
    lock_bucket.release();
  }

  return ret;
}

int ObTabletLsCacheCont::add_building_tablet_ls_entry(ObTabletLsCache &tablet_ls_cache,
                                                         const ObTabletLsEntryKey &key)
{
  int ret = OB_SUCCESS;
  ObTabletLsEntry *entry = NULL;
  // ObString sharding("NONE");
  // ObSEArray<ObTabletLsTableNameInfo, 1> table_names;
  // table_names.push_back(ObTabletLsTableNameInfo());
  OB_TABLET_TO_LS_MAP empty_map;
  empty_map.create(OB_TABLET_TO_LS_MAP_BUCKET_SIZE, ObModIds::OB_PROXY_TABLET_LS_ID_MAP);

  if (OB_FAIL(ObTabletLsEntry::alloc_and_init_tablet_ls_entry(key.tenant_id_,
                                                                 key.table_id_,
                                                                 empty_map,
                                                                 key.cr_version_,
                                                                 key.cr_id_,
                                                                 entry))) {
    LOG_WDIAG("fail to alloc and init tablet_ls entry", K(key), K(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("part entry is NULL", K(entry), K(ret));
  } else {
    entry->set_building_state();
    if (OB_FAIL(tablet_ls_cache.add_tablet_ls_entry(*entry, false))) {
      LOG_WDIAG("fail to add tablet_ls entry", KPC(entry), K(ret));
      entry->dec_ref();
      entry = NULL;
    } else {
      LOG_INFO("add building tablet_ls entry succ", KPC(entry));
      entry = NULL;
    }
  }

  return ret;
}

//---------------------------ObTabletLsCacheParam-------------------------//
const char *ObTabletLsCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_TABLET_LS_OP : {
      name = "INVALID_TABLET_LS_OP";
      break;
    }
    case ADD_TABLET_LS_OP : {
      name = "ADD_TABLET_LS_OP";
      break;
    }
    case REMOVE_TABLET_LS_OP : {
      name = "REMOVE_TABLET_LS_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

int64_t ObTabletLsCacheParam::to_string(char *buf, const int64_t buf_len) const
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

//---------------------------ObTabletLsCache-------------------------//
int ObTabletLsCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(TabletLsEntryHashMap::init(sub_bucket_size, TABLET_LS_ENTRY_MAP_LOCK, gc_tablet_ls_entry))) {
    LOG_WDIAG("fail to init hash tablet_ls of tablet_ls cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("tablet_ls_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObTabletLsCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObTabletLsCache::destroy()
{
  LOG_INFO("ObTabletLsCache will desotry");
  if (is_inited_) {
    // TODO oushen, modify later
    ObTabletLsCacheParam *param = NULL;
    ObTabletLsCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObTabletLsCacheParam *>(todo_lists_[i].popall()))) {
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

int ObTabletLsCache::get_tablet_ls_entry(
    event::ObContinuation *cont,
    const ObTabletLsEntryKey &key,
    ObTabletLsEntry **ppentry,
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
    LOG_DEBUG("begin to get tablet_ls location entry", K(ppentry), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObTabletLsEntry *tmp_entry = NULL;
    if (OB_FAIL(ObTabletLsCacheCont::get_tablet_ls_entry_local(*this, key, hash, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get tablet_ls entry", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppentry = tmp_entry;
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get_tablet_ls_entry, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObTabletLsCacheParam::SCHEDULE_TABLET_LS_CACHE_CONT_INTERVAL));
        ObTabletLsCacheCont *tablet_ls_cont = NULL;
        if (OB_ISNULL(tablet_ls_cont = op_alloc_args(ObTabletLsCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for tablet_ls cache continuation", K(ret));
        } else {
          tablet_ls_cont->action_.set_continuation(cont);
          tablet_ls_cont->mutex_ = cont->mutex_;
          tablet_ls_cont->hash_ = hash;
          tablet_ls_cont->ppentry_ = ppentry;
          tablet_ls_cont->key_ = key;

          SET_CONTINUATION_HANDLER(tablet_ls_cont, &ObTabletLsCacheCont::get_tablet_ls_entry);
          if (OB_ISNULL(self_ethread().schedule_in(tablet_ls_cont,
                  ObTabletLsCacheParam::SCHEDULE_TABLET_LS_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(tablet_ls_cont), K(ret));
          } else {
            action = &tablet_ls_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != tablet_ls_cont)) {
          tablet_ls_cont->destroy();
          tablet_ls_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppentry = NULL;
    }
  }
  return ret;
}

int ObTabletLsCache::add_tablet_ls_entry(ObTabletLsEntry &entry, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObTabletLsEntryKey key;
    entry.get_key(key);
    uint64_t hash = key.hash();
    LOG_DEBUG("add tablet_ls entry", K(part_num(hash)), K(entry), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObTabletLsEntry *tmp_entry = insert_entry(hash, key, &entry);
          if (NULL != tmp_entry) {
            LOG_DEBUG("remove from tablet_ls entry", KPC(tmp_entry));
            tmp_entry->set_deleted_state(); // used to update tc_tablet_ls_map
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
      ObTabletLsCacheParam *param = op_alloc(ObTabletLsCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for tablet_ls param", K(param), K(ret));
      } else {
        param->op_ = ObTabletLsCacheParam::ADD_TABLET_LS_OP;
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

int ObTabletLsCache::remove_tablet_ls_entry(const ObTabletLsEntryKey &key)
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
    ObTabletLsEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        entry = remove_entry(hash, key);
        LOG_INFO("this entry will be removed from tablet_ls cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
      }
    } else {
      ObTabletLsCacheParam *param = op_alloc(ObTabletLsCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObTabletLsCacheParam::REMOVE_TABLET_LS_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->entry_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObTabletLsCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObTabletLsCacheParam *pre = NULL;
    ObTabletLsCacheParam *cur = NULL;
    ObTabletLsCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObTabletLsCacheParam *>(todo_lists_[buck_id].popall()))) {
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
      ObTabletLsCacheParam *param = NULL;
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

int ObTabletLsCache::process(const int64_t buck_id, ObTabletLsCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObTabletLsCacheParam", K(buck_id), KPC(param));
    ObTabletLsEntry *entry = NULL;
    switch (param->op_) {
      case ObTabletLsCacheParam::ADD_TABLET_LS_OP: {
        entry = insert_entry(param->hash_, param->key_, param->entry_);
        if (NULL != entry) {
          entry->set_deleted_state(); // used to update tc_tablet_ls_map
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
      case ObTabletLsCacheParam::REMOVE_TABLET_LS_OP: {
        entry = remove_entry(param->hash_, param->key_);
        LOG_INFO("this entry will be removed from tablet_ls cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref(); // free old entry
          entry = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObTabletLsCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObTabletLsCache::gc_tablet_ls_entry(ObTabletLsEntry *entry)
{
  bool expired = false;
  ObRpcCacheCleaner *cleaner = self_ethread().rpc_cache_cleaner_;
  if ((NULL != cleaner) && (NULL != entry)) {
    if (cleaner->is_tablet_ls_entry_expired(*entry)) {
      LOG_INFO("this tablet_ls entry has expired, will be deleted", KPC(entry));
      expired = true;
      entry->set_deleted_state();
      entry->dec_ref();
    }
  }

  return expired;
}

ObTabletLsCache &get_global_tablet_ls_cache()
{
  static ObTabletLsCache tablet_ls_cache;
  return tablet_ls_cache;
}

int init_tablet_ls_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_NET];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_tablet_ls_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init tablet_ls_map", K(i), K(ret));
    }
  }
  return ret;
}

//TODO check need for Other thread LS zdw
int init_tablet_ls_map_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  ObEThread **ethreads = NULL;
  if (OB_ISNULL(ethreads = g_event_processor.event_thread_[ET_NET])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "fail to get ET_NET thread", K(ret));
  } else if (OB_ISNULL(ethreads[index])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "fail to get ET_NET thread", K(ret));
  } else {
    if (OB_ISNULL(ethreads[index]->tablet_ls_map_ = new (std::nothrow) ObTabletLsRefHashMap(ObModIds::OB_PROXY_TABLET_LS_ENTRY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObTabletLsRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->tablet_ls_map_->init())) {
      LOG_WDIAG("fail to init tablet_ls_map", K(ret));
    }
  }
  return ret;
}

int init_tablet_ls_map_for_one_thread(event::ObEThread *thread)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(thread)) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "unexpected thread", K(ret));
  } else {
    if (OB_ISNULL(thread->tablet_ls_map_
                  = new (std::nothrow) ObTabletLsRefHashMap(ObModIds::OB_PROXY_TABLET_LS_ENTRY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObTabletLsRefHashMap", K(ret));
    } else if (OB_FAIL(thread->tablet_ls_map_->init())) {
      LOG_WDIAG("fail to init tablet_ls_map", K(ret));
    }
  }
  return ret;
}

int ObTabletLsRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this tablet_ls entry will erase from tc map", KPC((*it)));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase tablet_ls entry", K(i), K(ret));
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
