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

#include "proxy/route/ob_routine_cache.h"
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
//---------------------------ObRoutineCacheCont----------------------//
class ObRoutineCacheCont : public event::ObContinuation
{
public:
  explicit ObRoutineCacheCont(ObRoutineCache &routine_cache)
    : ObContinuation(NULL), routine_cache_(routine_cache),
      ppentry_(NULL), hash_(0), is_add_building_entry_(false), key_(),
      name_buf_(NULL), name_buf_len_(0) {}
  virtual ~ObRoutineCacheCont() {}
  void destroy();
  int deep_copy_key(const ObRoutineEntryKey &other);
  int get_routine_entry(const int event, ObEvent *e);
  static int get_routine_entry_local(ObRoutineCache &routine_cache,
                                     const ObRoutineEntryKey &key,
                                     const uint64_t hash,
                                     const bool is_add_building_entry,
                                     bool &is_locked,
                                     ObRoutineEntry *&routine);


   static int add_building_routine_entry(ObRoutineCache &routine_cache,
                                         const ObRoutineEntryKey &key);
  event::ObAction action_;
  ObRoutineCache &routine_cache_;

  ObRoutineEntry **ppentry_;

  uint64_t hash_;
  bool is_add_building_entry_;
  ObRoutineEntryKey key_;
  char *name_buf_;
  int64_t name_buf_len_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRoutineCacheCont);
};

inline void ObRoutineCacheCont::ObRoutineCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  if (NULL != name_buf_) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }
  key_.reset();
  op_free(this);
}

int ObRoutineCacheCont::deep_copy_key(const ObRoutineEntryKey &other)
{
  int ret = OB_SUCCESS;
  if (NULL != name_buf_ && name_buf_len_ > 0) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }

  const int64_t name_size = other.name_->get_total_str_len();
  const int64_t obj_size = sizeof(ObTableEntryName);
  name_buf_len_ = name_size + obj_size;
  name_buf_ = static_cast<char *>(op_fixed_mem_alloc(name_buf_len_));
  if (OB_ISNULL(name_buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K_(name_buf_len), K(ret));
  } else {
    ObTableEntryName *name = new (name_buf_) ObTableEntryName();
    if (OB_FAIL(name->deep_copy(*other.name_, name_buf_ + obj_size, name_size))) {
      LOG_WDIAG("fail to deep copy table entry name", K(ret));
    } else {
      key_.name_ = name;
      key_.cr_version_ = other.cr_version_;
    }
  }

  if (OB_FAIL(ret) && (NULL != name_buf_)) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }
  return ret;
}

int ObRoutineCacheCont::get_routine_entry(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_routine_entry started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObRoutineEntry *tmp_entry = NULL;
    if (OB_FAIL(get_routine_entry_local(routine_cache_, key_, hash_,
            is_add_building_entry_, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get routine entry", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_routine_entry MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObRoutineCacheParam::SCHEDULE_ROUTINE_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObRoutineCacheParam::SCHEDULE_ROUTINE_CACHE_CONT_INTERVAL))) {
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
      action_.continuation_->handle_event(ROUTINE_ENTRY_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObRoutineCacheCont::get_routine_entry_local(
    ObRoutineCache &routine_cache,
    const ObRoutineEntryKey &key,
    const uint64_t hash,
    const bool is_add_building_entry,
    bool &is_locked,
    ObRoutineEntry *&entry)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  entry = NULL;

  ObProxyMutex *bucket_mutex = routine_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(routine_cache.run_todo_list(routine_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      entry = routine_cache.lookup_entry(hash, key);
      if (NULL != entry) {
        if (routine_cache.is_routine_entry_expired(*entry)) {
          LOG_INFO("the routine entry is expired", "expire_time_us",
                   routine_cache.get_cache_expire_time_us(), KPC(entry));
          entry = NULL;
          // remove the expired routine entry in locked
          if (OB_FAIL(routine_cache.remove_routine_entry(key))) {
            LOG_WDIAG("fail to remove routine entry", K(key), K(ret));
          }
        } else {
          entry->inc_ref();
          LOG_DEBUG("cont::get_routine_entry_local, entry found succ", KPC(entry));
        }
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == entry) {
      if (is_add_building_entry) {
        if (OB_FAIL(add_building_routine_entry(routine_cache, key))) {
          LOG_WDIAG("fail to building routine entry", K(key), K(ret));
        } else {
          // nothing
        }
      } else {
        LOG_DEBUG("cont::get_routine_entry_local, entry not found", K(key));
      }
    }
    lock_bucket.release();
  }

  return ret;
}

int ObRoutineCacheCont::add_building_routine_entry(ObRoutineCache &routine_cache,
                                                   const ObRoutineEntryKey &key)
{
  int ret = OB_SUCCESS;
  ObRoutineEntry *entry = NULL;
  ObString empty_sql;
  if (OB_FAIL(ObRoutineEntry::alloc_and_init_routine_entry(*key.name_, key.cr_version_, key.cr_id_,
      empty_sql, entry))) {
    LOG_WDIAG("fail to alloc and init routine entry", K(key), K(empty_sql), K(ret));
  } else if (OB_ISNULL(entry)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("routine entry is NULL", K(entry), K(ret));
  } else {
    entry->set_building_state();
    if (OB_FAIL(routine_cache.add_routine_entry(*entry, false))) {
      LOG_WDIAG("fail to add routine entry", KPC(entry), K(ret));
      entry->dec_ref();
      entry = NULL;
    } else {
      LOG_INFO("add building routine entry succ", KPC(entry));
      entry = NULL;
    }
  }

  return ret;
}

//---------------------------ObRoutineCacheParam-------------------------//
int ObRoutineCacheParam::deep_copy_key(const ObRoutineEntryKey &other)
{
  int ret = OB_SUCCESS;

  if (NULL != name_buf_ && name_buf_len_ > 0) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }

  const int64_t name_size = other.name_->get_total_str_len();
  const int64_t obj_size = sizeof(ObTableEntryName);
  name_buf_len_ = name_size + obj_size;
  name_buf_ = static_cast<char *>(op_fixed_mem_alloc(name_buf_len_));
  if (OB_ISNULL(name_buf_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K_(name_buf_len), K(ret));
  } else {
    ObTableEntryName *name = new (name_buf_) ObTableEntryName();
    if (OB_FAIL(name->deep_copy(*other.name_, name_buf_ + obj_size, name_size))) {
      LOG_WDIAG("fail to deep copy table entry name", K(ret));
    } else {
      key_ = other;
      key_.name_ = name;
    }
  }

  if (OB_FAIL(ret) && (NULL != name_buf_)) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }
  return ret;
}

const char *ObRoutineCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_ROUTINE_OP : {
      name = "INVALID_ROUTINE_OP";
      break;
    }
    case ADD_ROUTINE_OP : {
      name = "ADD_ROUTINE_OP";
      break;
    }
    case REMOVE_ROUTINE_OP : {
      name = "REMOVE_ROUTINE_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

int64_t ObRoutineCacheParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("op_name", get_op_name(op_),
      K_(hash),
      K_(key));
  J_COMMA();
  J_KV(KPC_(entry));
  J_OBJ_END();
  return pos;
}

//---------------------------ObRoutineCache-------------------------//
int ObRoutineCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start init ObTableQueryAsyncCache");
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(RoutineEntryHashMap::init(sub_bucket_size, ROUTINE_ENTRY_MAP_LOCK, gc_routine_entry))) {
    LOG_WDIAG("fail to init hash routine of routine cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("routine_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObRoutineCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObRoutineCache::destroy()
{
  LOG_INFO("ObRoutineCache will desotry");
  if (is_inited_) {
    // TODO modify later
    ObRoutineCacheParam *param = NULL;
    ObRoutineCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObRoutineCacheParam *>(todo_lists_[i].popall()))) {
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

int ObRoutineCache::get_routine_entry(
    event::ObContinuation *cont,
    const ObRoutineEntryKey &key,
    ObRoutineEntry **ppentry,
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
    LOG_DEBUG("begin to get routine location entry", K(ppentry), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObRoutineEntry *tmp_entry = NULL;
    if (OB_FAIL(ObRoutineCacheCont::get_routine_entry_local(*this, key, hash,
            is_add_building_entry, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get routine entry", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppentry = tmp_entry;
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get_routine_entry, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObRoutineCacheParam::SCHEDULE_ROUTINE_CACHE_CONT_INTERVAL));
        ObRoutineCacheCont *routine_cont = NULL;
        if (OB_ISNULL(routine_cont = op_alloc_args(ObRoutineCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for routine cache continuation", K(ret));
        } else if (OB_FAIL(routine_cont->deep_copy_key(key))) { // use to save name buf
          LOG_WDIAG("fail to deep_copy_key", K(key), K(ret));
        } else {
          routine_cont->action_.set_continuation(cont);
          routine_cont->mutex_ = cont->mutex_;
          routine_cont->hash_ = hash;
          routine_cont->ppentry_ = ppentry;
          routine_cont->is_add_building_entry_ = is_add_building_entry;

          SET_CONTINUATION_HANDLER(routine_cont, &ObRoutineCacheCont::get_routine_entry);
          if (OB_ISNULL(self_ethread().schedule_in(routine_cont,
                  ObRoutineCacheParam::SCHEDULE_ROUTINE_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(routine_cont), K(ret));
          } else {
            action = &routine_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != routine_cont)) {
          routine_cont->destroy();
          routine_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppentry = NULL;
    }
  }
  return ret;
}

int ObRoutineCache::add_routine_entry(ObRoutineEntry &entry, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObRoutineEntryKey key;
    entry.get_key(key);
    uint64_t hash = key.hash();
    LOG_DEBUG("add routine location", K(part_num(hash)), K(entry), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObRoutineEntry *tmp_entry = insert_entry(hash, key, &entry);
          if (NULL != tmp_entry) {
            tmp_entry->set_deleted_state(); // used to update tc_routine_map
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
      ObRoutineCacheParam *param = op_alloc(ObRoutineCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for routine param", K(param), K(ret));
      } else {
        param->op_ = ObRoutineCacheParam::ADD_ROUTINE_OP;
        param->hash_ = hash;
        // No deep copy is needed, because the key here is obtained from the entry
        param->key_ = key;
        entry.inc_ref();
        param->entry_ = &entry;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRoutineCache::remove_routine_entry(const ObRoutineEntryKey &key)
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
    ObRoutineEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        entry = remove_entry(hash, key);
        LOG_INFO("this entry will be removed from routine cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
      }
    } else {
      ObRoutineCacheParam *param = op_alloc(ObRoutineCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObRoutineCacheParam::REMOVE_ROUTINE_OP;
        param->hash_ = hash;
        param->deep_copy_key(key);
        param->entry_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRoutineCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObRoutineCacheParam *pre = NULL;
    ObRoutineCacheParam *cur = NULL;
    ObRoutineCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObRoutineCacheParam *>(todo_lists_[buck_id].popall()))) {
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
      ObRoutineCacheParam *param = NULL;
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

int ObRoutineCache::process(const int64_t buck_id, ObRoutineCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObRoutineCacheParam", K(buck_id), KPC(param));
    ObRoutineEntry *entry = NULL;
    switch (param->op_) {
      case ObRoutineCacheParam::ADD_ROUTINE_OP: {
        entry = insert_entry(param->hash_, param->key_, param->entry_);
        if (NULL != entry) {
          entry->set_deleted_state(); // used to update tc_routine_map
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
      case ObRoutineCacheParam::REMOVE_ROUTINE_OP: {
        entry = remove_entry(param->hash_, param->key_);
        LOG_INFO("this entry will be removed from routine cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref(); // free old entry
          entry = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObRoutineCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObRoutineCache::gc_routine_entry(ObRoutineEntry *entry)
{
  bool expired = false;
  ObCacheCleaner *cleaner = self_ethread().cache_cleaner_;
  if ((NULL != cleaner) && (NULL != entry)) {
    if (cleaner->is_routine_entry_expired(*entry)) {
      LOG_INFO("this routine entry has expired, will be deleted", KPC(entry));
      expired = true;
      entry->set_deleted_state();
      entry->dec_ref();
     if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
        ObProxyMutex *mutex_ = this_ethread()->mutex_;
        PROCESSOR_INCREMENT_DYN_STAT(GC_ROUTINE_ENTRY_FROM_GLOBAL_CACHE);
      }
    }
  }

  return expired;
}

ObRoutineCache &get_global_routine_cache()
{
  static ObRoutineCache routine_cache;
  return routine_cache;
}

int init_routine_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_routine_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init routine_map", K(i), K(ret));
    }
  }
  return ret;
}

int init_routine_map_for_one_thread(int64_t index)
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
    if (OB_ISNULL(ethreads[index]->routine_map_ = new (std::nothrow) ObRoutineRefHashMap(ObModIds::OB_PROXY_ROUTINE_ENTRY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObRoutineRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->routine_map_->init())) {
      LOG_WDIAG("fail to init routine_map", K(ret));
    }
  }
  return ret;
}

int ObRoutineRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this routine entry will erase from tc map", KPC((*it)));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase routine entry", K(i), K(ret));
        }

        if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
          ObProxyMutex *mutex_ = this_ethread()->mutex_;
          PROCESSOR_INCREMENT_DYN_STAT(GC_ROUTINE_ENTRY_FROM_THREAD_CACHE);
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
