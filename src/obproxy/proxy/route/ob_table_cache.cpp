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

#include "proxy/route/ob_table_cache.h"
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
//---------------------------ObTableCacheCont----------------------//
class ObTableCacheCont : public event::ObContinuation
{
public:
  explicit ObTableCacheCont(ObTableCache &table_cache)
    : ObContinuation(NULL), table_cache_(&table_cache), buf_entry_(NULL), ppentry_(NULL),
      hash_(0), key_() {}
  virtual ~ObTableCacheCont() {}

  void destroy();
  int get_table_entry(const int event, event::ObEvent *e);

  event::ObAction action_;
  ObTableCache *table_cache_;

  // here just use ObTableEntry to store the key_'s buffer
  ObTableEntry *buf_entry_;
  ObTableEntry **ppentry_;

  uint64_t hash_;
  ObTableEntryKey key_;
};

inline void ObTableCacheCont::ObTableCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  if (NULL != buf_entry_) {
    buf_entry_->dec_ref();
    buf_entry_ = NULL;
  }
  op_free(this);
}

int ObTableCacheCont::get_table_entry(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_table_entry started");

  if (action_.cancelled_) {
    LOG_DEBUG("cont::action has been cancelled", K(this));
    destroy();
  } else {
    ObProxyMutex *bucket_mutex = table_cache_->lock_for_key(hash_);
    MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
    if (lock_bucket.is_locked()) {
      if (OB_FAIL(table_cache_->run_todo_list(table_cache_->part_num(hash_)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        *ppentry_ = table_cache_->lookup_entry(hash_, key_);
        if (NULL != *ppentry_) {
          if (table_cache_->is_table_entry_expired(**ppentry_)) {
            // expire time mismatch
            LOG_INFO("the table entry is expired", "expire_time_us",
                     table_cache_->get_cache_expire_time_us(), KPC(*ppentry_));
            *ppentry_ = NULL;
            // remove the expired table entry in locked
            if (OB_FAIL(table_cache_->remove_table_entry(key_))) {
              LOG_WDIAG("fail to remove table entry", K_(key), K(ret));
            }
          } else {
            (*ppentry_)->inc_ref();
            LOG_DEBUG("cont::get_table_entry, entry found succ", KPC(*ppentry_));
          }
        } else {
          // non-existent, return NULL
          LOG_DEBUG("cont::get_table_entry, entry not found", K_(key));
        }
        lock_bucket.release();
        action_.continuation_->handle_event(TABLE_ENTRY_EVENT_LOOKUP_DONE, NULL);
        destroy();
      }
    } else {
      LOG_DEBUG("cont::get_table_entry MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObTableParam::SCHEDULE_TABLE_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObTableParam::SCHEDULE_TABLE_CACHE_CONT_INTERVAL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule in", K(ret));
      }
      he_ret = EVENT_CONT;
    }
  }

  return he_ret;
}

//---------------------------ObTableParam-------------------------//
const char *ObTableParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_TABLE_OP : {
      name = "INVALID_TABLE_OP";
      break;
    }
    case ADD_TABLE_OP : {
      name = "ADD_TABLE_OP";
      break;
    }
    case REMOVE_TABLE_OP : {
      name = "REMOVE_TABLE_OP";
      break;
    }
    case REMOVE_ALL_TABLE_OP : {
      name = "REMOVE_ALL_TABLE_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

int64_t ObTableParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("op_name", get_op_name(op_),
      K_(hash),
      K_(part_id),
      K_(key));
  J_COMMA();
  if (NULL != entry_) {
    J_KV(K_(*entry));
  }
  J_OBJ_END();
  return pos;
}

//--------------------------ObTableCacheHandlerCont-------------------------//
class ObTableCacheHandlerCont : public ObContinuation
{
public:
  enum ObTableCacheOp
  {
    ADD_TABLE_ENTRY_OP = 0,
  };

  ObTableCacheHandlerCont(ObTableCache &table_cache, ObTableEntry &table_entry)
    : ObContinuation(NULL), op_(ADD_TABLE_ENTRY_OP), table_cache_(table_cache),
      table_entry_(table_entry)
  {
    SET_HANDLER(&ObTableCacheHandlerCont::main_event);
  }

  virtual ~ObTableCacheHandlerCont() {}

  void destroy()
  {
    mutex_.release();
    op_free(this);
  }

  int main_event(int event, ObEvent *e)
  {
    UNUSED(event);
    UNUSED(e);
    int ret = OB_SUCCESS;

    switch (op_) {
      case ADD_TABLE_ENTRY_OP: {
        bool direct_add = false;
        if (OB_FAIL(table_cache_.add_table_entry(table_entry_, direct_add))) {
          LOG_WDIAG("fail to add table entry", K_(table_entry), K(direct_add));
          table_entry_.dec_ref(); // free the table entry
        }
        table_entry_.dec_ref();
        break;
      }
      default:
        break;
    }
    destroy();
    return EVENT_DONE;
  }

private:
  ObTableCacheOp op_;
  ObTableCache &table_cache_;
  ObTableEntry &table_entry_;
};

//---------------------------ObTableCache-------------------------//
int ObTableCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(TableEntryHashMap::init(sub_bucket_size, TABLE_ENTRY_MAP_LOCK, gc_table_entry))) {
    LOG_WDIAG("fail to init hash table of table cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("location_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObTableParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObTableCache::destroy()
{
  LOG_DEBUG("ObTableCache, will desotry");
  if (is_inited_) {
    ObTableParam *param = NULL;
    ObTableParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObTableParam *>(todo_lists_[i].popall()))) {
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

int ObTableCache::get_table_entry(
    event::ObContinuation *cont,
    const ObTableEntryKey &key,
    ObTableEntry **ppentry,
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
    LOG_DEBUG("begin to get table location entry", K(ppentry), K(key), K(cont), K(hash));

    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
    if (lock_bucket.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        *ppentry = lookup_entry(hash, key);
        if (NULL != *ppentry) {
          if (is_table_entry_expired(**ppentry)) {
            // expire time mismatch
            LOG_DEBUG("the table entry is expired", "expire_time_us",
                      get_cache_expire_time_us(), KPC(*ppentry));
            *ppentry = NULL;
            // remove the expired table entry in locked
            if (OB_FAIL(remove_table_entry(key))) {
              LOG_WDIAG("fail to remove table entry", K(key), K(ret));
            }
          } else {
            (*ppentry)->inc_ref();
            LOG_DEBUG("get_table_entry, entry found succ", KPC(*ppentry));
          }
        } else {
          // non-existent, return NULL
          LOG_DEBUG("get_table_entry, entry not found", K(key));
        }
      }
    } else {
      LOG_DEBUG("get_table_entry, trylock failed, reschedule cont interval(ns)",
                LITERAL_K(ObTableParam::SCHEDULE_TABLE_CACHE_CONT_INTERVAL));
      ObTableCacheCont *table_cont = NULL;
      if (OB_ISNULL(table_cont = op_alloc_args(ObTableCacheCont, *this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for table cache continuation", K(ret));
      } else if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(*key.name_, key.cr_version_,
          key.cr_id_, table_cont->buf_entry_))) { // use to save name buf
        LOG_WDIAG("fail to alloc and init pl entry", K(key), K(ret));
      } else {
        table_cont->buf_entry_->get_key(table_cont->key_);
        table_cont->action_.set_continuation(cont);
        table_cont->mutex_ = cont->mutex_;
        table_cont->hash_ = hash;
        table_cont->ppentry_ = ppentry;

        SET_CONTINUATION_HANDLER(table_cont, &ObTableCacheCont::get_table_entry);
        if (OB_ISNULL(cont->mutex_->thread_holding_)
            || OB_ISNULL(cont->mutex_->thread_holding_->schedule_in(table_cont,
                ObTableParam::SCHEDULE_TABLE_CACHE_CONT_INTERVAL))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WDIAG("fail to schedule imm", K(table_cont), K(ret));
        } else {
          action = &table_cont->action_;
        }
      }
      if (OB_FAIL(ret) && OB_LIKELY(NULL != table_cont)) {
        table_cont->destroy();
        table_cont = NULL;
      }
    }
    if (OB_FAIL(ret)) {
      *ppentry = NULL;
    }
  }
  return ret;
}

int ObTableCache::update_entry(ObTableEntry &entry, const ObTableEntryKey &key,
    const uint64_t hash)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObTableEntry *tmp_entry = insert_entry(hash, key, &entry);
    if (NULL != tmp_entry) {
      tmp_entry->set_deleted_state(); // used to update tc_table_map
      tmp_entry->dec_ref(); // paired inc_ref in alloc_and_init_pl_entry()
      tmp_entry = NULL;
    }
  }
  return ret;
}

int ObTableCache::add_table_entry(ObTableEntry &entry, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObTableEntryKey key;
    entry.get_key(key);

    uint64_t hash = key.hash();
    LOG_DEBUG("add table location", K(part_num(hash)), K(entry), K(direct_add), K(hash), K(key));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else if (OB_FAIL(update_entry(entry, key, hash))) {
          LOG_WDIAG("fail to update_entry", K(entry), K(ret));
        }
      } else {
        direct_add = true;
      }
    }

    if (direct_add) {
      // add todo list
      ObTableParam *param = op_alloc(ObTableParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for table param", K(param), K(ret));
      } else {
        param->op_ = ObTableParam::ADD_TABLE_OP;
        param->hash_ = hash;
        entry.get_key(param->key_);
        entry.inc_ref(); // paired dec_ref in run_todo_list()
        param->entry_ = &entry;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObTableCache::remove_table_entry(const ObTableEntryKey &key)
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
    ObTableEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        entry = remove_entry(hash, key);
        LOG_INFO("this entry will be removed from table cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref(); // paired inc_ref in alloc_and_init_pl_entry()
          entry = NULL;
        }
      }
    } else {
      ObTableParam *param = op_alloc(ObTableParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        // just use this entry to save entry name;
        if (OB_FAIL(ObTableEntry::alloc_and_init_table_entry(*key.name_, key.cr_version_, key.cr_id_, entry))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for location entry", K(key), K(entry), K(ret));
        } else  {
          param->op_ = ObTableParam::REMOVE_TABLE_OP;
          param->hash_ = hash;
          entry->get_key(param->key_);
          entry->inc_ref(); // paired dec_ref in run_todo_list()
          param->entry_ = entry;
          todo_lists_[part_num(hash)].push(param);
        }
      }
      if (OB_FAIL(ret)) {
        if (NULL != entry) {
          entry->dec_ref();
          entry = NULL;
        }
        if (NULL != param) {
          op_free(param);
          param = NULL;
        }
      }
    }
  }
  return ret;
}

int ObTableCache::remove_all_table_entry()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    ObTableEntry *entry = NULL;
    ObTableEntry *tmp_entry = NULL;
    TableIter it;
    for (int64_t part = 0; (part < MT_HASHTABLE_PARTITIONS) && (OB_SUCC(ret)); ++part) {
      ObProxyMutex *bucket_mutex = lock_for_key(part);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part))) {
          LOG_WDIAG("fail to run todo list", K(part), K(ret));
        } else {
          entry = first_entry(part, it);
          while (NULL != entry) {
            tmp_entry = remove_entry(part, it);
            if (tmp_entry != entry) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WDIAG("entry mismatch", K(tmp_entry), K(entry), K(ret));
            } else {
              entry->set_deleted_state();
              entry->dec_ref();
              entry = cur_entry(part, it);
            }
          }
        }
      } else {
        ObTableParam *param = op_alloc(ObTableParam);
        if (NULL != param) {
          param->op_ = ObTableParam::REMOVE_ALL_TABLE_OP;
          param->part_id_ = part;
          todo_lists_[part].push(param);
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for congest request parameter", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObTableParam *pre = NULL;
    ObTableParam *cur = NULL;
    ObTableParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObTableParam *>(todo_lists_[buck_id].popall()))) {
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
      ObTableParam *param = NULL;
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

int ObTableCache::process(const int64_t buck_id, ObTableParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObTableParam", K(buck_id), K(*param));
    ObTableEntry *old_entry = NULL;
    switch (param->op_) {
      case ObTableParam::ADD_TABLE_OP: {
        if (OB_FAIL(update_entry(*param->entry_, param->key_, param->hash_))) {
          LOG_WDIAG("fail to update_entry", K(param), K(ret));
        }
        if (NULL != param->entry_) {
          // dec_ref, it was inc before add param into todo list
          param->entry_->dec_ref();
          param->entry_ = NULL;
        }
        break;
      }
      case ObTableParam::REMOVE_TABLE_OP: {
        old_entry = remove_entry(param->hash_, param->key_);
        LOG_INFO("this entry will be removed from table cache", KPC(old_entry));
        if (NULL != old_entry) {
          old_entry->set_deleted_state();
          old_entry->dec_ref(); // free old entry
          old_entry = NULL;
        }
        if (NULL != param->entry_) {
          // dec_ref, it was inc before add param into todo list
          param->entry_->dec_ref();
          // free the str_buf_ entry, this entry used to store names in key;
          param->entry_->dec_ref();
          param->entry_ = NULL;
        }
        break;
      }
      case ObTableParam::REMOVE_ALL_TABLE_OP: {
        TableIter it;
        ObTableEntry *tmp_entry = NULL;
        old_entry = first_entry(param->part_id_, it);
        while (NULL != old_entry) {
          tmp_entry = remove_entry(param->part_id_, it);
          if (tmp_entry != old_entry) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("entry dismatch", K(tmp_entry), K(old_entry), K(ret));
          } else {
            old_entry->set_deleted_state();
            old_entry->dec_ref();
            old_entry = cur_entry(param->part_id_, it);
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObTableCache::process unrecognized op",
                  "op", param->op_, K(buck_id), K(*param), K(ret));
        break;
      }
    }
  }
  return ret;
}

int ObTableCache::add_table_entry(ObTableCache &table_cache, ObTableEntry &entry)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!entry.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid argument", K(entry), K(ret));
  } else {
    entry.inc_ref();
    ObTableCacheHandlerCont *handler_cont = op_alloc_args(ObTableCacheHandlerCont, table_cache, entry);
    if (OB_ISNULL(handler_cont)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to allocate memory for table cache handler continuation", K(ret));
    } else {
      g_event_processor.schedule_imm(handler_cont, ET_CALL);
    }
  }
  return ret;
}

bool ObTableCache::gc_table_entry(ObTableEntry *entry)
{
  bool expired = false;
  ObCacheCleaner *cleaner = self_ethread().cache_cleaner_;
  if ((NULL != cleaner) && (NULL != entry)) {
    if (cleaner->is_table_entry_expired(*entry)) {
      LOG_INFO("this table entry has expired, will be deleted", KPC(entry));
      expired = true;
      entry->set_deleted_state();
      entry->dec_ref();
      if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
        ObProxyMutex *mutex_ = this_ethread()->mutex_;
        PROCESSOR_INCREMENT_DYN_STAT(GC_TABLE_ENTRY_FROM_GLOBAL_CACHE);
      }
    }
  }

  return expired;
}

ObTableCache &get_global_table_cache()
{
  static ObTableCache tl_manager;
  return tl_manager;
}


int init_table_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_table_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init table_map", K(i), K(ret));
    }
  }
  return ret;
}

int init_table_map_for_one_thread(int64_t index)
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
    if (OB_ISNULL(ethreads[index]->table_map_ = new (std::nothrow) ObTableRefHashMap(ObModIds::OB_PROXY_TABLE_ENTRY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObTableRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->table_map_->init())) {
      LOG_WDIAG("fail to init table_map", K(ret));
    } else {
      LOG_DEBUG("succ to init table_map", K(ET_CALL), "ethread", reinterpret_cast<const void*>(ethreads[index]),
                "table_map", reinterpret_cast<const void*>(ethreads[index]->table_map_), K(ret));
    }
  }
  return ret;
}

int ObTableRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this table entry will erase from tc map", KPC(*it));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase table entry", K(i), K(ret));
        }
        if ((NULL != this_ethread()) && (NULL != this_ethread()->mutex_)) {
          ObProxyMutex *mutex_ = this_ethread()->mutex_;
          PROCESSOR_INCREMENT_DYN_STAT(GC_TABLE_ENTRY_FROM_THREAD_CACHE);
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
