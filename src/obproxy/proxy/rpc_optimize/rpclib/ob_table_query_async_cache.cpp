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

#include "proxy/rpc_optimize/rpclib/ob_table_query_async_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObTableQueryAsyncCacheCont : public event::ObContinuation
{
public:
  explicit ObTableQueryAsyncCacheCont(ObTableQueryAsyncCache &table_query_async_cache)
    : ObContinuation(NULL), table_query_async_cache_(table_query_async_cache), ppentry_(NULL),
      hash_(0), is_add_building_entry_(false), key_(0) {}
  virtual ~ObTableQueryAsyncCacheCont() {}
  void destroy();
  int get_table_query_async_entry(const int event, ObEvent *e);
  static int get_table_query_async_entry_local(ObTableQueryAsyncCache &table_query_async_cache,
                                                const uint64_t key,
                                                const uint64_t hash,
                                                bool &is_locked,
                                                ObTableQueryAsyncEntry *&table_query_async_entry);


  static int add_building_table_query_async_entry(ObTableQueryAsyncCache &table_query_async_cache,
                                                  const uint64_t key);
  event::ObAction action_;
  ObTableQueryAsyncCache &table_query_async_cache_;
  ObTableQueryAsyncEntry **ppentry_;
  uint64_t hash_;
  bool is_add_building_entry_;
  uint64_t key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncCacheCont);
};

int64_t ObTableQueryAsyncCacheParam::to_string(char *buf, const int64_t buf_len) const
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

inline void ObTableQueryAsyncCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

int ObTableQueryAsyncCacheCont::get_table_query_async_entry(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_table_query_async_entry started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObTableQueryAsyncEntry *tmp_entry = NULL;
    if (OB_FAIL(get_table_query_async_entry_local(table_query_async_cache_, key_, hash_, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get table query async entry", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_table_query_async_entry MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObTableQueryAsyncCacheParam::SCHEDULE_TABLE_QUERY_ASYNC_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObTableQueryAsyncCacheParam::SCHEDULE_TABLE_QUERY_ASYNC_CACHE_CONT_INTERVAL))) {
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
      action_.continuation_->handle_event(RPC_QUERY_ASYNC_ENTRY_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObTableQueryAsyncCacheCont::get_table_query_async_entry_local(
    ObTableQueryAsyncCache &table_query_async_cache,
    const uint64_t key,
    const uint64_t hash,
    bool &is_locked,
    ObTableQueryAsyncEntry *&entry)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  entry = NULL;

  ObProxyMutex *bucket_mutex = table_query_async_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(table_query_async_cache.run_todo_list(table_query_async_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      entry = table_query_async_cache.lookup_entry(hash, key);
      if (NULL != entry) {
        if (table_query_async_cache.is_table_query_async_entry_expired_in_time_mode(*entry)
            && entry->is_avail_state()) {
          entry->set_dirty_state();
        }
        entry->inc_ref();
        LOG_DEBUG("cont::get_table_query_async_entry_local, entry found succ", KPC(entry));
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == entry) {
      LOG_DEBUG("cont::get_table_query_async_entry_local, entry not found", K(key));
    }
    lock_bucket.release();
  }

  return ret;
}

int ObTableQueryAsyncCacheCont::add_building_table_query_async_entry(ObTableQueryAsyncCache &table_query_async_cache,
                                                                     uint64_t key)
{
  int ret = OB_SUCCESS;
  UNUSED(table_query_async_cache);
  UNUSED(key);
  // ObTableQueryAsyncEntry *entry = NULL;

  // if (OB_ISNULL(key.name_)) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_WDIAG("invalid ObTableQueryAsyncEntryKey", K(key), K(ret));
  // } else if (OB_FAIL(ObTableQueryAsyncEntry::alloc_and_init_table_query_async_entry(*key.name_, key.cr_version_, key.cr_id_, entry))) {
  //   LOG_WDIAG("fail to alloc and init table query async entry", K(key), K(ret));
  // } else if (OB_ISNULL(entry)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WDIAG("part entry is NULL", K(entry), K(ret));
  // } else {
  //   entry->set_building_state();
  //   if (OB_FAIL(table_query_async_cache.add_table_query_async_entry(*entry, false))) {
  //     LOG_WDIAG("fail to add part entry", KPC(entry), K(ret));
  //     entry->dec_ref();
  //     entry = NULL;
  //   } else {
  //     LOG_INFO("add building part entry succ", KPC(entry));
  //     entry = NULL;
  //   }
  // }

  return ret;
}

const char *ObTableQueryAsyncCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_TABLE_QUERY_ASYNC_OP : {
      name = "INVALID_TABLE_QUERY_ASYNC_OP";
      break;
    }
    case ADD_TABLE_QUERY_ASYNC_OP : {
      name = "ADD_TABLE_QUERY_ASYNC_OP";
      break;
    }
    case REMOVE_TABLE_QUERY_ASYNC_OP : {
      name = "REMOVE_TABLE_QUERY_ASYNC_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

//---------------------------ObTableQueryAsyncCache-------------------------//
int ObTableQueryAsyncCache::init(const int64_t bucket_size)
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
  } else if (OB_FAIL(TableQueryAsyncEntryHashMap::init(sub_bucket_size, RPC_QUERY_ASYNC_ENTRY_MAP_LOCK, gc_table_query_async_entry))) {
    LOG_WDIAG("fail to init hash index of table query async cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("table_query_async_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObTableQueryAsyncCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObTableQueryAsyncCache::destroy()
{
  LOG_INFO("ObTableQueryAsyncCache will desotry");
  if (is_inited_) {
    ObTableQueryAsyncCacheParam *param = NULL;
    ObTableQueryAsyncCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObTableQueryAsyncCacheParam *>(todo_lists_[i].popall()))) {
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

int ObTableQueryAsyncCache::get_table_query_async_entry(
    event::ObContinuation *cont,
    uint64_t key,
    ObTableQueryAsyncEntry **ppentry,
    ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(ppentry) || OB_ISNULL(cont)
             || OB_UNLIKELY(0 == key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid arugument", K(ppentry), K(key), K(cont), K(ret));
  } else {
    uint64_t hash = key;
    LOG_DEBUG("begin to get table query async entry", K(ppentry), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObTableQueryAsyncEntry *tmp_entry = NULL;
    if (OB_FAIL(ObTableQueryAsyncCacheCont::get_table_query_async_entry_local(*this, key, hash, is_locked, tmp_entry))) {
      if (NULL != tmp_entry) {
        tmp_entry->dec_ref();
        tmp_entry = NULL;
      }
      LOG_WDIAG("fail to get table query async entry", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppentry = tmp_entry;
        tmp_entry = NULL;
      } else {
        LOG_DEBUG("get_table_query_async_entry, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObTableQueryAsyncCacheParam::SCHEDULE_TABLE_QUERY_ASYNC_CACHE_CONT_INTERVAL));
        ObTableQueryAsyncCacheCont *table_query_async_cont = NULL;
        if (OB_ISNULL(table_query_async_cont = op_alloc_args(ObTableQueryAsyncCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for table query async cache continuation", K(ret));
        } else {
          table_query_async_cont->action_.set_continuation(cont);
          table_query_async_cont->mutex_ = cont->mutex_;
          table_query_async_cont->hash_ = hash;
          table_query_async_cont->ppentry_ = ppentry;
          table_query_async_cont->key_ = key;

          SET_CONTINUATION_HANDLER(table_query_async_cont, &ObTableQueryAsyncCacheCont::get_table_query_async_entry);
          if (OB_ISNULL(self_ethread().schedule_in(table_query_async_cont,
                  ObTableQueryAsyncCacheParam::SCHEDULE_TABLE_QUERY_ASYNC_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(table_query_async_cont), K(ret));
          } else {
            action = &table_query_async_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != table_query_async_cont)) {
          table_query_async_cont->destroy();
          table_query_async_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppentry = NULL;
    }
  }
  return ret;
}

int ObTableQueryAsyncCache::add_table_query_async_entry(ObTableQueryAsyncEntry &entry, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t key = entry.get_client_query_session_id();
    uint64_t hash = key;
    LOG_DEBUG("add table query async entry", K(part_num(hash)), K(entry), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObTableQueryAsyncEntry *tmp_entry = insert_entry(hash, key, &entry);
          if (NULL != tmp_entry) {
            LOG_DEBUG("remove from table query async entry", KPC(tmp_entry));
            tmp_entry->set_deleted_state();
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
      ObTableQueryAsyncCacheParam *param = op_alloc(ObTableQueryAsyncCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for table query async param", K(param), K(ret));
      } else {
        param->op_ = ObTableQueryAsyncCacheParam::ADD_TABLE_QUERY_ASYNC_OP;
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

int ObTableQueryAsyncCache::remove_table_query_async_entry(const uint64_t key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(0 == key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(key), K(ret));
  } else {
    uint64_t hash = key;
    ObTableQueryAsyncEntry *entry = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        entry = remove_entry(hash, key);
        LOG_INFO("this entry will be removed from table query async cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref();
          entry = NULL;
        }
      }
    } else {
      ObTableQueryAsyncCacheParam *param = op_alloc(ObTableQueryAsyncCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObTableQueryAsyncCacheParam::REMOVE_TABLE_QUERY_ASYNC_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->entry_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObTableQueryAsyncCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObTableQueryAsyncCacheParam *pre = NULL;
    ObTableQueryAsyncCacheParam *cur = NULL;
    ObTableQueryAsyncCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObTableQueryAsyncCacheParam *>(todo_lists_[buck_id].popall()))) {
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
      ObTableQueryAsyncCacheParam *param = NULL;
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

int ObTableQueryAsyncCache::process(const int64_t buck_id, ObTableQueryAsyncCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObTableQueryAsyncCacheParam", K(buck_id), KPC(param));
    ObTableQueryAsyncEntry *entry = NULL;
    switch (param->op_) {
      case ObTableQueryAsyncCacheParam::ADD_TABLE_QUERY_ASYNC_OP: {
        entry = insert_entry(param->hash_, param->key_, param->entry_);
        if (NULL != entry) {
          entry->set_deleted_state();
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
      case ObTableQueryAsyncCacheParam::REMOVE_TABLE_QUERY_ASYNC_OP: {
        entry = remove_entry(param->hash_, param->key_);
        LOG_INFO("this entry will be removed from table query async cache", KPC(entry));
        if (NULL != entry) {
          entry->set_deleted_state();
          entry->dec_ref(); // free old entry
          entry = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObTableQueryAsyncCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObTableQueryAsyncCache::gc_table_query_async_entry(ObTableQueryAsyncEntry *entry)
{
  UNUSED(entry);
  // gc_table_query_async_entry do nothing
  return false;
}

ObTableQueryAsyncCache &get_global_table_query_async_cache()
{
  static ObTableQueryAsyncCache table_query_async_cache;
  return table_query_async_cache;
}

int init_table_query_async_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_table_query_async_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init table_query_async_map", K(i), K(ret));
    }
  }
  return ret;
}

int init_table_query_async_map_for_one_thread(int64_t index)
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
    if (OB_ISNULL(ethreads[index]->table_query_async_map_ = new (std::nothrow) ObTableQueryAsyncRefHashMap(ObModIds::OB_PROXY_RPC_TABLE_QUERY_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObIndexRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->table_query_async_map_->init())) {
      LOG_WDIAG("fail to init table query async map", K(ret));
    }
  }
  return ret;
}

int ObTableQueryAsyncRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this table query async entry will erase from tc map", KPC((*it)));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase table query async entry", K(i), K(ret));
        }
      }
    }
  }
  return ret;
}
}
}
}