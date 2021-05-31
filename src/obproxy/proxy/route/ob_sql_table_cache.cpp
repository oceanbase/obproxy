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
#include "proxy/route/ob_sql_table_cache.h"
#include "stat/ob_lock_stats.h"
#include "lib/string/ob_string.h"

using namespace obsys;
using namespace oceanbase::common;
using namespace oceanbase::obproxy::obutils;
using namespace oceanbase::obproxy::event;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{

ObSqlTableCache &get_global_sql_table_cache()
{
  static ObSqlTableCache g_sql_table_cache;
  return g_sql_table_cache;
}

//----------ObSqlTableCache----------
int ObSqlTableCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sql_table_cache init twice", K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(SqlTableEntryHashMap::init(sub_bucket_size, SQL_TABLE_ENTRY_MAP_LOCK, gc_sql_table_entry))) {
    LOG_WARN("fail to init hash table of sql table cache", K(sub_bucket_size), K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObSqlTableCache::destroy()
{
  LOG_INFO("ObSqlTableCache will be destroyed");
}

void ObSqlTableCache::get_sql_table_entry_from_thread_cache(const ObSqlTableEntryKey &key, ObSqlTableEntry *&entry)
{
  entry = NULL;
  ObSqlTableRefHashMap &sql_table_map = self_ethread().get_sql_table_map();
  entry = sql_table_map.get(key); // get will inc entry's ref
  if (NULL != entry && !entry->is_avail_state()) {
    entry->dec_ref();
    entry = NULL;
  }
  if (NULL != entry) {
    LOG_DEBUG("succ to get ObSqlTableEntry from thread cache", KPC(entry));
  }
}

int ObSqlTableCache::get_table_name(const ObSqlTableEntryKey &key, char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf), K(len), K(key), K(ret));
  } else {
    ObSqlTableEntry *entry;
    get_sql_table_entry_from_thread_cache(key, entry);
    if (NULL == entry) {
      uint64_t hash = key.hash();
      DRWLock &rw_lock = rw_lock_for_key(hash);
      DRWLock::RDLockGuard lock(rw_lock);
      entry = lookup_entry(hash, key);
      if (NULL != entry && entry->is_avail_state()) {
        LOG_DEBUG("succ to get ObSqlTableEntry from global cache", KPC(entry));
        entry->inc_ref();
        // add into thread cache, will add inc_ref
        ObSqlTableRefHashMap &sql_table_map = self_ethread().get_sql_table_map();
        if (OB_FAIL(sql_table_map.set(entry))) {
          LOG_WARN("fail to set thread sql table map", KPC(entry), K(ret));
          ret = OB_SUCCESS; // ignore ret
        }
      } else {
        entry = NULL;
      }
    }
    if (OB_ISNULL(entry)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("ObSqlTableEntry does not exist", K(key), K(ret));
    } else {
      // no need read lock
      entry->renew_last_access_time_us();
      const ObString &table_name = entry->get_table_name();
      if (OB_UNLIKELY(len <= table_name.length())) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("buf len is not enough for table name", K(table_name), K(len), K(ret));
      } else {
        MEMCPY(buf, table_name.ptr(), table_name.length());
        buf[table_name.length()] = '\0';
      }
    }
    if (NULL != entry) {
      entry->dec_ref();
      entry = NULL;
    }
  } 
  return ret;
}

int ObSqlTableCache::add_sql_table_entry(ObSqlTableEntry *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("entry is null", K(ret));
  } else if (OB_FAIL(update_sql_table_entry(*entry))) {
    LOG_WARN("fail to add sql table entry", KPC(entry), K(ret));
  }
  return ret;
}

int ObSqlTableCache::update_sql_table_entry(ObSqlTableEntry &entry)
{
  int ret = OB_SUCCESS;
  const ObSqlTableEntryKey &key = entry.get_key();
  uint64_t hash = key.hash();
  LOG_DEBUG("begin to get sql table  entry", K(key), K(hash));
  DRWLock &rw_lock = rw_lock_for_key(hash);
  ObSqlTableEntry *tmp_entry = NULL;
  ObSqlTableEntry *old_entry = NULL;
  bool need_update_cache = true;
  {
    DRWLock::RDLockGuard lock(rw_lock);
    if (NULL != (old_entry = lookup_entry(hash, key))) {
      if (entry.get_table_name() == old_entry->get_table_name()
          || !entry.is_table_from_reroute()) {
        LOG_DEBUG("the same table name or table name from parse result, no need update", K(key), KPC(old_entry), K(entry));
        need_update_cache = false;
      } 
      old_entry = NULL;
    }
  }
  if (need_update_cache) {
    DRWLock::WRLockGuard lock(rw_lock);
    // double check
    if (NULL != (old_entry = lookup_entry(hash, key))) {
      if (entry.get_table_name() == old_entry->get_table_name()
          || !entry.is_table_from_reroute()) {
        need_update_cache = false;
        LOG_DEBUG("the same table name or table name from parse result, no need update", K(key), KPC(old_entry), K(entry));
      }
      old_entry = NULL;
    }
    if (need_update_cache) {
      entry.inc_ref();
      tmp_entry = insert_entry(hash, key, &entry);
      entry.renew_last_update_time_us();
      entry.renew_last_access_time_us();
      LOG_INFO("succ to update sql table entry", K(hash), KPC(tmp_entry), K(entry));
    }
  }
  // no need write lock, direct update thread cache
  if (need_update_cache) {
    ObSqlTableRefHashMap &sql_table_map = self_ethread().get_sql_table_map();
    ObSqlTableEntry *pentry = &entry;
    if (OB_FAIL(sql_table_map.set(pentry))) {
      LOG_WARN("fail to set thread sql table map", K(entry), K(ret));
      ret = OB_SUCCESS; // ignore ret
    }
  }
  if (NULL != tmp_entry) {
    tmp_entry->set_deleted_state();
    tmp_entry->dec_ref();
    tmp_entry = NULL;
  }
  return ret;
}

int ObSqlTableCache::update_table_name(const ObSqlTableEntryKey &key, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObSqlTableEntry *entry = NULL;
  if (OB_FAIL(ObSqlTableEntry::alloc_and_init_sql_table_entry(key, table_name, entry))) {
    LOG_WARN("fail to alloc sql table entry", K(key), K(table_name), K(ret));
  } else if (FALSE_IT(entry->set_table_from_reroute())) {
    // never come here
  } else if (OB_FAIL(update_sql_table_entry(*entry))) {
    LOG_WARN("fail to update sql table entry", K(key), K(table_name), K(ret));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to update table name", K(key), K(table_name));
  }
  if (NULL != entry) {
    // alloc and add into cache both  add inc_ref
    // remember to dec_ref
    entry->dec_ref();
    entry = NULL;
  }
  return ret;
}

bool ObSqlTableCache::gc_sql_table_entry(ObSqlTableEntry *entry)
{
  bool expired = false;
  ObCacheCleaner *cleaner = self_ethread().cache_cleaner_;
  if ((NULL != cleaner) && (NULL != entry)) {
    if (cleaner->is_sql_table_entry_expired(*entry)) {
      LOG_INFO("this sql table entry has expired, will be deleted", KPC(entry));
      expired = true;
      entry->set_deleted_state();
      entry->dec_ref();
    }
  }
  return expired;
}

int ObSqlTableCache::remove_sql_table_entry(const ObSqlTableEntryKey &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(key), K(ret));
  } else {
    uint64_t hash = key.hash();
    ObSqlTableEntry *entry = NULL;
    DRWLock &rw_lock = rw_lock_for_key(hash);
    DRWLock::WRLockGuard lock(rw_lock);
    entry = remove_entry(hash, key);
    LOG_INFO("this entry will be removed from sql table cache", KPC(entry));
    if (NULL != entry) {
      entry->set_deleted_state();
      entry->dec_ref();
      entry = NULL;
    }
  }
  return ret;
}

int init_sql_table_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  ObEThread **ethreads = NULL;
  if (OB_ISNULL(ethreads = g_event_processor.event_thread_[ET_CALL])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(ERROR, "fail to get ET_NET thread", K(ret));
  } else {
    for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(ethreads[i]->sql_table_map_ = new (std::nothrow) ObSqlTableRefHashMap(ObModIds::OB_PROXY_SQL_TABLE_ENTRY_MAP))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObSqlTableRefHashMap", K(i), K(ethreads[i]), K(ret));
      } else if (OB_FAIL(ethreads[i]->sql_table_map_->init())) {
        LOG_WARN("fail to init sql_table_map", K(ret));
      }
    }
  }
  return ret;

}

int ObSqlTableRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleted_state()) {
        LOG_INFO("this sql table entry will erase from tc map", KPC(*it));
        if (OB_FAIL(erase(it, i))) {
          LOG_WARN("fail to erase table entry", K(i), K(ret));
        }
      }
    }
  }
  return ret;
}

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
