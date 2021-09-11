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

#ifndef OBPROXY_TABLE_CACHE_H
#define OBPROXY_TABLE_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/route/ob_table_entry.h"
#include "proxy/route/ob_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObTableEntryKey, ObTableEntry *> TableEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObTableEntryKey, ObTableEntry *> TableIter;

struct ObTableParam
{
public:
  enum Op
  {
    INVALID_TABLE_OP = 0,
    ADD_TABLE_OP,
    REMOVE_TABLE_OP,
    REMOVE_ALL_TABLE_OP,
  };

  ObTableParam() : hash_(0), part_id_(-1), key_(), op_(INVALID_TABLE_OP), entry_(NULL) {}
  ~ObTableParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_TABLE_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  int64_t part_id_;
  ObTableEntryKey key_;
  Op op_;
  ObTableEntry *entry_;
  SLINK(ObTableParam, link_);
};

class ObTableCache : public TableEntryHashMap
{
public:
  static const int64_t TABLE_CACHE_MAP_SIZE = 1024;

  ObTableCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObTableCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

  int get_table_entry(event::ObContinuation *cont,
                      const ObTableEntryKey &key,
                      ObTableEntry **ppentry,
                      event::ObAction *&action);

  int add_table_entry(ObTableEntry &entry, bool direct_add);
  static int add_table_entry(ObTableCache &table_cache, ObTableEntry &entry);

  int remove_table_entry(const ObTableEntryKey &key);
  int remove_all_table_entry();
  int run_todo_list(const int64_t buck_id);

  int update_entry(ObTableEntry &new_entry, const ObTableEntryKey &key, const uint64_t hash);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_table_entry_expired(const ObTableEntry &entry);
  bool is_table_entry_expired_in_qa_mode(const ObTableEntry &entry);
  bool is_table_entry_expired_in_time_mode(const ObTableEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_table_entry(ObTableEntry *entry);

private:
  int process(const int64_t buck_id, ObTableParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObTableCache);
};

inline void ObTableCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObTableCache::is_table_entry_expired(const ObTableEntry &entry)
{
  return is_table_entry_expired_in_time_mode(entry) || is_table_entry_expired_in_qa_mode(entry);
}

bool ObTableCache::is_table_entry_expired_in_qa_mode(const ObTableEntry &entry)
{
  bool is_expire = false;
  if (OB_UNLIKELY(obutils::get_global_proxy_config().enable_qa_mode)) {
    int64_t period_us = common::msec_to_usec(obutils::get_global_proxy_config().location_expire_period);
    is_expire = period_us > 0 && common::ObTimeUtility::current_time() - entry.get_create_time_us() >= period_us;
  }

  return is_expire;
}

bool ObTableCache::is_table_entry_expired_in_time_mode(const ObTableEntry &entry)
{
  bool is_expire = (!entry.is_sys_dummy_entry() && (entry.get_create_time_us() <= expire_time_us_));
  if (!is_expire) {
    is_expire = (!entry.is_dummy_entry()) && entry.get_time_for_expired() > 0
                 && entry.get_time_for_expired() <= common::ObTimeUtility::current_time();
  }

  return is_expire;
}

extern ObTableCache &get_global_table_cache();

template<class K, class V>
struct ObGetTableEntryKey
{
  ObTableEntryKey operator() (const ObTableEntry *table_entry) const
  {
    ObTableEntryKey key;
    if (OB_LIKELY(NULL != table_entry)) {
      key.name_ = &table_entry->get_names();
      key.cr_version_ = table_entry->get_cr_version();
      key.cr_id_ = table_entry->get_cr_id();
    }
    return key;
  }
};

static const int64_t TABLE_ENTRY_HASH_MAP_SIZE = 16 * 1024; // 16KB
typedef obproxy::ObRefHashMap<ObTableEntryKey, ObTableEntry *, ObGetTableEntryKey, TABLE_ENTRY_HASH_MAP_SIZE> ObTableHashMap;

class ObTableRefHashMap : public ObTableHashMap
{
public:
  ObTableRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObTableHashMap(mod_id) {}
  virtual ~ObTableRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRefHashMap);
};

int init_table_map_for_thread();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_TABLE_CACHE_H */
