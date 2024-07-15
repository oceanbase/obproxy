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

#ifndef OBPROXY_INDEX_CACHE_H
#define OBPROXY_INDEX_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/route/ob_index_entry.h"
#include "proxy/route/ob_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObIndexEntryKey, ObIndexEntry *> IndexEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObIndexEntryKey, ObIndexEntry *> IndexIter;

struct ObIndexCacheParam
{
public:
  enum Op
  {
    INVALID_INDEX_OP = 0,
    ADD_INDEX_OP,
    REMOVE_INDEX_OP,
  };

  ObIndexCacheParam() : hash_(0), part_id_(-1), key_(), op_(INVALID_INDEX_OP), entry_(NULL) {}
  ~ObIndexCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_INDEX_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  int64_t part_id_;
  ObIndexEntryKey key_;
  Op op_;
  ObIndexEntry *entry_;
  SLINK(ObIndexCacheParam, link_);
};

class ObIndexCache : public IndexEntryHashMap
{
public:
  static const int64_t INDEX_CACHE_MAP_SIZE = 1024;

  ObIndexCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObIndexCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

 int get_index_entry(event::ObContinuation *cont, const ObIndexEntryKey &key,
                     ObIndexEntry **ppentry, event::ObAction *&action);

  int add_index_entry(ObIndexEntry &entry, bool direct_add);
  static int add_index_entry(ObIndexCache &table_cache, ObIndexEntry &entry);

  int remove_index_entry(const ObIndexEntryKey &key);
  int remove_all_index_entry();
  int run_todo_list(const int64_t buck_id);

  int update_entry(ObIndexEntry &new_entry, const ObIndexEntryKey &key, const uint64_t hash);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_index_entry_expired(const ObIndexEntry &entry);
  bool is_index_entry_expired_in_qa_mode(const ObIndexEntry &entry);
  bool is_index_entry_expired_in_time_mode(const ObIndexEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_index_entry(ObIndexEntry *entry);

private:
  int process(const int64_t buck_id, ObIndexCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObIndexCache);
};

inline void ObIndexCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObIndexCache::is_index_entry_expired(const ObIndexEntry &entry)
{
   return is_index_entry_expired_in_time_mode(entry) || is_index_entry_expired_in_qa_mode(entry);
}

bool ObIndexCache::is_index_entry_expired_in_qa_mode(const ObIndexEntry &entry)
{
  bool is_expire = false;
  if (OB_UNLIKELY(obutils::get_global_proxy_config().enable_qa_mode)) {
    int64_t period_us = common::msec_to_usec(obutils::get_global_proxy_config().location_expire_period);
    is_expire = period_us > 0 && common::ObTimeUtility::current_time() - entry.get_create_time_us() >= period_us;
  }

  return is_expire;
}

bool ObIndexCache::is_index_entry_expired_in_time_mode(const ObIndexEntry &entry)
{
  bool is_expire = entry.get_create_time_us() <= expire_time_us_;
  if (!is_expire) {
    is_expire = (entry.get_time_for_expired() > 0
                 && entry.get_time_for_expired() <= common::ObTimeUtility::current_time());
  }

  return is_expire;
}

extern ObIndexCache &get_global_index_cache();

template<class K, class V>
struct ObGetIndexEntryKey
{
  ObIndexEntryKey operator() (const ObIndexEntry *index_entry) const
  {
    ObIndexEntryKey key;
    if (OB_LIKELY(NULL != index_entry)) {
      key.name_ = &index_entry->get_names();
      key.cr_version_ = index_entry->get_cr_version();
      key.cr_id_ = index_entry->get_cr_id();
    }
    return key;
  }
};

static const int64_t INDEX_ENTRY_HASH_MAP_SIZE = 16 * 1024; // 16KB
typedef obproxy::ObRefHashMap<ObIndexEntryKey, ObIndexEntry *, ObGetIndexEntryKey, INDEX_ENTRY_HASH_MAP_SIZE> ObIndexHashMap;

class ObIndexRefHashMap : public ObIndexHashMap
{
public:
  ObIndexRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObIndexHashMap(mod_id) {}
  virtual ~ObIndexRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObIndexRefHashMap);
};

int init_index_map_for_thread();
int init_index_map_for_one_thread(int64_t index);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_INDEX_CACHE_H */
