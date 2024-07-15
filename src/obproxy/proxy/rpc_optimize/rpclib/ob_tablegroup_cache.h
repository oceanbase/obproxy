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

#ifndef OBPROXY_TABLEGROUP_CACHE_H
#define OBPROXY_TABLEGROUP_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc_optimize/rpclib/ob_tablegroup_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObTableGroupEntryKey, ObTableGroupEntry *> TableGroupEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObTableGroupEntryKey, ObTableGroupEntry *> TableGroupIter;

struct ObTableGroupCacheParam
{
public:
  enum Op
  {
    INVALID_TABLEGROUP_OP = 0,
    ADD_TABLEGROUP_OP,
    REMOVE_TABLEGROUP_OP,
  };

  ObTableGroupCacheParam() : hash_(0), part_id_(-1), key_(), op_(INVALID_TABLEGROUP_OP), entry_(NULL) {}
  ~ObTableGroupCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_TABLEGROUP_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  int64_t part_id_;
  ObTableGroupEntryKey key_;
  Op op_;
  ObTableGroupEntry *entry_;
  SLINK(ObTableGroupCacheParam, link_);
};

class ObTableGroupCache : public TableGroupEntryHashMap
{
public:
  static const int64_t TABLEGROUP_CACHE_MAP_SIZE = 1024;

  ObTableGroupCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObTableGroupCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

 int get_tablegroup_entry(event::ObContinuation *cont, const ObTableGroupEntryKey &key,
                     ObTableGroupEntry **ppentry, event::ObAction *&action);

  int add_tablegroup_entry(ObTableGroupEntry &entry, bool direct_add);
  static int add_tablegroup_entry(ObTableGroupCache &table_cache, ObTableGroupEntry &entry);

  int remove_tablegroup_entry(const ObTableGroupEntryKey &key);
  int remove_all_tablegroup_entry();
  int run_todo_list(const int64_t buck_id);

  int update_entry(ObTableGroupEntry &new_entry, const ObTableGroupEntryKey &key, const uint64_t hash);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_tablegroup_entry_expired(const ObTableGroupEntry &entry);
  bool is_tablegroup_entry_expired_in_qa_mode(const ObTableGroupEntry &entry);
  bool is_tablegroup_entry_expired_in_time_mode(const ObTableGroupEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_tablegroup_entry(ObTableGroupEntry *entry);

private:
  int process(const int64_t buck_id, ObTableGroupCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupCache);
};

inline void ObTableGroupCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObTableGroupCache::is_tablegroup_entry_expired(const ObTableGroupEntry &entry)
{
   return is_tablegroup_entry_expired_in_time_mode(entry) || is_tablegroup_entry_expired_in_qa_mode(entry);
}

bool ObTableGroupCache::is_tablegroup_entry_expired_in_qa_mode(const ObTableGroupEntry &entry)
{
  bool is_expire = false;
  if (OB_UNLIKELY(obutils::get_global_proxy_config().enable_qa_mode)) {
    int64_t period_us = common::msec_to_usec(obutils::get_global_proxy_config().location_expire_period);
    is_expire = period_us > 0 && common::ObTimeUtility::current_time() - entry.get_create_time_us() >= period_us;
  }

  return is_expire;
}

bool ObTableGroupCache::is_tablegroup_entry_expired_in_time_mode(const ObTableGroupEntry &entry)
{
  bool is_expire = entry.get_create_time_us() <= expire_time_us_;
  if (!is_expire) {
    is_expire = (entry.get_time_for_expired() > 0
                 && entry.get_time_for_expired() <= common::ObTimeUtility::current_time());
  }

  return is_expire;
}

extern ObTableGroupCache &get_global_tablegroup_cache();

template<class K, class V>
struct ObGetTableGroupEntryKey
{
  ObTableGroupEntryKey operator() (const ObTableGroupEntry *tablegroup_entry) const
  {
    ObTableGroupEntryKey key;
    if (OB_LIKELY(NULL != tablegroup_entry)) {
      tablegroup_entry->get_key(key);
    }
    return key;
  }
};

static const int64_t TABLEGROUP_ENTRY_HASH_MAP_SIZE = 4 * 1024; // 4KB
typedef obproxy::ObRefHashMap<ObTableGroupEntryKey, ObTableGroupEntry *, ObGetTableGroupEntryKey, TABLEGROUP_ENTRY_HASH_MAP_SIZE> ObTableGroupHashMap;

class ObTableGroupRefHashMap : public ObTableGroupHashMap
{
public:
  ObTableGroupRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObTableGroupHashMap(mod_id) {}
  virtual ~ObTableGroupRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableGroupRefHashMap);
};

int init_tablegroup_map_for_thread();
int init_tablegroup_map_for_one_thread(int64_t tablegroup);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_TABLEGROUP_CACHE_H */
