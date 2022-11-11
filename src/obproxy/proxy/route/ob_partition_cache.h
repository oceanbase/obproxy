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

#ifndef OBPROXY_PARTITION_CACHE_H
#define OBPROXY_PARTITION_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/route/ob_partition_entry.h"
#include "proxy/route/ob_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObPartitionEntryKey, ObPartitionEntry *> PartitionEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObPartitionEntryKey, ObPartitionEntry *> PartitionIter;

struct ObPartitionCacheParam
{
public:
  enum Op
  {
    INVALID_PARTITION_OP = 0,
    ADD_PARTITION_OP,
    REMOVE_PARTITION_OP,
  };

  ObPartitionCacheParam() : hash_(0), key_(), op_(INVALID_PARTITION_OP), entry_(NULL) {}
  ~ObPartitionCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_PARTITION_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  ObPartitionEntryKey key_;
  Op op_;
  ObPartitionEntry *entry_;
  SLINK(ObPartitionCacheParam, link_);
};

// ObPartitionCache and ObTableCache have the same sub partitions,
// that is MT_HASHTABLE_PARTITIONS(64);
class ObPartitionCache : public PartitionEntryHashMap
{
public:
  static const int64_t PARTITION_CACHE_MAP_SIZE = 10240;

  ObPartitionCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObPartitionCache() { destroy(); }
  int init(const int64_t bucket_size);

  // is_add_building_entry: if can not found, whether add building state partition entry.
  int get_partition_entry(event::ObContinuation *cont,
                          const ObPartitionEntryKey &key,
                          ObPartitionEntry **ppentry,
                          const bool is_add_building_entry,
                          event::ObAction *&action);
  int add_partition_entry(ObPartitionEntry &entry, bool direct_add);
  int remove_partition_entry(const ObPartitionEntryKey &key);

  int run_todo_list(const int64_t buck_id);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_partition_entry_expired(const ObPartitionEntry &entry);
  bool is_partition_entry_expired_in_qa_mode(const ObPartitionEntry &entry);
  bool is_partition_entry_expired_in_time_mode(const ObPartitionEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_partition_entry(ObPartitionEntry *entry);

private:
  void destroy();
  int process(const int64_t buck_id, ObPartitionCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObPartitionCache);
};

inline void ObPartitionCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObPartitionCache::is_partition_entry_expired(const ObPartitionEntry &entry)
{
  return is_partition_entry_expired_in_qa_mode(entry) || is_partition_entry_expired_in_time_mode(entry);
}

bool ObPartitionCache::is_partition_entry_expired_in_qa_mode(const ObPartitionEntry &entry)
{
  bool is_expire = false;
  if (OB_UNLIKELY(obutils::get_global_proxy_config().enable_qa_mode)) {
    int64_t period_us = common::msec_to_usec(obutils::get_global_proxy_config().location_expire_period);
    is_expire = period_us > 0 && common::ObTimeUtility::current_time() - entry.get_create_time_us() >= period_us;
  }

  return is_expire;
}

bool ObPartitionCache::is_partition_entry_expired_in_time_mode(const ObPartitionEntry &entry)
{
  bool is_expire = (entry.get_create_time_us() <= expire_time_us_);

  if (!is_expire) {
    is_expire = entry.get_time_for_expired() > 0
                 && entry.get_time_for_expired() <= common::ObTimeUtility::current_time();
  }

  return is_expire;
}

extern ObPartitionCache &get_global_partition_cache();

template<class K, class V>
struct ObGetPartitionEntryKey
{
  ObPartitionEntryKey operator() (const ObPartitionEntry *partition_entry) const
  {
    ObPartitionEntryKey key;
    if (OB_LIKELY(NULL != partition_entry)) {
      key = partition_entry->get_key();
    }
    return key;
  }
};

static const int64_t PARTITION_ENTRY_HASH_MAP_SIZE = 64 * 1024; // 64KB
typedef obproxy::ObRefHashMap<ObPartitionEntryKey, ObPartitionEntry *,
        ObGetPartitionEntryKey, PARTITION_ENTRY_HASH_MAP_SIZE> ObPartitionHashMap;

class ObPartitionRefHashMap : public ObPartitionHashMap
{
public:
  ObPartitionRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObPartitionHashMap(mod_id) {}
  virtual ~ObPartitionRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionRefHashMap);
};

int init_partition_map_for_thread();
int init_partition_map_for_one_thread(int64_t index);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_PARTITION_CACHE_H */
