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

#ifndef OB_TABLE_QUERY_ASYNC_CACHE_H
#define OB_TABLE_QUERY_ASYNC_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc_optimize/rpclib/ob_table_query_async_entry.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<uint64_t, ObTableQueryAsyncEntry *> TableQueryAsyncEntryHashMap;
typedef obutils::ObHashTableIteratorState<uint64_t, ObTableQueryAsyncEntry *> TableQueryAsyncIter;

struct ObTableQueryAsyncCacheParam
{
public:
  enum Op
  {
    INVALID_TABLE_QUERY_ASYNC_OP = 0,
    ADD_TABLE_QUERY_ASYNC_OP,
    REMOVE_TABLE_QUERY_ASYNC_OP,
  };

  ObTableQueryAsyncCacheParam() : hash_(0), key_(0), op_(INVALID_TABLE_QUERY_ASYNC_OP), entry_(NULL) {}
  ~ObTableQueryAsyncCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_TABLE_QUERY_ASYNC_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  uint64_t key_;
  Op op_;
  ObTableQueryAsyncEntry *entry_;
  SLINK(ObTableQueryAsyncCacheParam, link_);
};

class ObTableQueryAsyncCache : public TableQueryAsyncEntryHashMap
{
public:
  static const int64_t TABLE_QUERY_ASYNC_CACHE_MAP_SIZE = 1024;

  ObTableQueryAsyncCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObTableQueryAsyncCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

 int get_table_query_async_entry(event::ObContinuation *cont, uint64_t key,
                     ObTableQueryAsyncEntry **ppentry, event::ObAction *&action);

  int add_table_query_async_entry(ObTableQueryAsyncEntry &entry, bool direct_add);

  int remove_table_query_async_entry(uint64_t key);
  int run_todo_list(const int64_t buck_id);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_table_query_async_entry_expired(const ObTableQueryAsyncEntry &entry);
  bool is_table_query_async_entry_expired_in_time_mode(const ObTableQueryAsyncEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_table_query_async_entry(ObTableQueryAsyncEntry *entry);

private:
  int process(const int64_t buck_id, ObTableQueryAsyncCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncCache);
};

inline void ObTableQueryAsyncCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObTableQueryAsyncCache::is_table_query_async_entry_expired(const ObTableQueryAsyncEntry &entry)
{
   return is_table_query_async_entry_expired_in_time_mode(entry);
}

bool ObTableQueryAsyncCache::is_table_query_async_entry_expired_in_time_mode(const ObTableQueryAsyncEntry &entry)
{
  UNUSED(entry);
  // TODO
  return false;
  // bool is_expire = entry.get_create_time_us() <= expire_time_us_;
  // if (!is_expire) {
  //   is_expire = (entry.get_time_for_expired() > 0
  //                && entry.get_time_for_expired() <= common::ObTimeUtility::current_time());
  // }

  // return is_expire;
}

extern ObTableQueryAsyncCache &get_global_table_query_async_cache();

template<class K, class V>
struct ObGetTableQueryAsyncEntryKey
{
  uint64_t operator() (const ObTableQueryAsyncEntry *table_query_async_entry) const
  {
    uint64_t key = 0;
    if (OB_LIKELY(NULL != table_query_async_entry)) {
      key = table_query_async_entry->get_client_query_session_id();
    }
    return key;
  }
};

static const int64_t TABLE_QUERY_ASYNC_ENTRY_HASH_MAP_SIZE = 16 * 1024; // 16KB
typedef obproxy::ObRefHashMap<uint64_t, ObTableQueryAsyncEntry *, ObGetTableQueryAsyncEntryKey, TABLE_QUERY_ASYNC_ENTRY_HASH_MAP_SIZE> ObTableQueryAsyncHashMap;

class ObTableQueryAsyncRefHashMap: public ObTableQueryAsyncHashMap
{
public:
  ObTableQueryAsyncRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObTableQueryAsyncHashMap(mod_id) {}
  virtual ~ObTableQueryAsyncRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryAsyncRefHashMap);
};

int init_table_query_async_map_for_thread();
int init_table_query_async_map_for_one_thread(int64_t index);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OB_TABLE_QUERY_ASYNC_CACHE_H */