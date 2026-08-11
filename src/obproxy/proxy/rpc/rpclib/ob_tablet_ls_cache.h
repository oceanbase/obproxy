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

#ifndef OBPROXY_TABLET_LS_CACHE_H
#define OBPROXY_TABLET_LS_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc/rpclib/ob_tablet_ls_entry.h"
#include "proxy/rpc/rpclib/ob_rpc_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObTabletLsEntryKey, ObTabletLsEntry *> TabletLsEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObTabletLsEntryKey, ObTabletLsEntry *> TabletLsIter;

struct ObTabletLsCacheParam
{
public:
  enum Op
  {
    INVALID_TABLET_LS_OP = 0,
    ADD_TABLET_LS_OP,
    REMOVE_TABLET_LS_OP,
  };

  ObTabletLsCacheParam() : hash_(0), part_id_(-1), key_(), op_(INVALID_TABLET_LS_OP), entry_(NULL) {}
  ~ObTabletLsCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_TABLET_LS_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  int64_t part_id_;
  ObTabletLsEntryKey key_;
  Op op_;
  ObTabletLsEntry *entry_;
  SLINK(ObTabletLsCacheParam, link_);
};

class ObTabletLsCache : public TabletLsEntryHashMap
{
public:
  static const int64_t TABLET_LS_CACHE_MAP_SIZE = 1024;

  ObTabletLsCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObTabletLsCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

 int get_tablet_ls_entry(event::ObContinuation *cont, const ObTabletLsEntryKey &key,
                     ObTabletLsEntry **ppentry, event::ObAction *&action);

  int add_tablet_ls_entry(ObTabletLsEntry &entry, bool direct_add);
  static int add_tablet_ls_entry(ObTabletLsCache &table_cache, ObTabletLsEntry &entry);

  int remove_tablet_ls_entry(const ObTabletLsEntryKey &key);
  int remove_all_tablet_ls_entry();
  int run_todo_list(const int64_t buck_id);

  int update_entry(ObTabletLsEntry &new_entry, const ObTabletLsEntryKey &key, const uint64_t hash);

  void set_cache_expire_time(const int64_t relative_time_ms);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_tablet_ls_entry_expired(const ObTabletLsEntry &entry);
  bool is_tablet_ls_entry_expired_in_qa_mode(const ObTabletLsEntry &entry);
  bool is_tablet_ls_entry_expired_in_time_mode(const ObTabletLsEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_tablet_ls_entry(ObTabletLsEntry *entry);

private:
  int process(const int64_t buck_id, ObTabletLsCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObTabletLsCache);
};

inline void ObTabletLsCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObTabletLsCache::is_tablet_ls_entry_expired(const ObTabletLsEntry &entry)
{
   return is_tablet_ls_entry_expired_in_time_mode(entry) || is_tablet_ls_entry_expired_in_qa_mode(entry);
}

bool ObTabletLsCache::is_tablet_ls_entry_expired_in_qa_mode(const ObTabletLsEntry &entry)
{
  bool is_expire = false;
  if (OB_UNLIKELY(obutils::get_global_proxy_config().enable_qa_mode)) {
    int64_t period_us = common::msec_to_usec(obutils::get_global_proxy_config().location_expire_period);
    is_expire = period_us > 0 && common::ObTimeUtility::current_time() - entry.get_create_time_us() >= period_us;
  }

  return is_expire;
}

bool ObTabletLsCache::is_tablet_ls_entry_expired_in_time_mode(const ObTabletLsEntry &entry)
{
  bool is_expire = entry.get_create_time_us() <= expire_time_us_;
  if (!is_expire) {
    is_expire = (entry.get_time_for_expired() > 0
                 && entry.get_time_for_expired() <= common::ObTimeUtility::current_time());
  }

  return is_expire;
}

extern ObTabletLsCache &get_global_tablet_ls_cache();

template<class K, class V>
struct ObGetTabletLsEntryKey
{
  ObTabletLsEntryKey operator() (const ObTabletLsEntry *tablet_ls_entry) const
  {
    ObTabletLsEntryKey key;
    if (OB_LIKELY(NULL != tablet_ls_entry)) {
      tablet_ls_entry->get_key(key);
    }
    return key;
  }
};

static const int64_t TABLET_LS_ENTRY_HASH_MAP_SIZE = 4 * 1024; // 4KB
typedef obproxy::ObRefHashMap<ObTabletLsEntryKey, ObTabletLsEntry *, ObGetTabletLsEntryKey, TABLET_LS_ENTRY_HASH_MAP_SIZE> ObTabletLsHashMap;

class ObTabletLsRefHashMap : public ObTabletLsHashMap
{
public:
  ObTabletLsRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObTabletLsHashMap(mod_id) {}
  virtual ~ObTabletLsRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletLsRefHashMap);
};

int init_tablet_ls_map_for_thread();
int init_tablet_ls_map_for_one_thread(int64_t index);
int init_tablet_ls_map_for_one_thread(event::ObEThread *thread);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_TABLET_LS_CACHE_H */
