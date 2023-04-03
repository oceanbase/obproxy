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

#ifndef OBPROXY_ROUTINE_CACHE_H
#define OBPROXY_ROUTINE_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "proxy/route/ob_routine_entry.h"
#include "proxy/route/ob_cache_cleaner.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObRoutineEntryKey, ObRoutineEntry *> RoutineEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObRoutineEntryKey, ObRoutineEntry *> RoutineIter;

struct ObRoutineCacheParam
{
public:
  enum Op
  {
    INVALID_ROUTINE_OP = 0,
    ADD_ROUTINE_OP,
    REMOVE_ROUTINE_OP,
  };

  ObRoutineCacheParam() : hash_(0), key_(), op_(INVALID_ROUTINE_OP), entry_(NULL),
                          name_buf_len_(0), name_buf_(NULL) {}
  ~ObRoutineCacheParam() { reset(); }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);
  void reset();
  int deep_copy_key(const ObRoutineEntryKey &key);

  static const int64_t SCHEDULE_ROUTINE_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  ObRoutineEntryKey key_;
  Op op_;
  ObRoutineEntry *entry_;
  int64_t name_buf_len_;
  char *name_buf_;
  SLINK(ObRoutineCacheParam, link_);
};

inline void ObRoutineCacheParam::reset()
{
  hash_ = 0;
  key_.reset();
  op_ = INVALID_ROUTINE_OP;
  entry_ = NULL;

  if (NULL != name_buf_ && name_buf_len_ > 0) {
    op_fixed_mem_free(name_buf_, name_buf_len_);
    name_buf_ = NULL;
    name_buf_len_ = 0;
  }
}

// ObRoutineCache, ObPartitionCache and ObTableCache have the same sub partitions,
// that is MT_HASHTABLE_PARTITIONS(64);
class ObRoutineCache : public RoutineEntryHashMap
{
public:
  static const int64_t ROUTINE_CACHE_MAP_SIZE = 10240;

  ObRoutineCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObRoutineCache() { destroy(); }
  int init(const int64_t bucket_size);

  // is_add_building_entry: if can not found, whether add building state routine entry.
  int get_routine_entry(event::ObContinuation *cont,
                        const ObRoutineEntryKey &key,
                        ObRoutineEntry **ppentry,
                        const bool is_add_building_entry,
                        event::ObAction *&action);
  int add_routine_entry(ObRoutineEntry &entry, bool direct_add);
  int remove_routine_entry(const ObRoutineEntryKey &key);

  int run_todo_list(const int64_t buck_id);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_routine_entry_expired(const ObRoutineEntry &entry);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_routine_entry(ObRoutineEntry *entry);

private:
  void destroy();
  int process(const int64_t buck_id, ObRoutineCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObRoutineCache);
};

inline void ObRoutineCache::set_cache_expire_time(const int64_t relative_time_s)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::sec_to_usec(relative_time_s);
}

inline bool ObRoutineCache::is_routine_entry_expired(const ObRoutineEntry &entry)
{
  return (entry.get_create_time_us() <= expire_time_us_);
}

extern ObRoutineCache &get_global_routine_cache();

template<class K, class V>
struct ObGetRoutineEntryKey
{
  ObRoutineEntryKey operator() (const ObRoutineEntry *routine_entry) const
  {
    ObRoutineEntryKey key;
    if (OB_LIKELY(NULL != routine_entry)) {
      routine_entry->get_key(key);
    }
    return key;
  }
};

static const int64_t ROUTINE_ENTRY_HASH_MAP_SIZE = 64 * 1024; // 64KB
typedef obproxy::ObRefHashMap<ObRoutineEntryKey, ObRoutineEntry *,
        ObGetRoutineEntryKey, ROUTINE_ENTRY_HASH_MAP_SIZE> ObRoutineHashMap;

class ObRoutineRefHashMap : public ObRoutineHashMap
{
public:
  ObRoutineRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObRoutineHashMap(mod_id) {}
  virtual ~ObRoutineRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObRoutineRefHashMap);
};

int init_routine_map_for_thread();
int init_routine_map_for_one_thread(int64_t index);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_ROUTINE_CACHE_H */
