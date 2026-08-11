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

#ifndef OBPROXY_SQL_TABLE_CACHE_H
#define OBPROXY_SQL_TABLE_CACHE_H
#include "lib/tbsys.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/hash/ob_build_in_hashmap.h"
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/route/ob_sql_table_entry.h"
#include "proxy/route/ob_cache_cleaner.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<ObSqlTableEntryKey, ObSqlTableEntry *> SqlTableEntryHashMap;
typedef obutils::ObHashTableIteratorState<ObSqlTableEntryKey, ObSqlTableEntry *> SqlTableIter;

class ObSqlTableCache : public SqlTableEntryHashMap
{
public:
  static const int64_t SQL_TABLE_CACHE_MAP_SIZE = 1024;

  ObSqlTableCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObSqlTableCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();
  int get_table_and_db_name(const ObSqlTableEntryKey &key, char *tb_name_buf,
                            const int64_t tb_name_buf_len,
                            char* db_name_buf, const int64_t db_name_buf_len);

  int add_sql_table_entry(ObSqlTableEntry *entry);
  int update_table_and_db_name(const ObSqlTableEntryKey &key,
                               const common::ObString &table_name,
                               const common::ObString &origin_database_name);

  void set_cache_expire_time(const int64_t relative_time_ms);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_sql_table_entry_expired(const ObSqlTableEntry &entry);
  int remove_sql_table_entry(const ObSqlTableEntryKey &key);
  
  static bool gc_sql_table_entry(ObSqlTableEntry *entry);

private:
  void get_sql_table_entry_from_thread_cache(const ObSqlTableEntryKey &key, ObSqlTableEntry *&entry);
  int update_sql_table_entry(ObSqlTableEntry &entry);

  DECLARE_TO_STRING;

private:
  bool is_inited_;
  int64_t expire_time_us_;
  DISALLOW_COPY_AND_ASSIGN(ObSqlTableCache);
};

inline void ObSqlTableCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

inline bool ObSqlTableCache::is_sql_table_entry_expired(const ObSqlTableEntry &entry)
{
  return (entry.get_create_time_us() <= expire_time_us_);
}

extern ObSqlTableCache &get_global_sql_table_cache();

template<class K, class V>
struct ObGetSqlTableEntryKey
{
  ObSqlTableEntryKey operator() (const ObSqlTableEntry *sql_table_entry) const
  {
    ObSqlTableEntryKey key;
    if (OB_LIKELY(NULL != sql_table_entry)) {
      key = sql_table_entry->get_key();
    }
    return key;
  }
};

static const int64_t SQL_TABLE_ENTRY_HASH_MAP_SIZE = 16 * 1024; // 16KB
typedef obproxy::ObRefHashMap<ObSqlTableEntryKey, ObSqlTableEntry *, ObGetSqlTableEntryKey, SQL_TABLE_ENTRY_HASH_MAP_SIZE> ObSqlTableHashMap;

// ObRefHashMap get/put 操作时会自动加引用计数，put操作会把老的对象减引用计数
class ObSqlTableRefHashMap : public ObSqlTableHashMap
{
public:
  ObSqlTableRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObSqlTableHashMap(mod_id) {}
  virtual ~ObSqlTableRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlTableRefHashMap);
};

int init_sql_table_map_for_thread();
int init_sql_table_map_for_one_thread(int64_t index);
int init_sql_table_map_for_one_thread(event::ObEThread *thread);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OBPROXY_SQL_TABLE_CACHE_H */
