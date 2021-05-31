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

#ifndef OBPROXY_CACHE_CLEANER_H
#define OBPROXY_CACHE_CLEANER_H

#include "iocore/eventsystem/ob_continuation.h"
#include "iocore/eventsystem/ob_action.h"

namespace oceanbase
{
namespace obproxy
{
namespace obutils
{
class ObClusterResource;
class ObResourceDeleteActor;
}
namespace proxy
{
#define CLEANER_TRIGGER_EVENT (CACHE_CLEANER_EVENT_EVENTS_START + 1)

struct ObCountRange
{
  ObCountRange() : start_idx_(-1), end_idx_(-1), cur_idx_(start_idx_) {}
  ~ObCountRange() { reset(); }
  bool is_valid() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void reset();

  int64_t count() const;
  bool has_remain() const { return (end_idx_ >= cur_idx_); }
  void again() { cur_idx_ = start_idx_; }

  int64_t start_idx_;
  int64_t end_idx_;
  int64_t cur_idx_; // cur_idx_ must be in [start_idx_, end_idx_]
};

inline int64_t ObCountRange::count() const
{
  int64_t ret_value = 0;
  if (is_valid()) {
    if (start_idx_ >= 0) {
      ret_value = (end_idx_ - start_idx_ + 1);
    }
  }
  return ret_value;
}

inline bool ObCountRange::is_valid() const
{
  return ((end_idx_ >= start_idx_)
         && (cur_idx_ >= start_idx_)
         && (cur_idx_ <= end_idx_));
}

inline void  ObCountRange::reset()
{
  start_idx_ = -1;
  end_idx_ = -1;
  cur_idx_ = start_idx_;
}

class ObTableCache;
class ObSqlTableCache;
class ObPartitionCache;
class ObRoutineCache;
class ObTableEntry;
class ObPartitionEntry;
class ObRoutineEntry;
class ObSqlTableEntry;
// every work thread has one cache cleaner
class ObCacheCleaner : public event::ObContinuation
{
public:
  ObCacheCleaner();
  virtual ~ObCacheCleaner() {}

  int init(ObTableCache &table_cache, ObPartitionCache &partition_cache,
           ObRoutineCache &routine_cache, ObSqlTableCache &sql_table_cache,
           const ObCountRange &range, const int64_t total_count,
           const int64_t idx, const int64_t clean_interval_us);
  int main_handler(int event, void *data);
  static int schedule_cache_cleaner();
  static int update_clean_interval();

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int push_deleting_cr(obutils::ObResourceDeleteActor *actor);
  bool is_table_entry_expired(ObTableEntry &entry);
  bool is_partition_entry_expired(ObPartitionEntry &entry);
  bool is_routine_entry_expired(ObRoutineEntry &entry);
  bool is_sql_table_entry_expired(ObSqlTableEntry &entry);
  int set_clean_interval(const int64_t interval);
  int start_clean_cache(event::ObEThread &thread);
  // trigger the cleaner to work imm
  int trigger();

private:
  enum ObCleanAction
  {
    CLEAN_THREAD_CACHE_CONGESTION_ENTRY_ACTION = 0,
    CLEAN_CLUSTER_RESOURCE_ACTION,
    EXPIRE_TABLE_ENTRY_ACTION,
    CLEAN_TABLE_CACHE_ACTION,
    CLEAN_THREAD_CACHE_TABLE_ENTRY_ACTION,
    EXPIRE_PARTITION_ENTRY_ACTION,
    CLEAN_PARTITION_CACHE_ACTION,
    CLEAN_THREAD_CACHE_PARTITION_ENTRY_ACTION,
    EXPIRE_ROUTINE_ENTRY_ACTION,
    CLEAN_ROUTINE_CACHE_ACTION,
    CLEAN_THREAD_CACHE_ROUTINE_ENTRY_ACTION,
    EXPIRE_SQL_TABLE_ENTRY_ACTION,
    CLEAN_SQL_TABLE_CACHE_ACTION,
    CLEAN_THREAD_CACHE_SQL_TABLE_ENTRY_ACTION,
    IDLE_CLEAN_ACTION
  };

  int cleanup();
  int do_clean_job();
  int64_t calc_table_entry_clean_count();
  int clean_table_cache(bool &need_try_lock);
  int clean_one_part_table_cache(const int64_t part_idx);
  int schedule_in(const int64_t time_us);
  const char *get_cleaner_action_name(const ObCleanAction action) const;
  int do_delete_cluster_resource();
  int do_expire_cluster_resource();
  int do_expire_table_entry();
  // the first cleaner is responsible to do expire cluster resourece job
  bool need_expire_cluster_resource() { return (0 == this_cleaner_idx_); }
  bool is_table_cache_expire_time_changed();
  bool is_partition_cache_expire_time_changed();
  bool is_routine_cache_expire_time_changed();
  bool is_sql_table_cache_expire_time_changed();
  int cancel_pending_action();
  void clean_cluster_resource();
  int do_expire_partition_entry();
  int clean_partition_cache();
  int clean_one_sub_bucket_partition_cache(const int64_t bucket_idx, const int64_t clean_count);

  int do_expire_routine_entry();
  int clean_routine_cache();
  int clean_one_sub_bucket_routine_cache(const int64_t bucket_idx, const int64_t clean_count);

  int do_expire_sql_table_entry();
  int clean_sql_table_cache();
  int clean_one_sub_bucket_sql_table_cache(const int64_t bucket_idx, const int64_t clean_count);
private:
  const static int64_t RETRY_LOCK_INTERVAL_MS = 10; // 10ms

  const static int64_t AVG_TABLE_ENTRY_SIZE = 512; // 512 bytes
  const static int64_t AVG_PARTITION_ENTRY_SIZE = 512; // 512 bytes
  const static int64_t AVG_ROUTINE_ENTRY_SIZE = 512; // 512 bytes
  const static int64_t AVG_SQL_TABLE_ENTRY_SIZE = 512; // 512 bytes
  const static int64_t PART_TABLE_ENTRY_MIN_COUNT = 10;
  const static int64_t PART_PARTITION_ENTRY_MIN_COUNT = 10;
  const static int64_t PART_ROUTINE_ENTRY_MIN_COUNT = 10;
  const static int64_t PART_SQL_TABLE_ENTRY_MIN_COUNT = 10;

  const static int64_t MAX_COLSE_CLIENT_SESSION_RETYR_TIME = 5;

  bool is_inited_;
  bool triggered_;
  int64_t cleaner_reschedule_interval_us_;
  int64_t total_cleaner_count_;
  int64_t this_cleaner_idx_;
  ObCleanAction next_action_;
  event::ObEThread *ethread_; // this cleaner's ethread
  ObTableCache *table_cache_;
  ObPartitionCache *partition_cache_;
  ObRoutineCache *routine_cache_;
  ObSqlTableCache *sql_table_cache_;
  ObCountRange table_cache_range_;
  ObCountRange partition_cache_range_;
  ObCountRange routine_cache_range_;
  ObCountRange sql_table_cache_range_;
  int64_t tc_part_clean_count_; // table cache every partition clean count
  common::ObSEArray<int64_t, 8> table_cache_deleted_cr_version_; // for expire table entry
  common::ObSEArray<int64_t, 8> partition_cache_deleted_cr_version_; // for expir partition entry
  common::ObSEArray<int64_t, 8> routine_cache_deleted_cr_version_; // for expir routine entry
  common::ObSEArray<int64_t, 8> sql_table_cache_deleted_cr_version_; // for expire table entry
  int64_t table_cache_last_expire_time_us_;
  int64_t partition_cache_last_expire_time_us_;
  int64_t routine_cache_last_expire_time_us_;
  int64_t sql_table_cache_last_expire_time_us_;
  event::ObAction *pending_action_;
  common::ObAtomicList deleting_cr_list_;
  DISALLOW_COPY_AND_ASSIGN(ObCacheCleaner);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OBPROXY_CACHE_CLEANER_H
