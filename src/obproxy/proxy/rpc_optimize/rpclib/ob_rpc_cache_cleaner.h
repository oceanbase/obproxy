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

#ifndef OB_RPC_CACHE_CLEANER_H
#define OB_RPC_CACHE_CLEANER_H

#include "iocore/eventsystem/ob_continuation.h"
#include "iocore/eventsystem/ob_action.h"
#include "proxy/route/ob_cache_cleaner.h"

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
#define RPC_CLEANER_TRIGGER_EVENT (RPC_CACHE_CLEANER_EVENT_EVENTS_START + 1)

class ObTableQueryAsyncCache;
class ObTableQueryAsyncEntry;
class ObTableGroupCache;
class ObTableGroupEntry;
class ObRpcReqCtxCache;
class ObRpcReqCtx;
// every work thread has one cache cleaner
class ObRpcCacheCleaner : public event::ObContinuation
{
public:
  ObRpcCacheCleaner();
  virtual ~ObRpcCacheCleaner() {}

  int init(ObTableQueryAsyncCache &table_query_async_cache,
           ObTableGroupCache &tablegroup_cache,
           ObRpcReqCtxCache &rpc_ctx_cache,
           const ObCountRange &range, const int64_t total_count,
           const int64_t idx, const int64_t clean_interval_us);
  int main_handler(int event, void *data);
  static int schedule_cache_cleaner();
  static int update_clean_interval();
  static int schedule_one_cache_cleaner(int64_t index);

  bool is_tablegroup_entry_expired(ObTableGroupEntry &entry);

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int set_clean_interval(const int64_t interval);
  int start_clean_cache(event::ObEThread &thread);
  // trigger the cleaner to work imm
  int trigger();

private:
  enum ObRpcCleanAction
  {
    EXPIRE_TABLEGROUP_ENTRY_ACTION,
    CLEAN_TABLEGROUP_CACHE_ACTION,
    CLEAN_THREAD_CACHE_TABLEGROUP_ENTRY_ACTION,
    CLEAN_TABLE_QUERY_ASYNC_CACHE_ACTION,
    CLEAN_THREAD_CACHE_TABLE_QUERY_ASYNC_ENTRY_ACTION,
    CLEAN_RPC_CTX_CACHE_ACTION,
    CLEAN_THREAD_CACHE_RPC_CTX_ACTION,
    IDLE_CLEAN_ACTION
  };

  int cleanup();
  int do_clean_job();
  int schedule_in(const int64_t time_us);
  const char *get_cleaner_action_name(const ObRpcCleanAction action) const;
  int cancel_pending_action();

  int clean_table_query_async_cache();
  int clean_one_sub_bucket_table_query_async_cache(const int64_t bucket_idx);

  int clean_rpc_ctx_cache();
  int clean_one_sub_bucket_rpc_ctx_cache(const int64_t bucket_idx);

  bool is_tablegroup_cache_expire_time_changed();

  int do_expire_tablegroup_entry();
  int clean_tablegroup_cache();
  int clean_one_sub_bucket_tablegroup_cache(const int64_t bucket_idx, const int64_t clean_count);

private:
  const static int64_t RETRY_LOCK_INTERVAL_MS = 10; // 10ms

  const static int64_t AVG_TABLEGROUP_ENTRY_SIZE = 512; // 512 bytes
  const static int64_t PART_TABLEGROUP_ENTRY_MIN_COUNT = 10;
  
  const static int64_t MAX_COLSE_CLIENT_SESSION_RETYR_TIME = 5;

  bool is_inited_;
  bool triggered_;
  int64_t cleaner_reschedule_interval_us_;
  int64_t total_cleaner_count_;
  int64_t this_cleaner_idx_;
  ObRpcCleanAction next_action_;
  event::ObEThread *ethread_; // this cleaner's ethread
  ObTableGroupCache *tablegroup_cache_;
  ObTableQueryAsyncCache *table_query_async_cache_;
  ObRpcReqCtxCache *rpc_ctx_cache_;
  ObCountRange tablegroup_cache_range_;
  ObCountRange table_query_async_cache_range_;
  ObCountRange rpc_ctx_cache_range_;
  common::ObSEArray<int64_t, 8> tablegroup_cache_deleted_cr_version_; // for expir index entry
  int64_t tablegroup_cache_last_expire_time_us_;
  int64_t tc_part_clean_count_; // table cache every partition clean count
  event::ObAction *pending_action_;
  DISALLOW_COPY_AND_ASSIGN(ObRpcCacheCleaner);
};

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif // OB_RPC_CACHE_CLEANER_H