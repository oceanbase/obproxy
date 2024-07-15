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

#ifdef LATCH_DEF
LATCH_DEF(LATCH_WAIT_QUEUE_LOCK, 0, "latch wait queue lock", LATCH_FIFO, 1, 0, LATCH_WAIT_QUEUE_LOCK_WAIT, "latch wait queue lock")
LATCH_DEF(DEFAULT_SPIN_LOCK, 1, "default spin lock", LATCH_FIFO, 2000, 0, DEFAULT_SPIN_LOCK_WAIT, "default spin lock")
LATCH_DEF(DEFAULT_SPIN_RWLOCK, 2, "default spin rwlock", LATCH_FIFO, 2000, 0, DEFAULT_SPIN_RWLOCK_WAIT, "default spin rwlock")
LATCH_DEF(DEFAULT_MUTEX, 3, "default mutex", LATCH_FIFO, 2000, 0, DEFAULT_MUTEX_WAIT, "default mutex")
LATCH_DEF(KV_CACHE_BUCKET_LOCK, 4, "kv cache bucket latch", LATCH_FIFO, 2000, 0, KV_CACHE_BUCKET_LOCK_WAIT, "kv cache bucket latch")
LATCH_DEF(TIME_WHEEL_TASK_LOCK, 5, "time wheel task latch", LATCH_FIFO, 2000, 0, TIME_WHEEL_TASK_LOCK_WAIT, "time wheel task latch")
LATCH_DEF(TIME_WHEEL_BUCKET_LOCK, 6, "time wheel bucket latch", LATCH_FIFO, 2000, 0, TIME_WHEEL_BUCKET_LOCK_WAIT, "time wheel bucket latch")
LATCH_DEF(ELECTION_LOCK, 7, "election latch", LATCH_FIFO, 20000000L, 0, ELECTION_LOCK_WAIT, "election latch")
LATCH_DEF(TRANS_CTX_LOCK, 8, "trans ctx latch", LATCH_FIFO, 20000000L, 0, TRANS_CTX_LOCK_WAIT, "trans ctx latch")
LATCH_DEF(PARTITION_LOG_LOCK, 9, "partition log latch", LATCH_FIFO, 20000000L, 0, PARTITION_LOG_LOCK_WAIT, "partition log latch")
LATCH_DEF(PLAN_CACHE_VALUE_LOCK, 10, "plan cache value latch", LATCH_FIFO, 2000, 0, PLAN_CACHE_VALUE_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_HISTORY_REPORTER_LOCK, 11, "clog history reporter latch", LATCH_FIFO, 2000, 0, CLOG_HISTORY_REPORTER_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_EXTERNAL_EXEC_LOCK, 12, "clog external executor latch", LATCH_FIFO, 2000, 0, CLOG_EXTERNAL_EXEC_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_MEMBERSHIP_MGR_LOCK, 13, "clog member ship mgr latch", LATCH_FIFO, 2000, 0, CLOG_MEMBERSHIP_MGR_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_RECONFIRM_LOCK, 14, "clog reconfirm latch", LATCH_FIFO, 2000, 0, CLOG_RECONFIRM_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_SLIDING_WINDOW_LOCK, 15, "clog sliding window latch", LATCH_FIFO, 2000, 0, CLOG_SLIDING_WINDOW_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_STAT_MGR_LOCK, 16, "clog stat mgr latch", LATCH_FIFO, 2000, 0, CLOG_STAT_MGR_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_TASK_LOCK, 17, "clog task latch", LATCH_FIFO, 20000000L, 0, CLOG_TASK_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_IDMGR_LOCK, 18, "clog id mgr latch", LATCH_FIFO, 2000, 0, CLOG_IDMGR_LOCK_WAIT, "plan cache value latch")
LATCH_DEF(CLOG_CACHE_LOCK, 19, "clog cache latch", LATCH_FIFO, 2000, 0, CLOG_CACHE_LOCK_WAIT, "clog cache latch")
LATCH_DEF(ELECTION_MSG_LOCK, 20, "election msg latch", LATCH_FIFO, 2000, 0, ELECTION_MSG_LOCK_WAIT, "election msg latch")
LATCH_DEF(PLAN_SET_LOCK, 21, "plan set latch", LATCH_FIFO, 2000, 0, PLAN_SET_LOCK_WAIT, "plan set latch")
LATCH_DEF(PS_STORE_LOCK, 22, "ps store latch", LATCH_FIFO, 2000, 0, PS_STORE_LOCK_WAIT, "ps store latch")
LATCH_DEF(TRANS_CTX_MGR_LOCK, 23, "trans ctx mgr latch", LATCH_FIFO, 2000, 0, TRANS_CTX_MGR_LOCK_WAIT, "trans ctx mgr latch")
LATCH_DEF(ROW_LOCK, 24, "row latch", LATCH_READ_PREFER, 200, 0, ROW_LOCK_WAIT, "row latch")
LATCH_DEF(DEFAULT_RECURSIVE_MUTEX, 25, "default recursive mutex", LATCH_FIFO, 2000L, 0, DEFAULT_RECURSIVE_MUTEX_WAIT, "default recursive mutex")
LATCH_DEF(DEFAULT_DRW_LOCK, 26, "default drw lock", LATCH_FIFO, 200000, 0, DEFAULT_DRW_LOCK_WAIT, "default drw lock")
LATCH_DEF(DEFAULT_BUCKET_LOCK, 27, "default bucket lock", LATCH_FIFO, 2000, 0, DEFAULT_BUCKET_LOCK_WAIT, "default bucket lock")
LATCH_DEF(TRANS_CTX_BUCKET_LOCK, 28, "trans ctx bucket lock", LATCH_FIFO, 20000000L, 0, TRANS_CTX_BUCKET_LOCK_WAIT, "trans ctx bucket lock")
LATCH_DEF(MACRO_META_BUCKET_LOCK, 29, "macro meta bucket lock", LATCH_FIFO, 2000, 0, MACRO_META_BUCKET_LOCK_WAIT, "macro meta bucket lock")
LATCH_DEF(TOKEN_BUCKET_LOCK, 30, "token bucket lock", LATCH_FIFO, 20000000L, 0, TOKEN_BUCKET_LOCK_WAIT, "token bucket lock")
LATCH_DEF(LIGHTY_HASHMAP_BUCKET_LOCK, 31, "light hashmap bucket lock", LATCH_FIFO, 2000, 0, LIGHTY_HASHMAP_BUCKET_LOCK_WAIT, "lighty hashmap bucket lock")
LATCH_DEF(ROW_CALLBACK_LOCK, 32, "row callback lock", LATCH_FIFO, 2000, 0, ROW_CALLBACK_LOCK_WAIT, "row callback lock")
LATCH_DEF(PARTITION_LOCK, 33, "partition latch", LATCH_FIFO, 2000, 0, PARTITION_LOCK_WAIT, "partition latch")
LATCH_DEF(SWITCH_LEADER_LOCK, 35, "switch leader lock", LATCH_FIFO, 2000, 0, SWITCH_LEADER_WAIT, "switch leader lock")
LATCH_DEF(PARTITION_FREEZE_LOCK, 36, "partition freeze lock", LATCH_FIFO, 2000, 0, PARTITION_FREEZE_WAIT, "partition freeze lock")
LATCH_DEF(SCHEMA_SERVICE_LOCK, 37, "schema service lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_SERVICE_LOCK_WAIT, "schema service lock")
LATCH_DEF(SCHEMA_SERVICE_STATS_LOCK, 38, "schema service stats lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_SERVICE_STATS_LOCK_WAIT, "schema service stats lock")
LATCH_DEF(TENANT_LOCK, 39, "tenant lock", LATCH_READ_PREFER, 2000, 0, TENANT_LOCK_WAIT, "tenant lock")
LATCH_DEF(CONFIG_LOCK, 40, "config lock", LATCH_READ_PREFER, 2000, 0, CONFIG_LOCK_WAIT, "config lock")
LATCH_DEF(MAJOR_FREEZE_LOCK, 41, "major freeze lock", LATCH_READ_PREFER, 2000, 0, MAJOR_FREEZE_LOCK_WAIT, "major freeze lock")
LATCH_DEF(PARTITION_TABLE_UPDATER_LOCK, 42, "partition table updater lock", LATCH_READ_PREFER, 2000, 0, PARTITION_TABLE_UPDATER_LOCK_WAIT, "partition table updater lock")
LATCH_DEF(MULTI_TENANT_LOCK, 43, "multi tenant lock", LATCH_READ_PREFER, 2000, 0, MULTI_TENANT_LOCK_WAIT, "multi tenant lock")
LATCH_DEF(LEADER_COORDINATOR_LOCK, 44, "leader coordinator lock", LATCH_READ_PREFER, 2000, 0, LEADER_COORDINATOR_LOCK_WAIT, "leader coordinator lock")
LATCH_DEF(LEADER_STAT_LOCK, 45, "leader stat lock", LATCH_READ_PREFER, 2000, 0, LEADER_STAT_LOCK_WAIT, "leader stat lock")
LATCH_DEF(ROOT_MAJOR_FREEZE_LOCK, 46, "root major freeze lock", LATCH_READ_PREFER, 2000, 0, ROOT_MAJOR_FREEZE_LOCK_WAIT, "root major freeze lock")
LATCH_DEF(RS_BOOTSTRAP_LOCK, 47, "rs bootstrap lock", LATCH_READ_PREFER, 2000, 0, RS_BOOTSTRAP_LOCK_WAIT, "rs bootstrap lock")
LATCH_DEF(SCHEMA_MGR_ITEM_LOCK, 48, "schema mgr item lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_MGR_ITEM_LOCK_WAIT, "schema mgr item lock")
LATCH_DEF(SCHEMA_MGR_LOCK, 49, "schema mgr lock", LATCH_READ_PREFER, 2000, 0, SCHEMA_MGR_LOCK_WAIT, "schema mgr lock")
LATCH_DEF(SUPER_BLOCK_LOCK, 50, "super block lock", LATCH_FIFO, 2000, 0, SUPER_BLOCK_LOCK_WAIT, "super block lock")
LATCH_DEF(FROZEN_VERSION_LOCK, 51, "frozen version lock", LATCH_READ_PREFER, 2000, 0, FROZEN_VERSION_LOCK_WAIT, "frozen version lock")
LATCH_DEF(RS_BROADCAST_LOCK, 52, "rs broadcast lock", LATCH_READ_PREFER, 2000, 0, RS_BROADCAST_LOCK_WAIT, "rs broadcast lock")
LATCH_DEF(SERVER_STATUS_LOCK, 53, "server status lock", LATCH_READ_PREFER, 2000, 0, SERVER_STATUS_LOCK_WAIT, "server status lock")
LATCH_DEF(SERVER_MAINTAINCE_LOCK, 54, "server maintaince lock", LATCH_READ_PREFER, 2000, 0, SERVER_MAINTAINCE_LOCK_WAIT, "server maintaince lock")
LATCH_DEF(UNIT_MANAGER_LOCK, 55, "unit manager lock", LATCH_READ_PREFER, 2000, 0, UNIT_MANAGER_LOCK_WAIT, "unit manager lock")
LATCH_DEF(ZONE_MANAGER_LOCK, 56, "zone manager lock", LATCH_READ_PREFER, 2000, 0, ZONE_MANAGER_LOCK_WAIT, "zone manager lock")
LATCH_DEF(ALLOC_OBJECT_LOCK, 57, "object set lock", LATCH_READ_PREFER, 2000, 0, ALLOC_OBJECT_LOCK_WAIT, "alloc object lock")
LATCH_DEF(ALLOC_BLOCK_LOCK, 58, "block set lock", LATCH_READ_PREFER, 2000, 0, ALLOC_BLOCK_LOCK_WAIT, "alloc block lock")
LATCH_DEF(TRACE_RECORDER_LOCK, 59, "normal trace recorder lock", LATCH_FIFO, 2000, 0, TRACE_RECORDER_LOCK_WAIT, "normal trace recorder latch")
LATCH_DEF(SESSION_TRACE_RECORDER_LOCK, 60, "session trace recorder lock", LATCH_FIFO, 2000, 0, SESSION_TRACE_RECORDER_LOCK_WAIT, "session trace recorder latch")
LATCH_DEF(TRANS_TRACE_RECORDER_LOCK, 61, "trans trace recorder lock", LATCH_FIFO, 2000, 0, TRANS_TRACE_RECORDER_LOCK_WAIT, "trans trace recorder latch")
LATCH_DEF(ELECT_TRACE_RECORDER_LOCK, 62, "election trace recorder lock", LATCH_FIFO, 2000, 0, ELECT_TRACE_RECORDER_LOCK_WAIT, "election trace recorder latch")
LATCH_DEF(ALIVE_SERVER_TRACER_LOCK, 63, "alive server tracer lock", LATCH_READ_PREFER, 2000, 0, ALIVE_SERVER_TRACER_LOCK_WAIT, "alive server tracer lock")


LATCH_DEF(LATCH_END, 99999, "latch end", LATCH_FIFO, 2000, 0, WAIT_EVENT_END, "latch end")
#endif

#ifndef OB_LATCH_DEFINE_H_
#define OB_LATCH_DEFINE_H_
#include "lib/wait_event/ob_wait_event.h"

namespace oceanbase
{
namespace common
{

struct ObLatchIds
{
  enum ObLatchIdEnum
  {
#define LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, wait_event, display_name) def,
#include "lib/stat/ob_latch_define.h"
#undef LATCH_DEF
  };
};

struct ObLatchPolicy
{
  enum ObLatchPolicyEnum
  {
    LATCH_READ_PREFER = 0,
    LATCH_FIFO
  };
};

struct ObLatchDesc
{
  static const int64_t MAX_LATCH_NAME_LENGTH = 64;
  int64_t latch_id_;
  char latch_name_[MAX_LATCH_NAME_LENGTH];
  int32_t policy_;
  uint64_t max_spin_cnt_;
  uint64_t max_yield_cnt_;
  int64_t wait_event_idx_;
  char display_name_[MAX_LATCH_NAME_LENGTH];
};

static const ObLatchDesc OB_LATCHES[] = {
#define LATCH_DEF(def, id, name, policy, max_spin_cnt, max_yield_cnt, wait_event, display_name) \
    {id, name, ObLatchPolicy::policy, max_spin_cnt, max_yield_cnt, ObWaitEventIds::wait_event, display_name},
#include "lib/stat/ob_latch_define.h"
#undef LATCH_DEF
};


}//common
}//oceanbase
#endif /* OB_LATCH_DEFINE_H_ */
