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

#ifdef WAIT_EVENT_DEF
WAIT_EVENT_DEF(NULL_EVENT, 10000, "system internal wait", "", "", "", OTHER, "system internal wait", true)
WAIT_EVENT_DEF(DB_FILE_DATA_READ, 10001, "db file data read", "fd", "offset", "size", USER_IO, "db file data read", true)
WAIT_EVENT_DEF(DB_FILE_DATA_INDEX_READ, 10002, "db file data index read", "fd", "offset", "size", USER_IO, "db file data index read", true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_READ, 11001, "db file compact read", "fd", "offset", "size", SYSTEM_IO, "db file compact read", true)
WAIT_EVENT_DEF(DB_FILE_COMPACT_WRITE, 11002, "db file compact write", "fd", "offset", "size", SYSTEM_IO, "db file compact write", true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_READ, 11003, "db file index build read", "fd", "offset", "size", SYSTEM_IO, "db file index build read", true)
WAIT_EVENT_DEF(DB_FILE_INDEX_BUILD_WRITE, 11004, "db file index build write", "fd", "offset", "size", SYSTEM_IO, "db file index build write", true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_READ, 11005, "db file migrate read", "fd", "offset", "size", SYSTEM_IO, "db file migrate read", true)
WAIT_EVENT_DEF(DB_FILE_MIGRATE_WRITE, 11006, "db file migrate write", "fd", "offset", "size", SYSTEM_IO, "db file migrate write", true)
WAIT_EVENT_DEF(BLOOM_FILTER_BUILD_READ, 11007, "bloomfilter build read", "fd", "offset", "size", SYSTEM_IO, "bloomfilter build read", true)
//scheduler
WAIT_EVENT_DEF(
    OMT_WAIT, 12001, "sched wait",
    "req type", "req start timestamp", "wait start timestamp",
    SCHEDULER, "sched wait", true)
WAIT_EVENT_DEF(
    OMT_IDLE, 12002, "sched idle",
    "wait start timestamp", "", "",
    IDLE, "sched idle", true)
//network
WAIT_EVENT_DEF(SYNC_RPC, 13000, "sync rpc", "pcode", "size", "", NETWORK, "sync rpc", true)
//appliation
WAIT_EVENT_DEF(MT_READ_LOCK_WAIT,14001,"memstore read lock wait","lock","waiter","owner",APPLICATION,"memstore read lock wait", false)
WAIT_EVENT_DEF(MT_WRITE_LOCK_WAIT,14002,"memstore write lock wait","lock","waiter","owner",APPLICATION,"memstore write lock wait", false)
WAIT_EVENT_DEF(ROW_LOCK_WAIT,14003,"row lock wait","lock","waiter","owner",APPLICATION,"row lock wait", false)

//concurrency
WAIT_EVENT_DEF(PT_LOCATION_CACHE_LOCK_WAIT, 15001, "partition location cache lock wait", "table_id", "partition_id", "", CONCURRENCY, "partition location cache lock wait", true)
WAIT_EVENT_DEF(KV_CACHE_BUCKET_LOCK_WAIT, 15002, "latch: kvcache bucket wait", "address", "number", "tries", CONCURRENCY, "latch: kvcache bucket wait", true)
WAIT_EVENT_DEF(DEFAULT_SPIN_LOCK_WAIT, 15003, "latch: default spin lock wait", "address", "number", "tries", CONCURRENCY, "latch: default spin lock wait", true)
WAIT_EVENT_DEF(DEFAULT_SPIN_RWLOCK_WAIT, 15004, "latch: default spin rwlock wait", "address", "number", "tries", CONCURRENCY, "latch: default spin rwlock wait", true)
WAIT_EVENT_DEF(DEFAULT_MUTEX_WAIT, 15005, "latch: default mutex wait", "address", "number", "tries", CONCURRENCY, "latch: default mutex wait", true)
WAIT_EVENT_DEF(TIME_WHEEL_TASK_LOCK_WAIT, 15006, "latch: time wheel task lock wait", "address", "number", "tries", CONCURRENCY, "latch: time wheel task lock wait", true)
WAIT_EVENT_DEF(TIME_WHEEL_BUCKET_LOCK_WAIT, 15007, "latch: time wheel bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: time wheel bucket lock wait", true)
WAIT_EVENT_DEF(ELECTION_LOCK_WAIT, 15008, "latch: election lock wait", "address", "number", "tries", CONCURRENCY, "latch: election lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_LOCK_WAIT, 15009, "latch: trans ctx lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans ctx lock wait", true)
WAIT_EVENT_DEF(PARTITION_LOG_LOCK_WAIT, 15010, "latch: partition log lock wait", "address", "number", "tries", CONCURRENCY, "latch: partition log lock wait", true)
WAIT_EVENT_DEF(PLAN_CACHE_VALUE_LOCK_WAIT, 15011, "latch: plan cache value lock wait", "address", "number", "tries", CONCURRENCY, "latch: plan cache value lock wait", true)

WAIT_EVENT_DEF(CLOG_HISTORY_REPORTER_LOCK_WAIT, 15012, "latch: clog history reporter lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog history reporter lock wait", true)
WAIT_EVENT_DEF(CLOG_EXTERNAL_EXEC_LOCK_WAIT, 15013, "latch: clog external executor lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog external executor lock wait", true)
WAIT_EVENT_DEF(CLOG_MEMBERSHIP_MGR_LOCK_WAIT, 15014, "latch: clog membership mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog membership mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_RECONFIRM_LOCK_WAIT, 15015, "latch: clog reconfirm lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog reconfirm lock wait", true)
WAIT_EVENT_DEF(CLOG_SLIDING_WINDOW_LOCK_WAIT, 15016, "latch: clog sliding window lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog sliding window lock wait", true)
WAIT_EVENT_DEF(CLOG_STAT_MGR_LOCK_WAIT, 15017, "latch: clog stat mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog stat mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_TASK_LOCK_WAIT, 15018, "latch: clog task lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog task lock wait", true)
WAIT_EVENT_DEF(CLOG_IDMGR_LOCK_WAIT, 15019, "latch: clog id mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog id mgr lock wait", true)
WAIT_EVENT_DEF(CLOG_CACHE_LOCK_WAIT, 15020, "latch: clog cache lock wait", "address", "number", "tries", CONCURRENCY, "latch: clog cache lock wait", true)
WAIT_EVENT_DEF(ELECTION_MSG_LOCK_WAIT, 15021, "latch: election msg lock wait", "address", "number", "tries", CONCURRENCY, "latch: election msg lock wait", true)
WAIT_EVENT_DEF(PLAN_SET_LOCK_WAIT, 15022, "latch: plan set lock wait", "address", "number", "tries", CONCURRENCY, "latch: plan set lock wait", true)
WAIT_EVENT_DEF(PS_STORE_LOCK_WAIT, 15023, "latch: ps store lock wait", "address", "number", "tries", CONCURRENCY, "latch: ps store lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_MGR_LOCK_WAIT, 15024, "latch: trans context mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans context mgr lock wait", true)
WAIT_EVENT_DEF(DEFAULT_RECURSIVE_MUTEX_WAIT, 15025, "latch: default recursive mutex wait", "address", "number", "tries", CONCURRENCY, "latch: default recursive mutex wait", true)
WAIT_EVENT_DEF(DEFAULT_DRW_LOCK_WAIT, 15026, "latch: default drw lock wait", "address", "number", "tries", CONCURRENCY, "latch: default drw lock wait", true)
WAIT_EVENT_DEF(DEFAULT_BUCKET_LOCK_WAIT, 15027, "latch: default bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: default bucket lock wait", true)
WAIT_EVENT_DEF(TRANS_CTX_BUCKET_LOCK_WAIT, 15028, "latch: trans ctx bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans ctx bucket lock wait", true)
WAIT_EVENT_DEF(MACRO_META_BUCKET_LOCK_WAIT, 15029, "latch: macro meta bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: macro meta bucket lock wait", true)
WAIT_EVENT_DEF(TOKEN_BUCKET_LOCK_WAIT, 15030, "latch: token bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: token bucket lock wait", true)
WAIT_EVENT_DEF(LIGHTY_HASHMAP_BUCKET_LOCK_WAIT, 15031, "latch: lighty hashmap bucket lock wait", "address", "number", "tries", CONCURRENCY, "latch: lighty hashmap bucket lock wait", true)
WAIT_EVENT_DEF(ROW_CALLBACK_LOCK_WAIT, 15032, "latch: row callback lock wait", "address", "number", "tries", CONCURRENCY, "latch: row callback lock wait", true)
WAIT_EVENT_DEF(PARTITION_LOCK_WAIT, 15033, "latch: partition lock wait", "address", "number", "tries", CONCURRENCY, "latch: partition lock wait", true)
WAIT_EVENT_DEF(SWITCH_STAGGER_MERGE_FLOW_WAIT, 15034, "latch: switch stagger merge flow wait", "address", "number", "tries", CONCURRENCY, "latch: switch stagger merge flow wait", true)
WAIT_EVENT_DEF(SWITCH_LEADER_WAIT, 15035, "latch: switch leader wait", "address", "number", "tries", CONCURRENCY, "latch: switch leader wait", true)
WAIT_EVENT_DEF(PARTITION_FREEZE_WAIT, 15036, "latch: partition freeze wait", "address", "number", "tries", CONCURRENCY, "latch: partition freeze wait", true)
WAIT_EVENT_DEF(SCHEMA_SERVICE_LOCK_WAIT, 15037, "latch: schema service wait", "address", "number", "tries", CONCURRENCY, "latch: schema service wait", true)
WAIT_EVENT_DEF(SCHEMA_SERVICE_STATS_LOCK_WAIT, 15038, "latch: schema service stats wait", "address", "number", "tries", CONCURRENCY, "latch: schema service stats wait", true)
WAIT_EVENT_DEF(TENANT_LOCK_WAIT, 15039, "latch: tenant lock wait", "address", "number", "tries", CONCURRENCY, "latch: tenant lock wait", true)
WAIT_EVENT_DEF(CONFIG_LOCK_WAIT, 15040, "latch: config lock wait", "address", "number", "tries", CONCURRENCY, "latch: config lock wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_LOCK_WAIT, 15041, "latch: major freeze lock wait", "address", "number", "tries", CONCURRENCY, "latch: major freeze lock wait", true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_LOCK_WAIT, 15042, "latch: partition table updater lock wait", "address", "number", "tries", CONCURRENCY, "latch: partition table updater wait", true)
WAIT_EVENT_DEF(MULTI_TENANT_LOCK_WAIT, 15043, "latch: multi tenant lock wait", "address", "number", "tries", CONCURRENCY, "latch: multi tenant lock wait", true)
WAIT_EVENT_DEF(LEADER_COORDINATOR_LOCK_WAIT, 15044, "latch: leader coordinator lock wait", "address", "number", "tries", CONCURRENCY, "latch: leader coordinator lock wait", true)
WAIT_EVENT_DEF(LEADER_STAT_LOCK_WAIT, 15045, "latch: leader stat lock wait", "address", "number", "tries", CONCURRENCY, "latch: leader stat lock wait", true)
WAIT_EVENT_DEF(ROOT_MAJOR_FREEZE_LOCK_WAIT, 15046, "latch: root major freeze lock wait", "address", "number", "tries", CONCURRENCY, "latch: root major freeze lock wait", true)
WAIT_EVENT_DEF(RS_BOOTSTRAP_LOCK_WAIT, 15047, "latch: rs bootstrap lock wait", "address", "number", "tries", CONCURRENCY, "latch: rs bootstap lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_ITEM_LOCK_WAIT, 15048, "latch: schema mgr item lock wait", "address", "number", "tries", CONCURRENCY, "latch: schema mgr item lock wait", true)
WAIT_EVENT_DEF(SCHEMA_MGR_LOCK_WAIT, 15049, "latch: schema mgr lock wait", "address", "number", "tries", CONCURRENCY, "latch: schema mgr lock wait", true)
WAIT_EVENT_DEF(SUPER_BLOCK_LOCK_WAIT, 15050, "latch: super block lock wait", "address", "number", "tries", CONCURRENCY, "latch: super block lock wait", true)
WAIT_EVENT_DEF(FROZEN_VERSION_LOCK_WAIT, 15051, "latch: frozen version lock wait", "address", "number", "tries", CONCURRENCY, "latch: frozen version lock wait", true)
WAIT_EVENT_DEF(RS_BROADCAST_LOCK_WAIT, 15052, "latch: rs broadcast lock wait", "address", "number", "tries", CONCURRENCY, "latch: rs broadcast lock wait", true)
WAIT_EVENT_DEF(SERVER_STATUS_LOCK_WAIT, 15053, "latch: server status lock wait", "address", "number", "tries", CONCURRENCY, "latch: server status lock wait", true)
WAIT_EVENT_DEF(SERVER_MAINTAINCE_LOCK_WAIT, 15054, "latch: server maintaince lock wait", "address", "number", "tries", CONCURRENCY, "latch: server maintaince lock wait", true)
WAIT_EVENT_DEF(UNIT_MANAGER_LOCK_WAIT, 15055, "latch: unit manager lock wait", "address", "number", "tries", CONCURRENCY, "latch: unit manager lock wait", true)
WAIT_EVENT_DEF(ZONE_MANAGER_LOCK_WAIT, 15056, "latch: zone manager lock wait", "address", "number", "tries", CONCURRENCY, "latch: zone manager lock wait", true)
WAIT_EVENT_DEF(ALLOC_OBJECT_LOCK_WAIT, 15057, "latch: alloc object lock wait", "address", "number", "tries", CONCURRENCY, "latch: alloc object lock wait", true)
WAIT_EVENT_DEF(ALLOC_BLOCK_LOCK_WAIT, 15058, "latch: alloc block lock wait", "address", "number", "tries", CONCURRENCY, "latch: alloc block lock wait", true)
WAIT_EVENT_DEF(TRACE_RECORDER_LOCK_WAIT, 15059, "latch: trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: normal trace recorder lock wait", true)
WAIT_EVENT_DEF(SESSION_TRACE_RECORDER_LOCK_WAIT, 15060, "latch: session trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: session trace recorder lock wait", true)
WAIT_EVENT_DEF(TRANS_TRACE_RECORDER_LOCK_WAIT, 15061, "latch: trans trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: trans trace recorder lock wait", true)
WAIT_EVENT_DEF(ELECT_TRACE_RECORDER_LOCK_WAIT, 15062, "latch: election trace recorder lock wait", "address", "number", "tries", CONCURRENCY, "latch: election trace recorder lock wait", true)
WAIT_EVENT_DEF(ALIVE_SERVER_TRACER_LOCK_WAIT, 15063, "latch: alive server tracer lock wait", "address", "number", "tries", CONCURRENCY, "latch: alive server tracer lock wait", true)


WAIT_EVENT_DEF(DEFAULT_COND_WAIT, 15101, "default condition wait", "address", "", "", CONCURRENCY, "default condition wait", true)
WAIT_EVENT_DEF(DEFAULT_SLEEP, 15102, "default sleep", "", "", "", CONCURRENCY, "default sleep", true)
WAIT_EVENT_DEF(CLOG_WRITER_COND_WAIT, 15103, "clog writer condition wait", "address", "", "", CONCURRENCY, "clog writer condition wait", true)
WAIT_EVENT_DEF(IO_CONTROLLER_COND_WAIT, 15104, "io controller condition wait", "address", "", "", CONCURRENCY, "io controller condition wait", true)
WAIT_EVENT_DEF(IO_PROCESSOR_COND_WAIT, 15105, "io processor condition wait", "address", "", "", CONCURRENCY, "io processor condition wait", true)
WAIT_EVENT_DEF(DEDUP_QUEUE_COND_WAIT, 15106, "dedup queue condition wait", "address", "", "", CONCURRENCY, "dedup queue condition wait", true)
WAIT_EVENT_DEF(SEQ_QUEUE_COND_WAIT, 15107, "seq queue condition wait", "address", "", "", CONCURRENCY, "seq queue condition wait", true)
WAIT_EVENT_DEF(INNER_CONNECTION_POOL_COND_WAIT, 15108, "inner connection pool condition wait", "address", "", "", CONCURRENCY, "inner connection pool condition wait", true)
WAIT_EVENT_DEF(PARTITION_TABLE_UPDATER_COND_WAIT, 15109, "partition table updater condition wait", "address", "", "", CONCURRENCY, "partition table updater condition wait", true)
WAIT_EVENT_DEF(REBALANCE_TASK_MGR_COND_WAIT, 15110, "rebalance task mgr condition wait", "address", "", "", CONCURRENCY, "rebalance task mgr condition wait", true)
WAIT_EVENT_DEF(ASYNC_RPC_PROXY_COND_WAIT, 15111, "async rpc proxy condition wait", "address", "", "", CONCURRENCY, "async rpc proxy condition wait", true)
WAIT_EVENT_DEF(THREAD_IDLING_COND_WAIT, 15112, "thread idling condition wait", "address", "", "", CONCURRENCY, "thread idling condition wait", true)
WAIT_EVENT_DEF(RPC_SESSION_HANDLER_COND_WAIT, 15113, "rpc session handler condition wait", "address", "", "", CONCURRENCY, "rpc session handler condition wait", true)
WAIT_EVENT_DEF(LOCATION_CACHE_COND_WAIT, 15114, "location cache condition wait", "address", "", "", CONCURRENCY, "location cache condition wait", true)
WAIT_EVENT_DEF(REENTRANT_THREAD_COND_WAIT, 15115, "reentrant thread condition wait", "address", "", "", CONCURRENCY, "reentrant thread condition wait", true)
WAIT_EVENT_DEF(MAJOR_FREEZE_COND_WAIT, 15116, "major freeze condition wait", "address", "", "", CONCURRENCY, "major freeze condition wait", true)
WAIT_EVENT_DEF(MINOR_FREEZE_COND_WAIT, 15117, "minor freeze condition wait", "address", "", "", CONCURRENCY, "minor freeze condition wait", true)
WAIT_EVENT_DEF(TH_WORKER_COND_WAIT, 15118, "th worker condition wait", "address", "", "", CONCURRENCY, "th worker condition wait", true)
WAIT_EVENT_DEF(DEBUG_SYNC_COND_WAIT, 15119, "debug sync condition wait", "address", "", "", CONCURRENCY, "debug sync condition wait", true)
WAIT_EVENT_DEF(EMPTY_SERVER_CHECK_COND_WAIT, 15120, "empty server check condition wait", "address", "", "", CONCURRENCY, "empty server check condition wait", true)

//transaction
WAIT_EVENT_DEF(END_TRANS_WAIT, 16001, "wait end trans", "rollback", "trans_hash_value", "participant_count", COMMIT,"wait end trans", false)
WAIT_EVENT_DEF(START_STMT_WAIT, 16002, "wait start stmt", "trans_hash_value", "physic_plan_type", "participant_count", CLUSTER, "wait start stmt", false)
WAIT_EVENT_DEF(END_STMT_WAIT, 16003, "wait end stmt", "rollback", "trans_hash_value", "physic_plan_type", CLUSTER, "wait end stmt", false)
WAIT_EVENT_DEF(REMOVE_PARTITION_WAIT, 16004, "wait remove partition", "tenant_id", "table_id", "partition_id", ADMINISTRATIVE, "wait remove partition", false)

WAIT_EVENT_DEF(WAIT_EVENT_END, 99999, "event end", "", "", "", OTHER, "event end", false)
#endif

#ifndef OB_WAIT_EVENT_DEFINE_H_
#define OB_WAIT_EVENT_DEFINE_H_

#include "lib/wait_event/ob_wait_class.h"


namespace oceanbase
{
namespace common
{
static const int64_t MAX_WAIT_EVENT_NAME_LENGTH = 64;
static const int64_t MAX_WAIT_EVENT_PARAM_LENGTH = 64;
static const int64_t SESSION_WAIT_HISTORY_NEST = 10;

struct ObWaitEventIds
{
  enum ObWaitEventIdEnum
  {
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, display_name, is_phy) def,
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
  };
};

struct ObWaitEventDesc
{
  int64_t event_no_;
  uint64_t p1_;
  uint64_t p2_;
  uint64_t p3_;
  int64_t wait_begin_time_;
  int64_t wait_end_time_;
  int64_t wait_time_;
  uint64_t timeout_ms_;
  int64_t level_;
  int64_t parent_;
  bool is_phy_;
  ObWaitEventDesc();
  inline bool operator<(const ObWaitEventDesc &other) const;
  inline bool operator>(const ObWaitEventDesc &other) const;
  inline bool operator==(const ObWaitEventDesc &other) const;
  inline bool operator!=(const ObWaitEventDesc &other) const;
  inline int add(const ObWaitEventDesc &other);
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
};

struct ObWaitEventStat
{
  //total number of waits for a event
  uint64_t total_waits_;
  //total number of timeouts for a event
  uint64_t total_timeouts_;
  //total amount of time waited for a event
  uint64_t time_waited_;
  //max amount of time waited for a event
  uint64_t max_wait_;
  ObWaitEventStat();
  int add(const ObWaitEventStat &other);
  void reset();
  inline bool is_valid() const { return total_waits_ > 0; }
};

struct ObWaitEvent
{
  int64_t event_id_;
  char event_name_[MAX_WAIT_EVENT_NAME_LENGTH];
  char param1_[MAX_WAIT_EVENT_PARAM_LENGTH];
  char param2_[MAX_WAIT_EVENT_PARAM_LENGTH];
  char param3_[MAX_WAIT_EVENT_PARAM_LENGTH];
  int64_t wait_class_;
  char display_name_[MAX_WAIT_EVENT_NAME_LENGTH];
  bool is_phy_;
};


static const ObWaitEvent OB_WAIT_EVENTS[] = {
#define WAIT_EVENT_DEF(def, id, name, param1, param2, param3, wait_class, display_name, is_phy) \
  {id, name, param1, param2, param3, ObWaitClassIds::wait_class, display_name, is_phy},
#include "lib/wait_event/ob_wait_event.h"
#undef WAIT_EVENT_DEF
};

#define EVENT_NO_TO_CLASS_ID(event_no) OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_no].wait_class_].wait_class_id_
#define EVENT_NO_TO_CLASS(event_no) OB_WAIT_CLASSES[OB_WAIT_EVENTS[event_no].wait_class_].wait_class_

/**
 * -----------------------------------------------------Inline Methods------------------------------------------------------
 */
inline bool ObWaitEventDesc::operator <(const ObWaitEventDesc &other) const
{
  return wait_begin_time_ < other.wait_begin_time_;
}

inline bool ObWaitEventDesc::operator >(const ObWaitEventDesc &other) const
{
  return wait_begin_time_ > other.wait_begin_time_;
}

inline bool ObWaitEventDesc::operator ==(const ObWaitEventDesc &other) const
{
  return event_no_ == other.event_no_
           && p1_ == other.p1_
           && p2_ == other.p2_
           && p3_ == other.p3_
           && wait_begin_time_ == other.wait_begin_time_
           && wait_end_time_ == other.wait_end_time_
           && wait_time_ == other.wait_time_
           && timeout_ms_ == other.timeout_ms_
           && level_ == other.level_;
}

inline bool ObWaitEventDesc::operator !=(const ObWaitEventDesc &other) const
{
  return !(*this==(other));
}

inline int ObWaitEventDesc::add(const ObWaitEventDesc &other)
{
  int ret = OB_SUCCESS;
  if (other.wait_begin_time_ > wait_begin_time_) {
    *this = other;
  }
  return ret;
}

}
}

#endif /* OB_WAIT_EVENT_DEFINE_H_ */
