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

#ifndef OB_RPC_REDIS_MONITOR_CACHE_H
#define OB_RPC_REDIS_MONITOR_CACHE_H
#include "lib/hash/ob_hashset.h"
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc/redis/ob_rpc_redis_ctx_cache.h"
#include "proxy/rpc/rpclib/ob_rpc_cache_cleaner.h"
#include "obkv/table/ob_table.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcRedisCtx;

#define RPC_REDIS_MONITOR_LOOKUP_CACHE_DONE      (RPC_REDIS_MONITOR_EVENT_EVENTS_START + 1)
#define RPC_REDIS_MONITOR_LOOKUP_START_EVENT     (RPC_REDIS_MONITOR_EVENT_EVENTS_START + 2)
#define RPC_REDIS_MONITOR_LOOKUP_CACHE_EVENT     (RPC_REDIS_MONITOR_EVENT_EVENTS_START + 3)

class ObRpcRedisMonitorMsg : public common::ObSharedRefCount
{
public:
  enum ObRedisMonitorMsgState
  {
    RC_BORN = 0,
    RC_INITING,
    RC_AVAIL,
    RC_DELETING,
  };
  ObRpcRedisMonitorMsg()
    : common::ObSharedRefCount(), rc_state_(RC_BORN), generate_time_(0), msg_len_(0), monitor_msg_(NULL) {
    }
  ~ObRpcRedisMonitorMsg() {}
  static int alloc_and_init_redis_monitor_msg(ObRpcRedisMonitorMsg *&redis_monitor_msg, int &msg_len);
  void reset();
  int init()
  {
    int ret = OB_SUCCESS;
    rc_state_ = RC_AVAIL;
    return ret;
  }
  void free()
  {
    rc_state_ = RC_DELETING;
    generate_time_ = 0;
    // monitor_msg_[0] = '\0';
    op_fixed_mem_free(monitor_msg_, msg_len_);
    op_fixed_mem_free(this, sizeof(ObRpcRedisMonitorMsg));
  }
  int64_t get_generate_time() const { return generate_time_; }
  void set_generate_time(int64_t time_us) { generate_time_ = time_us; }
  void set_monitor_msg(char *buf);
  char* get_monitor_msg() { return monitor_msg_;}

  void set_avail_state() { rc_state_ = RC_AVAIL; }
  void set_initing_state() { rc_state_ = RC_INITING; }
  void set_deleting_state() { rc_state_ = RC_DELETING; }
  bool is_avail() const { return (RC_AVAIL == rc_state_); }
  bool is_initing() const { return (RC_INITING == rc_state_); }
  bool is_deleting()
  {
   if (RC_DELETING != rc_state_) {
    int64_t cur_time_us = common::hrtime_to_usec(get_hrtime_internal());
    if (cur_time_us - generate_time_ >= HRTIME_SECONDS(1)) {
      rc_state_ = RC_DELETING;
    }
   }
   return (RC_DELETING == rc_state_);
  }

  TO_STRING_KV(KP(this),
               K_(generate_time),
               K_(monitor_msg));
public:
  ObRedisMonitorMsgState rc_state_;
  int64_t generate_time_;
  int msg_len_;
  char *monitor_msg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisMonitorMsg);
};

template<class K, class V>
struct ObGetRpcRedisMonitorContextKey
{
  int64_t operator() (const ObRpcRedisMonitorMsg *redis_monitor_msg) const
  {
    int64_t key = 0;
    if (OB_LIKELY(NULL != redis_monitor_msg)) {
      key = redis_monitor_msg->get_generate_time();
    }
    return key;
  }
};

typedef obutils::ObMTHashTable<uint64_t, ObList<ObRpcRedisMonitorMsg *> *>ObRpcRedisMonitorMTHashMap;
typedef obutils::ObHashTableIteratorState<uint64_t, ObList<ObRpcRedisMonitorMsg *> *> RpcRedisMonitorIter;
struct ObRpcRedisMonitorCacheParam
{
public:
  enum Op
  {
    INVALID_RPC_REDIS_MONITOR_OP = 0,
    ADD_RPC_REDIS_MONITOR_OP,
    REMOVE_RPC_REDIS_MONITOR_OP,
    ADD_RPC_REDIS_MONITOR_IF_NOT_EXIST_OP,
  };

  ObRpcRedisMonitorCacheParam() : hash_(0), key_(), op_(INVALID_RPC_REDIS_MONITOR_OP), ctx_(NULL) {}
  ~ObRpcRedisMonitorCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_RPC_REDIS_MONITOR_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  uint64_t key_;
  Op op_;
  ObList<ObRpcRedisMonitorMsg *> *ctx_;
  SLINK(ObRpcRedisMonitorCacheParam, link_);
};

class ObRpcRedisMonitorCache : public ObRpcRedisMonitorMTHashMap
{
public:
  static const int64_t RPC_REDIS_MONITOR_CACHE_MAP_SIZE = 64;
  ObRpcRedisMonitorCache() : is_inited_(false) {}
  virtual ~ObRpcRedisMonitorCache() { destroy(); }
  int init(const int64_t bucket_size);
  void destroy();
  int feed_all_rpc_redis_monitor(ObRpcRedisMonitorMsg *msg);
  int add_rpc_redis_monitor_if_not_exist(ObList<ObRpcRedisMonitorMsg *> &monitor_list, uint64_t key, bool direct_add);
  int remove_rpc_redis_monitor(uint64_t key);
  int run_todo_list(const int64_t buck_id);
  TO_STRING_KV(K_(is_inited));
  static bool gc_rpc_redis_monitor(ObList<ObRpcRedisMonitorMsg *> *set_redis_monitor_msglist);
private:
  int feed_one_sub_bucket_redis_monitor(const int64_t bucket_idx, ObRpcRedisMonitorMsg *msg);
  int process(const int64_t buck_id, ObRpcRedisMonitorCacheParam *param);
private:
  bool is_inited_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisMonitorCache);
};

extern ObRpcRedisMonitorCache &get_global_rpc_redis_monitor_cache();

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OB_RPC_REDIS_MONITOR_CACHE_H */