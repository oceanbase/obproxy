/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_RPC_REDIS_CTX_CACHE_H
#define OB_RPC_REDIS_CTX_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc/rpclib/ob_rpc_cache_cleaner.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "obkv/table/ob_table.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcRedisClientNetHandler;

#define RPC_REDIS_CTX_LOOKUP_CACHE_DONE   (RPC_REQ_CTX_EVENT_EVENTS_START + 4)
#define RPC_REDIS_CTX_LOOKUP_START_EVENT  (RPC_REQ_CTX_EVENT_EVENTS_START + 5)
#define RPC_REDIS_CTX_LOOKUP_CACHE_EVENT  (RPC_REQ_CTX_EVENT_EVENTS_START + 6)
#define REDIS_CLIENT_NAME_LEN 512
#define REDIS_CLIENT_LIB_LEN 32

class ObRpcRedisCtx : public common::ObSharedRefCount
{
public:
  enum ObRedisCtxState
  {
    BORN = 0,
    BUILDING,
    AVAIL,
    DIRTY,
    UPDATING,
    DELETED
  };
  enum ObRPcRedisClientFlag
  {
    Normal = 0, // normal
    Monitor      // monitor
  };
  ObRpcRedisCtx()
    :common::ObSharedRefCount(), state_(AVAIL),cs_id_(0), addr_(), obproxy_addr_(),fd_(0),
    start_time_us_(0), last_time_us_(0), flags_(Normal), redis_db_(0),qbuf_(0), qbuf_free_(0), argv_mem_(0), client_name_(NULL), redis_monitor_msg_(NULL),
    allocator_(ObModIds::REDIS_CTX){
      memset(cmd_, 0, sizeof(cmd_));
      memset(user_name_, 0, sizeof(user_name_));
      memset(lib_name_, 0, sizeof(lib_name_));
      memset(lib_ver_, 0, sizeof(lib_ver_));
    }
  ~ObRpcRedisCtx(){}

  static int alloc_and_init_redis_ctx(ObRpcRedisCtx *&ob_rpc_redis_ctx);
  static const char *get_client_flag(const ObRpcRedisCtx::ObRPcRedisClientFlag flag);
  int init() {
    int ret = OB_SUCCESS;
    state_ = AVAIL;
    return ret;
  }
  void reset()
  {
    state_ = DELETED;
    addr_.reset();
    obproxy_addr_.reset();
    cs_id_ = 0;
    flags_ = Normal;
    start_time_us_ = 0;
    last_time_us_ = 0;
    redis_db_ = 0;
    qbuf_ = 0;
    qbuf_free_ = 0;
    cmd_[0]='\0';
    user_name_[0]= '\0';
    allocator_.reset();
  }

  void free()
  {
    reset();
    op_fixed_mem_free(this, sizeof(ObRpcRedisCtx));
  }

  char *get_client_name() {return client_name_;}
  int64_t get_client_name_len() { return strlen(client_name_);}
  void set_cs_id(int64_t cs_id) {cs_id_ = cs_id; }
  int64_t get_cs_id() const {return cs_id_; }
  void set_fd(int fd) { fd_ = fd; }
  void set_start_time(int64_t start_time_us) {start_time_us_ = start_time_us; }
  void set_last_time(int64_t last_time_us) {last_time_us_ = last_time_us; }
  void set_last_cmd(char *buf);
  void set_redis_monitor_msg(char *buf);
  char *get_redis_monitor_msg() {return redis_monitor_msg_;}
  int64_t get_redis_monitor_msg_len() { return strlen(redis_monitor_msg_);}
  void set_user_name(const ObString &user_name);
  void set_client_name(const ObString &name);
  void set_lib_name(const ObString &lib_name);
  void set_lib_ver(const ObString &lib_ver);
  void set_monitor_flag() { flags_ = Monitor; }
  void set_building_state() { state_ = BUILDING; }
  void set_avail_state() { state_ = AVAIL; }
  void set_dirty_state() { state_ = DIRTY; }
  void set_deleted_state() { state_ = DELETED; }
  void set_updating_state() { state_ = UPDATING; }
  bool is_monitor_mode() { return flags_ == Monitor; }
  bool is_building_state() const { return BUILDING == state_; }
  bool is_avail_state() const { return AVAIL == state_; }
  bool is_dirty_state() const { return DIRTY == state_; }
  bool is_updating_state() const { return UPDATING == state_; }
  bool is_deleted_state() const { return DELETED == state_; }

  int format_redis_client_ctx(char *buf, int64_t buf_len, int64_t &pos);
  // int format_redis_monitor_info(char *buf, int64_t buf_len, int64_t &pos);
  TO_STRING_KV(KP(this),
               K_(cs_id),
               K_(addr),
               K_(obproxy_addr),
               K_(start_time_us),
               K_(last_time_us),
               K_(redis_db),
               K_(qbuf),
               K_(qbuf_free),
               K_(argv_mem),
               KP_(cmd));

public:
  ObRedisCtxState state_;
  int64_t cs_id_;
  ObConnectionAttributes addr_;
  ObConnectionAttributes obproxy_addr_;
  int fd_;
  int64_t start_time_us_;
  int64_t last_time_us_;
  ObRPcRedisClientFlag flags_;
  uint64_t redis_db_;
  int64_t qbuf_;
  int64_t qbuf_free_;
  int64_t argv_mem_;
  char *client_name_;
  char *redis_monitor_msg_;
  char cmd_[50];
  char user_name_[50];
  char lib_name_[REDIS_CLIENT_LIB_LEN];
  char lib_ver_[REDIS_CLIENT_LIB_LEN];
  common::ObArenaAllocator allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisCtx);
};

typedef obutils::ObMTHashTable<uint64_t, ObRpcRedisCtx *> ObRpcRedisCtxMTHashMap;
typedef obutils::ObHashTableIteratorState<uint64_t, ObRpcRedisCtx *> RpcRedisCtxIter;

struct ObRpcRedisCtxCacheParam
{
public:
  enum Op
  {
    INVALID_RPC_REDIS_CTX_OP = 0,
    ADD_RPC_REDIS_CTX_OP,
    REMOVE_RPC_REDIS_CTX_OP,
    ADD_RPC_REDIS_CTX_IF_NOT_EXIST_OP,
  };

  ObRpcRedisCtxCacheParam() : hash_(0), key_(), op_(INVALID_RPC_REDIS_CTX_OP), ctx_(NULL) {}
  ~ObRpcRedisCtxCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_RPC_REDIS_CTX_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  uint64_t key_;
  Op op_;
  ObRpcRedisCtx *ctx_;
  SLINK(ObRpcRedisCtxCacheParam, link_);
};

class ObRpcRedisCtxCache : public ObRpcRedisCtxMTHashMap
{
public:
  static const int64_t RPC_REDIS_CTX_CACHE_MAP_SIZE = 1024;

  ObRpcRedisCtxCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObRpcRedisCtxCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

  int get_rpc_redis_ctx(event::ObContinuation *cont, uint64_t &key,
                     ObRpcRedisCtx **ppctx, event::ObAction *&action);
  int get_all_rpc_redis_ctx(char *buf, int64_t buf_len, int64_t &pos);

  int add_rpc_redis_ctx(ObRpcRedisCtx &ctx, bool direct_add);
  int add_rpc_redis_ctx_if_not_exist(ObRpcRedisCtx &ctx, bool direct_add);

  int remove_rpc_redis_ctx(uint64_t key);
  int run_todo_list(const int64_t buck_id);

  void set_cache_expire_time(const int64_t relative_time_ms);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_rpc_redis_ctx_expired(const ObRpcRedisCtx &ctx);
  bool is_rpc_redis_ctx_expired_in_time_mode(const ObRpcRedisCtx &ctx);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_rpc_redis_ctx(ObRpcRedisCtx *ctx);

private:
  int get_one_sub_bucket_redis_ctx(const int64_t bucket_idx, char *buf, int64_t buf_len, int64_t &pos);
  int process(const int64_t buck_id, ObRpcRedisCtxCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisCtxCache);
};

inline void ObRpcRedisCtxCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObRpcRedisCtxCache::is_rpc_redis_ctx_expired(const ObRpcRedisCtx &ctx)
{
   return is_rpc_redis_ctx_expired_in_time_mode(ctx);
}

bool ObRpcRedisCtxCache::is_rpc_redis_ctx_expired_in_time_mode(const ObRpcRedisCtx &ctx)
{
  UNUSED(ctx);
  return false;
}

extern ObRpcRedisCtxCache &get_global_rpc_redis_ctx_cache();


} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OB_RPC_REDIS_CTX_CACHE_H */