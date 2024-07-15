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

#ifndef OB_RPC_REQ_CTX_CACHE_H
#define OB_RPC_REQ_CTX_CACHE_H
#include "utils/ob_ref_hash_map.h"
#include "obutils/ob_mt_hashtable.h"
#include "obutils/ob_proxy_config.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx.h"
#include "proxy/rpc_optimize/rpclib/ob_rpc_cache_cleaner.h"
#include "obkv/table/ob_table.h"

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
typedef obutils::ObMTHashTable<obkv::ObTableApiCredential, ObRpcReqCtx *> ObRpcReqCtxMTHashMap;
typedef obutils::ObHashTableIteratorState<obkv::ObTableApiCredential, ObRpcReqCtx *> RpcReqCtxIter;

struct ObRpcReqCtxCacheParam
{
public:
  enum Op
  {
    INVALID_RPC_REQ_CTX_OP = 0,
    ADD_RPC_REQ_CTX_OP,
    REMOVE_RPC_REQ_CTX_OP,
    ADD_RPC_REQ_CTX_IF_NOT_EXIST_OP,
  };

  ObRpcReqCtxCacheParam() : hash_(0), key_(), op_(INVALID_RPC_REQ_CTX_OP), ctx_(NULL) {}
  ~ObRpcReqCtxCacheParam() {}
  int64_t to_string(char *buf, const int64_t buf_len) const;
  static const char *get_op_name(const Op op);

  static const int64_t SCHEDULE_RPC_REQ_CTX_CACHE_CONT_INTERVAL =  HRTIME_MSECONDS(1);
  uint64_t hash_;
  obkv::ObTableApiCredential key_;
  Op op_;
  ObRpcReqCtx *ctx_;
  SLINK(ObRpcReqCtxCacheParam, link_);
};

class ObRpcReqCtxCache : public ObRpcReqCtxMTHashMap
{
public:
  static const int64_t RPC_REQ_CTX_CACHE_MAP_SIZE = 1024;

  ObRpcReqCtxCache() : is_inited_(false), expire_time_us_(0) {}
  virtual ~ObRpcReqCtxCache() { destroy(); }

  int init(const int64_t bucket_size);
  void destroy();

  int get_rpc_req_ctx(event::ObContinuation *cont, const obkv::ObTableApiCredential &key,
                     ObRpcReqCtx **ppctx, event::ObAction *&action);

  int add_rpc_req_ctx(ObRpcReqCtx &ctx, bool direct_add);
  int add_rpc_req_ctx_if_not_exist(ObRpcReqCtx &ctx, bool direct_add);

  int remove_rpc_req_ctx(const obkv::ObTableApiCredential &key);
  int run_todo_list(const int64_t buck_id);

  void set_cache_expire_time(const int64_t relative_time_s);
  int64_t get_cache_expire_time_us() const { return expire_time_us_; }
  bool is_rpc_req_ctx_expired(const ObRpcReqCtx &ctx);
  bool is_rpc_req_ctx_expired_in_time_mode(const ObRpcReqCtx &ctx);
  TO_STRING_KV(K_(is_inited), K_(expire_time_us));

  static bool gc_rpc_req_ctx(ObRpcReqCtx *ctx);

private:
  int process(const int64_t buck_id, ObRpcReqCtxCacheParam *param);

private:
  bool is_inited_;
  int64_t expire_time_us_;
  common::ObAtomicList todo_lists_[obutils::MT_HASHTABLE_PARTITIONS];
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtxCache);
};

inline void ObRpcReqCtxCache::set_cache_expire_time(const int64_t relative_time_ms)
{
  expire_time_us_ = common::ObTimeUtility::current_time();
  expire_time_us_ += common::msec_to_usec(relative_time_ms);
}

bool ObRpcReqCtxCache::is_rpc_req_ctx_expired(const ObRpcReqCtx &ctx)
{
   return is_rpc_req_ctx_expired_in_time_mode(ctx);
}

bool ObRpcReqCtxCache::is_rpc_req_ctx_expired_in_time_mode(const ObRpcReqCtx &ctx)
{
  UNUSED(ctx);
  return false;
}

extern ObRpcReqCtxCache &get_global_rpc_req_ctx_cache();

template<class K, class V>
struct ObGetRpcReqContextKey
{
  obkv::ObTableApiCredential operator() (const ObRpcReqCtx *rpc_req_ctx) const
  {
    obkv::ObTableApiCredential key;
    if (OB_LIKELY(NULL != rpc_req_ctx)) {
      key = rpc_req_ctx->get_credential();
    }
    return key;
  }
};

static const int64_t RPC_REQ_CTX_ENTRY_HASH_MAP_SIZE = 16 * 1024; // 16KB
typedef obproxy::ObRefHashMap<obkv::ObTableApiCredential, ObRpcReqCtx *, ObGetRpcReqContextKey, RPC_REQ_CTX_ENTRY_HASH_MAP_SIZE> ObRpcReqCtxHashMap;

class ObRpcReqCtxRefHashMap: public ObRpcReqCtxHashMap
{
public:
  ObRpcReqCtxRefHashMap(const common::ObModIds::ObModIdEnum mod_id) : ObRpcReqCtxHashMap(mod_id) {}
  virtual ~ObRpcReqCtxRefHashMap() {}
  int clean_hash_map();

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtxRefHashMap);
};

int init_rpc_req_ctx_map_for_thread();
int init_rpc_req_ctx_map_for_one_thread(int64_t index);

} // end of namespace proxy
} // end of namespace obproxy
} // end of namespace oceanbase
#endif /* OB_RPC_REQ_CTX_CACHE_H */