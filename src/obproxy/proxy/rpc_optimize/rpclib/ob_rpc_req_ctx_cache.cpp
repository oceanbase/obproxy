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

#define USING_LOG_PREFIX PROXY

#include "proxy/rpc_optimize/rpclib/ob_rpc_req_ctx_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcReqCtxCacheCont : public event::ObContinuation
{
public:
  explicit ObRpcReqCtxCacheCont(ObRpcReqCtxCache &rpc_req_ctx_cache)
    : ObContinuation(NULL), rpc_req_ctx_cache_(rpc_req_ctx_cache), ppctx_(NULL),
      hash_(0), is_add_building_ctx_(false), key_() {}
  virtual ~ObRpcReqCtxCacheCont() {}
  void destroy();
  int get_rpc_req_ctx(const int event, ObEvent *e);
  static int get_rpc_req_ctx_local(ObRpcReqCtxCache &rpc_req_ctx_cache,
                                   const obkv::ObTableApiCredential &key,
                                   const uint64_t hash,
                                   bool &is_locked,
                                   ObRpcReqCtx *&rpc_req_ctx);
  static int add_building_rpc_req_ctx(ObRpcReqCtxCache &rpc_req_ctx_cache,
                                      const obkv::ObTableApiCredential &key);
  event::ObAction action_;
  ObRpcReqCtxCache &rpc_req_ctx_cache_;
  ObRpcReqCtx **ppctx_;
  uint64_t hash_;
  bool is_add_building_ctx_;
  obkv::ObTableApiCredential key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcReqCtxCacheCont);
};

int64_t ObRpcReqCtxCacheParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("op_name", get_op_name(op_),
      K_(hash),
      K_(key));
  J_COMMA();
  if (NULL != ctx_) {
    J_KV(K_(*ctx));
  }
  J_OBJ_END();
  return pos;
}

inline void ObRpcReqCtxCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

int ObRpcReqCtxCacheCont::get_rpc_req_ctx(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_rpc_req_ctx started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObRpcReqCtx *tmp_ctx = NULL;
    if (OB_FAIL(get_rpc_req_ctx_local(rpc_req_ctx_cache_, key_, hash_, is_locked, tmp_ctx))) {
      if (NULL != tmp_ctx) {
        tmp_ctx->dec_ref();
        tmp_ctx = NULL;
      }
      LOG_WDIAG("fail to get rpc ctx", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_rpc_req_ctx MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObRpcReqCtxCacheParam::SCHEDULE_RPC_REQ_CTX_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObRpcReqCtxCacheParam::SCHEDULE_RPC_REQ_CTX_CACHE_CONT_INTERVAL))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("fail to schedule in", K(ret));
      }
      he_ret = EVENT_CONT;
    } else {
      if (NULL != *ppctx_) {
        (*ppctx_)->dec_ref();
        (*ppctx_) = NULL;
      }

      *ppctx_ = tmp_ctx;
      tmp_ctx = NULL;
      // failed or locked
      action_.continuation_->handle_event(RPC_REQ_CTX_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObRpcReqCtxCacheCont::get_rpc_req_ctx_local(
    ObRpcReqCtxCache &rpc_req_ctx_cache,
    const obkv::ObTableApiCredential &key,
    const uint64_t hash,
    bool &is_locked,
    ObRpcReqCtx *&ctx)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  ctx = NULL;

  ObProxyMutex *bucket_mutex = rpc_req_ctx_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(rpc_req_ctx_cache.run_todo_list(rpc_req_ctx_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      ctx = rpc_req_ctx_cache.lookup_entry(hash, key);
      if (NULL != ctx) {
        // if (rpc_req_ctx_cache.is_rpc_req_ctx_expired_in_time_mode(*ctx)
        //     && ctx->is_avail_state()) {
        //   ctx->set_dirty_state();
        // }
        ctx->inc_ref();
        LOG_DEBUG("cont::get_rpc_req_ctx_local, ctx found succ", KPC(ctx));
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == ctx) {
      LOG_DEBUG("cont::get_rpc_req_ctx_local, ctx not found", K(key));
    }
    lock_bucket.release();
  }

  return ret;
}

int ObRpcReqCtxCacheCont::add_building_rpc_req_ctx(ObRpcReqCtxCache &rpc_req_ctx_cache,
                                                   const obkv::ObTableApiCredential &key)
{
  int ret = OB_SUCCESS;
  UNUSED(rpc_req_ctx_cache);
  UNUSED(key);
  // ObRpcReqCtx *ctx = NULL;

  // if (OB_ISNULL(key.name_)) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_WDIAG("invalid ObRpcReqCtxKey", K(key), K(ret));
  // } else if (OB_FAIL(ObRpcReqCtx::alloc_and_init_rpc_req_ctx(*key.name_, key.cr_version_, key.cr_id_, ctx))) {
  //   LOG_WDIAG("fail to alloc and init table query async ctx", K(key), K(ret));
  // } else if (OB_ISNULL(ctx)) {
  //   ret = OB_ERR_UNEXPECTED;
  //   LOG_WDIAG("part ctx is NULL", K(ctx), K(ret));
  // } else {
  //   ctx->set_building_state();
  //   if (OB_FAIL(rpc_req_ctx_cache.add_rpc_req_ctx(*ctx, false))) {
  //     LOG_WDIAG("fail to add part ctx", KPC(ctx), K(ret));
  //     ctx->dec_ref();
  //     ctx = NULL;
  //   } else {
  //     LOG_INFO("add building part ctx succ", KPC(ctx));
  //     ctx = NULL;
  //   }
  // }

  return ret;
}

const char *ObRpcReqCtxCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_RPC_REQ_CTX_OP : {
      name = "INVALID_RPC_REQ_CTX_OP";
      break;
    }
    case ADD_RPC_REQ_CTX_OP : {
      name = "ADD_RPC_REQ_CTX_OP";
      break;
    }
    case REMOVE_RPC_REQ_CTX_OP : {
      name = "REMOVE_RPC_REQ_CTX_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

//---------------------------ObRpcReqCtxCache-------------------------//
int ObRpcReqCtxCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(ObRpcReqCtxMTHashMap::init(sub_bucket_size, RPC_REQ_CTX_MAP_LOCK, gc_rpc_req_ctx))) {
    LOG_WDIAG("fail to init hash index of rpc req ctx cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("rpc_req_ctx_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObRpcReqCtxCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObRpcReqCtxCache::destroy()
{
  LOG_INFO("ObRpcReqCtxCache will desotry");
  if (is_inited_) {
    ObRpcReqCtxCacheParam *param = NULL;
    ObRpcReqCtxCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObRpcReqCtxCacheParam *>(todo_lists_[i].popall()))) {
        while (NULL != param) {
          cur = param;
          param = param->link_.next_;
          op_free(cur);
        }
      }
    }
    is_inited_ = false;
  }
}

int ObRpcReqCtxCache::get_rpc_req_ctx(
    event::ObContinuation *cont,
    const obkv::ObTableApiCredential &key,
    ObRpcReqCtx **ppctx,
    ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(ppctx) || OB_ISNULL(cont)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid arugument", K(ppctx), K(key), K(cont), K(ret));
  } else {
    uint64_t hash = key.hash_val_;
    LOG_DEBUG("begin to get rpc ctx", K(ppctx), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObRpcReqCtx *tmp_ctx = NULL;
    if (OB_FAIL(ObRpcReqCtxCacheCont::get_rpc_req_ctx_local(*this, key, hash, is_locked, tmp_ctx))) {
      if (NULL != tmp_ctx) {
        tmp_ctx->dec_ref();
        tmp_ctx = NULL;
      }
      LOG_WDIAG("fail to get rpc ctx", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppctx = tmp_ctx;
        tmp_ctx = NULL;
      } else {
        LOG_DEBUG("get_rpc_req_ctx, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObRpcReqCtxCacheParam::SCHEDULE_RPC_REQ_CTX_CACHE_CONT_INTERVAL));
        ObRpcReqCtxCacheCont *rpc_req_ctx_cont = NULL;
        if (OB_ISNULL(rpc_req_ctx_cont = op_alloc_args(ObRpcReqCtxCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for rpc ctx cache continuation", K(ret));
        } else {
          rpc_req_ctx_cont->action_.set_continuation(cont);
          rpc_req_ctx_cont->mutex_ = cont->mutex_;
          rpc_req_ctx_cont->hash_ = hash;
          rpc_req_ctx_cont->ppctx_ = ppctx;
          rpc_req_ctx_cont->key_ = key;

          SET_CONTINUATION_HANDLER(rpc_req_ctx_cont, &ObRpcReqCtxCacheCont::get_rpc_req_ctx);
          if (OB_ISNULL(self_ethread().schedule_in(rpc_req_ctx_cont,
                  ObRpcReqCtxCacheParam::SCHEDULE_RPC_REQ_CTX_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(rpc_req_ctx_cont), K(ret));
          } else {
            action = &rpc_req_ctx_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != rpc_req_ctx_cont)) {
          rpc_req_ctx_cont->destroy();
          rpc_req_ctx_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppctx = NULL;
    }
  }
  return ret;
}

int ObRpcReqCtxCache::add_rpc_req_ctx_if_not_exist(ObRpcReqCtx &ctx, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    obkv::ObTableApiCredential key = ctx.get_credential();
    uint64_t hash = key.hash_val_;
    LOG_DEBUG("add rpc ctx", K(part_num(hash)), K(ctx), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObRpcReqCtx *tmp_ctx = lookup_entry(hash, key);
          if (NULL != tmp_ctx) {
            // 不用插入，减少计数
            ctx.dec_ref();
          } else {
            tmp_ctx = insert_entry(hash, key, &ctx);
            if (NULL != tmp_ctx) {
              LOG_WDIAG("ctx is not NULL, unexpected");
            }
          }
        }
      } else {
        direct_add = true;
      }
    }

    if (direct_add) {
      // add todo list
      ObRpcReqCtxCacheParam *param = op_alloc(ObRpcReqCtxCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for rpc param", K(param), K(ret));
      } else {
        param->op_ = ObRpcReqCtxCacheParam::ADD_RPC_REQ_CTX_IF_NOT_EXIST_OP;
        param->hash_ = hash;
        param->key_ = key;
        ctx.inc_ref();
        param->ctx_ = &ctx;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRpcReqCtxCache::add_rpc_req_ctx(ObRpcReqCtx &ctx, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    obkv::ObTableApiCredential key = ctx.get_credential();
    uint64_t hash = key.hash_val_;
    LOG_DEBUG("add table rpc ctx", K(part_num(hash)), K(ctx), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObRpcReqCtx *tmp_ctx = insert_entry(hash, key, &ctx);
          if (NULL != tmp_ctx) {
            LOG_DEBUG("remove from table rpc ctx", KPC(tmp_ctx));
            // tmp_ctx->set_deleted_state();
            tmp_ctx->dec_ref();
            tmp_ctx = NULL;
          }
        }
      } else {
        direct_add = true;
      }
    }

    if (direct_add) {
      // add todo list
      ObRpcReqCtxCacheParam *param = op_alloc(ObRpcReqCtxCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for rpc ctx param", K(param), K(ret));
      } else {
        param->op_ = ObRpcReqCtxCacheParam::ADD_RPC_REQ_CTX_OP;
        param->hash_ = hash;
        param->key_ = key;
        ctx.inc_ref();
        param->ctx_ = &ctx;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRpcReqCtxCache::remove_rpc_req_ctx(const obkv::ObTableApiCredential &key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t hash = key.hash_val_;
    ObRpcReqCtx *ctx = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        ctx = remove_entry(hash, key);
        LOG_INFO("this ctx will be removed from rpc ctx cache", KPC(ctx));
        if (NULL != ctx) {
          // ctx->set_deleted_state();
          ctx->dec_ref();
          ctx = NULL;
        }
      }
    } else {
      ObRpcReqCtxCacheParam *param = op_alloc(ObRpcReqCtxCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObRpcReqCtxCacheParam::REMOVE_RPC_REQ_CTX_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->ctx_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRpcReqCtxCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObRpcReqCtxCacheParam *pre = NULL;
    ObRpcReqCtxCacheParam *cur = NULL;
    ObRpcReqCtxCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObRpcReqCtxCacheParam *>(todo_lists_[buck_id].popall()))) {
      // 1. start the work at the end of the list, so reverse the list
      next = cur->link_.next_;
      while (NULL != next) {
        cur->link_.next_ = pre;
        pre = cur;
        cur = next;
        next = cur->link_.next_;
      };
      cur->link_.next_ = pre;

      // 2. process the param
      ObRpcReqCtxCacheParam *param = NULL;
      while ((NULL != cur) && (OB_SUCC(ret))) {
        process(buck_id, cur); // ignore ret, must clear todo_list, or will cause mem leak;
        param = cur;
        cur = cur->link_.next_;
        op_free(param);
        param = NULL;
      }
    }
  }
  return ret;
}

int ObRpcReqCtxCache::process(const int64_t buck_id, ObRpcReqCtxCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObRpcReqCtxCacheParam", K(buck_id), KPC(param));
    ObRpcReqCtx *ctx = NULL;
    switch (param->op_) {
      case ObRpcReqCtxCacheParam::ADD_RPC_REQ_CTX_OP: {
        ctx = insert_entry(param->hash_, param->key_, param->ctx_);
        if (NULL != ctx) {
          // ctx->set_deleted_state();
          ctx->dec_ref(); // free old ctx
          ctx = NULL;
        }
        if (NULL != param->ctx_) {
          // dec_ref, it was inc before push param into todo list
          param->ctx_->dec_ref();
          param->ctx_ = NULL;
        }
        break;
      }
      // 存在一些情况，重复login，rpc_ctx可能已经包含在缓存中，如果有就不处理，否则插入
      case ObRpcReqCtxCacheParam::ADD_RPC_REQ_CTX_IF_NOT_EXIST_OP: {
        ctx = lookup_entry(param->hash_, param->key_);
        if (NULL != ctx) {
          // 不用插入，减少计数
          if (NULL != param->ctx_) {
            param->ctx_->dec_ref();
          }
        } else {
          ctx = insert_entry(param->hash_, param->key_, param->ctx_);
          if (NULL != ctx) {
            LOG_WDIAG("ctx is not NULL, unexpected");
          }
        }
        if (NULL != param->ctx_) {
          // dec_ref, it was inc before push param into todo list
          param->ctx_->dec_ref();
          param->ctx_ = NULL;
        }
        break;
      }
      case ObRpcReqCtxCacheParam::REMOVE_RPC_REQ_CTX_OP: {
        ctx = remove_entry(param->hash_, param->key_);
        LOG_INFO("this ctx will be removed from rpc ctx cache", KPC(ctx));
        if (NULL != ctx) {
          // ctx->set_deleted_state();
          ctx->dec_ref(); // free old ctx
          ctx = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObRpcReqCtxCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObRpcReqCtxCache::gc_rpc_req_ctx(ObRpcReqCtx *ctx)
{
  UNUSED(ctx);
  // gc_rpc_req_ctx do nothing
  return false;
}

ObRpcReqCtxCache &get_global_rpc_req_ctx_cache()
{
  static ObRpcReqCtxCache rpc_req_ctx_cache;
  return rpc_req_ctx_cache;
}

int init_rpc_req_ctx_map_for_thread()
{
  int ret = OB_SUCCESS;
  const int64_t event_thread_count = g_event_processor.thread_count_for_type_[ET_CALL];
  for (int64_t i = 0; (i < event_thread_count) && OB_SUCC(ret); ++i) {
    if (OB_FAIL(init_rpc_req_ctx_map_for_one_thread(i))) {
      LOG_WDIAG("fail to init rpc_req_ctx_map", K(i), K(ret));
    }
  }
  return ret;
}

int init_rpc_req_ctx_map_for_one_thread(int64_t index)
{
  int ret = OB_SUCCESS;
  ObEThread **ethreads = NULL;
  if (OB_ISNULL(ethreads = g_event_processor.event_thread_[ET_CALL])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "fail to get ET_NET thread", K(ret));
  } else if (OB_ISNULL(ethreads[index])) {
    ret = OB_ERR_UNEXPECTED;
    PROXY_NET_LOG(EDIAG, "fail to get ET_NET thread", K(ret));
  } else {
    if (OB_ISNULL(ethreads[index]->rpc_req_ctx_map_ = new (std::nothrow) ObRpcReqCtxRefHashMap(ObModIds::OB_PROXY_RPC_REQ_CTX_MAP))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WDIAG("fail to new ObIndexRefHashMap", K(index), K(ethreads[index]), K(ret));
    } else if (OB_FAIL(ethreads[index]->rpc_req_ctx_map_->init())) {
      LOG_WDIAG("fail to init rpc ctx map", K(ret));
    }
  }
  return ret;
}

int ObRpcReqCtxRefHashMap::clean_hash_map()
{
  int ret = OB_SUCCESS;
  int64_t sub_map_count = get_sub_map_count();
  for (int64_t i = 0; (i < sub_map_count) && OB_SUCC(ret); ++i) {
    for (EntryIterator it = begin(i); (it != end(i)) && OB_SUCC(ret); ++it) {
      if ((*it)->is_deleting()) {
        LOG_INFO("this rpc ctx will erase from tc map", KPC((*it)));
        if (OB_FAIL(erase(it, i))) {
          LOG_WDIAG("fail to erase table query async ctx", K(i), K(ret));
        }
      }
    }
  }
  return ret;
}
}
}
}