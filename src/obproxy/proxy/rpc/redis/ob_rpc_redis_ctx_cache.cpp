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

#include "proxy/rpc/redis/ob_rpc_redis_ctx_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
class ObRpcRedisCtxCacheCont : public event::ObContinuation
{
public:
  explicit ObRpcRedisCtxCacheCont(ObRpcRedisCtxCache &rpc_redis_ctx_cache)
    : ObContinuation(NULL), rpc_redis_ctx_cache_(rpc_redis_ctx_cache), ppctx_(NULL),
      hash_(0), is_add_building_ctx_(false), key_(0) {}
  virtual ~ObRpcRedisCtxCacheCont() {}
  void destroy();
  int get_rpc_redis_ctx(const int event, ObEvent *e);
  static int get_rpc_redis_ctx_local(ObRpcRedisCtxCache &rpc_redis_ctx_cache,
                                   const uint64_t &key,
                                   const uint64_t hash,
                                   bool &is_locked,
                                   ObRpcRedisCtx *&rpc_redis_ctx);
  static int add_building_rpc_redis_ctx(ObRpcRedisCtxCache &rpc_redis_ctx_cache,
                                      const uint64_t &key);
  event::ObAction action_;
  ObRpcRedisCtxCache &rpc_redis_ctx_cache_;
  ObRpcRedisCtx **ppctx_;
  uint64_t hash_;
  bool is_add_building_ctx_;
  uint64_t key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRedisCtxCacheCont);
};

int64_t ObRpcRedisCtxCacheParam::to_string(char *buf, const int64_t buf_len) const
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

inline void ObRpcRedisCtxCacheCont::destroy()
{
  action_.set_continuation(NULL);
  mutex_.release();
  op_free(this);
}

int ObRpcRedisCtxCacheCont::get_rpc_redis_ctx(const int event, ObEvent *e)
{
  UNUSED(event);
  UNUSED(e);
  int he_ret = EVENT_DONE;
  int ret = OB_SUCCESS;
  LOG_DEBUG("cont::get_rpc_redis_ctx started");

  if (action_.cancelled_) {
    LOG_INFO("cont::action has been cancelled", K_(key), K(this));
    destroy();
  } else {
    bool is_locked = false;
    ObRpcRedisCtx *tmp_ctx = NULL;
    if (OB_FAIL(get_rpc_redis_ctx_local(rpc_redis_ctx_cache_, key_, hash_, is_locked, tmp_ctx))) {
      if (NULL != tmp_ctx) {
        tmp_ctx->dec_ref();
        tmp_ctx = NULL;
      }
      LOG_WDIAG("fail to get redis ctx", K_(key), K(ret));
    }

    if (OB_SUCC(ret) && !is_locked) {
      LOG_DEBUG("cont::get_rpc_redis_ctx MUTEX_TRY_LOCK failed, and will schedule in interval(ns)",
                LITERAL_K(ObRpcRedisCtxCacheParam::SCHEDULE_RPC_REDIS_CTX_CACHE_CONT_INTERVAL));
      if (OB_ISNULL(self_ethread().schedule_in(this, ObRpcRedisCtxCacheParam::SCHEDULE_RPC_REDIS_CTX_CACHE_CONT_INTERVAL))) {
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
      action_.continuation_->handle_event(RPC_REDIS_CTX_LOOKUP_CACHE_DONE, NULL);
      destroy();
    }
  }

  return he_ret;
}

int ObRpcRedisCtxCacheCont::get_rpc_redis_ctx_local(
    ObRpcRedisCtxCache &rpc_redis_ctx_cache,
    const uint64_t &key,
    const uint64_t hash,
    bool &is_locked,
    ObRpcRedisCtx *&ctx)
{
  int ret = OB_SUCCESS;
  is_locked = false;
  ctx = NULL;

  ObProxyMutex *bucket_mutex = rpc_redis_ctx_cache.lock_for_key(hash);
  MUTEX_TRY_LOCK(lock_bucket, bucket_mutex, this_ethread());
  if (lock_bucket.is_locked()) {
    is_locked = true;
    if (OB_FAIL(rpc_redis_ctx_cache.run_todo_list(rpc_redis_ctx_cache.part_num(hash)))) {
      LOG_WDIAG("fail to run todo list", K(key), K(hash), K(ret));
    } else {
      ctx = rpc_redis_ctx_cache.lookup_entry(hash, key);
      if (NULL != ctx) {
        ctx->inc_ref();
        LOG_DEBUG("cont::get_rpc_redis_ctx_local, ctx found succ", KPC(ctx));
      } else {
        // non-existent, return NULL
      }
    }

    if (NULL == ctx) {
      LOG_DEBUG("cont::get_rpc_redis_ctx_local, ctx not found", K(key));
    }
    lock_bucket.release();
  }

  return ret;
}

int ObRpcRedisCtxCacheCont::add_building_rpc_redis_ctx(ObRpcRedisCtxCache &rpc_redis_ctx_cache,
                                                   const uint64_t &key)
{
  int ret = OB_SUCCESS;
  UNUSED(rpc_redis_ctx_cache);
  UNUSED(key);
  return ret;
}

int ObRpcRedisCtx::alloc_and_init_redis_ctx(ObRpcRedisCtx *&ob_rpc_redis_ctx)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(ObRpcRedisCtx);
  void *buf = op_fixed_mem_alloc(alloc_size);

  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    PROXY_LOG(WDIAG, "fail to alloc mem", K(alloc_size), K(ret));
  } else {
    ob_rpc_redis_ctx = new (buf) ObRpcRedisCtx();
    if (OB_FAIL(ob_rpc_redis_ctx->init())) {
      PROXY_LOG(WDIAG, "fail to init redis ctx", KPC(ob_rpc_redis_ctx), K(ret));
    } else {
      ob_rpc_redis_ctx->inc_ref();
    }
  }
  if (OB_FAIL(ret) && (NULL!=buf)) {
    op_fixed_mem_free(buf, alloc_size);
    ob_rpc_redis_ctx = NULL;
    alloc_size = 0;
  }

  return ret;
}

const char* ObRpcRedisCtx::get_client_flag(const ObRpcRedisCtx::ObRPcRedisClientFlag flag)
{
  const char *name = NULL;
  switch (flag) {
    case Normal : {
      name = "N";
      break;
    }
    case Monitor : {
      name = "O";
      break;
    }
    default : {
      name = "N";
      break;
    }
  }
  return name;
}

void ObRpcRedisCtx::set_last_cmd(char *buf)
{
  if (OB_NOT_NULL(buf) &&strlen(buf)> 0 && strlen(buf) < 50) {
    MEMCPY(cmd_, buf, strlen(buf));
    cmd_[strlen(buf)] = '\0';
  }
}

void ObRpcRedisCtx::set_redis_monitor_msg(char *buf)
{
  int ret = OB_SUCCESS;
  int alloc_size  = OB_RPC_REDIS_MONITOR_MAX_LEN + 32;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "redis monitor msg is null", K(ret));
  } else if (OB_ISNULL(redis_monitor_msg_)) {
    redis_monitor_msg_ = (char *)allocator_.alloc(alloc_size);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(redis_monitor_msg_)) {
    int64_t sec = last_time_us_ / (1000 * 1000);
    int usec = last_time_us_ % (1000 * 1000);
    uint16_t client_port = addr_.get_port();
    char client_ip[64]{0};
    ops_ip_ntop(addr_.addr_, client_ip, sizeof(client_ip));
    int len = snprintf(redis_monitor_msg_, alloc_size,
            "+%ld.%06d [%ld %s:%d] %s\r\n",
            sec, usec,
            redis_db_,
            client_ip, client_port,
            buf
            );
    if (len < 0) {
      ret = OB_ERR_SYS;
      PROXY_CS_LOG(WDIAG, "build redis monitor info failed", K(len));
    } else {
      redis_monitor_msg_[len++]='\0';
    }
  } else {
    LOG_WDIAG("redis_monitor_msg is NULL, alloc failed", K(redis_monitor_msg_));
  }
}

void ObRpcRedisCtx::set_user_name(const ObString &user_name)
{
  if (user_name.length() > 0 && user_name.length() < 50) {
    MEMCPY(user_name_, user_name.ptr(), user_name.length());
    user_name_[user_name.length()] = '\0';
  }
}
void ObRpcRedisCtx::set_client_name(const ObString &name)
{
  if (name.length() > 0 && name.length() < REDIS_CLIENT_NAME_LEN) {
    if (OB_ISNULL(client_name_)) {
      client_name_ = (char *)allocator_.alloc(REDIS_CLIENT_NAME_LEN);
      if (OB_UNLIKELY(OB_ISNULL(client_name_))) {
        LOG_WDIAG("alloc client name failed", K(client_name_));
      }
    }
    if (OB_NOT_NULL(client_name_)) {
      MEMCPY(client_name_, name.ptr(), name.length());
      client_name_[name.length()] = '\0';
    }
  }
}

void ObRpcRedisCtx::set_lib_name(const ObString &name)
{
  if (name.length() > 0 && name.length() < REDIS_CLIENT_LIB_LEN) {
    MEMCPY(lib_name_, name.ptr(), name.length());
    lib_name_[name.length()] = '\0';
  }
}

void ObRpcRedisCtx::set_lib_ver(const ObString &name)
{
  if(name.length() > 0 && name.length() < REDIS_CLIENT_LIB_LEN) {
    MEMCPY(lib_ver_, name.ptr(), name.length());
    lib_ver_[name.length()]= '\0';
  }
}

int ObRpcRedisCtx::format_redis_client_ctx(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    ret = common::OB_ERR_UNEXPECTED;
    PROXY_CS_LOG(WDIAG, "redis inner msg is null", K(ret));
  } else {
    int64_t cur_time_us = common::hrtime_to_usec(get_hrtime_internal());
    int64_t age = (cur_time_us - start_time_us_) / (1000 * 1000);
    int64_t idle = (cur_time_us > last_time_us_) ? ((cur_time_us - last_time_us_) / (1000 * 1000)) : 0;
    uint16_t client_port = addr_.get_port();
    uint16_t obproxy_port = obproxy_addr_.get_port();
    char client_ip[64]{0};
    char obproxy_ip[64]{0};
    ops_ip_ntop(addr_.addr_, client_ip, sizeof(client_ip));
    ops_ip_ntop(obproxy_addr_.addr_, obproxy_ip, sizeof(obproxy_ip));
    int len = snprintf(buf + pos, buf_len,
            "id=%ld addr=%s:%d laddr=%s:%d fd=%d name=%s age=%ld idle=%ld flags=%s db=%ld sub=%d psub=%d "
            "ssub=%d multi=%d qbuf=%ld qbuf-free=%ld argv-mem=%ld multi-mem=%d rbs=%d rbp=%d obl=%d "
            "oll=%d omem=%d tot-mem=%d event=%s cmd=%s user=%s redir=%d resp=%d lib-name=%s lib-ver=%s \r\n",
            cs_id_,
            client_ip, client_port,
            obproxy_ip, obproxy_port,
            fd_,
            client_name_ == NULL? "":client_name_,
            age,
            idle,
            get_client_flag(flags_),
            redis_db_,
            0,
            0,
            0,
            -1,
            qbuf_,
            qbuf_free_,
            argv_mem_,
            0,
            0,  //rbs
            0,  //rbp
            0,  //obl
            0,  //oll
            0,  //omem
            0,  //tot-mem
            "",
            cmd_,
            strlen(user_name_) == 0?"(superuser)":user_name_,
            -1,
            2,
            lib_name_,
            lib_ver_
            );
    if (len < 0) {
      pos += 0;
      ret = OB_ERR_SYS;
      PROXY_CS_LOG(WDIAG, "build redis client info failed", K(len));
    } else {
      pos += len;
    }
  }
  return ret;
}

// int ObRpcRedisCtx::format_redis_monitor_info(char *buf, int64_t buf_len, int64_t &pos)
// {
//   int ret = OB_SUCCESS;
//   if (OB_UNLIKELY(OB_ISNULL(buf))) {
//     ret = common::OB_ERR_UNEXPECTED;
//     PROXY_CS_LOG(WDIAG, "redis monitor msg is null", K(ret));
//   } else {
//     int64_t sec = last_time_us_ / (1000 * 1000);
//     int usec = last_time_us_ % (1000 * 1000);
//     uint16_t client_port = addr_.get_port();
//     char client_ip[64]{0};
//     ops_ip_ntop(addr_.addr_, client_ip, sizeof(client_ip));
//     int len = snprintf(buf + pos, buf_len,
//             "%ld.%d [%ld %s:%d] %s",
//             sec, usec,
//             redis_db_,
//             client_ip, client_port,
//             redis_monitor_msg_
//             );
//     if (len < 0) {
//       pos += 0;
//       ret = OB_ERR_SYS;
//       PROXY_CS_LOG(WDIAG, "build redis monitor info failed", K(len));
//     } else {
//       pos += len;
//     }
//   }
//   return ret;
// }

const char *ObRpcRedisCtxCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_RPC_REDIS_CTX_OP : {
      name = "INVALID_RPC_REDIS_CTX_OP";
      break;
    }
    case ADD_RPC_REDIS_CTX_OP : {
      name = "ADD_RPC_REDIS_CTX_OP";
      break;
    }
    case REMOVE_RPC_REDIS_CTX_OP : {
      name = "REMOVE_RPC_REDIS_CTX_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}

//---------------------------ObRpcRedisCtxCache-------------------------//
int ObRpcRedisCtxCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(ObRpcRedisCtxMTHashMap::init(sub_bucket_size, RPC_REDIS_CTX_MAP_LOCK, gc_rpc_redis_ctx))) {
    LOG_WDIAG("fail to init hash index of rpc req ctx cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("rpc_redis_ctx_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObRpcRedisCtxCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObRpcRedisCtxCache::destroy()
{
  LOG_INFO("ObRpcRedisCtxCache will desotry");
  if (is_inited_) {
    ObRpcRedisCtxCacheParam *param = NULL;
    ObRpcRedisCtxCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObRpcRedisCtxCacheParam *>(todo_lists_[i].popall()))) {
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

int ObRpcRedisCtxCache::get_rpc_redis_ctx(
    event::ObContinuation *cont,
    uint64_t &key,
    ObRpcRedisCtx **ppctx,
    ObAction *&action)
{
  int ret = OB_SUCCESS;
  action = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(ppctx) || OB_ISNULL(cont) || OB_UNLIKELY(0 == key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid arugument", K(ppctx), K(key), K(cont), K(ret));
  } else {
    uint64_t hash = key;
    LOG_DEBUG("begin to get redis ctx", K(ppctx), K(key), K(cont), K(hash));

    bool is_locked = false;
    ObRpcRedisCtx *tmp_ctx = NULL;
    if (OB_FAIL(ObRpcRedisCtxCacheCont::get_rpc_redis_ctx_local(*this, key, hash, is_locked, tmp_ctx))) {
      if (NULL != tmp_ctx) {
        tmp_ctx->dec_ref();
        tmp_ctx = NULL;
      }
      LOG_WDIAG("fail to get redis ctx", K(key), K(ret));
    } else {
      if (is_locked) {
        *ppctx = tmp_ctx;
        tmp_ctx = NULL;
      } else {
        LOG_DEBUG("get_rpc_redis_ctx, trylock failed, reschedule cont interval(ns)",
                  LITERAL_K(ObRpcRedisCtxCacheParam::SCHEDULE_RPC_REDIS_CTX_CACHE_CONT_INTERVAL));
        ObRpcRedisCtxCacheCont *rpc_redis_ctx_cont = NULL;
        if (OB_ISNULL(rpc_redis_ctx_cont = op_alloc_args(ObRpcRedisCtxCacheCont, *this))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_EDIAG("fail to allocate memory for rpc ctx cache continuation", K(ret));
        } else {
          rpc_redis_ctx_cont->action_.set_continuation(cont);
          rpc_redis_ctx_cont->mutex_ = cont->mutex_;
          rpc_redis_ctx_cont->hash_ = hash;
          rpc_redis_ctx_cont->ppctx_ = ppctx;
          rpc_redis_ctx_cont->key_ = key;

          SET_CONTINUATION_HANDLER(rpc_redis_ctx_cont, &ObRpcRedisCtxCacheCont::get_rpc_redis_ctx);
          if (OB_ISNULL(self_ethread().schedule_in(rpc_redis_ctx_cont,
                  ObRpcRedisCtxCacheParam::SCHEDULE_RPC_REDIS_CTX_CACHE_CONT_INTERVAL))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WDIAG("fail to schedule imm", K(rpc_redis_ctx_cont), K(ret));
          } else {
            action = &rpc_redis_ctx_cont->action_;
          }
        }
        if (OB_FAIL(ret) && OB_LIKELY(NULL != rpc_redis_ctx_cont)) {
          rpc_redis_ctx_cont->destroy();
          rpc_redis_ctx_cont = NULL;
        }
      }
    }

    if (OB_FAIL(ret)) {
      *ppctx = NULL;
    }
  }
  return ret;
}

int ObRpcRedisCtxCache::get_all_rpc_redis_ctx(char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid arugument", K(buf), K(buf_len), K(ret));
  } else {
    int64_t bucket_num = get_sub_part_count();
    for (int64_t i = 0; i < bucket_num && OB_SUCC(ret); ++i) {
      if (OB_FAIL(get_one_sub_bucket_redis_ctx(i, buf, buf_len, pos))) {
        LOG_WDIAG("fail to get one sub bucket redis ctx", K(i), K(ret));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObRpcRedisCtxCache::get_one_sub_bucket_redis_ctx(const int64_t bucket_idx, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = get_sub_part_count();
  if ((bucket_idx < 0 || (bucket_idx >= RPC_REDIS_CTX_CACHE_MAP_SIZE))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_idx), K(bucket_num), K(ret));
  } else {
    ObRpcRedisCtx *redis_ctx_ = NULL;
    RpcRedisCtxIter it;
    ObProxyMutex *bucket_mutex = lock_for_part(bucket_idx);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(bucket_idx))) {
        LOG_WDIAG("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        redis_ctx_ = first_entry(bucket_idx, it);
        while (NULL != redis_ctx_) {
          if (!redis_ctx_->is_deleted_state()) {
            if (OB_FAIL(redis_ctx_->format_redis_client_ctx(buf, buf_len, pos))) {
              LOG_WDIAG("fail to format redis client ctx", K(ret));
            }
          }
          redis_ctx_ = next_entry(bucket_idx, it);
        }
      }
    } else {
      LOG_INFO("fail to get lock, try again later", K(bucket_idx));
    }
  }
  return ret;
}

int ObRpcRedisCtxCache::add_rpc_redis_ctx_if_not_exist(ObRpcRedisCtx &ctx, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t key =ctx.get_cs_id();
    uint64_t hash = key;
    LOG_DEBUG("add redis ctx", K(part_num(hash)), K(ctx), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObRpcRedisCtx *tmp_ctx = lookup_entry(hash, key);
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
      ObRpcRedisCtxCacheParam *param = op_alloc(ObRpcRedisCtxCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for rpc param", K(param), K(ret));
      } else {
        param->op_ = ObRpcRedisCtxCacheParam::ADD_RPC_REDIS_CTX_IF_NOT_EXIST_OP;
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

int ObRpcRedisCtxCache::add_rpc_redis_ctx(ObRpcRedisCtx &ctx, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t key = ctx.get_cs_id();
    uint64_t hash = key;
    LOG_DEBUG("add table redis ctx", K(part_num(hash)), K(ctx), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObRpcRedisCtx *tmp_ctx = insert_entry(hash, key, &ctx);
          if (NULL != tmp_ctx) {
            LOG_DEBUG("remove from table rpc ctx", KPC(tmp_ctx));
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
      ObRpcRedisCtxCacheParam *param = op_alloc(ObRpcRedisCtxCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for redis ctx param", K(param), K(ret));
      } else {
        param->op_ = ObRpcRedisCtxCacheParam::ADD_RPC_REDIS_CTX_OP;
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

int ObRpcRedisCtxCache::remove_rpc_redis_ctx(const uint64_t key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t hash = key;
    ObRpcRedisCtx *ctx = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        ctx = remove_entry(hash, key);
        LOG_INFO("this ctx will be removed from redis ctx cache", KPC(ctx));
        if (NULL != ctx) {
          ctx->set_deleted_state();
          ctx->dec_ref();
          ctx = NULL;
        }
      }
    } else {
      ObRpcRedisCtxCacheParam *param = op_alloc(ObRpcRedisCtxCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObRpcRedisCtxCacheParam::REMOVE_RPC_REDIS_CTX_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->ctx_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRpcRedisCtxCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObRpcRedisCtxCacheParam *pre = NULL;
    ObRpcRedisCtxCacheParam *cur = NULL;
    ObRpcRedisCtxCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObRpcRedisCtxCacheParam *>(todo_lists_[buck_id].popall()))) {
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
      ObRpcRedisCtxCacheParam *param = NULL;
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

int ObRpcRedisCtxCache::process(const int64_t buck_id, ObRpcRedisCtxCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObRpcRedisCtxCacheParam", K(buck_id), KPC(param));
    ObRpcRedisCtx *ctx = NULL;
    switch (param->op_) {
      case ObRpcRedisCtxCacheParam::ADD_RPC_REDIS_CTX_OP: {
        ctx = insert_entry(param->hash_, param->key_, param->ctx_);
        if (NULL != ctx) {
          ctx->set_deleted_state();
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
      case ObRpcRedisCtxCacheParam::ADD_RPC_REDIS_CTX_IF_NOT_EXIST_OP: {
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
      case ObRpcRedisCtxCacheParam::REMOVE_RPC_REDIS_CTX_OP: {
        ctx = remove_entry(param->hash_, param->key_);
        LOG_INFO("this ctx will be removed from rpc ctx cache", KPC(ctx));
        if (NULL != ctx) {
          ctx->set_deleted_state();
          ctx->dec_ref(); // free old ctx
          ctx = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObRpcRedisCtxCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObRpcRedisCtxCache::gc_rpc_redis_ctx(ObRpcRedisCtx *ctx)
{
  UNUSED(ctx);
  // gc_rpc_req_ctx do nothing
  return false;
}

ObRpcRedisCtxCache &get_global_rpc_redis_ctx_cache()
{
  static ObRpcRedisCtxCache rpc_redis_ctx_cache;
  return rpc_redis_ctx_cache;
}

}
}
}