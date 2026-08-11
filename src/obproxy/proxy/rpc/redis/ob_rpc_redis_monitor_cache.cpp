/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX PROXY

#include "proxy/rpc/redis/ob_rpc_redis_monitor_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::obproxy::event;
using namespace oceanbase::obproxy::obutils;

namespace oceanbase
{
namespace obproxy
{
namespace proxy
{
//---------------------------ObRpcRedisMonoitorMsg-------------------------//
int ObRpcRedisMonitorMsg::alloc_and_init_redis_monitor_msg(ObRpcRedisMonitorMsg *&redis_monitor_msg, int &msg_len)
{
  int ret = OB_SUCCESS;
  int64_t alloc_size = sizeof(ObRpcRedisMonitorMsg);
  void *buf = op_fixed_mem_alloc(alloc_size);
  void *msg_buf = op_fixed_mem_alloc(msg_len);

  if (OB_ISNULL(buf)|| OB_ISNULL(msg_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem", K(alloc_size), K(ret));
  } else {
    redis_monitor_msg = new (buf) ObRpcRedisMonitorMsg();
    if (OB_NOT_NULL(redis_monitor_msg)) {
      redis_monitor_msg->monitor_msg_ = reinterpret_cast<char *>(msg_buf);
      redis_monitor_msg->msg_len_ = msg_len;
      if (OB_FAIL(redis_monitor_msg->init())) {
        LOG_WDIAG("fail to init redis monitor msg", KPC(redis_monitor_msg), K(ret));
      } else {
        redis_monitor_msg->inc_ref();
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(buf)) {
      op_fixed_mem_free(buf, alloc_size);
      buf = NULL;
    }
    if (OB_NOT_NULL(msg_buf)) {
      op_fixed_mem_free(msg_buf, msg_len);
      msg_buf = NULL;
    }
    redis_monitor_msg = NULL;
    alloc_size = 0;
  }
  return ret;
}
void ObRpcRedisMonitorMsg::set_monitor_msg(char *buf)
{
  if (OB_UNLIKELY(OB_ISNULL(buf))) {
    LOG_WDIAG("monitor msg is NULL");
  } else {
    int64_t len = strlen(buf);
    if (len > 0) {
      MEMCPY(monitor_msg_, buf, len);
      monitor_msg_[len] = '\0';
    }
  }
}

int64_t ObRpcRedisMonitorCacheParam::to_string(char *buf, const int64_t buf_len) const
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

const char *ObRpcRedisMonitorCacheParam::get_op_name(const Op op)
{
  const char *name = NULL;
  switch (op) {
    case INVALID_RPC_REDIS_MONITOR_OP : {
      name = "INVALID_RPC_REDIS_MONITOR_OP";
      break;
    }
    case ADD_RPC_REDIS_MONITOR_OP : {
      name = "ADD_RPC_REDIS_MONITOR_OP";
      break;
    }
    case REMOVE_RPC_REDIS_MONITOR_OP : {
      name = "REMOVE_RPC_REDIS_MONITOR_OP";
      break;
    }
    default : {
      name = "UNKNOWN_OP";
      break;
    }
  }
  return name;
}


int ObRpcRedisMonitorCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;
  int64_t sub_bucket_size = bucket_size / MT_HASHTABLE_PARTITIONS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WDIAG("init twice", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(bucket_size <= 0 || sub_bucket_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_size), K(sub_bucket_size), K(ret));
  } else if (OB_FAIL(ObRpcRedisMonitorMTHashMap::init(sub_bucket_size, RPC_REDIS_CTX_MAP_LOCK, gc_rpc_redis_monitor))) {
    LOG_WDIAG("fail to init hash index of rpc req ctx cache", K(sub_bucket_size), K(ret));
  } else {
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      todo_lists_[i].init("rpc_redis_monitor_todo_list",
                          reinterpret_cast<int64_t>(&(reinterpret_cast<ObRpcRedisMonitorCacheParam *>(0))->link_));
    }
    is_inited_ = true;
  }
  return ret;
}

void ObRpcRedisMonitorCache::destroy()
{
  LOG_INFO("ObRpcRedisMonitorCache will desotry");
  if (is_inited_) {
    ObRpcRedisMonitorCacheParam *param = NULL;
    ObRpcRedisMonitorCacheParam *cur = NULL;
    for (int64_t i = 0; i < MT_HASHTABLE_PARTITIONS; ++i) {
      if (NULL != (param = reinterpret_cast<ObRpcRedisMonitorCacheParam *>(todo_lists_[i].popall()))) {
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

int ObRpcRedisMonitorCache::feed_all_rpc_redis_monitor(ObRpcRedisMonitorMsg *msg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_ISNULL(msg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid arugument", K(ret));
  } else {
    int64_t bucket_num = get_sub_part_count();
    for (int64_t i = 0; i < bucket_num && OB_SUCC(ret); ++i) {
      if (OB_FAIL(feed_one_sub_bucket_redis_monitor(i, msg))) {
        LOG_WDIAG("fail to get one sub bucket redis ctx", K(i), K(ret));
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObRpcRedisMonitorCache::feed_one_sub_bucket_redis_monitor(const int64_t bucket_idx, ObRpcRedisMonitorMsg *msg)
{
  int ret = OB_SUCCESS;
  int64_t bucket_num = get_sub_part_count();
  if ((bucket_idx < 0 || (bucket_idx >= RPC_REDIS_MONITOR_CACHE_MAP_SIZE))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(bucket_idx), K(bucket_num), K(ret));
  } else {
    ObList<ObRpcRedisMonitorMsg *> *monitor_list;
    RpcRedisMonitorIter it;
    ObProxyMutex *bucket_mutex = lock_for_part(bucket_idx);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(bucket_idx))) {
        LOG_WDIAG("fail to run todo list", K(bucket_idx), K(ret));
      } else {
        monitor_list = first_entry(bucket_idx, it);
        while (NULL != monitor_list) {
          if (OB_FAIL(monitor_list->push_back(msg))) {
            LOG_WDIAG("fail to add monitor msg", K(ret));
          } else {
            msg->inc_ref();
          }
          monitor_list = next_entry(bucket_idx, it);
        }
      }
    } else {
      LOG_INFO("fail to get lock, try again later", K(bucket_idx));
    }
  }
  return ret;
}

int ObRpcRedisMonitorCache::add_rpc_redis_monitor_if_not_exist(ObList<ObRpcRedisMonitorMsg*> &ctx, uint64_t key, bool direct_add)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t hash = key;
    LOG_DEBUG("add redis monitor list", K(part_num(hash)), K(ctx), K(direct_add), K(hash));
    if (!direct_add) {
      ObProxyMutex *bucket_mutex = lock_for_key(hash);
      MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
      if (lock.is_locked()) {
        if (OB_FAIL(run_todo_list(part_num(hash)))) {
          LOG_WDIAG("fail to run todo list", K(ret));
        } else {
          ObList<ObRpcRedisMonitorMsg *> *tmp_ctx = lookup_entry(hash, key);
          if (NULL != tmp_ctx) {
            // do nothing
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
      ObRpcRedisMonitorCacheParam *param = op_alloc(ObRpcRedisMonitorCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for rpc param", K(param), K(ret));
      } else {
        param->op_ = ObRpcRedisMonitorCacheParam::ADD_RPC_REDIS_MONITOR_IF_NOT_EXIST_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->ctx_ = &ctx;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRpcRedisMonitorCache::remove_rpc_redis_monitor(const uint64_t key)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else {
    uint64_t hash = key;
    ObList<ObRpcRedisMonitorMsg*> *ctx = NULL;
    ObProxyMutex *bucket_mutex = lock_for_key(hash);
    MUTEX_TRY_LOCK(lock, bucket_mutex, this_ethread());
    if (lock.is_locked()) {
      if (OB_FAIL(run_todo_list(part_num(hash)))) {
        LOG_WDIAG("fail to run todo list", K(ret));
      } else {
        ctx = remove_entry(hash, key);
        LOG_INFO("this ctx will be removed from redis ctx cache", KPC(ctx));
        if (NULL != ctx) {
          ctx = NULL;
        }
      }
    } else {
      ObRpcRedisMonitorCacheParam *param = op_alloc(ObRpcRedisMonitorCacheParam);
      if (OB_ISNULL(param)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_EDIAG("fail to allocate memory for location param", K(param), K(ret));
      } else {
        param->op_ = ObRpcRedisMonitorCacheParam::REMOVE_RPC_REDIS_MONITOR_OP;
        param->hash_ = hash;
        param->key_ = key;
        param->ctx_ = NULL;
        todo_lists_[part_num(hash)].push(param);
      }
    }
  }
  return ret;
}

int ObRpcRedisMonitorCache::run_todo_list(const int64_t buck_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WDIAG("not init", K_(is_inited), K(ret));
  } else if (OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(ret));
  } else {
    ObRpcRedisMonitorCacheParam *pre = NULL;
    ObRpcRedisMonitorCacheParam *cur = NULL;
    ObRpcRedisMonitorCacheParam *next = NULL;
    if (NULL != (cur = reinterpret_cast<ObRpcRedisMonitorCacheParam *>(todo_lists_[buck_id].popall()))) {
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
      ObRpcRedisMonitorCacheParam *param = NULL;
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

int ObRpcRedisMonitorCache::process(const int64_t buck_id, ObRpcRedisMonitorCacheParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_UNLIKELY(buck_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid input value", K(buck_id), K(param), K(ret));
  } else {
    LOG_DEBUG("begin to process ObRpcRedisMonitorCacheParam", K(buck_id), KPC(param));
    ObList<ObRpcRedisMonitorMsg*> *ctx = NULL;
    switch (param->op_) {
      case ObRpcRedisMonitorCacheParam::ADD_RPC_REDIS_MONITOR_OP: {
        // do nothing
        break;
      }
      case ObRpcRedisMonitorCacheParam::ADD_RPC_REDIS_MONITOR_IF_NOT_EXIST_OP: {
        ctx = lookup_entry(param->hash_, param->key_);
        if (NULL != ctx) {
          // 不用插入
        } else {
          ctx = insert_entry(param->hash_, param->key_, param->ctx_);
          if (NULL != ctx) {
            LOG_WDIAG("ctx is not NULL, unexpected");
          }
        }
        if (NULL != param->ctx_) {
          // dec_ref, it was inc before push param into todo list
          param->ctx_ = NULL;
        }
        break;
      }
      case ObRpcRedisMonitorCacheParam::REMOVE_RPC_REDIS_MONITOR_OP: {
        ctx = remove_entry(param->hash_, param->key_);
        LOG_INFO("this ctx will be removed from rpc ctx cache", KPC(ctx));
        if (NULL != ctx) {
          ctx = NULL;
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_EDIAG("ObRpcRedisMonitorCache::process unrecognized op",
                  "op", param->op_, K(buck_id), KPC(param), K(ret));
        break;
      }
    }
  }
  return ret;
}

bool ObRpcRedisMonitorCache::gc_rpc_redis_monitor(ObList<ObRpcRedisMonitorMsg *> *monitor_list)
{
  UNUSED(monitor_list);
  // gc_rpc_req_ctx do nothing
  return false;
}

ObRpcRedisMonitorCache &get_global_rpc_redis_monitor_cache()
{
  static ObRpcRedisMonitorCache rpc_redis_monitor_cache;
  return rpc_redis_monitor_cache;
}

}
}
}