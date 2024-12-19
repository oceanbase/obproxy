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

#include "proxy/rpc/redis/ob_rpc_redis_command_factory.h"
#include "proxy/rpc/redis/ob_rpc_redis_stat.h"
#include "obkv/redis/ob_redis_rpc_request.h"
#include "obkv/redis/ob_redis_rpc_response.h"
#include "obkv/table/ob_table_rpc_struct.h"
#include "proxy/rpc/net/ob_rpc_redis_client_net_handler.h"
#include "proxy/rpc/ob_rpc_req.h"
#include "proxy/rpc/redis/ob_rpc_redis_analyzer.h"
#include "proxy/rpc/redis/ob_rpc_redis_ctx_cache.h"

using namespace oceanbase::obproxy::proxy;
using namespace oceanbase::obproxy::obkv;

#define REDIS_MAX_INFO_LENGTH 2048
#define REDIS_MAX_CLIENT_INFO_LENGTH 512
//set mset psetex setex
void ObRpcRedisCommandMetaMap::init()
{
  int ret = OB_SUCCESS;
  redis_cmd_meta_map_.create(REDIS_COMMAND_MAX, ObModIds::OB_HASH_BUCKET);
  /* auth */
  REG_OB_REDIS_CMD("AUTH", REDIS_COMMAND_AUTH, REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_AUTH, NULL);
  REG_OB_REDIS_CMD("HELLO", REDIS_COMMAND_HELLO, REDIS_EMPTY_TABLE_NAME, 5, OB_REDIS_IS_AUTH, NULL);

  /* string */
  REG_OB_REDIS_CMD("APPEND",    REDIS_COMMAND_GET,        REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("BITCOUNT",  REDIS_COMMAND_BITCOUNT,   REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("DECR",      REDIS_COMMAND_DECR,       REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("DECRBY",    REDIS_COMMAND_DECRBY,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("GET",       REDIS_COMMAND_GET,        REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("GETBIT",    REDIS_COMMAND_GETBIT,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("GETRANGE",  REDIS_COMMAND_GETRANGE,   REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("GETSET",    REDIS_COMMAND_GETSET,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("INCR",      REDIS_COMMAND_INCR,       REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("INCRBY",    REDIS_COMMAND_INCRBY,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("INCRBYFLOAT", REDIS_COMMAND_INCRBYFLOAT, REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("MGET",      REDIS_COMMAND_MGET,       REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("MSET",      REDIS_COMMAND_MSET,       REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("PSETEX",    REDIS_COMMAND_PSETEX,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SET",       REDIS_COMMAND_SETBIT,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SETBIT",    REDIS_COMMAND_SETBIT,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SETEX",     REDIS_COMMAND_SETEX,      REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SETNX",     REDIS_COMMAND_SETNX,      REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SETRANGE",  REDIS_COMMAND_SETRANGE,   REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("STRLEN",    REDIS_COMMAND_STRLEN,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);

  /* hash */
  REG_OB_REDIS_CMD("HDEL",      REDIS_COMMAND_HDEL,       REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HEXISTS",   REDIS_COMMAND_HEXISTS,    REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HGET",      REDIS_COMMAND_HGET,       REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HGETALL",   REDIS_COMMAND_HGETALL,    REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HINCRBY",   REDIS_COMMAND_HINCRBY,    REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HINCRBYFLOAT", REDIS_COMMAND_HINCRBYFLOAT, REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HKEYS",     REDIS_COMMAND_HKEYS,      REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HLEN",      REDIS_COMMAND_HLEN,       REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HMGET",     REDIS_COMMAND_HMGET,      REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HMSET",     REDIS_COMMAND_HMSET,      REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HSET",      REDIS_COMMAND_HSET,       REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HSETNX",    REDIS_COMMAND_HSETNX,     REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("HVALS",     REDIS_COMMAND_HVALS,      REDIS_HASH_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);

  /* set */
  REG_OB_REDIS_CMD("SADD",      REDIS_COMMAND_SADD,       REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SCARD",     REDIS_COMMAND_SCARD,      REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SDIFF",     REDIS_COMMAND_SDIFF,      REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SDIFFSTORE", REDIS_COMMAND_SDIFFSTORE,    REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SINTER",    REDIS_COMMAND_SINTER,     REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SINTERSTORE", REDIS_COMMAND_SINTERSTORE,  REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SISMEMBER", REDIS_COMMAND_SISMEMBER,  REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SMEMBERS",  REDIS_COMMAND_SMEMBERS,   REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SMOVE",     REDIS_COMMAND_SMOVE,      REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SPOP",      REDIS_COMMAND_SPOP,       REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SRANDMEMBER", REDIS_COMMAND_SRANDMEMBER,  REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SREM",      REDIS_COMMAND_SREM,       REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SUNION",    REDIS_COMMAND_SUNION,     REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("SUNIONSTORE", REDIS_COMMAND_SUNIONSTORE,  REDIS_SET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);

  /* zset */
  REG_OB_REDIS_CMD("ZADD",      REDIS_COMMAND_ZADD,       REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZCARD",     REDIS_COMMAND_ZCARD,      REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZCOUNT",    REDIS_COMMAND_ZCOUNT,     REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZINCRBY",   REDIS_COMMAND_ZINCRBY,    REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZINTERSTORE", REDIS_COMMAND_ZINTERSTORE,  REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZRANGE",    REDIS_COMMAND_ZRANGE,     REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZRANGEBYSCORE", REDIS_COMMAND_ZRANGEBYSCORE, REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZRANK",     REDIS_COMMAND_ZRANK,      REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZREM",      REDIS_COMMAND_ZREM,       REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZREMRANGEBYRANK", REDIS_COMMAND_ZREMRANGEBYRANK,    REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZREMRANGEBYSCORE", REDIS_COMMAND_ZREMRANGEBYSCORE,  REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZREVRANGE", REDIS_COMMAND_ZREVRANGE,  REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZREVRANGEBYSCORE", REDIS_COMMAND_ZREVRANGEBYSCORE,  REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZREVRANK",  REDIS_COMMAND_ZREVRANK,   REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZSCORE",    REDIS_COMMAND_ZSCORE,     REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("ZUNIONSTORE", REDIS_COMMAND_ZUNIONSTORE,  REDIS_ZSET_TABLE_NAME, 3, OB_REDIS_IS_VK_PARTITION_KEY, ObRedisRowKeyIter::RedisSingleKeyIterator);

  /* list */
  REG_OB_REDIS_CMD("LINDEX",    REDIS_COMMAND_LINDEX,     REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LSET",      REDIS_COMMAND_LSET,       REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LRANGE",    REDIS_COMMAND_LRANGE,     REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LTRIM",     REDIS_COMMAND_LTRIM,      REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LPUSH",     REDIS_COMMAND_LPUSH,      REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LPUSHX",    REDIS_COMMAND_LPUSHX,     REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("RPUSH",     REDIS_COMMAND_RPUSH,      REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("RPUSHX",    REDIS_COMMAND_RPUSHX,     REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LPOP",      REDIS_COMMAND_LPOP,       REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("RPOP",      REDIS_COMMAND_RPOP,       REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LREM",      REDIS_COMMAND_LREM,       REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LINSERT",   REDIS_COMMAND_LINSERT,    REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LLEN",      REDIS_COMMAND_LLEN,       REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("LDEL",      REDIS_COMMAND_LDEL,       REDIS_LIST_TABLE_NAME, 3, OB_REDIS_EMPTY_FLAG, ObRedisRowKeyIter::RedisSingleKeyIterator);

  /* common global command  */ /* just use string table to route */
  REG_OB_REDIS_CMD("TYPE",      REDIS_COMMAND_TYPE,       REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("DEL",       REDIS_COMMAND_DEL,        REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("TTL",       REDIS_COMMAND_TTL,        REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("EXISTS",    REDIS_COMMAND_EXISTS,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("PTTL",      REDIS_COMMAND_PTTL,       REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("PEXPIRE",   REDIS_COMMAND_PEXPIRE,    REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("PEXPIREAT", REDIS_COMMAND_PEXPIREAT,  REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("PERSIST",   REDIS_COMMAND_PERSIST,    REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("EXPIRE",    REDIS_COMMAND_EXPIRE,     REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);
  REG_OB_REDIS_CMD("EXPIREAT",  REDIS_COMMAND_EXPIREAT,   REDIS_STRING_TABLE_NAME, 3, OB_REDIS_IS_GLOBAL, ObRedisRowKeyIter::RedisSingleKeyIterator);

  /* inner (connection) */
  REG_OB_REDIS_CMD("CLIENT",    REDIS_COMMAND_CLIENT,     REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
  REG_OB_REDIS_CMD("INFO",      REDIS_COMMAND_INFO,       REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
  REG_OB_REDIS_CMD("MONITOR",   REDIS_COMMAND_MONITOR,    REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);

  /* inner (other) */
  REG_OB_REDIS_CMD("ECHO",      REDIS_COMMAND_ECHO,       REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
  REG_OB_REDIS_CMD("PING",      REDIS_COMMAND_PING,       REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
  REG_OB_REDIS_CMD("QUIT",      REDIS_COMMAND_QUIT,       REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
  REG_OB_REDIS_CMD("SELECT",    REDIS_COMMAND_SELECT,     REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
  REG_OB_REDIS_CMD("SWAPDB",    REDIS_COMMAND_SWAPDB,     REDIS_EMPTY_TABLE_NAME, 3, OB_REDIS_IS_INTERNAL, ObRedisRowKeyIter::RedisNullKeyIterator);
}

int ObRpcRedisCommandMetaMap::get_meta_info(const ObString &cmd_name, ObRpcRedisCommandMeta *&redis_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(redis_cmd_meta_map_.get_refactored(cmd_name, redis_meta))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_ERR_REDIS_UNKNOWN_COMMAND;
      LOG_WDIAG("unknow command, redis command not supported", K(ret), K(cmd_name));
    } else {
      LOG_WDIAG("fail to get redis meta info", K(ret));
    }
  } else if (OB_ISNULL(redis_meta)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis meta info", K(ret));
  }
  return ret;
}

int ObRpcRedisCommandMetaMap::register_cmd(const ObString &cmd_string,
                                           const RedisCommandType redis_command,
                                           const ObString &table_name,
                                           int64_t param_limit,
                                           uint64_t option_flag,
                                           RedisRowkeyIterator rowkey_iter)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObRpcRedisCommandMeta *meta = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(sizeof(ObRpcRedisCommandMeta))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for redis meta info", K(ret));
  } else {
    meta = new(buf)ObRpcRedisCommandMeta(redis_command);
    meta->set_table_name(table_name);
    meta->set_param_limit(param_limit);
    meta->set_option_flag(option_flag);
    meta->set_rowkey_iterator(rowkey_iter);
  }

  if (OB_SUCC(ret) && OB_FAIL(redis_cmd_meta_map_.set_refactored(cmd_string, meta))) {
    LOG_WDIAG("fail register record redis cmd", K(cmd_string), K(redis_command));
  }
  return ret;
}

int ObRedisRowKeyIter::RedisNullKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ROWKEY_VALUE_PARAM &rowkey)
{
  int ret = OB_SUCCESS;
  UNUSEDx(args, rowkey);
  return ret;
}

int ObRedisRowKeyIter::RedisSingleKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ROWKEY_VALUE_PARAM &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj ob_obj;
  if (OB_ISNULL(args) || OB_UNLIKELY(args->count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(ob_obj.set_varchar(args->at(1)))) {
    //not to be here
  } else if (FALSE_IT(ob_obj.set_default_collation_type())) { //use defualt collation type utf8mb4
    //not to be here
  } else if (OB_FAIL(rowkey.push_back(ob_obj))) {
  // } else if (OB_FAIL(rowkey.push_back(ObObj(args->at(1))))) {
    LOG_WDIAG("fail to push back rowkey", K(ret));
  } else {
    // LOG_DEBUG("inited rowkey info", K(rowkey), "info", ObObj(args->at(1)), "infox", args->at(1));
    // do nothing
  }
  return ret;
}

int ObRedisRowKeyIter::RedisSequenceMultiKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ROWKEY_VALUE_PARAM &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(args) || OB_UNLIKELY(args->count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t argc_len = args->count();
    for (int64_t i = 1; OB_SUCC(ret) && i < argc_len; i++) {
       if (OB_FAIL(rowkey.push_back(ObObj(args->at(i))))) {
         LOG_WDIAG("fail to push back rowkey", K(ret), K(i));
       }
    }
  }
  return ret;
}

int ObRedisRowKeyIter::RedisSkipMultiKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ROWKEY_VALUE_PARAM &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(args) || OB_UNLIKELY(args->count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t argc_len = args->count();
    for (int64_t i = 1; OB_SUCC(ret) && i < argc_len; ) {
      if (OB_FAIL(rowkey.push_back(ObObj(args->at(i))))) {
        LOG_WDIAG("fail to push back rowkey", K(ret), K(i));
      } else {
        i += 2;
      }
    }
  }
  return ret;
}

int ObRedisRowKeyIter::RedisSecSequenceMultiKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ROWKEY_VALUE_PARAM &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(args) || OB_UNLIKELY(args->count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t argc_len = args->count();
    for (int64_t i = 2; OB_SUCC(ret) && i < argc_len; i++) {
       if (OB_FAIL(rowkey.push_back(ObObj(args->at(i))))) {
         LOG_WDIAG("fail to push back rowkey", K(ret), K(i));
       }
    }
  }
  return ret;
}

int ObRpcRedisCommandFactory::gen_redis_request(ObRpcRedisInfo *redis_info,
                                                obkv::ObRpcRedisRequest *&redis_request,
                                                ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_info)
      || OB_ISNULL(redis_info->get_redis_args())
      || OB_UNLIKELY(redis_info->get_redis_args()->count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis args", K(ret), KPC(redis_info));
  } else {
    ObRpcRedisCommandMetaMap &meta_map = get_global_redis_command_meta_map();
    ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args = redis_info->get_redis_args();
    ObString cmd = args->at(0);
    ObRpcRedisCommandMeta *redis_meta = NULL;
    ObString upper_cmd;
    if (OB_FAIL(ob_simple_low_to_up(allocator, cmd, upper_cmd))) {
      LOG_WDIAG("fail to convert cmd str to uppercase", K(ret));
    } else if (OB_FAIL(meta_map.get_meta_info(upper_cmd, redis_meta))) {
      LOG_WDIAG("fail to get meta info", K(ret));
    } else {
      bool is_need_init_rowkey = false;
      if (redis_meta->is_auth()) {
        LOG_DEBUG("get an auth redis request", KPC(redis_meta));
        if (OB_UNLIKELY(RedisCommandType::REDIS_COMMAND_HELLO == redis_meta->get_redis_cmd_type())) {
          ret = OB_ERR_REDIS_UNKNOWN_COMMAND;
          LOG_WDIAG("unsupport redis command hello", K(ret));
        } else {
          if (OB_ISNULL(redis_request = op_reclaim_alloc(ObRpcRedisAuthRequest))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
          } else {
            redis_info->set_auth_request(true);
          }
        }
      } else if (redis_meta->is_internal()) {
        LOG_DEBUG("get an internal redis request", KPC(redis_meta));
        if (OB_ISNULL(redis_request = op_reclaim_alloc(ObRpcRedisInternalRequest))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis internal request", K(ret));
        } else {
          redis_info->set_inner_request(true);
        }
      } else if (redis_meta->is_global()) {
        LOG_DEBUG("get a global redis request", KPC(redis_meta));
        if (OB_ISNULL(redis_request = op_reclaim_alloc(ObRpcRedisGlobalRequest))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis global request", K(ret));
        } else {
          is_need_init_rowkey = true;
        }
      } else {
        LOG_DEBUG("get a common redis request", KPC(redis_meta));
        if (OB_ISNULL(redis_request = op_reclaim_alloc(ObRpcRedisCommonRequest))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis common request", K(ret));
        } else {
          is_need_init_rowkey = true;
        }
      }

      if (OB_SUCC(ret)) {
        redis_request->meta_info_ = redis_meta;
        redis_request->redis_db_ = redis_info->get_redis_db();

        if (is_need_init_rowkey) {
          ROWKEY_VALUE &rowkey = redis_request->rowkey_;
          /*
          // ObObj redis_db;
          ObObj redis_req_text;
          // redis_db.set_int(redis_info->get_redis_db());
          ObString value = args->at(1); //TODO need update it base on meta info
          redis_req_text.set_varchar(value);
          // rowkey.push_back(redis_db);
          rowkey.push_back(redis_req_text);
          */
          if (OB_ISNULL(redis_meta) || OB_ISNULL(redis_meta->get_rowkey_iterator())) {
            ret = OB_NOT_SUPPORTED;;
            LOG_WDIAG("invalid redis command to support in obproxy", K(ret), "command", args->at(0));
          } else if (OB_FAIL(redis_meta->get_rowkey_iterator()(args, rowkey))) {
            LOG_WDIAG("failed to init rowkey for redis command", K(ret), "command", args->at(0));
          } else {
            // LOG_DEBUG("succ to init rowkey info", K(ret), K(rowkey), "command", args->at(0), "info", args->at(1));
          }
        }
      }
    }
    if (OB_LIKELY(!upper_cmd.empty())) {
      allocator.free(upper_cmd.ptr());
    }
  }
  return ret;
}

int ObRpcRedisCommandFactory::free_redis_request(ObRpcRedisRequest *redis_request)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_request) || OB_ISNULL(redis_request->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis args", K(ret), KPC(redis_request));
  } else {
    const ObRpcRedisCommandMeta *redis_meta = redis_request->get_redis_command_meta();
    if (redis_meta->is_auth()) {
      op_reclaim_free(static_cast<ObRpcRedisAuthRequest*>(redis_request));
    } else if (redis_meta->is_internal()) {
      op_reclaim_free(static_cast<ObRpcRedisInternalRequest*>(redis_request));
    } else if (redis_meta->is_global()) {
      op_reclaim_free(static_cast<ObRpcRedisGlobalRequest*>(redis_request));
    } else {
      op_reclaim_free(static_cast<ObRpcRedisCommonRequest*>(redis_request));
    }
  }
  return ret;
}

int ObRpcRedisCommandFactory::gen_redis_response(obkv::ObRpcRedisRequest *redis_request,
                                                 obkv::ObRpcRedisResponse *&redis_response)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_request) || OB_ISNULL(redis_request->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis args", K(ret));
  } else {
    const ObRpcRedisCommandMeta *redis_meta = redis_request->get_redis_command_meta();
    if (redis_meta->is_auth()) {
      if (OB_ISNULL(redis_response = op_reclaim_alloc(ObRpcRedisAuthResponse))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
      }
    } else if (redis_meta->is_internal()) {
      if (OB_ISNULL(redis_response = op_reclaim_alloc(ObRpcRedisInternalResponse))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for redis internal request", K(ret));
      }
    } else if (redis_meta->is_global()) {
      if (OB_ISNULL(redis_response = op_reclaim_alloc(ObRpcRedisGlobalResponse))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for redis global request", K(ret));
      }
    } else {
      if (OB_ISNULL(redis_response = op_reclaim_alloc(ObRpcRedisCommonResponse))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WDIAG("fail to alloc mem for redis common request", K(ret));
      }
    }
  }
  return ret;
}

int ObRpcRedisCommandFactory::gen_redis_internal_response(obkv::ObRpcRedisRequest *redis_request,
                                                          obkv::ObRpcRedisResponse *&redis_response)
{
  int ret = OB_SUCCESS;
  UNUSEDx(redis_request);
  if (OB_ISNULL(redis_response = op_reclaim_alloc(ObRpcRedisInternalResponse))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WDIAG("fail to alloc mem for redis internal request", K(ret));
  }
  return ret;
}

int ObRpcRedisCommandFactory::free_redis_response(obkv::ObRpcRedisRequest *redis_request,
                                                  obkv::ObRpcRedisResponse *&redis_response)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_request) || OB_ISNULL(redis_request->get_redis_command_meta())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis args", K(ret), KPC(redis_request));
  } else {
    const ObRpcRedisCommandMeta *redis_meta = redis_request->get_redis_command_meta();
    if (redis_meta->is_auth()) {
      op_reclaim_free(static_cast<ObRpcRedisAuthResponse *>(redis_response));
    } else if (redis_meta->is_internal()) {
      op_reclaim_free(static_cast<ObRpcRedisInternalResponse *>(redis_response));
    } else if (redis_meta->is_global()) {
      op_reclaim_free(static_cast<ObRpcRedisGlobalResponse *>(redis_response));
    } else {
      op_reclaim_free(static_cast<ObRpcRedisCommonResponse *>(redis_response));
    }
  }
  return ret;
}

int ObRpcRedisCommandFactory::free_redis_internal_response(obkv::ObRpcRedisResponse *&redis_response)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_response)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis response", K(ret), KPC(redis_response));
  } else {
    op_reclaim_free(static_cast<ObRpcRedisInternalResponse*>(redis_response));
  }
  return ret;
}

char *ObRpcRedisCommandFactory::alloc_redis_table_request(uint32_t len)
{
  char *ret = NULL;
  ret = static_cast<char *>(op_fixed_mem_alloc(len));
  return ret;
}

void ObRpcRedisCommandFactory::free_redis_table_request(obkv::ObRpcRequest *rpc_request, uint32_t len)
{
  if (OB_NOT_NULL(rpc_request)) {
    rpc_request->~ObRpcRequest();
    op_fixed_mem_free(rpc_request, len);
  }
}

int ObRpcRedisCommandFactory::gen_redis_result(ObRpcRedisInfo *redis_info, RedisResultType type, obkv::ObRedisResult *&redis_result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(redis_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("unexpected redis args", K(ret), KPC(redis_info));
  } else {
    common::ObArenaAllocator &allocator = redis_info->get_allocator();
    char *buf = NULL;
    switch(type) {
      case OB_REDIS_SINGLE_LINE:
        if (OB_ISNULL(buf = (char *)allocator.alloc(sizeof(ObRedisSingeLineResult)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
        } else {
          redis_result = new (buf) ObRedisSingeLineResult();
        }
      break;
      case OB_REDIS_ERROR:
        if (OB_ISNULL(buf = (char *)allocator.alloc(sizeof(ObRedisErrorResult)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
        } else {
          redis_result = new (buf) ObRedisErrorResult();
        }
      break;
      case OB_REDIS_INTEGER:
        if (OB_ISNULL(buf = (char *)allocator.alloc(sizeof(ObRedisIntegerResult)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
        } else {
          redis_result = new (buf) ObRedisIntegerResult();
        }
      break;
      case OB_REDIS_BULK_STRING:
        if (OB_ISNULL(buf = (char *)allocator.alloc(sizeof(ObRedisBulkStringResult)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
        } else {
          redis_result = new (buf) ObRedisBulkStringResult();
        }
      break;
      case OB_REDIS_ARRAY:
        if (OB_ISNULL(buf = (char *)allocator.alloc(sizeof(ObRedisArrayResult)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WDIAG("fail to alloc mem for redis auth request", K(ret));
        } else {
          redis_result = new (buf) ObRedisArrayResult();
        }
      break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WDIAG("invalid type to handle", K(ret), K(type));
      break;
    }
  }
  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_client(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  ObRpcRedisClientNetHandler *redis_client = dynamic_cast<ObRpcRedisClientNetHandler *>(rpc_request.get_cnet_sm());
  ObString param;
  ObString upper_param;
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> * redis_args = redis_info->get_redis_args();
  if (OB_ISNULL(redis_args) || 2 > redis_args->count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid args to handle", K(ret));
  } else if (OB_ISNULL(redis_client)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WDIAG("redis client net handler is null", K(ret), K(rpc_request));
  } else {
    upper_param = redis_args->at(1);
    if (0 == upper_param.case_compare("LIST")) {
      uint64_t size = REDIS_MAX_CLIENT_INFO_LENGTH * get_global_redis_info_stat().get_connected_clients();
      if (OB_FAIL(redis_info->init_redis_inner_msg_buf(size))) {
        LOG_WDIAG("fail to alloc msg buf for redis inner msg", K(redis_info), K(ret));
      } else {
        int64_t pos = 0;
        char *buf = redis_info->get_redis_inner_msg_buf();
        if (OB_FAIL(get_global_rpc_redis_ctx_cache().get_all_rpc_redis_ctx(buf, size, pos))) {
          LOG_WDIAG("fail to format redis ctx", K(buf), K(ret));
        } else {
          ObString content;
          content.assign(buf, strlen(buf));
          ret = ObRpcRedisAnalyzer::build_common_resp(rpc_request, content);
        }
      }
    } else if (0 == upper_param.case_compare("INFO")) {
      uint64_t size = REDIS_MAX_CLIENT_INFO_LENGTH;
      if (OB_FAIL(redis_info->init_redis_inner_msg_buf(size))) {
        LOG_WDIAG("fail to alloc msg buf for redis inner msg", K(redis_info), K(ret));
      } else {
        int64_t pos = 0;
        char *buf = redis_info->get_redis_inner_msg_buf();
        if (OB_FAIL(redis_client->get_redis_ctx()->format_redis_client_ctx(buf, size, pos))) {
          LOG_WDIAG("fail to format redis ctx", K(buf), K(ret));
        } else {
          int len = strlen(buf);
          buf[len-2] = '\n';
          ObString content;
          content.assign(buf, len - 1);
          ret = ObRpcRedisAnalyzer::build_common_resp(rpc_request, content);
        }
      }
    } else if (0 == upper_param.case_compare("SETNAME")) {
      if (redis_args->count() != 3) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid args to handle", K(ret));
      } else {
        ObString &name = redis_args->at(2);
        redis_client->get_redis_ctx()->set_client_name(name);
        ret = ObRpcRedisAnalyzer::build_ok_resp(rpc_request);
      }
    } else if (0 == upper_param.case_compare("GETNAME")) {
      if (redis_args->count() != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid args to handle", K(ret));
      } else {
        ObString content;
        char *name = redis_client->get_redis_ctx()->get_client_name();
        if (OB_NOT_NULL(name)) {
          content.assign(name, strlen(name));
        }
        ret = ObRpcRedisAnalyzer::build_common_resp(rpc_request,content);
      }
    } else if (0 == upper_param.case_compare("ID")) {
      if (redis_args->count() != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid args to handle", K(ret));
      } else {
        ret = ObRpcRedisAnalyzer::build_int_resp(rpc_request);
      }
    } else if (0 == upper_param.case_compare("SETINFO")) {
      if (redis_args->count() != 4) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WDIAG("invalid args to handle", K(ret));
      } else {
        if (0 == redis_args->at(2).case_compare("LIB-NAME")) {
          ObString lib_name = redis_args->at(3);
          redis_client->get_redis_ctx()->set_lib_name(lib_name);
          ret = ObRpcRedisAnalyzer::build_ok_resp(rpc_request);
        } else if (0 == redis_args->at(2).case_compare("LIB-VER")) {
          ObString lib_ver = redis_args->at(3);
          redis_client->get_redis_ctx()->set_lib_ver(lib_ver);
          ret = ObRpcRedisAnalyzer::build_ok_resp(rpc_request);
        } else {
          ret = OB_NOT_SUPPORTED;
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WDIAG("not supported redis inner command", K(ret), K(upper_param));
    }
  }
  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_info(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  ObArenaAllocator &allocator = redis_info->get_allocator();
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> * redis_args = redis_info->get_redis_args();
  bool all_sections = false;
  int sections = 0;
  ObString section;
  ObString upper_section;
  if (OB_ISNULL(redis_args) || redis_args->count() > 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid args to handle", K(ret));
  } else if (redis_args->count() == 1) {
    all_sections = true;
  } else {
    section = redis_args->at(1);
    if (OB_FAIL(ob_simple_low_to_up(allocator, section, upper_section))) {
      LOG_WDIAG("fail to convert section str to uppercase", K(ret));
    } else if (0 == upper_section.case_compare("DEFAULT") || 0 == upper_section.case_compare("ALL")) {
      all_sections = true;
    }
  }
    if (OB_FAIL(redis_info->init_redis_inner_msg_buf(REDIS_MAX_INFO_LENGTH))){
      LOG_WDIAG("fail to alloc msg_buf for redis inner request", K(redis_info), K(ret));
    } else{
      int64_t pos = 0;
      char *info_buf = redis_info->get_redis_inner_msg_buf();
      if (OB_SUCC(ret) && (all_sections || 0 == upper_section.case_compare("SERVER"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections += 1;
        ObRpcRedisInfoFactory::format_redis_server_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      if (OB_SUCC(ret) && (all_sections || 0 == upper_section.case_compare("Clients"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections +=1;
        ObRpcRedisInfoFactory::format_redis_clients_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      if (OB_SUCC(ret) && (all_sections || 0 == upper_section.case_compare("MEMORY"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections +=1;
        ObRpcRedisInfoFactory::format_redis_memory_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      if (OB_SUCC(ret) && (all_sections || 0 == upper_section.case_compare("PERSISTENCE"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections +=1;
        ObRpcRedisInfoFactory::format_redis_persistence_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      if (OB_SUCC(ret) && (all_sections || 0 == upper_section.case_compare("STATS"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections +=1;
        ObRpcRedisInfoFactory::format_redis_stats_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      if (OB_SUCC(ret) && (all_sections || 0 == upper_section.case_compare("CPU"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections +=1;
        ObRpcRedisInfoFactory::format_redis_cpu_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      if (OB_SUCC(ret) &&(all_sections || 0 == upper_section.case_compare("CLUSTER"))) {
        if (0 != sections) {
          int len = snprintf(info_buf + pos, REDIS_MAX_INFO_LENGTH , "\r\n");
          pos += len;
        }
        sections +=1;
        ObRpcRedisInfoFactory::format_redis_cluster_info(info_buf, REDIS_MAX_INFO_LENGTH, pos);
      }
      ObString content;
      content.assign(info_buf, strlen(info_buf));
      ret = ObRpcRedisAnalyzer::build_common_resp(rpc_request, content);
    }

  if (OB_LIKELY(!upper_section.empty())) {
    allocator.free(upper_section.ptr());
  }

  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_monitor(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  redis_info->set_monitor_cmd(true);
  ret = ObRpcRedisAnalyzer::build_ok_resp(rpc_request);

  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_echo(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> * redis_args = redis_info->get_redis_args();
  if (OB_ISNULL(redis_args) || redis_args->count() != 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid args to handle", K(ret));
  } else {
    ObString &content = redis_args->at(1);
    ret = ObRpcRedisAnalyzer::build_common_resp(rpc_request, content);
  }

  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_ping(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> * redis_args = redis_info->get_redis_args();
  if (OB_ISNULL(redis_args) || !(redis_args->count() == 1 || redis_args->count() == 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid args to handle", K(ret));
  } else if (redis_args->count() == 2) {
    //same as ECHO cmd
    ret = ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_echo(rpc_request);
  } else {
    ObString content("PONG");
    ret = ObRpcRedisAnalyzer::build_common_resp(rpc_request, content);
  }
  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_quit(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  redis_info->set_need_quit(true);
  ret = ObRpcRedisAnalyzer::build_ok_resp(rpc_request);

  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_select(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  ObRpcRedisInfo *redis_info = rpc_request.get_redis_info();
  common::ObSEArray<common::ObString, COMMON_REDIS_ARGS_COUNT> * redis_args = redis_info->get_redis_args();
  if (OB_ISNULL(redis_args) || redis_args->count() != 2 || redis_args->at(1).length() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WDIAG("invalid args to handle", K(ret));
  } else {
    uint64_t db_info = strtol(redis_args->at(1).ptr(), NULL, 10);
    redis_info->set_redis_db(db_info);
    ret = ObRpcRedisAnalyzer::build_ok_resp(rpc_request);
  }
  return ret;
}

int ObRpcRedisInnerRequestHandle::handle_redis_inner_cmd_swap(ObRpcReq &rpc_request)
{
  int ret = OB_SUCCESS;
  UNUSEDx(rpc_request);
  ret = OB_ERR_REDIS_UNKNOWN_COMMAND;
  return ret;
}