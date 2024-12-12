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
#ifndef OBPROXY_RPC_REDIS_COMMAND_FACOTRY_H
#define OBPROXY_RPC_REDIS_COMMAND_FACOTRY_H

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "ob_rpc_redis_info.h"
#include "obproxy/obkv/table/ob_rpc_struct.h"
#include "obproxy/obkv/redis/ob_redis_rpc_response.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obproxy
{
namespace obkv
{
class ObRpcRedisRequest;
}
namespace proxy
{


// class ObRpcRedisPartIDCalculator
// {
// public:
//   virtual int calc_redis_rowkey(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> &rowkey) = 0;
// };

// class ObRpcRedisRowkeyCalculator
// {
//   virtual int calc_redis_rowkey(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> &rowkey) = 0;
// };


// class ObRpcRedisInternalCmdHandler
// {
// };

// class ObRpcRedisSingleRowkey : public ObRpcRedisRowkeyCalculator
// {
// public:
//   virtual int calc_redis_rowkey(ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> &rowkey);
// };

typedef int (*RedisRowkeyIterator) (const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, obkv::ROWKEY_VALUE_PARAM &rowkey);

class ObRedisRowKeyIter
{
  /* OBServer has support shard redis request in one node, just use default RedisSingleKeyIterator to support */
public:
  // do nothing for rowkey
  static int RedisNullKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, obkv::ROWKEY_VALUE_PARAM &rowkey);

  // rowkey string is at index [1] (such as GET key_str)
  static int RedisSingleKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, obkv::ROWKEY_VALUE_PARAM &rowkey);

  //rowkey string is at index [1 .. length-1] (such as MGET key1 key2 key3)
  static int RedisSequenceMultiKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, obkv::ROWKEY_VALUE_PARAM &rowkey);

  //rowkey string is at inde [1, 2x - 1], x is number of rowkey word (such as MSET key1 value1 key2 values)
  static int RedisSkipMultiKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, obkv::ROWKEY_VALUE_PARAM &rowkey);

  //rowkey string is at index [2 .. length-1] (such as SDIFFSTORE destination key [key ...], spcial command to support, same as SINTERSTORE/SUNIONSTORE/SMOVE)
  static int RedisSecSequenceMultiKeyIterator(const ObSEArray<ObString, COMMON_REDIS_ARGS_COUNT> *args, obkv::ROWKEY_VALUE_PARAM &rowkey);
};

enum ObRpcRedisCommandMetaFlag
{
  OB_REDIS_IS_AUTH_CMD_SHIFT = 0,
  OB_REDIS_IS_BATCH_CMD_SHIFT,    //1LL << 2
  OB_REDIS_IS_INTERNAL_CMD_SHIFT, //1LL << 3
  OB_REDIS_IS_GLOBAL_CMD_SHIFT,   //1LL << 4
  OB_REDIS_IS_VK_PARTITION_KEY,   //1LL << 5
};

#define OB_RPC_REDIS_CMD_FLAG_TEST(cap, tg_cap) (((cap) & (tg_cap)) == (tg_cap))
#define OB_RPC_REDIS_CMD_GET_FLAG(i) (1LL << i)
#define OB_REDIS_EMPTY_FLAG 0
#define OB_REDIS_IS_AUTH              OB_RPC_REDIS_CMD_GET_FLAG(OB_REDIS_IS_AUTH_CMD_SHIFT)
#define OB_REDIS_IS_BATCH             OB_RPC_REDIS_CMD_GET_FLAG(OB_REDIS_IS_BATCH_CMD_SHIFT)
#define OB_REDIS_IS_INTERNAL          OB_RPC_REDIS_CMD_GET_FLAG(OB_REDIS_IS_INTERNAL_CMD_SHIFT)
#define OB_REDIS_IS_GLOBAL            OB_RPC_REDIS_CMD_GET_FLAG(OB_REDIS_IS_GLOBAL_CMD_SHIFT)
#define OB_REDIS_IS_VK_PARTITION_KEY  OB_RPC_REDIS_CMD_GET_FLAG(OB_REDIS_IS_VK_PARTITION_KEY)

//TODO add shard_ref to CommandMeta
class ObRpcRedisCommandMeta
{
public:
  ObRpcRedisCommandMeta(RedisCommandType cmd)
      : redis_cmd_(cmd), table_name_(), param_limit_(0), option_flag_(OB_REDIS_EMPTY_FLAG), rowkey_iterator_(NULL)
  {
  }
  bool is_auth() const { return OB_RPC_REDIS_CMD_FLAG_TEST(option_flag_, OB_REDIS_IS_AUTH); }
  bool is_batch() const { return OB_RPC_REDIS_CMD_FLAG_TEST(option_flag_, OB_REDIS_IS_BATCH); }
  bool is_internal() const { return OB_RPC_REDIS_CMD_FLAG_TEST(option_flag_, OB_REDIS_IS_INTERNAL); }
  bool is_global() const { return OB_RPC_REDIS_CMD_FLAG_TEST(option_flag_, OB_REDIS_IS_GLOBAL); }
  bool is_need_vk() const { return OB_RPC_REDIS_CMD_FLAG_TEST(option_flag_, OB_REDIS_IS_VK_PARTITION_KEY); }

  void set_rowkey_iterator(RedisRowkeyIterator rowkey_iterator) { rowkey_iterator_ = rowkey_iterator; }
  const RedisRowkeyIterator get_rowkey_iterator() const { return rowkey_iterator_; }

  void set_param_limit(int32_t param_limit) { param_limit_ = param_limit; }
  int32_t get_param_limit() const { return param_limit_; }

  void set_table_name(ObString table_name) { table_name_ = table_name; }
  const ObString &get_table_name() const { return table_name_; }

  void set_option_flag(uint64_t option_flag) { option_flag_ = option_flag; }
  uint64_t get_option_flag() const { return option_flag_; }

  void set_redis_cmd(RedisCommandType redis_cmd) { redis_cmd_ = redis_cmd; }
  RedisCommandType get_redis_cmd_type() const { return redis_cmd_; }
  TO_STRING_KV(K_(redis_cmd), K_(table_name), K_(param_limit), K_(option_flag))
private:
  RedisCommandType redis_cmd_;
  ObString table_name_;
  int32_t param_limit_;
  uint64_t option_flag_;
  RedisRowkeyIterator rowkey_iterator_;
};

typedef hash::ObHashMap<ObString, ObRpcRedisCommandMeta *, hash::NoPthreadDefendMode> OB_REDIS_CMD_META_MAP;

#define REG_OB_REDIS_CMD(cmd_str, cmd_type, table_name, param_limit, option_flag, rowkey_iter)                         \
  do {                                                                                                                 \
    if (OB_SUCC(ret)) {                                                                                                \
      if (OB_FAIL(register_cmd(cmd_str, cmd_type, table_name, param_limit, option_flag, rowkey_iter))) {               \
        LOG_EDIAG("fail to register cmd", K(ret));                                                                     \
      }                                                                                                                \
    }                                                                                                                  \
  } while (0)

// init while proxy startup, only for read meta info
class ObRpcRedisCommandMetaMap
{
public:
  ObRpcRedisCommandMetaMap() : allocator_() { init(); }
  ~ObRpcRedisCommandMetaMap() { redis_cmd_meta_map_.clear(); redis_cmd_meta_map_.clear(); allocator_.reset(); }
  void init(); // register redis command meta info
  int get_meta_info(const ObString &cmd_name, ObRpcRedisCommandMeta *& redis_meta);
private:
  int register_cmd(const ObString &cmd_string,
                   const RedisCommandType redis_command,
                   const ObString &table_name,
                   int64_t param_limit,
                   uint64_t option_flag,
                   RedisRowkeyIterator rowkey_iterator);

private:
  OB_REDIS_CMD_META_MAP redis_cmd_meta_map_;
  ObArenaAllocator allocator_;
};

// ObRpcRedisCommandMetaMap &get_global_redis_command_meta_map();
ObRpcRedisCommandMetaMap &get_global_redis_command_meta_map()
{
  static ObRpcRedisCommandMetaMap g_proxy_redis_meta_info_map;
  return g_proxy_redis_meta_info_map;
}


// todo: to impl ObRpcRedisRequest Obj pool
class ObRpcRedisCommandFactory
{
public:
  ObRpcRedisCommandFactory() {}
  static int gen_redis_request(ObRpcRedisInfo *redis_info, obkv::ObRpcRedisRequest *&redis_request, ObIAllocator &allocator);
  static int free_redis_request(obkv::ObRpcRedisRequest *redis_request);

  static int gen_redis_response(obkv::ObRpcRedisRequest *redis_request, obkv::ObRpcRedisResponse *&redis_response);
  static int gen_redis_internal_response(obkv::ObRpcRedisRequest *redis_request, obkv::ObRpcRedisResponse *&redis_response);
  static int free_redis_response(obkv::ObRpcRedisRequest *redis_request, obkv::ObRpcRedisResponse *&redis_response);
  static int free_redis_internal_response(obkv::ObRpcRedisResponse *&redis_response);

  static char* alloc_redis_table_request(uint32_t len);
  static void free_redis_table_request(obkv::ObRpcRequest *rpc_request, uint32_t len);

  static int gen_redis_result(ObRpcRedisInfo *redis_info, RedisResultType type, obkv::ObRedisResult *&redis_result);

};

class ObRpcRedisInnerRequestHandle
{
public:
  static int handle_redis_inner_cmd_client(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_info(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_monitor(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_echo(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_ping(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_quit(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_select(ObRpcReq &rpc_request);
  static int handle_redis_inner_cmd_swap(ObRpcReq &rpc_request);
};

} // namespace proxy
} // namespace obproxy
} // namespace oceanbase
#endif
